use async_nats::{
    jetstream,
    jetstream::{
        consumer::{pull::Config, AckPolicy, Consumer, DeliverPolicy},
        kv::{Config as KvConfig, Store},
        stream::{Config as StreamConfig, RetentionPolicy, StorageType},
        AckKind, Message,
    },
    Client,
};
use bytes::BytesMut;
use futures::StreamExt;
use tokio::sync::oneshot;
use tracing::{debug, error, info};
use wasmcloud_provider_sdk::get_connection;

use cosmonic::longrunning_operation::types::{Error as OperationError, Operation, OperationResult};
use wasi::io::streams::InputStream;
use wrpc_transport::{Encode, Receive};

wit_bindgen_wrpc::generate!();

#[derive(Debug, Clone)]
pub struct OperationHandler {
    pub consumer: Consumer<Config>,
    pub context: jetstream::Context,
    pub component_id: String,
    pub client: Client,
}

pub struct MessageHandler {
    pub consumer_name: String,
    pub context: jetstream::Context,
    pub component_id: String,
    pub client: Client,
}

impl MessageHandler {
    async fn save_operation_result(&mut self, operation: Operation) -> anyhow::Result<()> {
        let js = &self.context;

        let mut encoded: BytesMut = BytesMut::new();
        (&operation).encode(&mut encoded).await.unwrap();

        let bucket = js.get_key_value(&self.consumer_name.clone()).await?;
        bucket.put(operation.name.clone(), encoded.freeze()).await?;
        Ok(())
    }

    async fn run_operation(&self, msg: Message, operation: Operation) -> anyhow::Result<bool> {
        let m2 = msg.clone();
        let progress = tokio::spawn(async move {
            // TODO this should be based on a fraction of the max wait setting of the consumer
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
            loop {
                interval.tick().await;
                _ = m2
                    .ack_with(AckKind::Progress)
                    .await
                    .map_err(|e| error!("Error acking message: {:?}", e));
            }
        });

        let subject = msg.subject.clone();
        let base_subject = subject
            .as_str()
            .strip_suffix(format!(".{}", operation.name).as_str())
            .unwrap()
            .to_string();

        let nats_client = self.client.clone();
        let op_name = operation.name.clone();
        let cancellation = tokio::spawn(async move {
            let cancellation_subject = format!("{}.cancel.{}", base_subject, op_name);
            let mut cancellation = nats_client.subscribe(cancellation_subject).await.unwrap();
            while let Some(msg) = cancellation.next().await {
                let subject = msg.subject.clone();
                if subject.as_str().ends_with(&op_name) {
                    break;
                }
            }
        });

        let wrpc = get_connection().get_wrpc_client(&self.component_id);
        let op = cosmonic::longrunning_operation::client::handle(&wrpc, &operation);

        // Returns Ok(true) if the operation was cancelled, Ok(false) if the operation completed
        // successfully, and Err if there was an error.
        let result = tokio::select! {
            result = op => {
                match result {
                    Ok(_) => {
                        info!("Operation completed successfully");
                        if let Err(e) = msg.double_ack().await {
                            error!("Error acking message: {:?}", e);
                        }
                        Ok(false)
                    },
                    Err(e) => {
                        error!(err=%e, "Error handling operation");
                        if let Err(err) = msg.ack_with(AckKind::Nak(None)).await {
                            error!("Error nacking message: {:?}", err);
                        }
                        Err(e)
                    }
                }
            },
            _ = cancellation => {
                if let Err(e) = msg.double_ack().await {
                    error!("Error nacking message: {:?}", e);
                }
                Ok(true)
            },
                // This feels wrong but not sure what to do about it right now
            _ = progress => {
                error!("Progress acking task was cancelled");
                if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                    error!("Error nacking message: {:?}", e);
                }
                Ok(false)
            }
        };
        result
    }
}

impl OperationHandler {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut messages = self.consumer.stream().messages().await?;
        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    error!(err = %e, "Error receiving message");
                    continue;
                }
            };
            if msg.ack_with(AckKind::Progress).await.is_err() {
                error!("Error acking message");
                continue;
            }

            debug!(msg = ?msg, "Received message");

            let mut empty = futures::stream::empty();
            let (mut decoded, _) = Operation::receive_sync(msg.payload.clone(), &mut empty)
                .await
                .unwrap();

            let info = match self.consumer.info().await {
                Ok(info) => info,
                Err(e) => {
                    error!("Error getting consumer info: {:?}", e);
                    continue;
                }
            };

            let name = info.name.clone();
            let ctx = self.context.clone();
            let id = self.component_id.clone();
            let client = self.client.clone();

            tokio::spawn(async move {
                let mut mh = MessageHandler {
                    consumer_name: name,
                    context: ctx,
                    component_id: id,
                    client,
                };

                decoded.done = true;
                match mh.run_operation(msg, decoded.clone()).await {
                    Ok(true) => {
                        debug!("Operation cancelled");
                        decoded.result = Some(OperationResult::Cancelled);
                        decoded.done = false;
                    }
                    Ok(false) => {
                        debug!("Operation completed successfully");
                    }
                    Err(e) => {
                        error!(err = %e, "Error running operation");
                        decoded.result = Some(OperationResult::Error(Some(OperationError {
                            message: Some(e.to_string()),
                        })));
                        decoded.done = false;
                    }
                };

                if let Err(e) = mh.save_operation_result(decoded).await {
                    error!(err = %e, "Error saving operation result");
                }
            });
        }

        Ok(())
    }
}
