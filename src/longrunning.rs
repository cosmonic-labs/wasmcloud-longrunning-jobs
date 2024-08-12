use anyhow::{anyhow, bail, Context as _};
use async_nats::{
    jetstream,
    jetstream::{
        consumer::{pull::Config, AckPolicy, Consumer, DeliverPolicy},
        kv::Config as KvConfig,
        stream::{Config as StreamConfig, RetentionPolicy, StorageType},
        AckKind,
    },
    Client,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use svix_ksuid::{Ksuid, KsuidLike};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use wasmcloud_provider_sdk::core::HostData;
use wasmcloud_provider_sdk::{
    get_connection, load_host_data, run_provider, Context, LinkConfig, Provider,
};
use wrpc_transport::{Encode, Receive};

use crate::operation::OperationHandler;

use cosmonic::longrunning_operation::types::{Error as OperationError, Operation};
use wasi::clocks::monotonic_clock::Duration;
use wasi::io::streams::Pollable;

use crate::config::OperationConfig;

wit_bindgen_wrpc::generate!();

const OPERATIONS_STREAM_PREFIX: &str = "wasmcloud_operations";

#[derive(Debug)]
struct OperationState {
    config: OperationConfig,
    client: Client,
    handler: JoinHandle<anyhow::Result<()>>,
}

pub struct OperationID {
    inner: Ksuid,
}

impl OperationID {
    pub fn new() -> Self {
        Self {
            inner: Ksuid::new(None, None),
        }
    }
}

impl std::fmt::Display for OperationID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "op-{}", self.inner)
    }
}

#[derive(Default, Clone)]
pub struct LongRunningOperationProvider {
    /// Map of NATS connection clients (including subscriptions) per component
    components: Arc<RwLock<HashMap<String, OperationState>>>,
    /// Default configuration to use when configuration is not provided on the link
    default_config: OperationConfig,
}

impl LongRunningOperationProvider {
    pub async fn run() -> anyhow::Result<()> {
        let host_data = load_host_data().context("failed to load host data")?;
        let provider = Self::from_host_data(host_data);
        let shutdown = run_provider(provider.clone(), "operations-provider")
            .await
            .context("failed to run provider")?;
        let connection = get_connection();
        eprintln!("here");
        serve(
            &connection.get_wrpc_client(connection.provider_key()),
            provider,
            shutdown,
        )
        .await
    }
    /// Build a [`NatsMessagingProvider`] from [`HostData`]
    pub fn from_host_data(host_data: &HostData) -> LongRunningOperationProvider {
        let default_config = OperationConfig::from(&host_data.config);
        LongRunningOperationProvider {
            default_config,
            ..Default::default()
        }
    }

    async fn upsert_stream(
        &self,
        source: String,
        client: Client,
        stream_name: String,
    ) -> anyhow::Result<Consumer<Config>> {
        let js = jetstream::new(client.clone());

        info!(stream_name, "upserting stream");
        let stream = js
            .get_or_create_stream(StreamConfig {
                name: stream_name.clone(),
                description: Some("Stream for cosmonic long running operations".to_string()),
                max_age: StdDuration::from_secs(60 * 60 * 24 * 7), // 7 days
                retention: RetentionPolicy::WorkQueue,
                allow_rollup: false,
                allow_direct: true,
                //num_replicas: 3,
                storage: StorageType::File,
                subjects: vec![format!("{}.>", stream_name)],
                ..Default::default()
            })
            .await?;
        info!(consumer_name = stream_name, "upserting consumer");
        let consumer = stream
            .get_or_create_consumer(
                stream_name.as_str(),
                Config {
                    description: Some("Consumer for cosmonic long running operations".to_string()),
                    durable_name: Some(stream_name.clone()),
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: StdDuration::from_secs(5),
                    // TODO figure out sensible backoff strategy here
                    // It would be great if we could define something dynamic
                    backoff: vec![
                        StdDuration::from_secs(5),
                        StdDuration::from_secs(15),
                        StdDuration::from_secs(45),
                        StdDuration::from_secs(120),
                        StdDuration::from_secs(300),
                        StdDuration::from_secs(1500),
                    ],
                    max_deliver: 10,
                    max_ack_pending: -1,
                    deliver_policy: DeliverPolicy::All,
                    // TODO does this make sense or should there be more levels on the subject?
                    filter_subjects: vec![format!("{}.>", stream_name)],
                    ..Default::default()
                },
            )
            .await?;

        js.create_key_value(KvConfig {
            bucket: stream_name.clone(),
            description: "KV for cosmonic long running operations".to_string(),
            storage: StorageType::File,
            ..Default::default()
        })
        .await?;

        Ok(consumer)
    }
}

impl Provider for LongRunningOperationProvider {
    /// This function is called when a new link is created between a component and this provider.
    /// For each linked component, the NATS provider should create or update a set of WorkQueue
    /// streams and a corresponding NATS Consumer to the topics specified in the link
    /// configuration.
    async fn receive_link_config_as_target(
        &self,
        LinkConfig {
            source_id, config, ..
        }: LinkConfig<'_>,
    ) -> anyhow::Result<()> {
        let config = if config.is_empty() {
            self.default_config.clone()
        } else {
            self.default_config.merge(OperationConfig::from(config))
        };

        info!(
            "received link configuration for component [{}]: {:?}",
            source_id, config
        );
        // TODO CREDS?!
        let opts = async_nats::ConnectOptions::default();
        let client = opts
            .name("Operations Provider") // allow this to show up uniquely in a NATS connection list
            .connect(config.uri.clone())
            .await?;
        let consumer = self
            .upsert_stream(
                source_id.into(),
                client.clone(),
                config.processing_stream_name(),
            )
            .await?;
        let mut handler = OperationHandler {
            consumer,
            context: jetstream::new(client.clone()),
            client: client.clone(),
            component_id: source_id.to_string(),
        };

        let join_handle = tokio::spawn(async move { handler.run().await });

        let operation_state = OperationState {
            config: config.clone(),
            client: client.clone(),
            handler: join_handle,
        };
        let mut update_map = self.components.write().await;
        update_map.insert(source_id.into(), operation_state);
        Ok(())
    }

    /// Handle notification that a link is dropped: close the connection which removes all subscriptions
    // TODO should this delete consumers? Probably not unless there's an option to
    async fn delete_link(&self, source_id: &str) -> anyhow::Result<()> {
        let mut all_components = self.components.write().await;

        if all_components.remove(source_id).is_some() {
            // Note: subscriptions will be closed via Drop on the NatsClientBundle
            debug!(
                "closing NATS subscriptions for component [{}]...",
                source_id,
            );
            all_components.get(source_id).unwrap().handler.abort();
        }

        debug!(
            "finished processing delete link for component [{}]",
            source_id
        );
        Ok(())
    }

    /// Handle shutdown request by closing all connections
    async fn shutdown(&self) -> anyhow::Result<()> {
        let mut all_components = self.components.write().await;
        all_components.clear();
        Ok(())
    }
}

impl exports::cosmonic::longrunning_operation::service::Handler<Option<Context>>
    for LongRunningOperationProvider
{
    async fn start(
        &self,
        ctx: Option<Context>,
    ) -> anyhow::Result<Result<Operation, OperationError>> {
        let component = match ctx.and_then(|Context { component, .. }| component) {
            Some(component) => component,
            None => bail!("no component specified"),
        };
        let (client, stream_name) = {
            let components = self.components.read().await;
            let state = match components.get(&component) {
                Some(state) => state,
                None => bail!("component not found"),
            };
            (state.client.clone(), state.config.processing_stream_name())
        };
        let id = OperationID::new();
        let operation = Operation {
            name: id.to_string(),
            done: false,
            result: None,
        };
        let mut encoded = BytesMut::new();
        (&operation).encode(&mut encoded).await?;

        let js = jetstream::new(client);
        js.publish(format!("{}.{}", stream_name, id), encoded.freeze())
            .await?;
        Ok(Ok(operation))
    }

    async fn get(
        &self,
        ctx: Option<Context>,
        id: String,
    ) -> anyhow::Result<Result<Operation, OperationError>> {
        let component = match ctx.and_then(|Context { component, .. }| component) {
            Some(component) => component,
            None => bail!("no component specified"),
        };
        let (client, stream_name) = {
            let components = self.components.read().await;
            let state = match components.get(&component) {
                Some(state) => state,
                None => bail!("component not found"),
            };
            (state.client.clone(), state.config.processing_stream_name())
        };

        let js = jetstream::new(client);
        let store = js.get_key_value(&stream_name).await.map_err(|e| {
            anyhow!(
                "failed to get key value store for stream {}: {}",
                stream_name,
                e
            )
        })?;
        let op = store.get(&id).await.map_err(|e| {
            anyhow!(
                "failed to get operation with id {} from stream {}: {}",
                id,
                stream_name,
                e
            )
        })?;
        if let Some(op) = op {
            let mut empty = futures::stream::empty();
            let (decoded, _) = Operation::receive_sync(op, &mut empty).await?;
            Ok(Ok(decoded))
        } else {
            bail!("operation not found")
        }
    }
    async fn list(
        &self,
        ctx: Option<Context>,
        filter: Option<String>,
    ) -> anyhow::Result<Result<Vec<Operation>, OperationError>> {
        let component = match ctx.and_then(|Context { component, .. }| component) {
            Some(component) => component,
            None => bail!("no component specified"),
        };
        let (nats_client, stream_name) = {
            let components = self.components.read().await;
            let state = match components.get(&component) {
                Some(state) => state,
                None => bail!("component not found"),
            };
            (state.client.clone(), state.config.processing_stream_name())
        };

        let js = jetstream::new(nats_client);
        let store = match js.get_key_value(stream_name.clone()).await {
            Ok(store) => store,
            Err(e) => {
                bail!(
                    "failed to get key value store for stream {}: {}",
                    stream_name.clone(),
                    e
                )
            }
        };
        let keys = store.keys().await?.try_collect::<Vec<String>>().await?;

        let mut operations = vec![];
        for key in keys.into_iter() {
            let value = store.get(&key).await?;
            if let Some(value) = value {
                let mut empty = futures::stream::empty();
                let (op, _) = Operation::receive_sync(value, &mut empty).await?;
                operations.push(op);
            }
        }
        Ok(Ok(operations))
    }

    async fn delete(
        &self,
        ctx: Option<Context>,
        id: String,
    ) -> anyhow::Result<Result<(), OperationError>> {
        let component = match ctx.and_then(|Context { component, .. }| component) {
            Some(component) => component,
            None => bail!("no component specified"),
        };
        let (nats_client, stream_name) = {
            let components = self.components.read().await;
            let state = match components.get(&component) {
                Some(state) => state,
                None => bail!("component not found"),
            };
            (state.client.clone(), state.config.processing_stream_name())
        };

        let js = jetstream::new(nats_client);
        let store = match js.get_key_value(stream_name.clone()).await {
            Ok(store) => store,
            Err(e) => {
                bail!(
                    "failed to get key value store for stream {}: {}",
                    stream_name.clone(),
                    e
                )
            }
        };
        if let Err(e) = store.delete(&id).await {
            bail!(
                "failed to delete operation with id {} from stream {}: {}",
                id,
                stream_name,
                e
            )
        }
        Ok(Ok(()))
    }

    async fn cancel(
        &self,
        ctx: Option<Context>,
        id: String,
    ) -> anyhow::Result<Result<(), OperationError>> {
        todo!()
    }

    async fn wait(
        &self,
        ctx: Option<Context>,
        id: String,
        timeout: Option<Duration>,
    ) -> anyhow::Result<Pollable, anyhow::Error> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_op_decode() {
        let operation = Operation {
            name: "test".to_string(),
            done: false,
            result: None,
        };

        let mut encoded: BytesMut = BytesMut::new();
        (&operation).encode(&mut encoded).await.unwrap();
        let buf = bytes::Bytes::from(encoded);

        let mut empty = futures::stream::empty();
        let (decoded, _) = Operation::receive_sync(buf, &mut empty).await.unwrap();
        assert_eq!(operation.name, decoded.name);
    }
}
