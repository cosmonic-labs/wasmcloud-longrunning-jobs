use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const DEFAULT_NATS_URI: &str = "0.0.0.0:4222";
const CONFIG_NATS_URI: &str = "uri";
const CONFIG_PROCESSING_STREAM_PREFIX: &str = "processing";
const STREAM_PREFIX: &str = "wasmcloud_operations";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperationConfig {
    processing_stream_name: String,
    processing_consumer_name: String,
    pub operation_namespace: String,
    pub uri: String,
}

impl OperationConfig {
    pub fn merge(&self, extra: OperationConfig) -> OperationConfig {
        let mut out = self.clone();
        if !extra.processing_stream_name.is_empty() {
            out.processing_stream_name = extra.processing_stream_name;
        }
        if !extra.processing_consumer_name.is_empty() {
            out.processing_consumer_name = extra.processing_consumer_name;
        }
        if !extra.operation_namespace.is_empty() {
            out.operation_namespace = extra.operation_namespace;
        }
        if !extra.uri.is_empty() {
            out.uri = extra.uri;
        }
        out
    }

    pub fn processing_stream_name(&self) -> String {
        format!(
            "{}_{}",
            self.processing_stream_name, self.operation_namespace
        )
    }
}

impl Default for OperationConfig {
    fn default() -> OperationConfig {
        OperationConfig {
            processing_stream_name: format!(
                "{}_{}",
                STREAM_PREFIX, CONFIG_PROCESSING_STREAM_PREFIX
            ),
            processing_consumer_name: format!(
                "{}_{}",
                STREAM_PREFIX, CONFIG_PROCESSING_STREAM_PREFIX
            ),
            operation_namespace: "operation".to_string(),
            uri: DEFAULT_NATS_URI.to_string(),
        }
    }
}

impl From<&HashMap<String, String>> for OperationConfig {
    fn from(values: &HashMap<String, String>) -> OperationConfig {
        let mut config = OperationConfig::default();

        if let Some(uri) = values.get(CONFIG_NATS_URI) {
            config.uri = uri.to_string();
        }
        if let Some(stream_name) = values.get("processing_stream_name") {
            config.processing_stream_name = stream_name.to_string();
        }
        if let Some(consumer_name) = values.get("processing_consumer_name") {
            config.processing_consumer_name = consumer_name.to_string();
        }
        if let Some(namespace) = values.get("operation_namespace") {
            config.operation_namespace = namespace.to_string();
        }
        config
    }
}
