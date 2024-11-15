pub(crate) mod vendor;

use std::collections::HashMap;

#[cfg(feature = "kafka")]
use crate::service::event_publisher::kafka::vendor::cloudevents::binding::rdkafka::{
    FutureRecordExt, MessageRecord,
};
use axum::async_trait;
use cloudevents::Event;
#[cfg(feature = "kafka")]
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use veil::Redact;

use super::CloudEventBackend;

#[derive(Clone, Serialize, Deserialize, PartialEq, Redact)]
pub struct KafkaConfig {
    #[serde(alias = "sasl.password")]
    #[redact]
    pub sasl_password: Option<String>,
    #[serde(alias = "sasl.oauthbearer.client.secret")]
    #[redact]
    pub sasl_oauthbearer_client_secret: Option<String>,
    #[serde(alias = "ssl.key.password")]
    #[redact]
    pub ssl_key_password: Option<String>,
    #[serde(alias = "ssl.keystore.password")]
    #[redact]
    pub ssl_keystore_password: Option<String>,
    #[serde(flatten)]
    pub conf: HashMap<String, String>,
}

#[cfg(feature = "kafka")]
pub struct KafkaBackend {
    pub producer: FutureProducer,
    pub topic: String,
}

#[cfg(feature = "kafka")]
impl std::fmt::Debug for KafkaBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBackend")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "kafka")]
#[async_trait]
impl CloudEventBackend for KafkaBackend {
    async fn publish(&self, event: Event) -> anyhow::Result<()> {
        let message_record = MessageRecord::from_event(event)?;
        let delivery_status = self
            .producer
            .send(
                FutureRecord::to(&self.topic)
                    .message_record(&message_record)
                    .key(""),
                Duration::from_secs(1),
            )
            .await;

        match delivery_status {
            Ok((partition, offset)) => {
                tracing::debug!("CloudEvents event send via kafka to topic: {} and partition: {} with offset: {}", &self.topic, partition, offset);
                Ok(())
            }
            Err((e, _)) => Err(anyhow::anyhow!(e)),
        }
    }

    fn name(&self) -> &'static str {
        "kafka-publisher"
    }
}
