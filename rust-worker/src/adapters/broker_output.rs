//! Independently selected local, HTTP, TCP or NATS output publication.
use crate::{
    broker_config::Config,
    ports::broker::{Publication, Publisher, Request, Response},
    runtime::broker::Broker,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

enum Target {
    Local(Arc<Broker>),
    Http {
        client: reqwest::Client,
        url: String,
    },
    Tcp(super::broker_tcp::Client),
    Nats {
        client: async_nats::Client,
        subject: String,
    },
}

pub(crate) struct OutputPublisher {
    target: Target,
    token: String,
}

impl OutputPublisher {
    pub async fn new(config: &Config, broker: Arc<Broker>) -> Result<Self> {
        let target = match config.output.as_str() {
            "local" => Target::Local(broker),
            "http" => Target::Http {
                client: reqwest::Client::builder()
                    .timeout(Duration::from_secs(25))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?,
                url: format!("{}/v1/broker/rpc", config.output_url.trim_end_matches('/')),
            },
            "tcp" => Target::Tcp(super::broker_tcp::Client::new(config.output_addr.clone())),
            "nats" => Target::Nats {
                client: super::broker_nats::connect(&config.output_nats_url).await?,
                subject: config.output_nats_subject.clone(),
            },
            _ => bail!("invalid broker output transport"),
        };
        Ok(Self {
            target,
            token: config.output_token.clone(),
        })
    }
}

#[async_trait]
impl Publisher for OutputPublisher {
    async fn publish(&self, output: Publication) -> Result<()> {
        if let Target::Local(broker) = &self.target {
            broker
                .append(
                    &output.topic,
                    &output.tenant,
                    &output.key,
                    output.payload,
                    false,
                )
                .await?;
            return Ok(());
        }
        let request = Request {
            version: 1,
            id: Uuid::new_v4().to_string(),
            operation: "append".into(),
            tenant_id: output.tenant,
            token: self.token.clone(),
            topic: output.topic,
            key: output.key,
            payload: Some(output.payload),
            ..Default::default()
        };
        let data = serde_json::to_vec(&request)?;
        if data.len() > 1 << 20 {
            bail!("broker output request exceeds 1 MiB");
        }
        let response = match &self.target {
            Target::Local(_) => unreachable!("local publication handled above"),
            Target::Tcp(client) => client.call(&request).await?,
            Target::Http { client, url } => {
                let mut response = client
                    .post(url)
                    .header("Content-Type", "application/json")
                    .body(data)
                    .send()
                    .await?;
                if response.status() != reqwest::StatusCode::OK {
                    bail!("HTTP broker rejected request");
                }
                let mut data = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    if data.len() + chunk.len() > 2 << 20 {
                        bail!("HTTP broker response exceeds 2 MiB");
                    }
                    data.extend_from_slice(&chunk);
                }
                serde_json::from_slice::<Response>(&data).context("decode HTTP broker response")?
            }
            Target::Nats { client, subject } => {
                if data.len() > client.server_info().max_payload {
                    bail!("NATS broker request exceeds server limit");
                }
                let message = tokio::time::timeout(
                    Duration::from_secs(25),
                    client.request(subject.clone(), data.into()),
                )
                .await??;
                if message.payload.len() > 2 << 20 {
                    bail!("NATS broker response exceeds 2 MiB");
                }
                serde_json::from_slice::<Response>(&message.payload)?
            }
        };
        if response.version != 1 || response.id != request.id || response.code != "accepted" {
            bail!("remote broker rejected publication");
        }
        Ok(())
    }
}
