//! Shared broker operations and local-rule processing for every selected transport.
use crate::{
    broker_config::Config,
    engine::{EventEnvelope, local::LocalRuleSet},
    ports::broker::{Request, Response},
    runtime::broker::{Broker, Claim, valid_name},
};
use anyhow::{Context, Result, bail};
use serde_json::json;
use std::{collections::BTreeMap, fs, io::Read, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio_util::sync::CancellationToken;

pub(crate) struct Service {
    pub broker: Arc<Broker>,
    token: String,
}

impl Service {
    pub fn new(broker: Arc<Broker>, token: String) -> Self {
        Self { broker, token }
    }

    pub async fn execute(&self, request: Request) -> Response {
        let invalid = || Response::new(&request.id, "invalid", None);
        if request.version != 1 || request.id.len() > 128 {
            return Response::new("", "invalid", None);
        }
        if !self.token.is_empty()
            && !bool::from(self.token.as_bytes().ct_eq(request.token.as_bytes()))
        {
            return Response::new(&request.id, "unauthorized", None);
        }
        if !valid_name(&request.tenant_id) || request.tenant_id.trim() != request.tenant_id {
            return invalid();
        }
        match request.operation.as_str() {
            "publish" => match request.event.as_ref() {
                Some(event) => self.publish(&request.id, &request.tenant_id, event).await,
                None => invalid(),
            },
            "append" => {
                let Some(payload) = request.payload.as_ref() else {
                    return invalid();
                };
                if request.topic == "events" {
                    let Ok(event) = serde_json::from_value::<EventEnvelope>(payload.clone()) else {
                        return invalid();
                    };
                    if !request.key.is_empty() && request.key != event.partition_key {
                        return invalid();
                    }
                    return self.publish(&request.id, &request.tenant_id, &event).await;
                }
                if !valid_name(&request.topic) || request.key.is_empty() || request.key.len() > 1024
                {
                    return invalid();
                }
                match self
                    .broker
                    .append(
                        &request.topic,
                        &request.tenant_id,
                        &request.key,
                        payload.clone(),
                        false,
                    )
                    .await
                {
                    Ok(record) => Response::new(
                        &request.id,
                        "accepted",
                        Some(
                            json!({"topic": record.topic, "partition": record.partition, "offset": record.offset}),
                        ),
                    ),
                    Err(_) => Response::new(&request.id, "unavailable", None),
                }
            }
            "consume" => {
                if !valid_name(&request.topic)
                    || !valid_name(&request.group)
                    || !(0..=20000).contains(&request.wait_ms)
                {
                    return invalid();
                }
                match self
                    .broker
                    .next(
                        &request.tenant_id,
                        &request.topic,
                        &request.group,
                        Duration::from_millis(request.wait_ms as u64),
                    )
                    .await
                {
                    Ok(Some(claim)) => match serde_json::to_value(claim) {
                        Ok(data) => Response::new(&request.id, "message", Some(data)),
                        Err(_) => Response::new(&request.id, "unavailable", None),
                    },
                    Ok(None) => Response::new(&request.id, "empty", None),
                    Err(_) => Response::new(&request.id, "unavailable", None),
                }
            }
            "ack" | "retry" => {
                if request.partition >= self.broker.partition_count() {
                    return invalid();
                }
                if !valid_name(&request.topic)
                    || !valid_name(&request.group)
                    || request.receipt.is_empty()
                    || !(0..=300000).contains(&request.delay_ms)
                {
                    return invalid();
                }
                let delay = (request.operation == "retry")
                    .then(|| Duration::from_millis(request.delay_ms as u64));
                match self
                    .broker
                    .settle(
                        &request.tenant_id,
                        &request.topic,
                        &request.group,
                        request.partition,
                        &request.receipt,
                        delay,
                    )
                    .await
                {
                    Ok(true) => Response::new(&request.id, "ok", None),
                    Ok(false) => Response::new(&request.id, "conflict", None),
                    Err(_) => Response::new(&request.id, "unavailable", None),
                }
            }
            _ => invalid(),
        }
    }

    async fn publish(&self, id: &str, tenant: &str, event: &EventEnvelope) -> Response {
        if event.tenant_id != tenant {
            return Response::new(id, "forbidden", None);
        }
        if event.validate().is_err() || event.contract.is_some() {
            return Response::new(id, "invalid", None);
        }
        let Ok(payload) = serde_json::to_value(event) else {
            return Response::new(id, "invalid", None);
        };
        match self
            .broker
            .append("events", tenant, &event.partition_key, payload, true)
            .await
        {
            Ok(record) => Response::new(
                id,
                "accepted",
                Some(
                    json!({"topic": "events", "partition": record.partition, "offset": record.offset, "event_id": event.id}),
                ),
            ),
            Err(_) => Response::new(id, "unavailable", None),
        }
    }
}

pub(crate) struct RuleWorker {
    broker: Arc<Broker>,
    rules: BTreeMap<String, LocalRuleSet>,
    retry_delay: Duration,
    operation_timeout: Duration,
}

impl RuleWorker {
    pub async fn new(config: &Config, broker: Arc<Broker>) -> Result<Self> {
        let mut paths = fs::read_dir(&config.rules_dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.retain(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        });
        paths.sort();
        if paths.is_empty() || paths.len() > 100 {
            bail!("local rules directory requires 1..100 JSON files");
        }
        let mut rules = BTreeMap::new();
        for path in paths {
            let mut data = Vec::new();
            fs::File::open(path)?
                .take(256 * 1024 + 1)
                .read_to_end(&mut data)?;
            let rule = LocalRuleSet::compile(&data)?;
            if rules.insert(rule.name().into(), rule).is_some() {
                bail!("duplicate local ruleset");
            }
        }
        broker.register_rules().await?;
        Ok(Self {
            broker,
            rules,
            retry_delay: config.retry_delay,
            operation_timeout: config.ack_wait / 2,
        })
    }

    pub async fn run(
        &self,
        output: &dyn crate::ports::broker::Publisher,
        stop: CancellationToken,
    ) -> Result<()> {
        loop {
            let claim = tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                claim = self.broker.next("", "events", "__rules", Duration::from_secs(1)) => claim?,
            };
            let Some(claim) = claim else { continue };
            let result = tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                result = tokio::time::timeout(self.operation_timeout, self.process(&claim, output)) => result.context("rule processing deadline").and_then(|result| result),
            };
            let retry = match result {
                Ok(()) => None,
                Err(error) => {
                    tracing::warn!(error = %error, "broker rule delivery will retry");
                    Some(self.retry_delay)
                }
            };
            if !self
                .broker
                .settle(
                    "",
                    "events",
                    "__rules",
                    claim.record.partition,
                    &claim.receipt,
                    retry,
                )
                .await?
            {
                tracing::warn!("broker rule receipt expired before settlement");
            }
        }
    }

    async fn process(
        &self,
        claim: &Claim,
        output: &dyn crate::ports::broker::Publisher,
    ) -> Result<()> {
        let event: EventEnvelope = serde_json::from_value(claim.record.payload.clone())?;
        let rule = self
            .rules
            .get(&event.event_type)
            .context("local rule is not ready")?;
        for effect in rule.evaluate(&event)? {
            output.publish(effect).await?;
        }
        Ok(())
    }
}
