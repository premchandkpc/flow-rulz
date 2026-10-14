use crate::domain::EffectIntent; use async_trait::async_trait; use thiserror::Error;
#[derive(Debug,Error)] pub enum EffectError {#[error("retryable destination failure: {0}")] Retryable(String),#[error("permanent destination rejection: {0}")] Permanent(String),#[error("destination outcome unknown: {0}")] Unknown(String)}
#[derive(Debug,Clone)] pub struct DeliveryReceipt {pub destination:String,pub idempotency_key:String,pub destination_receipt:Option<String>}
#[async_trait] pub trait EffectSink:Send+Sync {async fn deliver(&self,effect:&EffectIntent)->Result<DeliveryReceipt,EffectError>;}