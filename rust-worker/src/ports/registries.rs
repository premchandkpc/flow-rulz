use crate::domain::{ContractRef,RuleRevisionRef};
use async_trait::async_trait; use std::sync::Arc; use thiserror::Error;
#[derive(Debug,Error)] pub enum RegistryError { #[error("immutable revision not found: {0}")] NotFound(String), #[error("content hash mismatch: {0}")] HashMismatch(String), #[error("registry unavailable: {0}")] Unavailable(String) }
#[derive(Debug,Clone)] pub struct CompiledRuleSet {pub reference:RuleRevisionRef,pub accepted_inputs:Vec<ContractRef>,pub compiler_version:String,pub opaque_plan:Arc<[u8]>}
#[derive(Debug,Clone)] pub struct CompiledContract {pub reference:ContractRef,pub opaque_schema:Arc<[u8]>}
#[async_trait] pub trait RuleRegistry:Send+Sync {async fn get_revision(&self,reference:&RuleRevisionRef)->Result<Arc<CompiledRuleSet>,RegistryError>;}
#[async_trait] pub trait ContractRegistry:Send+Sync {async fn get_contract(&self,reference:&ContractRef)->Result<Arc<CompiledContract>,RegistryError>;}