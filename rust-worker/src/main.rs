//! Entrypoint for the standalone Rust worker service.
mod config;
mod engine;
mod runtime;

use anyhow::Result;
use std::process::ExitCode;
use tracing::{error, info};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => { error!(error = %err, "Rust worker stopped"); ExitCode::FAILURE }
    }
}

async fn run() -> Result<()> {
    let config = config::Config::from_env()?;
    info!(worker_id = %config.worker_id, shards = config.num_shards, replicas = config.worker_replicas,
        rulesets = config.rulesets.len(), "Rust worker runtime starting");
    runtime::run(config).await
}
