//! Runtime orchestration. Domain work lives in focused submodules.
mod consumer;
mod effects;
mod quarantine;
mod scheduler;

use crate::{config::Config, engine};
use anyhow::{bail, Context, Result};
use async_nats::jetstream;
use chrono::Utc;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::HashMap;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

pub async fn run(config: Config) -> Result<()> {
    let (pool, js) = connect_services(&config).await?;
    let leases = acquire_initial_leases(&pool, &config).await?;
    if leases.is_empty() { bail_no_leases(&config)?; }
    info!(owned_shards = leases.len(), "Rust worker acquired shard leases");

    let mut tasks = JoinSet::new();
    spawn_consumer_tasks(&mut tasks, &pool, &js, &config, &leases);
    spawn_background_tasks(&mut tasks, &pool, &js, &config);
    wait_for_shutdown(&mut tasks).await;
    abort_remaining_tasks(&mut tasks).await;
    release_owned_leases(&pool, &leases, &config.worker_id).await;
    pool.close().await;
    Ok(())
}

fn bail_no_leases(config: &Config) -> Result<()> {
    bail!(
        "worker {} could not acquire any shard leases; verify replica count and lease availability",
        config.worker_id
    )
}

async fn connect_services(config: &Config) -> Result<(PgPool, jetstream::Context)> {
    let pool = PgPoolOptions::new().max_connections(20).connect(&config.database_url)
        .await.context("connect PostgreSQL")?;
    sqlx::query("SELECT 1").execute(&pool).await.context("verify PostgreSQL")?;

    let client = async_nats::connect(config.nats_url.clone()).await.context("connect NATS")?;
    let js = jetstream::new(client);
    let _stream = js.get_stream(&config.stream).await
        .with_context(|| format!("get JetStream stream {}", config.stream))?;
    Ok((pool, js))
}

fn spawn_consumer_tasks(
    tasks: &mut JoinSet<()>,
    pool: &PgPool,
    js: &jetstream::Context,
    config: &Config,
    leases: &HashMap<u32, i64>,
) {
    for (shard, token) in leases {
        for (subject, durable) in consumer::subjects_for_shard(*shard, config) {
            let pool = pool.clone();
            let js = js.clone();
            let config = config.clone();
            let shard = *shard;
            let token = *token;
            tasks.spawn(async move {
                if let Err(error) = consumer::consume_shard_subject(pool, js, config, shard, token, subject, durable).await {
                    error!(shard, error = %error, "shard consumer stopped");
                }
            });
        }
    }
}

fn spawn_background_tasks(tasks: &mut JoinSet<()>, pool: &PgPool, js: &jetstream::Context, config: &Config) {
    let outbox_pool = pool.clone();
    let outbox_js = js.clone();
    let outbox_config = config.clone();
    tasks.spawn(async move {
        if let Err(error) = effects::outbox_loop(outbox_pool, outbox_js, outbox_config).await {
            error!(error = %error, "outbox publisher stopped");
        }
    });

    let scheduler_pool = pool.clone();
    let scheduler_js = js.clone();
    let scheduler_config = config.clone();
    tasks.spawn(async move {
        if let Err(error) = scheduler::scheduler_loop(scheduler_pool, scheduler_js, scheduler_config).await {
            error!(error = %error, "scheduled-event publisher stopped");
        }
    });
}

async fn wait_for_shutdown(tasks: &mut JoinSet<()>) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("shutdown signal received"),
        result = tasks.join_next() => {
            if let Some(Err(error)) = result { warn!(error=%error, "runtime task join failed"); }
            warn!("a core runtime task exited; shutting down remaining tasks");
        }
    }
}

async fn abort_remaining_tasks(tasks: &mut JoinSet<()>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

async fn release_owned_leases(pool: &PgPool, leases: &HashMap<u32, i64>, worker_id: &str) {
    for shard in leases.keys() {
        if let Err(error) = engine::release_lease(pool, *shard, worker_id).await {
            warn!(shard, error = %error, "failed to release shard lease");
        }
    }
}

async fn acquire_initial_leases(pool: &PgPool, config: &Config) -> Result<HashMap<u32, i64>> {
    let target = config.num_shards.div_ceil(config.worker_replicas);
    let start = engine::fnv1a32(config.worker_id.as_bytes()) % config.num_shards;
    let ttl_ms = i64::try_from(config.lease_ttl.as_millis())?;
    let mut leases = HashMap::with_capacity(target as usize);
    for offset in 0..config.num_shards {
        if leases.len() >= target as usize { break; }
        let shard = (start + offset) % config.num_shards;
        match engine::acquire_lease(pool, shard, &config.worker_id, ttl_ms).await {
            Ok(Some(token)) => { leases.insert(shard, token); }
            Ok(None) => {}
            Err(err) => warn!(shard, error=%err, "initial shard lease attempt failed"),
        }
    }
    Ok(leases)
}

fn validate_subject(subject: &str) -> Result<()> {
    if subject.trim().is_empty() || subject.split('.').any(|part| part.is_empty() || (part.contains('*') || part.contains('>'))) {
        bail!("invalid concrete NATS subject");
    }
    Ok(())
}

fn uuid_token() -> String {
    format!("{}-{}", Utc::now().timestamp_nanos_opt().unwrap_or_default(), std::process::id())
}
