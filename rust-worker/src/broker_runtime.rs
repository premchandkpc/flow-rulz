//! Composition and lifecycle for the transport-optional broker/rule host.
use crate::{
    adapters::{
        broker_http, broker_nats, broker_output::OutputPublisher, broker_store::DiskStore,
        broker_tcp,
    },
    application::broker::{RuleWorker, Service},
    broker_config::Config,
    ports::broker::Store,
    runtime::broker::{Broker, Limits},
};
use anyhow::{Context, Result, bail};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub(crate) async fn run(config: Config) -> Result<()> {
    let store: Option<Arc<dyn Store>> = if config.storage == "file" {
        Some(Arc::new(DiskStore::open(&config.data_dir)?))
    } else {
        None
    };
    let broker = Arc::new(Broker::new(
        Limits {
            partitions: config.partitions,
            max_messages: config.max_messages,
            max_bytes: config.max_bytes,
            ack_wait: config.ack_wait,
        },
        store,
    )?);
    let worker = RuleWorker::new(&config, broker.clone()).await?;
    let output = OutputPublisher::new(&config, broker.clone()).await?;
    let service = Arc::new(Service::new(broker, config.token.clone()));
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let startup = start_transports(&config, service, stop.clone(), &mut tasks).await;
    if let Err(error) = startup {
        stop.cancel();
        if let Err(cleanup) = drain(&mut tasks).await {
            return Err(error.context(format!("broker startup cleanup also failed: {cleanup:#}")));
        }
        return Err(error);
    }
    let worker_stop = stop.clone();
    tasks.spawn(async move { worker.run(&output, worker_stop).await });
    tracing::info!(transports = ?config.transports, output = %config.output, storage = %config.storage, "Rust broker initialized");
    let result = tokio::select! {
        result = shutdown_signal() => result,
        result = tasks.join_next() => match result {
            Some(Ok(Err(error))) => Err(error),
            Some(Err(error)) => Err(error.into()),
            _ => Err(anyhow::anyhow!("broker task stopped unexpectedly")),
        },
    };
    stop.cancel();
    let cleanup = drain(&mut tasks).await;
    match (result, cleanup) {
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("broker cleanup also failed: {cleanup:#}")))
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}

async fn start_transports(
    config: &Config,
    service: Arc<Service>,
    stop: CancellationToken,
    tasks: &mut JoinSet<Result<()>>,
) -> Result<()> {
    for transport in &config.transports {
        let service = service.clone();
        let stop = stop.clone();
        match transport.as_str() {
            "none" => (),
            "http" => {
                let listener = TcpListener::bind(&config.http_addr)
                    .await
                    .context("bind broker HTTP listener")?;
                tracing::info!(address = %listener.local_addr()?, "Rust broker HTTP listening");
                tasks.spawn(async move { broker_http::serve(listener, service, stop).await });
            }
            "tcp" => {
                let listener = TcpListener::bind(&config.tcp_addr)
                    .await
                    .context("bind broker TCP listener")?;
                tracing::info!(address = %listener.local_addr()?, "Rust broker TCP listening");
                tasks.spawn(async move { broker_tcp::serve(listener, service, stop).await });
            }
            "nats" => {
                let client = broker_nats::connect(&config.nats_url).await?;
                let subscription = client.subscribe(config.nats_subject.clone()).await?;
                tokio::time::timeout(Duration::from_secs(5), client.flush()).await??;
                tasks.spawn(async move {
                    broker_nats::serve(client, subscription, service, stop).await
                });
            }
            _ => bail!("invalid broker ingress transport"),
        }
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => ()}
    Ok(())
}

async fn drain(tasks: &mut JoinSet<Result<()>>) -> Result<()> {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        let mut failures = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("{error:#}")),
                Err(error) => failures.push(error.to_string()),
            }
        }
        if !failures.is_empty() {
            bail!("broker task cleanup failed: {}", failures.join(" | "));
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            bail!("broker shutdown timed out");
        }
    }
}
