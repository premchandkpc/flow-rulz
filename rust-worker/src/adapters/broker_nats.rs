//! Optional request/reply adaptation of broker protocol v1 on a unique NATS subject.
use crate::{
    application::broker::Service,
    ports::broker::{Request, Response},
};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub(crate) async fn connect(url: &str) -> Result<async_nats::Client> {
    tokio::time::timeout(
        Duration::from_secs(5),
        async_nats::ConnectOptions::new()
            .subscription_capacity(128)
            .connect(url),
    )
    .await?
    .context("connect broker NATS adapter")
}

pub(crate) async fn serve(
    client: async_nats::Client,
    mut subscription: async_nats::Subscriber,
    service: Arc<Service>,
    stop: CancellationToken,
) -> Result<()> {
    let permits = Arc::new(Semaphore::new(64));
    let mut requests = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            message = subscription.next() => {
                let message = message.context("broker NATS subscription closed")?;
                let Some(reply) = message.reply else {continue};
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    send(&client, reply, Response::new("", "unavailable", None)).await?;
                    continue;
                };
                let client = client.clone(); let service = service.clone(); let stop = stop.clone();
                requests.spawn(async move {
                    let _permit = permit;
                    let work = async {
                        let response = if message.payload.len() <= 1 << 20 {
                            match serde_json::from_slice::<Request>(&message.payload) {
                                Ok(request) => service.execute(request).await,
                                Err(_) => Response::new("", "invalid", None),
                            }
                        } else {Response::new("", "invalid", None)};
                        send(&client, reply, response).await
                    };
                    tokio::select! {_ = stop.cancelled() => Ok(()), result = tokio::time::timeout(Duration::from_secs(25), work) => result?}
                });
            },
            result = requests.join_next(), if !requests.is_empty() => {report(result)?;},
        }
    }
    subscription.unsubscribe().await?;
    while let Some(result) = requests.join_next().await {
        report(Some(result))?;
    }
    Ok(())
}

async fn send(
    client: &async_nats::Client,
    reply: async_nats::Subject,
    response: Response,
) -> Result<()> {
    let mut data = serde_json::to_vec(&response)?;
    if data.len() > client.server_info().max_payload {
        data = serde_json::to_vec(&Response::new(&response.id, "too_large", None))?;
    }
    tokio::time::timeout(Duration::from_secs(5), client.publish(reply, data.into())).await??;
    Ok(())
}

fn report(result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>) -> Result<()> {
    match result {
        Some(Err(error)) => Err(error.into()),
        Some(Ok(Err(error))) => {
            tracing::warn!(error = %error, "broker NATS request failed");
            Ok(())
        }
        _ => Ok(()),
    }
}
