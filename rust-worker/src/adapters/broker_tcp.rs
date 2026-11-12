//! Length-prefixed JSON protocol over reusable TCP connections.
use crate::{
    application::broker::Service,
    ports::broker::{Request, Response},
};
use anyhow::{Context, Result, bail};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

pub(crate) async fn read_frame(stream: &mut TcpStream, limit: usize) -> Result<Vec<u8>> {
    let size = stream.read_u32().await? as usize;
    if size == 0 || size > limit {
        bail!("invalid broker TCP frame length");
    }
    let mut data = vec![0; size];
    stream.read_exact(&mut data).await?;
    Ok(data)
}

async fn write_frame(stream: &mut TcpStream, data: &[u8], limit: usize) -> Result<()> {
    if data.len() > limit {
        bail!("broker TCP frame exceeds limit");
    }
    stream.write_u32(u32::try_from(data.len())?).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

pub(crate) async fn serve(
    listener: TcpListener,
    service: Arc<Service>,
    stop: CancellationToken,
) -> Result<()> {
    let permits = Arc::new(Semaphore::new(64));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            result = listener.accept() => {
                let (stream, _) = result?;
                let Ok(permit) = permits.clone().try_acquire_owned() else {continue};
                let service = service.clone(); let stop = stop.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    tokio::select! {_ = stop.cancelled() => Ok(()), result = connection(stream, service) => result}
                });
            },
            result = connections.join_next(), if !connections.is_empty() => {report_connection(result)?;},
        }
    }
    while let Some(result) = connections.join_next().await {
        report_connection(Some(result))?;
    }
    Ok(())
}

fn report_connection(
    result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>,
) -> Result<()> {
    match result {
        Some(Err(error)) => Err(error.into()),
        Some(Ok(Err(error))) => {
            tracing::debug!(error = %error, "broker TCP connection ended");
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn connection(mut stream: TcpStream, service: Arc<Service>) -> Result<()> {
    loop {
        let data = tokio::time::timeout(Duration::from_secs(30), read_frame(&mut stream, 1 << 20))
            .await??;
        let response = match serde_json::from_slice::<Request>(&data) {
            Ok(request) => {
                tokio::time::timeout(Duration::from_secs(25), service.execute(request)).await?
            }
            Err(_) => Response::new("", "invalid", None),
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            write_frame(&mut stream, &serde_json::to_vec(&response)?, 2 << 20),
        )
        .await??;
    }
}

pub(crate) struct Client {
    address: String,
    stream: Mutex<Option<TcpStream>>,
}

impl Client {
    pub fn new(address: String) -> Self {
        Self {
            address,
            stream: Mutex::new(None),
        }
    }

    pub async fn call(&self, request: &Request) -> Result<Response> {
        let mut connection = self.stream.lock().await;
        let mut stream = match connection.take() {
            Some(stream) => stream,
            None => {
                tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&self.address))
                    .await??
            }
        };
        // Keep the connection outside the cache during I/O. If this future is
        // cancelled, the incomplete stream is dropped rather than reused.
        let result = tokio::time::timeout(Duration::from_secs(25), async {
            write_frame(&mut stream, &serde_json::to_vec(request)?, 1 << 20).await?;
            let data = read_frame(&mut stream, 2 << 20).await?;
            serde_json::from_slice::<Response>(&data).context("decode TCP broker response")
        })
        .await?;
        if result.is_ok() {
            *connection = Some(stream);
        }
        result
    }
}
