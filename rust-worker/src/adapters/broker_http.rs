//! HTTP resource and RPC endpoints around the shared broker service.
use crate::{
    application::broker::Service,
    engine::EventEnvelope,
    ports::broker::{Request, Response},
};
use anyhow::Result;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request as HttpRequest, Response as HttpResponse, StatusCode, body::Incoming,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

type Body = Full<Bytes>;

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
                    let handler = service_fn(move |request| handle(request, service.clone()));
                    let mut builder = hyper::server::conn::http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5)).max_buf_size(32 << 10);
                    tokio::select! {
                        _ = stop.cancelled() => Ok::<(), hyper::Error>(()),
                        result = builder.serve_connection(TokioIo::new(stream), handler) => result,
                    }
                });
            },
            result = connections.join_next(), if !connections.is_empty() => {if let Some(result) = result {let _ = result?;}},
        }
    }
    while let Some(result) = connections.join_next().await {
        let _ = result?;
    }
    Ok(())
}

fn response(status: StatusCode, value: Option<Value>) -> HttpResponse<Body> {
    let bytes = value.map(|value| value.to_string()).unwrap_or_default();
    let mut response = HttpResponse::new(Full::new(Bytes::from(bytes)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

async fn handle(
    request: HttpRequest<Incoming>,
    service: Arc<Service>,
) -> std::result::Result<HttpResponse<Body>, Infallible> {
    let result = tokio::time::timeout(Duration::from_secs(25), dispatch(request, service)).await;
    Ok(result.unwrap_or_else(|_| {
        response(
            StatusCode::SERVICE_UNAVAILABLE,
            Some(json!({"error":"deadline"})),
        )
    }))
}

async fn dispatch(request: HttpRequest<Incoming>, service: Arc<Service>) -> HttpResponse<Body> {
    let path = request.uri().path().to_owned();
    if path == "/health" && request.method() == hyper::Method::GET {
        return response(StatusCode::OK, None);
    }
    if request.method() != hyper::Method::POST {
        return response(StatusCode::METHOD_NOT_ALLOWED, None);
    }
    let tenant = request
        .headers()
        .get("X-Tenant-ID")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let token = request
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default()
        .to_owned();
    let Ok(body) = Limited::new(request.into_body(), 1 << 20).collect().await else {
        return response(StatusCode::BAD_REQUEST, None);
    };
    let data = body.to_bytes();
    if path == "/v1/broker/rpc" {
        let Ok(request) = serde_json::from_slice::<Request>(&data) else {
            return response(StatusCode::BAD_REQUEST, None);
        };
        return response(
            StatusCode::OK,
            serde_json::to_value(service.execute(request).await).ok(),
        );
    }
    let mut request = Request {
        version: 1,
        tenant_id: tenant,
        token,
        ..Default::default()
    };
    if path == "/v1/broker/events" || path.starts_with("/v1/events/") {
        let Ok(event) = serde_json::from_slice::<EventEnvelope>(&data) else {
            return response(StatusCode::BAD_REQUEST, None);
        };
        if let Some(event_type) = path.strip_prefix("/v1/events/")
            && event_type != event.event_type
        {
            return response(StatusCode::BAD_REQUEST, None);
        }
        request.operation = "publish".into();
        request.event = Some(event);
    } else {
        let operation = match path.as_str() {
            "/v1/broker/messages" => "append",
            "/v1/broker/consume" => "consume",
            "/v1/broker/ack" => "ack",
            "/v1/broker/retry" => "retry",
            _ => return response(StatusCode::NOT_FOUND, None),
        };
        let Ok(value) = serde_json::from_slice::<Value>(&data) else {
            return response(StatusCode::BAD_REQUEST, None);
        };
        let Some(object) = value.as_object() else {
            return response(StatusCode::BAD_REQUEST, None);
        };
        let allowed: &[&str] = match operation {
            "append" => &["topic", "key", "payload"],
            "consume" => &["topic", "group", "wait_ms"],
            _ => &["topic", "group", "partition", "receipt", "delay_ms"],
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return response(StatusCode::BAD_REQUEST, None);
        }
        let mut object = object.clone();
        object.insert("version".into(), json!(1));
        object.insert("tenant_id".into(), json!(request.tenant_id));
        object.insert("token".into(), json!(request.token));
        object.insert("operation".into(), json!(operation));
        let Ok(decoded) = serde_json::from_value::<Request>(Value::Object(object)) else {
            return response(StatusCode::BAD_REQUEST, None);
        };
        request = decoded;
    }
    resource_response(service.execute(request).await)
}

fn resource_response(result: Response) -> HttpResponse<Body> {
    let status = match result.code.as_str() {
        "accepted" => StatusCode::ACCEPTED,
        "message" => StatusCode::OK,
        "ok" | "empty" => StatusCode::NO_CONTENT,
        "invalid" => StatusCode::BAD_REQUEST,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "conflict" => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    response(
        status,
        if status.is_success() {
            result.data
        } else {
            Some(json!({"error":result.code}))
        },
    )
}
