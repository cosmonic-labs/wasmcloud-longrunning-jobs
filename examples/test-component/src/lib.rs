wit_bindgen::generate!();

use crate::wasi::io::streams::StreamError;
use anyhow::{anyhow, bail};
use axum::{
    extract::Path,
    response::Response,
    routing::{delete, get, post},
    Router,
};
use cosmonic::longrunning_operation::types::*;
use cosmonic::longrunning_operation::*;
use exports::cosmonic::longrunning_operation::client::Guest;
use exports::wasi::http::incoming_handler::Guest as HttpGuest;

use http::Request;
use std::io::Write;
use tower_service::Service;
use wasi::clocks::monotonic_clock;
use wasi::http::types::*;
use wasi::io::poll::Pollable;
use wasi::logging::logging::{log, Level};

const MAX_READ_BYTES: u32 = 2048;

// This struct implementation is from
// https://github.com/wasmCloud/wasmCloud/blob/ef3955a754ab59d2597dd1ef1801ac667eaf19a5/crates/actor/src/wrappers/io.rs#L45-L89
// Copied here for convenience so we don't have to depend on the wasmcloud crate that implements
// it.
pub struct OutputStreamWriter<'a> {
    stream: &'a mut crate::wasi::io::streams::OutputStream,
}

impl<'a> From<&'a mut crate::wasi::io::streams::OutputStream> for OutputStreamWriter<'a> {
    fn from(stream: &'a mut crate::wasi::io::streams::OutputStream) -> Self {
        Self { stream }
    }
}

impl std::io::Write for OutputStreamWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        //use crate::wasi::io::streams::StreamError;
        use std::io;

        let n = match self.stream.check_write().map(std::num::NonZeroU64::new) {
            Ok(Some(n)) => n,
            Ok(None) | Err(StreamError::Closed) => return Ok(0),
            Err(StreamError::LastOperationFailed(e)) => {
                return Err(io::Error::new(io::ErrorKind::Other, e.to_debug_string()))
            }
        };
        let n = n
            .get()
            .try_into()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let n = buf.len().min(n);
        self.stream.write(&buf[..n]).map_err(|e| match e {
            StreamError::Closed => io::ErrorKind::UnexpectedEof.into(),
            StreamError::LastOperationFailed(e) => {
                io::Error::new(io::ErrorKind::Other, e.to_debug_string())
            }
        })?;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream
            .blocking_flush()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}

struct Handler {}

impl Guest for Handler {
    fn handle(request: Operation) -> Result<Operation, Error> {
        log(Level::Info, request.name.as_str(), "work started");
        let sub = monotonic_clock::subscribe_duration(
            std::time::Duration::from_secs(10).as_nanos() as u64,
        );
        sub.block();
        log(Level::Info, request.name.as_str(), "work finished");
        let mut result = request.clone();
        result.done = true;
        Ok(result)
    }

    fn wait(id: String, duration: Option<u64>) -> Pollable {
        todo!()
    }
}

impl Handler {
    fn router() -> Router {
        Router::new()
            .route("/operations", get(Handler::list))
            .route("/operations/start", get(Handler::start))
            .route("/operations/:id", get(Handler::get))
    }

    async fn start() -> Response {
        let op = match service::start() {
            Ok(o) => o,
            Err(e) => {
                log(
                    Level::Error,
                    "start",
                    &format!("failed to start operation: {:?}", e),
                );
                return Response::builder()
                    .status(500)
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
        };
        let resp = serde_json::json!({
            "operation": op.name,
        });
        let resp = serde_json::to_string(&resp).unwrap();
        Response::builder()
            .status(200)
            .body(axum::body::Body::new(resp))
            .unwrap()
    }

    async fn get(Path(id): Path<String>) -> Response {
        let result = service::get(id.as_str()).unwrap();
        let resp = serde_json::json!({
            "operation": result.name,
            "done": result.done,
        });
        let r = serde_json::to_string(&resp).unwrap();
        Response::builder()
            .status(200)
            .body(axum::body::Body::new(r))
            .unwrap()
    }

    async fn list() -> Response {
        let result = match service::list(None) {
            Ok(r) => r,
            Err(e) => {
                log(
                    Level::Error,
                    "list",
                    &format!("failed to list operations: {:?}", e),
                );
                return Response::builder()
                    .status(500)
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
        };
        let mut operations = Vec::new();
        for op in result.iter() {
            let o = serde_json::json!({
                "operation": op.name,
                "done": op.done,
            });
            operations.push(o);
        }

        let r = match serde_json::to_string(&operations) {
            Ok(r) => r,
            Err(e) => {
                log(
                    Level::Error,
                    "list",
                    &format!("failed to serialize operations: {:?}", e),
                );
                return Response::builder()
                    .status(500)
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
        };
        Response::builder()
            .status(200)
            .body(axum::body::Body::new(r))
            .unwrap()
    }

    async fn to_wasi_response(resp: Response) -> OutgoingResponse {
        let fields = Fields::new();
        if !resp.headers().is_empty() {
            for (k, v) in resp.headers() {
                let value = [v.to_str().unwrap().to_string().into_bytes()];
                fields.set(&k.as_str().to_string(), &value).unwrap();
            }
        }
        let status = resp.status().as_u16();

        let body = resp.into_body();
        let b = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let b2 = b.to_vec();

        let response = OutgoingResponse::new(fields);
        response.set_status_code(status).unwrap();
        let response_body = response.body().unwrap();
        // Apparently wasmtime is really sensitive to holding resources for too long, so this
        // ensures that the writer is fully dropped by the time we want to finish the body stream.
        {
            let mut writer = response_body.write().unwrap();
            let mut w = OutputStreamWriter::from(&mut writer);
            w.write_all(&b2).expect("failed to write");
            w.flush().expect("failed to flush");
        }

        OutgoingBody::finish(response_body, None).expect("failed to finish response body");
        response
    }
}

impl HttpGuest for Handler {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let mut router = Handler::router();
        let path = request.path_with_query().unwrap();
        log(Level::Info, "http", &format!("path: {}", path));
        let method = request.method().to_string();

        let body = request.read_body().unwrap_or_else(|e| {
            log(
                Level::Error,
                "http",
                &format!("failed to read body: {:?}", e),
            );
            Vec::new()
        });
        let bytes = axum::body::Body::from(body);
        let req = Request::builder()
            .method(method.as_str())
            .uri(path)
            .body(bytes)
            .unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async { router.call(req).await.unwrap() });
        let response = rt.block_on(async { Handler::to_wasi_response(result).await });
        ResponseOutparam::set(response_out, Ok(response));
    }
}
impl IncomingRequest {
    /// This is a convenience function that writes out the body of a IncomingRequest (from wasi:http)
    /// into anything that supports [`std::io::Write`]
    fn read_body(self) -> anyhow::Result<Vec<u8>> {
        // Read the body
        let incoming_req_body = self
            .consume()
            .map_err(|()| anyhow!("failed to consume incoming request body"))?;
        let incoming_req_body_stream = incoming_req_body
            .stream()
            .map_err(|()| anyhow!("failed to build stream for incoming request body"))?;
        let mut buf = Vec::<u8>::with_capacity(MAX_READ_BYTES as usize);
        loop {
            match incoming_req_body_stream.read(MAX_READ_BYTES as u64) {
                Ok(bytes) => buf.extend(bytes),
                Err(StreamError::Closed) => break,
                Err(e) => bail!("failed to read bytes: {e}"),
            }
        }
        buf.shrink_to_fit();
        Ok(buf)
    }
}

// NOTE: since wit-bindgen creates these types in our namespace,
// we can hang custom implementations off of them
impl ToString for Method {
    fn to_string(&self) -> String {
        match self {
            Method::Get => "GET".into(),
            Method::Post => "POST".into(),
            Method::Patch => "PATCH".into(),
            Method::Put => "PUT".into(),
            Method::Delete => "DELETE".into(),
            Method::Options => "OPTIONS".into(),
            Method::Head => "HEAD".into(),
            Method::Connect => "CONNECT".into(),
            Method::Trace => "TRACE".into(),
            Method::Other(m) => m.into(),
        }
    }
}

export!(Handler);
