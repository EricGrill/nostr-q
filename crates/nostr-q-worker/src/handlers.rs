use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::{Handler, HandlerOutcome, JobContext};

/// Default request timeout for `HttpHandler::new` — protects direct SDK
/// users of `HttpHandler` from a hung endpoint blocking a job forever, even
/// outside `run_worker`'s lease-timeout safety net.
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs `sh -c <command>` with the payload JSON on stdin and job metadata
/// in NQ_* environment variables. Exit 0 => ack, anything else => nack.
pub struct ExecHandler {
    pub command: String,
}

#[async_trait]
impl Handler for ExecHandler {
    async fn handle(&self, job: &JobContext) -> HandlerOutcome {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(&self.command)
            .env("NQ_MID", &job.mid)
            .env("NQ_QUEUE", &job.queue)
            .env("NQ_TRACE", &job.trace_id)
            .env("NQ_ATTEMPT", job.attempt.to_string())
            .env("NQ_GENERATION", job.generation.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(idem) = &job.idem {
            cmd.env("NQ_IDEM", idem);
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => return HandlerOutcome::Failure(format!("spawn failed: {e}")),
        };
        let mut stdin_error = None;
        if let Some(mut stdin) = child.stdin.take() {
            let payload = job.payload.to_string();
            if let Err(e) = stdin.write_all(payload.as_bytes()).await {
                if e.kind() != std::io::ErrorKind::BrokenPipe {
                    stdin_error = Some(e);
                }
            }
        }
        match child.wait_with_output().await {
            Ok(out) if out.status.success() && stdin_error.is_none() => HandlerOutcome::Success,
            Ok(out) if out.status.success() => {
                HandlerOutcome::Failure(format!("stdin write failed: {}", stdin_error.unwrap()))
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                let stderr = stderr.trim();
                let detail = if stderr.is_empty() {
                    stdin_error
                        .map(|e| format!("stdin write failed: {e}"))
                        .unwrap_or_default()
                } else {
                    stderr.to_string()
                };
                HandlerOutcome::Failure(format!(
                    "exit {}: {}",
                    out.status
                        .code()
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "signal".into()),
                    detail
                ))
            }
            Err(e) => HandlerOutcome::Failure(format!("wait failed: {e}")),
        }
    }
}

/// POSTs job JSON to an HTTP endpoint. 2xx response => ack, else nack.
pub struct HttpHandler {
    url: String,
    client: reqwest::Client,
}

impl HttpHandler {
    /// Uses `DEFAULT_HTTP_TIMEOUT` (30s) as the request timeout.
    pub fn new(url: String) -> Self {
        Self::with_timeout(url, DEFAULT_HTTP_TIMEOUT)
    }

    /// Like `new`, but with a caller-supplied request timeout so a hung
    /// endpoint can't block a job past this bound, even when `HttpHandler`
    /// is used directly (not via `run_worker`, which has its own lease
    /// timeout).
    pub fn with_timeout(url: String, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build reqwest client");
        Self { url, client }
    }
}

#[async_trait]
impl Handler for HttpHandler {
    async fn handle(&self, job: &JobContext) -> HandlerOutcome {
        let body = serde_json::json!({
            "mid": job.mid,
            "queue": job.queue,
            "trace": job.trace_id,
            "attempt": job.attempt,
            "generation": job.generation,
            "idem": job.idem,
            "payload": job.payload,
        });
        match self.client.post(&self.url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => HandlerOutcome::Success,
            Ok(resp) => HandlerOutcome::Failure(format!("http status {}", resp.status())),
            Err(e) => HandlerOutcome::Failure(format!("http request failed: {e}")),
        }
    }
}
