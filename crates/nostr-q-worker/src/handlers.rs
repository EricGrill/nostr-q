use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::{Handler, HandlerOutcome, JobContext};

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
        if let Some(mut stdin) = child.stdin.take() {
            let payload = job.payload.to_string();
            if let Err(e) = stdin.write_all(payload.as_bytes()).await {
                return HandlerOutcome::Failure(format!("stdin write failed: {e}"));
            }
        }
        match child.wait_with_output().await {
            Ok(out) if out.status.success() => HandlerOutcome::Success,
            Ok(out) => HandlerOutcome::Failure(format!(
                "exit {}: {}",
                out.status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".into()),
                String::from_utf8_lossy(&out.stderr).trim()
            )),
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
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
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
