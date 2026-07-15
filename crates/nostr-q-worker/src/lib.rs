pub mod handlers;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nostr_q::envelope::Envelope;
use nostr_q::store_crate::MessageRecord;
use nostr_q::NostrQ;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct JobContext {
    pub mid: String,
    pub queue: String,
    pub trace_id: String,
    pub attempt: u32,
    pub idem: Option<String>,
    pub payload: serde_json::Value,
}

impl JobContext {
    pub fn from_record(rec: &MessageRecord) -> Self {
        let payload = Envelope::from_json(&rec.envelope_json)
            .map(|e| e.body)
            .unwrap_or(serde_json::Value::Null);
        Self {
            mid: rec.mid.clone(),
            queue: rec.queue.clone(),
            trace_id: rec.trace_id.clone(),
            attempt: rec.attempts,
            idem: rec.idem_key.clone(),
            payload,
        }
    }
}

#[derive(Debug)]
pub enum HandlerOutcome {
    Success,
    Failure(String),
}

#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, job: &JobContext) -> HandlerOutcome;
}

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    pub concurrency: usize,
    pub lease_seconds: u64,
    pub heartbeat_seconds: u64,
    pub settle_ms: u64,
    pub poll_ms: u64,
}

pub async fn run_worker(
    nq: Arc<NostrQ>,
    queue: String,
    handler: Arc<dyn Handler>,
    opts: WorkerOptions,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ingest = nq.spawn_ingest(&queue).await?;

    // heartbeat loop (ephemeral events; best effort)
    {
        let nq = nq.clone();
        let queue = queue.clone();
        let shutdown = shutdown.clone();
        let every = Duration::from_secs(opts.heartbeat_seconds.max(1));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = nq.heartbeat(&queue).await {
                            tracing::debug!(error = %e, "heartbeat publish failed");
                        }
                    }
                    _ = shutdown.cancelled() => break,
                }
            }
        });
    }

    let semaphore = Arc::new(Semaphore::new(opts.concurrency));
    tracing::info!(queue = %queue, concurrency = opts.concurrency, "worker started");

    while !shutdown.is_cancelled() {
        let now = chrono::Utc::now().timestamp();
        let batch = nq.store().claimable(&queue, now, opts.concurrency as u32)?;
        if batch.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(opts.poll_ms)) => {}
                _ = shutdown.cancelled() => break,
            }
            continue;
        }
        for rec in batch {
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => break, // all slots busy; re-poll after they free up
            };
            let nq = nq.clone();
            let handler = handler.clone();
            let (lease, settle) = (opts.lease_seconds, opts.settle_ms);
            tokio::spawn(async move {
                let _permit = permit;
                match nq.try_claim(&rec, lease, settle).await {
                    Ok(true) => {
                        let job = JobContext::from_record(&rec);
                        tracing::info!(mid = %job.mid, attempt = job.attempt, "running handler");
                        let outcome = match tokio::time::timeout(
                            Duration::from_secs(lease),
                            handler.handle(&job),
                        )
                        .await
                        {
                            Ok(outcome) => outcome,
                            // The lease has expired anyway — another worker may reclaim the
                            // message. Nack so the attempt is recorded and retried.
                            Err(_) => HandlerOutcome::Failure(format!(
                                "handler timed out after {lease}s (lease expired)"
                            )),
                        };
                        let settled = match outcome {
                            HandlerOutcome::Success => nq.ack(&rec.mid).await,
                            HandlerOutcome::Failure(reason) => {
                                tracing::warn!(mid = %rec.mid, reason = %reason, "handler failed");
                                nq.nack(&rec.mid, &reason).await.map(|_| ())
                            }
                        };
                        if let Err(e) = settled {
                            tracing::error!(mid = %rec.mid, error = %e, "failed to settle job");
                        }
                    }
                    Ok(false) => tracing::debug!(mid = %rec.mid, "lost claim race"),
                    Err(e) => tracing::warn!(mid = %rec.mid, error = %e, "claim attempt failed"),
                }
            });
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(opts.poll_ms)) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    // graceful shutdown: wait for in-flight jobs to finish
    let _drain = semaphore.acquire_many(opts.concurrency as u32).await;
    tracing::info!("worker stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use nostr::Keys;
    use nostr_q::queue::QueueConfig;
    use nostr_q::relay::MockTransport;
    use nostr_q::store_crate::Store;
    use nostr_q::NostrQ;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    fn make_nq() -> Arc<NostrQ> {
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .upsert_queue(&QueueConfig::work_queue("jobs.email"))
            .unwrap();
        Arc::new(NostrQ::new(
            Keys::generate(),
            store,
            Arc::new(MockTransport::new()),
        ))
    }

    fn job() -> JobContext {
        JobContext {
            mid: "m1".into(),
            queue: "jobs.email".into(),
            trace_id: "t1".into(),
            attempt: 0,
            idem: Some("i1".into()),
            payload: json!({"n": 1}),
        }
    }

    #[tokio::test]
    async fn exec_handler_success_on_exit_zero() {
        let h = crate::handlers::ExecHandler {
            // proves stdin + env are wired: fails unless payload and NQ_MID arrive
            command: r#"payload=$(cat); test "$payload" = '{"n":1}' && test "$NQ_MID" = m1"#.into(),
        };
        assert!(matches!(h.handle(&job()).await, HandlerOutcome::Success));
    }

    #[tokio::test]
    async fn exec_handler_failure_captures_exit_and_stderr() {
        let h = crate::handlers::ExecHandler {
            command: "echo oops >&2; exit 3".into(),
        };
        match h.handle(&job()).await {
            HandlerOutcome::Failure(reason) => {
                assert!(
                    reason.contains('3'),
                    "reason should mention exit code: {reason}"
                );
                assert!(
                    reason.contains("oops"),
                    "reason should include stderr: {reason}"
                );
            }
            HandlerOutcome::Success => panic!("expected failure"),
        }
    }

    #[tokio::test]
    async fn worker_loop_claims_runs_and_acks() {
        let nq = make_nq();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

        let shutdown = CancellationToken::new();
        let opts = WorkerOptions {
            concurrency: 2,
            lease_seconds: 60,
            heartbeat_seconds: 3600,
            settle_ms: 10,
            poll_ms: 50,
        };
        let handle = tokio::spawn(run_worker(
            nq.clone(),
            "jobs.email".into(),
            Arc::new(crate::handlers::ExecHandler {
                command: "cat > /dev/null".into(),
            }),
            opts,
            shutdown.clone(),
        ));

        let mut acked = false;
        for _ in 0..100 {
            if nq
                .store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status
                == "acked"
            {
                acked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(acked, "worker should claim, run handler, and ack");
    }

    #[tokio::test]
    async fn worker_loop_nacks_failures() {
        let nq = make_nq();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let opts = WorkerOptions {
            concurrency: 1,
            lease_seconds: 60,
            heartbeat_seconds: 3600,
            settle_ms: 10,
            poll_ms: 50,
        };
        let handle = tokio::spawn(run_worker(
            nq.clone(),
            "jobs.email".into(),
            Arc::new(crate::handlers::ExecHandler {
                command: "exit 1".into(),
            }),
            opts,
            shutdown.clone(),
        ));
        // attempts should start climbing (retry backoff defers re-runs)
        let mut nacked = false;
        for _ in 0..100 {
            if nq
                .store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .attempts
                >= 1
            {
                nacked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(nacked);
    }

    #[tokio::test]
    async fn slow_handler_times_out_and_nacks() {
        let nq = make_nq();
        let receipt = nq.publish("jobs.email", json!({"n": 1}), None).await.unwrap();
        let shutdown = CancellationToken::new();
        let opts = WorkerOptions {
            concurrency: 1,
            lease_seconds: 1, // handler bounded to 1s
            heartbeat_seconds: 3600,
            settle_ms: 10,
            poll_ms: 50,
        };
        let handle = tokio::spawn(run_worker(
            nq.clone(),
            "jobs.email".into(),
            Arc::new(crate::handlers::ExecHandler { command: "sleep 30".into() }),
            opts,
            shutdown.clone(),
        ));
        let mut nacked = false;
        for _ in 0..100 {
            let rec = nq.store().get_message(&receipt.mid).unwrap().unwrap();
            if rec.attempts >= 1 {
                nacked = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(nacked, "timed-out handler must be nacked");
    }
}
