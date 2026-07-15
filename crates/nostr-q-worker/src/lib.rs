pub mod handlers;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nostr_q::envelope::Envelope;
use nostr_q::store::MessageRecord;
use nostr_q::{NackOutcome, NostrQ};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct JobContext {
    pub mid: String,
    pub queue: String,
    pub trace_id: String,
    /// Attempts since the last DLQ retry granted a fresh budget
    /// (`rec.attempts - rec.attempt_floor`). This is what handlers usually
    /// want: it resets to a small number after an operator requeues a
    /// dead-lettered message, instead of climbing forever.
    pub attempt: u32,
    /// The raw global retry generation (`rec.attempts`), monotonic across
    /// DLQ retries. Useful for handlers that want the full history rather
    /// than the floor-relative count.
    pub generation: u32,
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
            attempt: rec.attempts.saturating_sub(rec.attempt_floor),
            generation: rec.attempts,
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

    // The local row for a claimed-but-not-yet-settled message stays `pending`
    // until try_claim's settle sleep elapses, so consecutive polls (every
    // poll_ms) can re-fetch the same claimable row and spawn a second task
    // for it. With pubkey-based claim identity, both tasks then "win" the
    // same claim and the handler would run twice. Guard against spawning a
    // second task for a mid that already has one in flight.
    let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    struct InFlightGuard {
        set: Arc<Mutex<HashSet<String>>>,
        mid: String,
    }
    impl Drop for InFlightGuard {
        fn drop(&mut self) {
            self.set.lock().unwrap().remove(&self.mid);
        }
    }

    while !shutdown.is_cancelled() {
        // Enforce TTL for every message on the queue once per poll cycle,
        // not just ones that happen to get claimed — `try_claim`'s own TTL
        // check only fires for rows a worker actually surveys, so a message
        // that never gets claimed (e.g. the queue sits idle, or every
        // worker is busy) would otherwise never leave `pending` once its
        // expiry passes.
        if let Err(e) = nq.sweep_expired(&queue).await {
            tracing::warn!(queue = %queue, error = %e, "sweep_expired failed");
        }

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
            if !in_flight.lock().unwrap().insert(rec.mid.clone()) {
                // already being processed by an in-flight task from a prior
                // poll; the local row hasn't settled yet — skip it.
                continue;
            }
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // all slots busy; re-poll after they free up
                    in_flight.lock().unwrap().remove(&rec.mid);
                    break;
                }
            };
            let nq = nq.clone();
            let handler = handler.clone();
            let (lease, settle) = (opts.lease_seconds, opts.settle_ms);
            let guard = InFlightGuard {
                set: in_flight.clone(),
                mid: rec.mid.clone(),
            };
            tokio::spawn(async move {
                let _permit = permit;
                let _guard = guard;
                match nq.try_claim(&rec, lease, settle).await {
                    Ok(true) => {
                        // try_claim heals the local `attempts` counter (and,
                        // via a DLQ retry, `attempt_floor`) as part of
                        // claiming, so `rec` — fetched from the pre-claim
                        // `claimable` batch — can be stale. Re-fetch the
                        // current row so the handler sees an accurate
                        // attempt/generation, not a lagging one.
                        let current = match nq.store().get_message(&rec.mid) {
                            Ok(Some(current)) => current,
                            Ok(None) => rec.clone(),
                            Err(e) => {
                                tracing::warn!(
                                    mid = %rec.mid,
                                    error = %e,
                                    "failed to re-fetch record after claim; using pre-claim record"
                                );
                                rec.clone()
                            }
                        };
                        let job = JobContext::from_record(&current);
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
                                nq.nack(&rec.mid, &reason)
                                    .await
                                    .map(|outcome| match outcome {
                                        NackOutcome::Retry {
                                            attempt,
                                            visible_at,
                                        } => {
                                            tracing::info!(
                                                mid = %rec.mid,
                                                attempt,
                                                visible_at,
                                                "retry scheduled"
                                            );
                                        }
                                        NackOutcome::DeadLettered => {
                                            tracing::warn!(mid = %rec.mid, "dead-lettered");
                                        }
                                    })
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
    use nostr_q::store::Store;
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
            generation: 0,
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
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
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
            Arc::new(crate::handlers::ExecHandler {
                command: "sleep 30".into(),
            }),
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

    #[tokio::test]
    async fn http_handler_acks_on_2xx_and_nacks_on_500() {
        use wiremock::matchers::{body_partial_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/jobs/ok"))
            .and(body_partial_json(
                serde_json::json!({"mid": "m1", "payload": {"n": 1}}),
            ))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/jobs/fail"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let ok = crate::handlers::HttpHandler::new(format!("{}/jobs/ok", server.uri()));
        assert!(matches!(ok.handle(&job()).await, HandlerOutcome::Success));

        let fail = crate::handlers::HttpHandler::new(format!("{}/jobs/fail", server.uri()));
        match fail.handle(&job()).await {
            HandlerOutcome::Failure(reason) => assert!(reason.contains("500"), "{reason}"),
            HandlerOutcome::Success => panic!("expected failure"),
        }
    }

    #[tokio::test]
    async fn http_handler_times_out_on_hung_endpoint() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/jobs/slow"))
            .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(5)))
            .mount(&server)
            .await;

        // Without a request timeout, this would hang for the full 5s delay
        // (or forever, for a truly hung endpoint) with no protection outside
        // run_worker's lease timeout. with_timeout must bound it.
        let h = crate::handlers::HttpHandler::with_timeout(
            format!("{}/jobs/slow", server.uri()),
            std::time::Duration::from_millis(200),
        );
        let start = std::time::Instant::now();
        let outcome = h.handle(&job()).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "handler must respect its configured timeout, took {elapsed:?}"
        );
        match outcome {
            HandlerOutcome::Failure(reason) => assert!(!reason.is_empty()),
            HandlerOutcome::Success => panic!("expected the request to time out"),
        }
    }

    struct CapturingHandler {
        captured: Arc<Mutex<Option<(u32, u32)>>>, // (attempt, generation)
    }

    #[async_trait::async_trait]
    impl Handler for CapturingHandler {
        async fn handle(&self, job: &JobContext) -> HandlerOutcome {
            *self.captured.lock().unwrap() = Some((job.attempt, job.generation));
            HandlerOutcome::Success
        }
    }

    #[tokio::test]
    async fn handler_sees_floor_relative_attempt_after_dlq_retry() {
        let nq = make_nq();
        // tighten the budget so two nacks dead-letter the message
        let mut q = nq.store().get_queue("jobs.email").unwrap().unwrap();
        q.max_attempts = 2;
        nq.store().upsert_queue(&q).unwrap();

        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();

        // drive to dead-lettered: two failures push the raw generation to 2
        nq.nack(&receipt.mid, "f1").await.unwrap();
        nq.nack(&receipt.mid, "f2").await.unwrap();
        let dead = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert_eq!(dead.status, "dead");
        assert_eq!(dead.attempts, 2, "raw generation should be large pre-retry");

        // an operator retries: this grants a fresh budget (attempt_floor=2)
        // while leaving the raw `attempts` generation untouched
        nq.store().dlq_retry(&receipt.mid).unwrap();
        let retried = nq.store().get_message(&receipt.mid).unwrap().unwrap();
        assert_eq!(retried.attempts, 2);
        assert_eq!(retried.attempt_floor, 2);

        let captured: Arc<Mutex<Option<(u32, u32)>>> = Arc::new(Mutex::new(None));
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
            Arc::new(CapturingHandler {
                captured: captured.clone(),
            }),
            opts,
            shutdown.clone(),
        ));

        let mut seen = None;
        for _ in 0..100 {
            if let Some(c) = *captured.lock().unwrap() {
                seen = Some(c);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        handle.await.unwrap().unwrap();

        let (attempt, generation) = seen.expect("handler must have run and captured its job");
        assert_eq!(
            attempt, 0,
            "floor-relative attempt must be small right after a DLQ retry, not the raw generation"
        );
        assert_eq!(
            generation, 2,
            "raw generation must still reflect full history for handlers that want it"
        );
    }

    #[tokio::test]
    async fn worker_sweeps_expired_messages_without_running_handler() {
        use nostr_q::PublishOptions;

        let nq = make_nq();
        let past = chrono::Utc::now().timestamp() - 10;
        let receipt = nq
            .publish_opts(
                "jobs.email",
                json!({"n": 1}),
                None,
                PublishOptions {
                    not_before: None,
                    expires_at: Some(past),
                },
            )
            .await
            .unwrap();

        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
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
            Arc::new(CountingHandler {
                calls: calls.clone(),
            }),
            opts,
            shutdown.clone(),
        ));

        let mut expired = false;
        for _ in 0..100 {
            if nq
                .store()
                .get_message(&receipt.mid)
                .unwrap()
                .unwrap()
                .status
                == "expired"
            {
                expired = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // give any (incorrect) handler dispatch a moment to fire before shutdown
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(
            expired,
            "worker's per-poll sweep must expire a never-claimed message once its TTL passes"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "an expired message must never reach the handler"
        );
    }

    struct CountingHandler {
        calls: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl Handler for CountingHandler {
        async fn handle(&self, _job: &JobContext) -> HandlerOutcome {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            HandlerOutcome::Success
        }
    }

    #[tokio::test]
    async fn concurrency_does_not_double_execute_a_job() {
        let nq = make_nq();
        let receipt = nq
            .publish("jobs.email", json!({"n": 1}), None)
            .await
            .unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let shutdown = CancellationToken::new();
        let opts = WorkerOptions {
            concurrency: 4,
            lease_seconds: 60,
            heartbeat_seconds: 3600,
            settle_ms: 200, // settle longer than poll: the exact window that double-executed
            poll_ms: 50,
        };
        let handle = tokio::spawn(run_worker(
            nq.clone(),
            "jobs.email".into(),
            Arc::new(CountingHandler {
                calls: calls.clone(),
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
        // allow any duplicate task to fire before shutdown
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        shutdown.cancel();
        handle.await.unwrap().unwrap();
        assert!(acked);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a job must execute exactly once per process even at concurrency > 1"
        );
    }
}
