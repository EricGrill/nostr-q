//! CHA-2355: protocol compatibility and conformance suite.
//!
//! These tests drive the compiled `nostr-q` binary as a black box against a
//! real embedded relay (real NIP-01 websocket wire protocol, not
//! `MockTransport`), so they verify the *protocol's* observable behavior —
//! the same behavior any other client or relay implementation would need to
//! match — rather than re-testing this crate's internal APIs (already
//! covered by the unit tests in `crates/nostr-q/src/lib.rs`).
//!
//! Covered: relay connectivity, publish -> claim -> ack, nack -> retry ->
//! DLQ (+ DLQ retry granting a fresh attempt budget), delayed delivery
//! (`--delay`/`--not-before`), TTL expiry (`--ttl`/`--expires`), and
//! request/reply RPC (`nostr-q call`).
//!
//! Each test spawns its own embedded relay + isolated `$HOME`, so they're
//! independent and safe to run concurrently (`cargo test --workspace`).

mod common;

use std::time::Duration;

/// The protocol's first promise: a configured relay is reachable and
/// reports itself connected via `nostr-q relay health`.
#[tokio::test(flavor = "multi_thread")]
async fn relay_health_reports_connected_to_embedded_relay() {
    let env = common::Env::new().await;
    env.init_with_relay();

    let health = env.run_json(&["relay", "health"]);
    let entries = health.as_array().expect("relay health --json is an array");
    assert_eq!(entries.len(), 1, "expected exactly one configured relay");
    assert_eq!(entries[0]["url"], env.relay_url);
    assert_eq!(
        entries[0]["connected"], true,
        "embedded relay should report connected: {health:?}"
    );

    env.relay.shutdown().await;
}

/// The core work-queue contract: publish a job, a worker claims it, the
/// handler succeeds, and the message is acked exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn publish_claim_ack_happy_path() {
    let env = common::Env::new().await;
    env.init_with_relay();
    env.run_ok(&["queue", "create", "jobs.echo", "--mode", "work_queue"]);

    let receipt = env.run_json(&["pub", "jobs.echo", r#"{"n":1}"#]);
    assert!(receipt["mid"].is_string(), "publish must return a mid");

    let worker = env.spawn_bg(&["worker", "jobs.echo", "--exec", "cat", "--lease", "5"]);
    let stats = env.poll_inspect_until("jobs.echo", Duration::from_secs(10), |s| {
        s["acked"].as_u64() == Some(1)
    });
    common::kill(worker);

    assert_eq!(
        stats["acked"], 1,
        "job should be acked exactly once: {stats:?}"
    );
    assert_eq!(stats["pending"], 0);
    assert_eq!(stats["in_flight"], 0);

    env.relay.shutdown().await;
}

/// A handler that always fails must nack the job, exhaust retries, and land
/// it in the DLQ — and a manual `dlq retry` must give it a fresh attempt
/// budget rather than immediately re-dead-lettering it.
#[tokio::test(flavor = "multi_thread")]
async fn nack_retry_then_dlq_and_dlq_retry_grants_fresh_budget() {
    let env = common::Env::new().await;
    env.init_with_relay();
    env.run_ok(&[
        "queue",
        "create",
        "jobs.fail",
        "--mode",
        "work_queue",
        "--max-attempts",
        "2",
    ]);

    let receipt = env.run_json(&["pub", "jobs.fail", r#"{"x":1}"#]);
    let mid = receipt["mid"].as_str().unwrap().to_string();

    let worker = env.spawn_bg(&["worker", "jobs.fail", "--exec", "exit 1", "--lease", "5"]);
    let stats = env.poll_inspect_until("jobs.fail", Duration::from_secs(30), |s| {
        s["dead"].as_u64() == Some(1)
    });
    common::kill(worker);
    assert_eq!(
        stats["dead"], 1,
        "message should be dead-lettered after max_attempts failures: {stats:?}"
    );

    let dlq = env.run_json(&["dlq", "list", "--queue", "jobs.fail"]);
    let rows = dlq.as_array().expect("dlq list --json is an array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["mid"], mid);
    assert!(
        rows[0]["attempts"].as_u64().unwrap_or(0) >= 2,
        "dlq row should reflect the exhausted attempts: {rows:?}"
    );

    let retry = env.run_json(&["dlq", "retry", &mid]);
    assert_eq!(retry["requeued"], true);

    let stats_after = env.inspect("jobs.fail");
    assert_eq!(
        stats_after["pending"], 1,
        "dlq retry must requeue the message as pending: {stats_after:?}"
    );
    assert_eq!(
        stats_after["dead"], 0,
        "dlq retry must clear the dead count: {stats_after:?}"
    );

    env.relay.shutdown().await;
}

/// `--delay`/`--not-before`: a message published with a future visibility
/// time must not be claimable until that time passes.
#[tokio::test(flavor = "multi_thread")]
async fn delayed_delivery_blocks_claim_until_visible() {
    let env = common::Env::new().await;
    env.init_with_relay();
    env.run_ok(&["queue", "create", "jobs.delayed", "--mode", "work_queue"]);

    env.run_json(&["pub", "jobs.delayed", r#"{"a":1}"#, "--delay", "3s"]);

    // A worker running from the very start must not be able to claim the
    // job before its delay elapses.
    let worker = env.spawn_bg(&["worker", "jobs.delayed", "--exec", "true", "--lease", "5"]);

    std::thread::sleep(Duration::from_millis(1200));
    let early = env.inspect("jobs.delayed");
    assert_eq!(
        early["acked"], 0,
        "message must not be claimable before its --delay elapses: {early:?}"
    );

    let stats = env.poll_inspect_until("jobs.delayed", Duration::from_secs(10), |s| {
        s["acked"].as_u64() == Some(1)
    });
    common::kill(worker);
    assert_eq!(
        stats["acked"], 1,
        "message must become claimable once its delay elapses: {stats:?}"
    );

    env.relay.shutdown().await;
}

/// `--ttl`/`--expires`: a message that expires before any worker claims it
/// must be marked expired, not silently acked or left pending forever.
#[tokio::test(flavor = "multi_thread")]
async fn ttl_expiry_marks_never_claimed_message_expired() {
    let env = common::Env::new().await;
    env.init_with_relay();
    env.run_ok(&["queue", "create", "jobs.ttl", "--mode", "work_queue"]);

    env.run_json(&["pub", "jobs.ttl", r#"{"a":1}"#, "--ttl", "1s"]);

    // Let the TTL pass *before* any worker looks at the queue, so this
    // exercises the sweep path rather than racing a live claim.
    std::thread::sleep(Duration::from_millis(1500));

    let worker = env.spawn_bg(&["worker", "jobs.ttl", "--exec", "true", "--lease", "5"]);
    let stats = env.poll_inspect_until("jobs.ttl", Duration::from_secs(10), |s| {
        s["expired"].as_u64() == Some(1)
    });
    common::kill(worker);

    assert_eq!(
        stats["expired"], 1,
        "expired message must be counted: {stats:?}"
    );
    assert_eq!(
        stats["acked"], 0,
        "an expired message must never be acked: {stats:?}"
    );
    assert_eq!(stats["pending"], 0);

    env.relay.shutdown().await;
}

/// Request/reply RPC: `nostr-q call` must block for and return the
/// correlated reply from whichever worker handles the request.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_call_receives_correlated_reply() {
    let env = common::Env::new().await;
    env.init_with_relay();
    env.run_ok(&["queue", "create", "jobs.rpc", "--mode", "work_queue"]);

    // `cat` echoes the request payload back on stdout; since it's valid
    // JSON, the worker treats it as the RPC reply (CHA-2348).
    let worker = env.spawn_bg(&["worker", "jobs.rpc", "--exec", "cat", "--lease", "5"]);

    let reply = env.run_json(&["call", "jobs.rpc", r#"{"ping":true}"#, "--timeout", "15"]);
    common::kill(worker);

    assert_eq!(
        reply["ping"], true,
        "call must return the handler's correlated reply body: {reply:?}"
    );

    env.relay.shutdown().await;
}
