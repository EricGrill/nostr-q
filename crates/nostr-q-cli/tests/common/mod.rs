//! Shared helpers for black-box protocol conformance tests (CHA-2355).
//!
//! Every test here drives the real, compiled `nostr-q` binary as a
//! subprocess against a real (embedded, in-memory) Nostr relay speaking the
//! actual NIP-01 websocket wire protocol — no `MockTransport`, no calling
//! into `Store`/`NostrQ` directly. This is deliberately black-box: it
//! verifies the *observable* protocol behavior (publish/claim/ack/retry/
//! DLQ/TTL/RPC/relay-health) the way any third-party client or relay
//! implementation would be judged, rather than this crate's internals.
//!
//! Isolation note: `dirs::home_dir()` resolves the OS profile directory
//! directly (a native API call on Windows), so it ignores `HOME`/
//! `USERPROFILE` env vars entirely. Instead of faking a home directory, each
//! test points the binary at isolated paths using the app's own override
//! env vars (`NOSTR_Q_CONFIG`, `NOSTR_Q_STATE`, `NOSTR_Q_PRIVATE_KEY`),
//! which are honored on every OS and never touch the real home directory.

use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// A fully isolated test environment: its own config/state paths, its own
/// generated keypair, and its own embedded relay. Dropping this does not
/// stop the relay — call `shutdown().await` explicitly once the test is
/// done with it.
pub struct Env {
    pub cwd: PathBuf,
    pub config_path: PathBuf,
    pub state_path: PathBuf,
    pub private_key_hex: String,
    pub relay: nostr_q::relay::DevRelay,
    pub relay_url: String,
}

impl Env {
    /// Spin up an embedded relay on an ephemeral loopback port and prepare
    /// isolated config/state paths plus a fresh keypair under a new tempdir.
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap().keep();
        let cwd = dir.join("cwd");
        std::fs::create_dir_all(&cwd).unwrap();

        let config_path = dir.join("config.toml");
        let state_path = dir.join("state.db");
        let private_key_hex = nostr::Keys::generate().secret_key().to_secret_hex();

        let relay = nostr_q::relay::serve_dev_relay("127.0.0.1:0".parse().unwrap())
            .await
            .expect("embedded dev relay must bind");
        let relay_url = relay.url();

        Self {
            cwd,
            config_path,
            state_path,
            private_key_hex,
            relay,
            relay_url,
        }
    }

    /// `nostr-q init` + `nostr-q relay add <embedded relay>`. Key material
    /// comes from `NOSTR_Q_PRIVATE_KEY` (set on every invocation below), so
    /// there's no `key generate` step and no key file ever touches disk.
    pub fn init_with_relay(&self) {
        self.run_ok(&["init"]);
        self.run_ok(&["relay", "add", &self.relay_url]);
    }

    fn apply_isolation(&self, cmd: &mut Command) {
        cmd.current_dir(&self.cwd)
            .env("NOSTR_Q_CONFIG", &self.config_path)
            .env("NOSTR_Q_STATE", &self.state_path)
            .env("NOSTR_Q_PRIVATE_KEY", &self.private_key_hex)
            .env_remove("NQ_CONFIG")
            .env_remove("NQ_STATE")
            .env_remove("NQ_PRIVATE_KEY");
    }

    /// Run the compiled `nostr-q` binary to completion and return the raw
    /// output (does not assert success — use `run_ok` when the command is
    /// expected to succeed).
    pub fn run(&self, args: &[&str]) -> Output {
        let exe = std::env::var("CARGO_BIN_EXE_nostr-q")
            .expect("CARGO_BIN_EXE_nostr-q must be set when running `cargo test`");
        let mut cmd = Command::new(exe);
        cmd.args(args);
        self.apply_isolation(&mut cmd);
        cmd.output().expect("failed to run nostr-q binary")
    }

    /// Run to completion, assert success, and return stdout as a string.
    pub fn run_ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "`nostr-q {}` failed: stdout={} stderr={}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// Run with `--json` prepended, assert success, and parse the last
    /// stdout line as JSON (matches every command's `--json` contract:
    /// exactly one JSON value on stdout).
    pub fn run_json(&self, args: &[&str]) -> serde_json::Value {
        let mut full = vec!["--json"];
        full.extend_from_slice(args);
        let stdout = self.run_ok(&full);
        let line = stdout.lines().next_back().unwrap_or_default();
        serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("stdout not a single JSON line ({e}): {stdout:?}"))
    }

    /// Launch a long-running command (e.g. `worker ...`) in the background.
    /// The caller is responsible for killing it once assertions are done.
    pub fn spawn_bg(&self, args: &[&str]) -> Child {
        let exe = std::env::var("CARGO_BIN_EXE_nostr-q")
            .expect("CARGO_BIN_EXE_nostr-q must be set when running `cargo test`");
        let mut cmd = Command::new(exe);
        cmd.args(args);
        self.apply_isolation(&mut cmd);
        cmd.stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn nostr-q worker")
    }

    /// `nostr-q inspect <queue> --json`, parsed.
    pub fn inspect(&self, queue: &str) -> serde_json::Value {
        self.run_json(&["inspect", queue])
    }

    /// Poll `inspect <queue>` until `predicate` holds or `timeout` elapses.
    /// Returns the last observed stats either way; callers assert on the
    /// return value so failures show the actual last-seen state.
    pub fn poll_inspect_until(
        &self,
        queue: &str,
        timeout: Duration,
        mut predicate: impl FnMut(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let start = Instant::now();
        loop {
            let stats = self.inspect(queue);
            if predicate(&stats) || start.elapsed() > timeout {
                return stats;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

/// Best-effort kill; background worker processes are test scaffolding, not
/// something we need a graceful drain from.
pub fn kill(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}
