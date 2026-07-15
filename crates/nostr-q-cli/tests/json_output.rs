//! CHA-2272 item 1: verifies the exact `--json` wire format for the
//! network-free mutating commands (init/key/relay/queue never call
//! `Ctx::connect()`, so no real relay is contacted here) against the real
//! compiled `nostr-q` binary. Each run gets an isolated `$HOME` and `cwd`
//! so it can't touch the developer's real config or state.

use std::path::Path;
use std::process::Command;

fn run(args: &[&str], cwd: &Path, home: &Path) -> serde_json::Value {
    let exe = std::env::var("CARGO_BIN_EXE_nostr-q")
        .expect("CARGO_BIN_EXE_nostr-q must be set when running `cargo test`");
    let output = Command::new(exe)
        .args(args)
        .current_dir(cwd)
        .env("HOME", home)
        .env_remove("NOSTR_Q_CONFIG")
        .env_remove("NQ_CONFIG")
        .env_remove("NOSTR_Q_STATE")
        .env_remove("NQ_STATE")
        .env_remove("NOSTR_Q_PRIVATE_KEY")
        .env_remove("NQ_PRIVATE_KEY")
        .output()
        .expect("failed to run nostr-q binary");
    assert!(
        output.status.success(),
        "`nostr-q {}` failed: stdout={} stderr={}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().next_back().unwrap_or_default();
    serde_json::from_str(line)
        .unwrap_or_else(|e| panic!("stdout not a single JSON line ({e}): {stdout:?}"))
}

#[test]
fn init_key_relay_queue_emit_expected_json_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().join("cwd");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&home).unwrap();

    let v = run(&["--json", "init"], &cwd, &home);
    assert_eq!(v["created"], true);
    assert!(v.get("config").is_some());
    assert!(v.get("state").is_some());

    let v = run(&["--json", "key", "generate"], &cwd, &home);
    assert!(v.get("public_key").is_some());
    assert!(v.get("key_file").is_some());
    assert_eq!(
        v.as_object().unwrap().len(),
        2,
        "key_generate --json must only ever contain public_key and key_file, never the secret: {v}"
    );

    let shown = run(&["--json", "key", "show"], &cwd, &home);
    assert_eq!(shown["public_key"], v["public_key"]);
    assert_eq!(shown.as_object().unwrap().len(), 1);

    let v = run(
        &["--json", "relay", "add", "wss://relay.example"],
        &cwd,
        &home,
    );
    assert_eq!(v["action"], "relay_add");
    assert_eq!(v["url"], "wss://relay.example");
    assert_eq!(v["ok"], true);

    let v = run(
        &["--json", "relay", "remove", "wss://relay.example"],
        &cwd,
        &home,
    );
    assert_eq!(v["action"], "relay_remove");
    assert_eq!(v["ok"], true);

    let v = run(
        &[
            "--json",
            "queue",
            "create",
            "jobs.email",
            "--mode",
            "work_queue",
        ],
        &cwd,
        &home,
    );
    assert_eq!(v["name"], "jobs.email");
    assert_eq!(v["mode"], "work_queue");

    // Second `init` against the same cwd must report created=false and
    // still surface the existing config/state paths.
    let v = run(&["--json", "init"], &cwd, &home);
    assert_eq!(v["created"], false);
    assert!(v.get("config").is_some());
}

#[test]
fn worker_rejects_invalid_flags_via_the_real_cli() {
    // No queue or handler needed — flag validation runs before any store
    // lookup or relay connection.
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().join("cwd");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    run(&["--json", "init"], &cwd, &home);

    let exe = std::env::var("CARGO_BIN_EXE_nostr-q").unwrap();
    let out = Command::new(&exe)
        .args([
            "worker",
            "jobs.email",
            "--exec",
            "cat",
            "--concurrency",
            "0",
        ])
        .current_dir(&cwd)
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(!out.status.success(), "worker --concurrency 0 must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--concurrency"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = Command::new(&exe)
        .args(["worker", "jobs.email", "--exec", "cat", "--lease", "0"])
        .current_dir(&cwd)
        .env("HOME", &home)
        .output()
        .unwrap();
    assert!(!out.status.success(), "worker --lease 0 must fail");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--lease"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
