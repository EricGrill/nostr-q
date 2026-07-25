# Contributing

Thanks for helping improve Nostr-Q.

## Development Setup

```sh
git clone https://github.com/EricGrill/nostr-q.git
cd nostr-q

cargo check --workspace
cargo test --workspace
```

Optional local relay:

```sh
mkdir -p .local/nostr-rs-relay
docker run --rm -it \
  --name nostr-q-relay \
  -p 7000:8080 \
  --mount "src=$(pwd)/.local/nostr-rs-relay,target=/usr/src/app/db,type=bind" \
  scsibug/nostr-rs-relay:latest
```

## Pull Requests

Before opening a PR, run:

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Include the output summary in the PR description. If a check is not relevant or
cannot run on your machine, say why.

## Coverage

CI measures line coverage with [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov)
and fails the build if it drops below a floor. To reproduce locally:

```sh
cargo install cargo-llvm-cov          # once
cargo llvm-cov --workspace --summary-only   # per-file + total summary
cargo llvm-cov --workspace --html --open    # browsable line-by-line report
```

New behavior should come with tests; coverage is a backstop, not the goal.

## Project Expectations

- Keep changes small and reviewable.
- Add tests for behavior changes.
- Prefer clear error messages over silent fallback behavior.
- Do not commit private keys, SQLite databases, relay state, or local planning
  files.
- Keep CLI examples on the public command name: `nostr-q`.

## Local Files

The repo ignores local agent/runtime/planning files such as `.codex/`, `.omx/`,
`.superpowers/`, `docs/superpowers/`, and `nostr-q.srs.md`. Keep those local.
