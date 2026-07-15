# Releasing Nostr-Q

Releases are built with `cargo-dist` and published from GitHub Actions.

## One-Time Setup

1. Create the Homebrew tap repository:

   ```text
   EricGrill/homebrew-tap
   ```

2. Create a GitHub personal access token with access to that tap.

3. Add the token to this repository as:

   ```text
   HOMEBREW_TAP_TOKEN
   ```

4. Confirm release packaging locally:

   ```sh
   cargo install cargo-dist --locked
   dist plan
   ```

Run `dist init` again whenever cargo-dist config changes. It updates
`.github/workflows/release.yml` while preserving supported settings. After any
regeneration, confirm the Homebrew tap commit identity remains
`github-actions[bot]`, not the cargo-dist template default.

## Release Checklist

1. Update versions in `Cargo.toml` if needed.
2. Update `CHANGELOG.md`.
3. Run:

   ```sh
   cargo fmt --all -- --check
   cargo check --workspace
   cargo test --workspace
   cargo clippy --workspace --all-targets -- -D warnings
   dist plan
   ```

4. Commit the release prep.
5. Push a SemVer tag:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

The Release workflow builds macOS, Linux, and Windows artifacts, uploads shell
and PowerShell installers, creates the GitHub Release, and publishes the
Homebrew formula to `EricGrill/homebrew-tap`.

## Package Names

- Cargo package: `nostr-q-cli`
- Installed binary: `nostr-q`
- Homebrew formula: `nostr-q`
- Shell installer: `nostr-q-cli-installer.sh`
- PowerShell installer: `nostr-q-cli-installer.ps1`

Do not publish an `nq` Homebrew formula. Homebrew Core already has one.
