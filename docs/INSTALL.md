# Install Nostr-Q

The public command is `nostr-q`.

## Recommended Paths

| Platform | Recommended install | Status |
| --- | --- | --- |
| macOS | `brew install EricGrill/tap/nostr-q` | Available after the first release and tap setup |
| Linux | Shell installer from GitHub Releases | Available after the first release |
| Windows | PowerShell installer from GitHub Releases | Available after the first release |
| Any Rust dev machine | `cargo install --git ... --package nostr-q-cli --locked` | Works from source |

## macOS With Homebrew

```sh
brew install EricGrill/tap/nostr-q
```

`brew install nq` is not used for Nostr-Q. Homebrew Core already owns `nq` for
an unrelated Unix command-line queue utility, so this project publishes
`nostr-q`.

## Linux and macOS Shell Installer

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/EricGrill/nostr-q/releases/latest/download/nostr-q-cli-installer.sh | sh
```

## Windows PowerShell Installer

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/EricGrill/nostr-q/releases/latest/download/nostr-q-cli-installer.ps1 | iex"
```

## Windows Package Managers

Winget is the mainstream Windows package-manager target once the project has a
stable release artifact and a submitted manifest:

```powershell
winget install EricGrill.NostrQ
```

That command is a target, not available until the package is accepted by the
Windows Package Manager community repository.

Scoop is a developer-friendly follow-up because a Scoop app manifest can point
directly at GitHub Release archives:

```powershell
scoop bucket add ericgrill https://github.com/EricGrill/scoop-bucket
scoop install nostr-q
```

That bucket is also a target until a bucket repository is created.

## Cargo

Install from Git:

```sh
cargo install --git https://github.com/EricGrill/nostr-q.git --package nostr-q-cli --locked
```

Install from a local checkout:

```sh
cargo install --path crates/nostr-q-cli --locked
```

Run from source without installing:

```sh
cargo run -p nostr-q-cli -- --help
```

## Optional Short Alias

Nostr-Q does not publish an `nq` package or binary alias because that name
collides with an existing Homebrew Core formula. Add a private shell alias if
you want the shortcut:

```sh
alias nq=nostr-q
```
