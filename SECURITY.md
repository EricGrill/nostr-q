# Security Policy

Nostr-Q is pre-1.0. Please treat it as experimental for production workloads.

## Reporting Vulnerabilities

Do not report vulnerabilities in a public issue.

Use GitHub private vulnerability reporting if it is enabled for the repository.
If it is not available, contact the maintainer privately before publishing
details.

Please include:

- Affected version or commit.
- Reproduction steps.
- Expected impact.
- Any known workaround.

## Security Notes

- Nostr-Q messages are signed by Nostr keys.
- Message bodies are plaintext unless an encryption mode is implemented and
  enabled.
- Production queue workloads should use private or self-hosted relays.
- Never commit private keys, generated local state, or relay databases.
