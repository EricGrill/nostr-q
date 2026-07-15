# Nostr-Q polyglot examples

These examples talk to Nostr-Q entirely over HTTP through `nostr-q serve`
(the HTTP publish ingress, CHA-2344) — no language needs to link the Rust
SDK or speak the Nostr protocol directly.

- `python/publish.py` — publish a message using only the Python standard
  library (`urllib`), no `pip install` required.
- `python/worker_handler.py` — a tiny stdlib `http.server` job consumer you
  can point `nostr-q worker <queue> --http <url>` at.
- `typescript/publish.ts` — publish using Node 18+'s built-in `fetch`.

## 1. Start a relay

Any Nostr relay works. For local testing, [`nak`](https://github.com/fiatjaf/nak)
ships an in-memory one:

```sh
nak serve --port 10547
```

## 2. Set up Nostr-Q and start the ingress

```sh
export NOSTR_Q_CONFIG=/tmp/nq-demo/config.toml
export NOSTR_Q_STATE=/tmp/nq-demo/state.db

nostr-q init
nostr-q key generate
nostr-q relay add ws://localhost:10547
nostr-q queue create jobs.email --mode work_queue

nostr-q serve --addr 127.0.0.1:8787 --token devsecret
```

`nostr-q serve` signs and publishes with the node's private key, so it's
access-controlled: `--token` (or `NQ_INGRESS_TOKEN`) is required on any
non-loopback bind, and every `/pub/*` request must carry
`Authorization: Bearer <token>` once a token is configured. See the
[HTTP Ingress section of the main README](../README.md#http-ingress) for
the full auth model.

## 3. Publish from Python or TypeScript

```sh
# Python (stdlib only)
NQ_INGRESS_TOKEN=devsecret python3 python/publish.py jobs.email '{"to":"a@b.c"}'

# TypeScript (Node 18+, run with tsx or compile with tsc)
NQ_INGRESS_TOKEN=devsecret npx tsx typescript/publish.ts jobs.email '{"to":"a@b.c"}'
```

Both print the publish receipt:

```json
{
  "mid": "01J...",
  "trace_id": "01J...",
  "event_id": "abcd..."
}
```

## 4. Consume with a worker written in any language

Run the example job handler, then point a `nostr-q worker` at it — the
worker claims messages over Nostr and POSTs each job to your HTTP endpoint;
a 2xx response acks, anything else nacks (retry, then dead-letter):

```sh
python3 python/worker_handler.py --port 8099
nostr-q worker jobs.email --http http://localhost:8099
```

The job POST body shape (see `crates/nostr-q-worker/src/handlers.rs`):

```json
{
  "mid": "...", "queue": "jobs.email", "trace": "...",
  "attempt": 0, "generation": 0, "idem": null,
  "payload": {"to": "a@b.c"}
}
```
