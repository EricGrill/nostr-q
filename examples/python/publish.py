#!/usr/bin/env python3
"""Publish a JSON message to a Nostr-Q queue via the HTTP ingress
(`nostr-q serve`), using only the Python standard library — no pip
install required.

Usage:
    NQ_INGRESS_TOKEN=devsecret python3 publish.py jobs.email '{"to":"a@b.c"}'
    python3 publish.py jobs.email '{"to":"a@b.c"}' --token devsecret --idem order-1
    python3 publish.py jobs.email '{"to":"a@b.c"}' --url http://localhost:8787 --delay 30 --ttl 3600
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request


def publish(base_url: str, queue: str, payload: dict, token: str | None,
            idem: str | None, delay: int | None, ttl: int | None) -> dict:
    query = []
    if delay is not None:
        query.append(f"delay={delay}")
    if ttl is not None:
        query.append(f"ttl={ttl}")
    qs = ("?" + "&".join(query)) if query else ""
    url = f"{base_url.rstrip('/')}/pub/{queue}{qs}"

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if idem:
        req.add_header("Idempotency-Key", idem)

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"publish failed: HTTP {e.code}: {detail}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("queue", help="target queue, e.g. jobs.email")
    parser.add_argument("payload", help="JSON payload body")
    parser.add_argument("--url", default=os.environ.get("NQ_INGRESS_URL", "http://localhost:8787"))
    parser.add_argument("--token", default=os.environ.get("NQ_INGRESS_TOKEN"))
    parser.add_argument("--idem", default=None, help="Idempotency-Key header")
    parser.add_argument("--delay", type=int, default=None, help="delay delivery by N seconds")
    parser.add_argument("--ttl", type=int, default=None, help="expire after N seconds")
    args = parser.parse_args()

    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as e:
        raise SystemExit(f"payload must be valid JSON: {e}")

    receipt = publish(args.url, args.queue, payload, args.token, args.idem, args.delay, args.ttl)
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
