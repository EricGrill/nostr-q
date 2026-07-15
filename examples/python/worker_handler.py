#!/usr/bin/env python3
"""A tiny stdlib HTTP handler that consumes jobs dispatched by
`nostr-q worker <queue> --http http://localhost:PORT`.

`nostr-q worker` POSTs one JSON body per job:

    {"mid": "...", "queue": "...", "trace": "...", "attempt": 0,
     "generation": 0, "idem": null, "payload": {...}}

Any 2xx response acks the message; any other status (or a connection
failure) nacks it, which schedules a retry (and eventually a
dead-letter, per the queue's max_attempts).

Usage:
    python3 worker_handler.py --port 8099
    nostr-q worker jobs.email --http http://localhost:8099
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class JobHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 (stdlib naming convention)
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            job = json.loads(raw)
        except json.JSONDecodeError:
            self._respond(400, "bad request: malformed JSON job body")
            return

        try:
            self.process(job)
        except Exception as e:  # noqa: BLE001 - any handler failure means "nack"
            self._respond(500, f"handler error: {e}")
            return

        self._respond(200, "ok")

    def process(self, job: dict) -> None:
        """Replace this with real work. Raise to nack the job."""
        print(
            f"[worker_handler] mid={job.get('mid')} queue={job.get('queue')} "
            f"attempt={job.get('attempt')} payload={job.get('payload')}"
        )

    def _respond(self, status: int, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt: str, *args) -> None:  # quieter default logging
        print(f"[worker_handler] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), JobHandler)
    print(f"[worker_handler] listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
