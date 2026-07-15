// Publish a JSON message to a Nostr-Q queue via the HTTP ingress
// (`nostr-q serve`), using Node's built-in `fetch` — no dependencies.
//
// Requires Node 18+ (global fetch). Run with:
//   NQ_INGRESS_TOKEN=devsecret npx tsx publish.ts jobs.email '{"to":"a@b.c"}'
// or compile with `tsc` first and run with plain `node`.

interface PublishReceipt {
  mid: string;
  trace_id: string;
  event_id: string;
}

interface PublishOpts {
  url?: string;
  token?: string;
  idem?: string;
  delaySecs?: number;
  ttlSecs?: number;
}

async function publish(
  queue: string,
  payload: unknown,
  opts: PublishOpts = {},
): Promise<PublishReceipt> {
  const base = opts.url ?? process.env.NQ_INGRESS_URL ?? "http://localhost:8787";
  const token = opts.token ?? process.env.NQ_INGRESS_TOKEN;

  const query = new URLSearchParams();
  if (opts.delaySecs !== undefined) query.set("delay", String(opts.delaySecs));
  if (opts.ttlSecs !== undefined) query.set("ttl", String(opts.ttlSecs));
  const qs = query.toString();
  const url = `${base.replace(/\/$/, "")}/pub/${queue}${qs ? `?${qs}` : ""}`;

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (opts.idem) headers["Idempotency-Key"] = opts.idem;

  const resp = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`publish failed: HTTP ${resp.status}: ${detail}`);
  }
  return (await resp.json()) as PublishReceipt;
}

async function main() {
  const [queue, payloadJson] = process.argv.slice(2);
  if (!queue || !payloadJson) {
    console.error('usage: publish.ts <queue> <json-payload>');
    process.exit(1);
  }
  const payload = JSON.parse(payloadJson);
  const receipt = await publish(queue, payload, { idem: `ts-example-${Date.now()}` });
  console.log(JSON.stringify(receipt, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
