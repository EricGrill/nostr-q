Nostr-Q Software Requirements Specification Status: Draft v0.1 Product name: Nostr-Q CLI command: nq Primary implementation language: Rust 
Primary delivery model: SDK core + CLI + future TUI 1. Purpose Nostr-Q is a generic Nostr-backed message queue toolkit intended to provide 
message queue, pub/sub, work queue, and developer-operations workflows over Nostr relays. It should make Nostr usable in places where a 
developer would otherwise reach for RabbitMQ, Redis Streams, NATS, SQS, or a lightweight internal event bus. The project will start with the 
needs of the current system, but must be designed as an open-source, general-purpose toolkit for other applications and teams. 2. Product 
Vision Nostr-Q provides: a reusable Rust SDK for publishing, subscribing, claiming, acknowledging, retrying, tracing, and inspecting queue 
messages over Nostr; a CLI named nq for daily development, debugging, scripting, and production operations; a future TUI for live queue 
inspection, relay health, worker status, dead-letter queues, and trace exploration; a protocol profile for representing queue semantics using 
Nostr events, tags, signatures, relays, and optional encryption. Nostr-Q should feel like a practical message queue toolkit, not like a 
social Nostr client. 3. Goals Provide hybrid messaging: work queues with competing consumers; pub/sub topics with fanout delivery. Support 
configurable delivery semantics per queue. Default to at-least-once delivery for work queues. Default to best-effort fanout for pub/sub 
topics. Use SQLite local state by default for queue metadata, ack state, retries, traces, and development ergonomics. Support both 
private/self-hosted and public Nostr relays, while recommending private relays for production queue workloads. Use plaintext signed messages 
by default, with configurable encryption. Expose a generic SDK so the CLI/TUI are clients of the same core primitives. Provide a clear path 
to open-source adoption, local development, examples, and integrations. 4. Non-Goals for Initial MVP True exactly-once delivery. Replacing 
all RabbitMQ exchange types in v1. Providing a full web management UI in v1. Depending on public relays for reliable production work queues. 
Hiding Nostr concepts entirely; operators should be able to inspect relays, events, keys, tags, and signatures. Solving global ordering 
across relays. Guaranteed relay-side persistence unless the selected relay supports it and the deployment is configured accordingly. 5. 
Target Users 5.1 Application Developers Developers who want simple message queue primitives without operating RabbitMQ or another centralized 
broker. 5.2 Distributed System Builders Teams using Nostr as a decentralized/federated event fabric and needing queue-like semantics. 5.3 
Worker/Automation Authors Developers building background workers, AI agent task runners, notification processors, workflow steps, and 
event-driven automation. 5.4 Operators People who need to inspect queue health, relay behavior, consumer status, retries, dead-lettered 
messages, and traces from a terminal. 6. System Context Nostr-Q sits between applications/workers and one or more Nostr relays. Producer apps 
Nostr-Q SDK / nq Nostr relays Worker apps <----> Nostr-Q protocol <----> private/public/local relays Operators nq CLI / future TUI optional 
local SQLite state Nostr relays provide message transport and event distribution. Nostr-Q adds queue semantics, local state, worker 
coordination, acks, retries, DLQs, traces, and operational tooling. 7. Messaging Modes 7.1 Work Queue Mode A work queue is used when each 
message should be processed by one consumer from a pool of competing workers. Default delivery guarantee: at-least-once. Required semantics: 
publish job message; discover pending jobs; claim a job; prevent or reduce duplicate processing through claim state and lease timeouts; ack 
successful completion; nack failed completion; retry according to queue policy; dead-letter after max attempts or explicit DLQ operation; 
support idempotency keys because duplicates are possible. 7.2 Pub/Sub Topic Mode A pub/sub topic is used when every interested subscriber may 
receive the same event. Default delivery guarantee: best-effort fanout. Required semantics: publish event; subscribe by topic, tags, 
producer, kind, time range, and relay; optionally persist seen offsets locally; optionally replay from relay history when available; no 
default ack required. 7.3 Hybrid Mode Nostr-Q must support both work queues and pub/sub topics in the same deployment. Queue configuration 
determines behavior. Example: queues:
  jobs.email:
    mode: work_queue delivery: at_least_once
  events.user.created:
    mode: pubsub delivery: best_effort 8. Recommended Initial Queues / Topics Until the current system's exact topic list is finalized, the 
SRS recommends validating Nostr-Q with these generic system queues: 8.1 Work Queues agent.job.requested - background AI/agent work requests. 
agent.tool.call.requested - tool-execution jobs that may be handled by separate workers. notification.send - outbound notifications. 
media.process - asynchronous media or attachment processing. workflow.step.run - workflow/orchestration step execution. 8.2 Pub/Sub Topics 
user.message.received - incoming user/application message event. agent.job.completed - job completion event. agent.job.failed - job failure 
event. system.health.changed - health/status event. audit.event.recorded - append-only audit event. These names are placeholders and should 
be revised during implementation planning. 9. Delivery Semantics Delivery behavior must be configurable per queue/topic. Supported delivery 
policies: best_effort - publish and subscribe with no ack requirement. at_most_once - consumer handles a message without retrying after 
delivery. at_least_once - message remains pending or retryable until acked or dead-lettered. Future candidate: effectively_once - 
at-least-once delivery with enforced idempotency keys and dedupe state. Nostr-Q must explicitly document that true exactly-once delivery is 
not guaranteed. 10. Queue State Model Nostr-Q must maintain queue state locally by default using SQLite. 10.1 Local State Responsibilities 
The local state store should track: configured relays; configured queues/topics; seen events; published messages; claims; leases; acks; 
nacks; retry attempts; DLQ entries; consumer heartbeats; trace/correlation index; relay health observations; optional idempotency keys. 10.2 
SQLite Default SQLite is the v1 default because it is: easy for local development; portable; suitable for CLI/TUI inspection; enough for a 
single node or developer workstation; simple to bundle in a Rust application. 10.3 Future State Adapters Future adapters may include: 
Postgres; MySQL/MariaDB; Redis; remote Nostr-Q state service; embedded append-only log. 11. Nostr Protocol Profile Nostr-Q should define a 
protocol profile using custom event kinds and tags. This profile may become a published mini-spec. 11.1 Event Categories Nostr-Q should 
represent at least these lifecycle events: message published; message claimed; message acked; message nacked; message retried; message 
dead-lettered; consumer heartbeat; queue declaration/config snapshot; trace event; relay health observation, optional/local only. 11.2 Event 
Kinds Exact custom kind numbers are TBD and should be selected after reviewing current Nostr conventions and avoiding collisions where 
possible. Possible approach: kind: 30078 or another application-specific parameterized replaceable kind for queue metadata/config kind: 
custom regular event kind for queue message lifecycle events Open question: decide final Nostr event kinds after a focused NIP/kind review. 
11.3 Required Tags Every Nostr-Q message event should include tags such as: q queue/topic name t topic name / routing tag mode work_queue | 
pubsub mid Nostr-Q message id/job id trace trace/correlation id parent parent message id, optional idem idempotency key, optional attempt 
attempt number exp expiration timestamp, optional producer producer id/name, optional schema payload schema name/version, optional enc 
encryption mode, optional Nostr event id, pubkey, created_at, kind, tags, content, and sig remain authoritative at the Nostr layer. 11.4 
Payload Format Default payload content should be JSON. The SDK must allow raw bytes or encoded payloads later, but v1 CLI should optimize for 
JSON and text payloads. Recommended envelope: {
  "version": "0.1", "content_type": "application/json", "body": {}, "headers": {}, "created_at": "RFC3339 timestamp" } Open question: whether 
the Nostr event content should be the raw payload or a standardized envelope. The SRS recommends using a standardized envelope for better 
interoperability. 12. Security Model 12.1 Identity Nostr public/private key pairs identify producers, consumers, operators, and optionally 
queues. Nostr-Q must support: loading keys from config; loading keys from environment variables; generating dev keys; showing public keys 
safely; never printing private keys unless explicitly requested through a dedicated unsafe export command. 12.2 Plaintext Default v1 default: 
signed plaintext messages. Rationale: easier local development; easier inspection; easier debugging; lower initial complexity. 12.3 
Configurable Encryption Encryption must be configurable per queue/topic. Supported or planned modes: none - plaintext signed events; nip04 - 
legacy direct-message style encryption where appropriate; nip44 - preferred encrypted payload mode where supported; future 
group/shared-secret encryption for multi-consumer queues. Open question: encryption design for competing consumer queues, where multiple 
consumers need to decrypt the same job. 12.4 Authorization The SDK and CLI should support allow/deny rules by pubkey. Examples: producers 
allowed for a queue; consumers allowed for a queue; operators allowed to retry or purge DLQ; trusted relay list. 13. Relay Strategy Nostr-Q 
must support both private/self-hosted relays and public relays. 13.1 Production Recommendation Private/self-hosted relays should be the 
default recommendation for production queue workloads. Reasons: predictable retention; lower spam/noise; configurable access control; 
controlled rate limits; better operational ownership; easier debugging. 13.2 Public Relay Support Public relays may be used for: experiments; 
public event streams; redundant broadcast; low-criticality pub/sub topics. Public relays should not be recommended as the only transport for 
durable production work queues. 13.3 Local Development Relay Nostr-Q should provide a local development flow: nq dev Potential behavior: 
start or connect to a local relay; initialize SQLite state; generate dev keys; create example queues; optionally start a sample worker and 
sample producer. 14. CLI Requirements The CLI command is nq. 14.1 CLI Design Principles Scriptable by default. Human-readable output by 
default. --json available for automation. Explicit relay and config selection. Safe handling of keys/secrets. Good error messages for relay 
failures, invalid events, duplicate messages, and malformed payloads. No hidden dependency on a long-running daemon for basic commands. 14.2 
Proposed Command Groups nq init nq config get|set|list nq key generate|import|show nq relay add|remove|list|health nq queue 
create|list|show|delete nq pub <queue-or-topic> [payload] nq sub <topic> nq worker <queue> nq ack <message-id> nq nack <message-id> nq retry 
<message-id> nq dlq list|show|retry|purge nq inspect [queue-or-topic] nq trace <trace-id> nq dev nq tui 14.3 MVP CLI Commands MVP should 
include: nq init nq key generate nq relay add nq relay list nq relay health nq queue create nq queue list nq pub nq sub nq worker --exec nq 
worker --http nq inspect nq trace nq dlq list nq dlq retry 14.4 Example Commands Create a work queue: nq queue create jobs.email --mode 
work_queue --delivery at_least_once Create a pub/sub topic: nq queue create events.user.created --mode pubsub --delivery best_effort Publish 
JSON: nq pub jobs.email '{"to":"user@example.com","template":"welcome"}' Subscribe to a pub/sub topic: nq sub events.user.created Run a 
worker via shell command: nq worker jobs.email --exec './send-email.sh' Run a worker via HTTP handler: nq worker jobs.email --http 
http://localhost:4000/jobs/email Inspect a queue: nq inspect jobs.email Trace a job: nq trace 01JABCDEF123456789 Check relay health: nq relay 
health 15. Worker Runtime Requirements Nostr-Q must be able to run workers directly. 15.1 v1 Handler Types Required v1 handler types: --exec 
- invoke a command with message data provided via stdin and/or environment variables. --http - POST message data to an HTTP endpoint and map 
response to ack/nack. 15.2 Future Handler Types Future candidate handler types: Docker container; WebSocket; gRPC; WASM plugin; 
language-specific adapters; durable workflow engine adapter. 15.3 Worker Behavior Workers must support: configurable concurrency; lease 
timeout; heartbeat interval; graceful shutdown; max attempts; retry delay/backoff; dead-letter policy; idempotency key exposure; structured 
logs; JSON output mode. Example: nq worker jobs.email \
  --exec './send-email.sh' \ --concurrency 5 \ --lease 60s \ --max-attempts 5 \ --retry-backoff exponential 16. TUI Requirements The TUI 
should be designed now but implemented after the CLI stabilizes. 16.1 TUI Command Preferred command: nq tui nq inspect --tui may be added as 
an alias later. 16.2 TUI Screens Required design targets: Overview dashboard queues/topics; message rates; pending/in-flight/acked/failed/DLQ 
counts; relay health summary; active consumers. Queue inspector recent messages; pending messages; in-flight claims; ack/nack/retry events; 
raw Nostr event viewer. Worker/consumer monitor consumer id/pubkey; heartbeat age; current claimed jobs; success/failure counts. Relay health 
screen connection latency; publish latency; subscribe latency; retention test result; auth/rate-limit warnings. DLQ browser dead-lettered 
messages; failure reason; attempts; retry/purge actions. Trace viewer timeline by trace id; parent/child message graph; lifecycle events. 
16.3 TUI Safety Mutating TUI actions, such as purge or retry-all, must require confirmation. 17. Observability Requirements Nostr-Q must 
provide first-class observability through CLI/TUI and machine-readable output. Required observability features: queue depth estimate; pending 
count; in-flight count; ack count; nack/failure count; DLQ count; oldest pending message age; consumer heartbeat status; relay connectivity; 
relay latency; duplicate detection; trace timeline; event validation errors. Future integrations: Prometheus exporter; OpenTelemetry traces; 
JSON log streaming; web dashboard. 18. Failure Handling Nostr-Q must explicitly handle common distributed-queue failures. 18.1 Relay Failure 
Behavior: retry relay connection; mark relay degraded/down; continue with remaining relays when possible; report partial publish/subscribe 
success; expose relay divergence diagnostics. 18.2 Worker Crash Behavior: claimed messages have leases; expired leases become claimable 
again; duplicate processing is possible and must be documented; idempotency keys are recommended. 18.3 Duplicate Messages Behavior: detect by 
Nostr event id and Nostr-Q message id; optionally detect by idempotency key; expose duplicate status in inspect/trace output. 18.4 Poison 
Messages Behavior: retry until max attempts; then move to DLQ; keep failure reason and handler output metadata; allow manual retry/purge. 
18.5 Clock Skew Behavior: tolerate moderate timestamp skew; warn when events appear too far in future/past; avoid relying only on local 
wall-clock for correctness where possible. 19. Configuration Requirements Nostr-Q should use a config file plus command-line overrides. 
Suggested default config path: ~/.config/nostr-q/config.toml Project-local override: ./nostr-q.toml Environment override prefix: NQ_ Example 
config: [profile.default] state = "sqlite://~/.local/share/nostr-q/state.db" keys = "env:NQ_PRIVATE_KEY" [[relays]] url = 
"wss://relay.example.com" role = ["publish", "subscribe"] [queues."jobs.email"] mode = "work_queue" delivery = "at_least_once" encryption = 
"none" max_attempts = 5 lease_seconds = 60 [queues."events.user.created"] mode = "pubsub" delivery = "best_effort" encryption = "none" 20. 
SDK Requirements The Rust SDK should be the core product foundation. 20.1 Core Crates / Modules Possible crate layout: crates/nostr-q-core 
protocol, message model, queue semantics crates/nostr-q-nostr relay client abstraction and Nostr mapping crates/nostr-q-store SQLite state 
and store traits crates/nostr-q-cli nq CLI crates/nostr-q-tui future TUI crates/nostr-q-worker worker runtime helpers 20.2 Public SDK 
Capabilities The SDK must support: connecting to relays; publishing messages; subscribing to topics; claiming work; ack/nack/retry/DLQ; 
managing queue configs; loading keys/config; storing state; querying traces and queue status; validating Nostr-Q protocol events. 20.3 
Integration Goal Applications should be able to embed Nostr-Q without shelling out to nq. 21. Acceptance Criteria 21.1 MVP Acceptance 
Criteria A developer can initialize a local Nostr-Q configuration. A developer can add at least one relay. A developer can create a work 
queue and a pub/sub topic. A developer can publish a JSON message to a work queue. A worker can claim that message and execute a shell 
command handler. The worker can ack successful completion. A failed handler can cause nack/retry and eventually DLQ. A developer can 
subscribe to a pub/sub topic and see events. A developer can inspect queue status from CLI. A developer can trace a message lifecycle by 
trace id. Relay health can be checked from CLI. State is persisted in SQLite locally. The SDK exposes the same primitives used by CLI 
commands. 21.2 v1.0 Acceptance Criteria Work queues and pub/sub topics are stable and documented. --exec and --http workers are supported. 
Relay health diagnostics are useful for development and production. DLQ inspection and retry are supported. Configurable encryption is 
available, even if plaintext remains default. The protocol profile is documented well enough for third parties to implement. The project 
includes examples, tests, and a local dev flow. 22. Roadmap Phase 0: Specification and Architecture Finalize SRS. Review Nostr event kind 
choices. Define message envelope and tags. Define SQLite schema. Define CLI command contract. Phase 1: Rust SDK MVP Relay client abstraction. 
Message envelope. Queue configuration model. SQLite state store. Publish/subscribe primitives. Work claim/ack/nack/retry/DLQ primitives. 
Phase 2: CLI MVP nq init. nq relay commands. nq queue commands. nq pub. nq sub. nq worker --exec. nq inspect. nq trace. nq dlq. Phase 3: 
Worker Runtime and HTTP Adapter --http worker mode. concurrency controls. lease and heartbeat support. structured handler result mapping. 
Phase 4: TUI nq tui overview. queue inspector. relay health screen. DLQ browser. trace viewer. Phase 5: Hardening and Ecosystem encryption 
modes. Prometheus/OpenTelemetry support. additional state stores. examples and templates. public protocol profile/spec. package releases. 23. 
Open Questions Which exact Nostr event kinds should Nostr-Q use? Should the event content be a standardized envelope, raw payload, or 
configurable? Which Rust Nostr library should be used initially? What local relay should nq dev start or recommend? Should queue config be 
published to relays, stored locally only, or both? What is the correct encryption design for multi-consumer work queues? Should claims/acks 
be represented as separate Nostr events, local state only, or both? Should Nostr-Q support request/reply RPC in v1 or later? Should queue 
names be unconstrained strings, DNS-like names, or NIP-style tags? Should the CLI support RabbitMQ-like routing keys/exchanges in v1, or 
defer until routing needs are concrete? Should nq tui support mutating actions initially, or be read-only for safety? Should public relay 
support include relay allowlists and relay scoring from day one? 24. Additional Clarifying Questions for Next Revision Should this live 
inside the current repository temporarily, or should we create a new repository for Nostr-Q? Should the initial Rust workspace use the crate 
name nostr-q, nostrq, or nq? Do you want compatibility with an existing Nostr relay implementation for local dev, or should Nostr-Q 
eventually ship its own minimal dev relay? For the current system, which first queue should we implement against: agent.job.requested, 
notification.send, workflow.step.run, or something else? Should v1 include request/reply RPC semantics, or only async queue/pubsub semantics? 
Do queue messages need attachments/blob references, or JSON-only is enough for MVP? Should operator actions like DLQ purge require local 
confirmation only, or signed admin events?
