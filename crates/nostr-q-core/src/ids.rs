use ulid::Ulid;

/// Nostr-Q message id (mid). ULID: sortable, 26 chars.
pub fn new_mid() -> String {
    Ulid::new().to_string()
}

/// Trace/correlation id. ULID.
pub fn new_trace_id() -> String {
    Ulid::new().to_string()
}
