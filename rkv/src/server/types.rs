use crate::Key;

/// Parse URL key segment: try i64 first, fall back to Str.
/// Same logic as the REPL's `parse_key`.
pub fn parse_key(raw: &str) -> Key {
    match raw {
        "true" => Key::Int(1),
        "false" => Key::Int(0),
        _ => raw
            .parse::<i64>()
            .map(Key::Int)
            .unwrap_or_else(|_| Key::from(raw)),
    }
}

/// Format key for JSON output in scan results.
pub fn format_key(key: &Key) -> String {
    match key {
        Key::Int(n) => n.to_string(),
        Key::Str(s) => s.clone(),
    }
}
