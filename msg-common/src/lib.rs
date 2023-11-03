use std::time::SystemTime;

/// Returns the current UNIX timestamp in microseconds.
pub fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
