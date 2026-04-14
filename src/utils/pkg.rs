pub const fn parse_version(s: &str) -> [u8; 3] {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut parts = [0u8; 3];
    let mut part = 0;
    let mut val: u8 = 0;

    while i < bytes.len() {
        if bytes[i] == b'.' {
            parts[part] = val;
            val = 0;
            part += 1;
        } else {
            val = val * 10 + (bytes[i] - b'0');
        }
        i += 1;
    }
    parts[part] = val;
    parts
}
