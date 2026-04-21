use tokio_util::bytes::Bytes;

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Bytes,
    pub exp: Expiry,
}

#[derive(Clone, Debug)]
pub enum Expiry {
    Keep,
    At(u64),
    None,
}

impl Entry {
    pub fn is_expired(&self, now: u64) -> bool {
        match self.exp {
            Expiry::At(exp) => now > exp,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(exp: Expiry) -> Entry {
        Entry {
            value: Bytes::from_static(b"val"),
            exp,
        }
    }

    #[test]
    fn expired_when_now_past_expiry() {
        let entry = make_entry(Expiry::At(100));
        assert!(entry.is_expired(101));
    }

    #[test]
    fn not_expired_when_now_equals_expiry() {
        let entry = make_entry(Expiry::At(100));
        assert!(!entry.is_expired(100));
    }

    #[test]
    fn not_expired_when_now_before_expiry() {
        let entry = make_entry(Expiry::At(100));
        assert!(!entry.is_expired(99));
    }

    #[test]
    fn no_expiry_never_expired() {
        let entry = make_entry(Expiry::None);
        assert!(!entry.is_expired(u64::MAX));
    }

    #[test]
    fn keep_expiry_never_expired() {
        let entry = make_entry(Expiry::Keep);
        assert!(!entry.is_expired(u64::MAX));
    }
}
