use crate::domain::limits::MAX_SEQ;
#[derive(Debug)]
pub(crate) struct Generation {
    pub(crate) id: i64,
    pub(crate) requested: i64,
    pub(crate) base: Option<i64>,
    pub(crate) confirmed: i64,
    pub(crate) written: Option<i64>,
    pub(crate) enqueued: i64,
    pub(crate) target: Option<i64>,
    pub(crate) replay_complete: bool,
    pub(crate) blocked: bool,
}
impl Generation {
    pub(crate) fn new(id: i64, requested: i64) -> Self {
        Self {
            id,
            requested,
            base: None,
            confirmed: 0,
            written: None,
            enqueued: requested,
            target: None,
            replay_complete: false,
            blocked: false,
        }
    }
    pub(crate) fn valid(id: i64, after: i64) -> bool {
        (1..=MAX_SEQ).contains(&id) && (0..=MAX_SEQ).contains(&after)
    }
    pub(crate) fn accept_ack(&mut self, ack: i64) -> bool {
        let Some(base) = self.base else {
            return true;
        };
        if ack < base || ack > self.written.unwrap_or(base) {
            return false;
        }
        if ack >= self.confirmed {
            self.confirmed = ack;
        }
        true
    }
    pub(crate) fn record(&mut self, seq: i64) -> bool {
        if self.written.or(self.base).and_then(next_sequence) != Some(seq) {
            return false;
        }
        self.written = Some(seq);
        true
    }
}

pub(crate) fn next_sequence(seq: i64) -> Option<i64> {
    seq.checked_add(1).filter(|n| *n <= MAX_SEQ)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contiguous_write_fences_ack() {
        let mut g = Generation::new(2, 9);
        assert!(g.accept_ack(42));
        g.base = Some(9);
        g.confirmed = 9;
        assert!(g.accept_ack(9));
        assert!(!g.accept_ack(8));
        assert!(!g.accept_ack(10));
        assert!(!g.record(11));
        assert!(g.record(10));
        assert!(g.accept_ack(10));
        assert!(g.accept_ack(9));
        assert_eq!(g.confirmed, 10);
        assert_eq!(g.written, Some(10));
    }

    #[test]
    fn safe_integer_boundary() {
        assert!(Generation::valid(MAX_SEQ, MAX_SEQ));
        assert!(!Generation::valid(MAX_SEQ + 1, 0));
        assert_eq!(next_sequence(MAX_SEQ - 1), Some(MAX_SEQ));
        assert_eq!(next_sequence(MAX_SEQ), None);
        assert_eq!(next_sequence(i64::MAX), None);
    }
}

/// Replay chunks may be partial but must begin at the exact requested sequence.
pub(crate) fn contiguous_chunk(
    sequences: impl IntoIterator<Item = i64>,
    start: i64,
    end: i64,
) -> bool {
    if !(1..=MAX_SEQ).contains(&start) || end < start || end > MAX_SEQ {
        return false;
    }
    let mut sequences = sequences.into_iter();
    let Some(mut last) = sequences.next() else {
        return false;
    };
    if last != start {
        return false;
    }
    for seq in sequences {
        if next_sequence(last) != Some(seq) || seq > end {
            return false;
        }
        last = seq;
    }
    true
}
#[cfg(test)]
mod chunk_tests {
    use super::*;
    #[test]
    fn gaps_empty_and_past_target_are_rejected() {
        assert!(!contiguous_chunk([], 101, 200));
        assert!(!contiguous_chunk([102], 101, 200));
        assert!(!contiguous_chunk([101, 103], 101, 200));
        assert!(!contiguous_chunk([101, 102], 101, 101));
        assert!(contiguous_chunk([101, 102], 101, 200));
        assert!(contiguous_chunk([MAX_SEQ], MAX_SEQ, MAX_SEQ));
    }
}
