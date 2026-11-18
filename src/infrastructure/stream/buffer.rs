use super::{Commit, MAX_GAP, MAX_GAP_BYTES, integrity};
use crate::infrastructure::support::{Error, Result};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BufferMode {
    Initialization,
    Journal,
}

/// Shared verified payload journal. Both its item and byte caps are enforced.
#[derive(Default)]
pub(crate) struct CommitBuffer {
    rows: BTreeMap<i64, Commit>,
    bytes: usize,
}

impl CommitBuffer {
    pub(super) fn insert(&mut self, row: Commit, mode: BufferMode) -> Result<()> {
        let size = row.encoded()?.len() + 32;
        match self.rows.get(&row.tenant_sequence) {
            Some(old) if old.payload_sha256 != row.payload_sha256 => return Err(integrity()),
            Some(_) => return Ok(()),
            None => {}
        }
        while mode == BufferMode::Journal
            && (self.rows.len() >= MAX_GAP as usize || self.bytes + size > MAX_GAP_BYTES)
            && let Some((_, old)) = self.rows.pop_first()
        {
            self.bytes -= old.encoded()?.len() + 32;
        }
        if self.rows.len() >= MAX_GAP as usize || self.bytes + size > MAX_GAP_BYTES {
            self.rows.clear();
            self.bytes = 0;
            return Err(Error::new(409, "OUTBOX_GAP_OR_EXPIRED"));
        }
        self.bytes += size;
        self.rows.insert(row.tenant_sequence, row);
        Ok(())
    }

    pub(super) fn into_rows(self) -> VecDeque<Commit> {
        self.rows.into_values().collect()
    }

    pub(super) fn range(&self, start: i64, end: i64) -> Option<Vec<Commit>> {
        if !self.rows.contains_key(&start) {
            return None;
        }
        let rows = self
            .rows
            .range(start..=end)
            .take(64)
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>();
        Some(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::commit;
    use super::*;
    #[test]
    fn duplicate_never_grows_buffer_and_different_digest_is_fatal() {
        let mut b = CommitBuffer::default();
        let first = commit(1, "é");
        let exact = first.encoded().unwrap().len() + 32;
        b.insert(first.clone(), BufferMode::Initialization).unwrap();
        b.insert(first, BufferMode::Initialization).unwrap();
        assert_eq!(b.bytes, exact);
        assert_eq!(b.rows.len(), 1);
        assert_eq!(
            b.insert(commit(1, "changed"), BufferMode::Initialization)
                .unwrap_err()
                .code,
            "STREAM_INTEGRITY_VIOLATION"
        );
    }
    #[test]
    fn initialization_overflow_is_atomic_and_live_journal_evicts_only_its_oldest_prefix() {
        let mut b = CommitBuffer::default();
        for seq in 1..=MAX_GAP {
            b.insert(commit(seq, ""), BufferMode::Initialization)
                .unwrap();
        }
        assert!(
            b.insert(commit(MAX_GAP + 1, ""), BufferMode::Initialization)
                .is_err()
        );
        assert_eq!(b.bytes, 0);
        assert!(b.rows.is_empty());
        for seq in 1..=MAX_GAP + 1 {
            b.insert(commit(seq, ""), BufferMode::Journal).unwrap();
        }
        assert!(b.range(1, 2).is_none());
        assert_eq!(b.rows.len(), MAX_GAP as usize);
        assert_eq!(b.rows.first_key_value().map(|(seq, _)| *seq), Some(2));
    }
    #[test]
    fn bytes_count_utf8_and_metadata_and_stop_before_overflow() {
        let mut b = CommitBuffer::default();
        let row = commit(1, &"é".repeat(MAX_GAP_BYTES / 2));
        assert!(b.insert(row, BufferMode::Initialization).is_err());
        assert!(b.rows.is_empty());
        assert_eq!(b.bytes, 0);
    }
}
