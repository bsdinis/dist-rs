#[derive(Debug, Clone)]
pub struct LogEntry<Op: Clone> {
    pub term: u64,
    pub idx: usize,
    pub op: Op,
}

impl<Op: Clone> LogEntry<Op> {
    pub fn new(term: u64, idx: usize, op: Op) -> Self {
        LogEntry { term, idx, op }
    }
}

pub struct Log<Op: Clone> {
    /// Log entries
    entries: Vec<LogEntry<Op>>,

    /// Index of the last committed operation
    committed_idx: usize,
}

impl<Op> Log<Op>
where
    Op: Clone,
{
    /// Create an empty log
    pub fn new() -> Self {
        Log {
            entries: Vec::new(),
            committed_idx: 0,
        }
    }

    /// Check whether the log is past the (term, idx) pair
    pub fn is_past(&self, term: u64, idx: usize) -> bool {
        match self.entries.last() {
            None => false,
            Some(log_entry) => {
                log_entry.term > term || (log_entry.term == term && log_entry.idx > idx)
            }
        }
    }

    /// Append a single operation
    /// Returns the index of the new entry
    pub fn append(&mut self, term: u64, op: Op) -> usize {
        let entry = LogEntry::new(term, &self.last_index() + 1, op);

        let idx = entry.idx;
        self.entries.push(entry);
        idx
    }

    /// Append a batch of operations
    /// Returns a vector of indexes for the new entries
    pub fn append_batch(&mut self, term: u64, batch: Vec<Op>) -> Vec<usize> {
        self.entries.reserve(batch.len());
        let first_idx = &self.last_index() + 1;
        let len = batch.len();
        batch
            .into_iter()
            .enumerate()
            .map(|(idx, op)| LogEntry::new(term, first_idx + idx, op))
            .for_each(|entry| self.entries.push(entry));
        (first_idx..(first_idx + len)).into_iter().collect()
    }

    /// Find the point of trunctation for a log, given a sorted list of new entries
    ///
    /// The point of trunctation is the smallest index in the log such that:
    ///   1. there is an entry with the same index in the new entries
    ///   2. the terms do not match
    ///
    /// The point of trunctation is the index to the vector of entries (*not* the index of the
    /// entry)
    fn find_truncation(&mut self, entries: &Vec<LogEntry<Op>>) -> Option<usize> {
        // truncate the log
        // we walk with two iterators in order until one of them runs out (if that happens there is
        // nothing to truncate)
        //
        // if an entry in matches the index of another in the log,
        // but the terms don't match, the log is truncated, from that point forward
        // this assumes that the (input) log entries are sorted

        if let Some(smallest_idx) = entries.iter().map(|x| x.idx).min() {
            loop {
                let mut entries_iter = entries.iter();
                let mut log_iter = self
                    .entries
                    .iter()
                    .enumerate()
                    .filter(|(_, x)| x.idx >= smallest_idx);

                loop {
                    let entry_opt = entries_iter.next();
                    if entry_opt.is_none() {
                        return None;
                    }
                    let mut entry = entry_opt.unwrap();

                    let log_entry_opt = log_iter.next();
                    if log_entry_opt.is_none() {
                        return None;
                    }
                    let mut log_entry = log_entry_opt.unwrap();

                    while entry.idx != log_entry.1.idx {
                        while entry.idx < log_entry.1.idx {
                            let entry_opt = entries_iter.next();
                            if entry_opt.is_none() {
                                return None;
                            }
                            entry = entry_opt.unwrap();
                        }
                        while log_entry.1.idx < entry.idx {
                            let log_entry_opt = log_iter.next();
                            if log_entry_opt.is_none() {
                                return None;
                            }
                            log_entry = log_entry_opt.unwrap();
                        }
                    }

                    if entry.term != log_entry.1.term {
                        return Some(log_entry.0);
                    }
                }
            }
        }

        None
    }

    /// Append entries to the log
    ///
    /// The entries are assumed to be sorted
    pub fn append_entries(
        &mut self,
        prev_log_term: u64,
        prev_log_idx: usize,
        entries: Vec<LogEntry<Op>>,
    ) -> bool {
        let prev_log = self.find_entry(prev_log_idx);
        if prev_log_idx > 0 && (prev_log.is_none() || prev_log.unwrap().term != prev_log_term) {
            return false;
        }

        if let Some(truncate_len) = self.find_truncation(&entries) {
            self.entries.truncate(truncate_len);
        }

        self.entries.append(
            &mut entries
                .into_iter()
                .filter(|x| x.idx > self.last_index())
                .collect(),
        );
        true
    }

    /// Execute operations which are safe to execute
    ///
    /// If the results are important (they probably are), a map of results can be passed the
    /// context of the closures.
    ///
    /// ```
    /// use raft::log::*;
    /// use raft::state_machine::StateMachine;
    /// use std::sync::RwLock;
    ///
    /// struct Reg { v: i32 }
    /// #[derive(Clone)]
    /// enum Ops { Inc, Get };
    /// impl StateMachine for Reg {
    ///     type Op = Ops;
    ///     type Result = i32;
    ///
    ///     fn execute(&mut self, op: Self::Op) -> i32 {
    ///         match op {
    ///             Ops::Inc => self.v += 1,
    ///             _ => ()
    ///         };
    ///         self.v
    ///     }
    /// }
    ///
    /// let register = RwLock::new( Reg { v: 0 } );
    /// let mut log = Log::<Ops>::new();
    ///
    /// log.append_batch(0, vec![ Ops::Get, Ops::Inc, Ops::Get, Ops::Inc, Ops::Inc, Ops::Get, ]);
    ///
    /// let results = RwLock::new(std::collections::BTreeMap::new());
    /// log.execute(6, |idx, op| {
    ///     results.write().unwrap()
    ///         .insert(idx, register.write().unwrap().execute(op));
    /// });
    ///
    /// assert_eq!(
    ///     vec![0, 1, 1, 2, 3, 3],
    ///     results.into_inner().unwrap().into_iter().map(|(_, v)| v).collect::<Vec<i32>>()
    ///     );
    ///
    /// ```
    pub fn execute(&mut self, leader_commit_idx: usize, mut op_fn: impl FnMut(usize, Op)) {
        self.entries
            .iter()
            .filter(|x| x.idx > self.committed_idx && x.idx <= leader_commit_idx)
            .for_each(|entry| op_fn(entry.idx, entry.op.clone()));
        self.committed_idx = leader_commit_idx
    }

    /// Get entries after a specific index (inclusively)
    /// Includes the previous term and index
    ///
    pub fn get_entries_after(&self, idx: usize) -> (u64, usize, Vec<LogEntry<Op>>) {
        let (term, idx) = self
            .entries
            .iter()
            .find(|entry| entry.idx < idx)
            .map(|entry| (entry.term, entry.idx))
            .unwrap_or((0, 0));
        (
            term,
            idx,
            self.entries
                .iter()
                .filter(|x| x.idx >= idx)
                .cloned()
                .collect(),
        )
    }

    /// Number of entries in the log
    pub fn size(&self) -> usize {
        self.entries.len()
    }

    /// Number of unstable entries in the log
    pub fn unstable(&self) -> usize {
        self.entries
            .iter()
            .filter(|x| x.idx > self.committed_idx)
            .count()
    }

    /// Number of stable entries in the log
    pub fn stable(&self) -> usize {
        self.size() - self.unstable()
    }

    /// Last committed index
    pub fn commit_idx(&self) -> usize {
        self.committed_idx
    }

    /// Get index of the last entry
    pub fn last_index(&self) -> usize {
        self.entries.last().map(|entry| entry.idx).unwrap_or(0)
    }

    /// Get term of the last entry
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|entry| entry.term).unwrap_or(0)
    }

    /// Get term and index of the last entry
    pub fn last_term_and_index(&self) -> (u64, usize) {
        self.entries
            .last()
            .map(|entry| (entry.term, entry.idx))
            .unwrap_or((0, 0))
    }

    /// Find an entry which matches an index
    pub fn find_entry(&self, idx: usize) -> Option<&LogEntry<Op>> {
        self.entries.iter().filter(|entry| entry.idx == idx).next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::StateMachine;
    use std::sync::RwLock;

    struct Counter {
        v: i32,
    }
    #[derive(Clone)]
    enum Ops {
        Get, // not putting this down as readonly because it's too much of a hassle
        Inc(i32),
        AtomicInc(i32, i32),
    }

    impl StateMachine for Counter {
        type Op = Ops;
        type Result = i32;

        fn execute(&mut self, op: Self::Op) -> i32 {
            match op {
                Ops::Inc(inc) => self.v += inc,
                Ops::AtomicInc(inc, before) => self.v += if self.v == before { inc } else { 0 },
                _ => (),
            }
            self.v
        }
    }

    #[test]
    fn create() {
        let log = Log::<Ops>::new();
        assert_eq!(log.size(), 0);
        assert_eq!(log.unstable(), 0);
        assert_eq!(log.stable(), 0);
    }

    #[test]
    fn append() {
        let mut log = Log::<Ops>::new();

        log.append(0, Ops::Inc(3));
        assert_eq!(log.size(), 1);
        assert_eq!(log.unstable(), 1);
        assert_eq!(log.stable(), 0);
        log.append(0, Ops::Get);
        assert_eq!(log.size(), 2);
        assert_eq!(log.unstable(), 2);
        assert_eq!(log.stable(), 0);
        log.append_batch(0, vec![Ops::Get, Ops::AtomicInc(2, 4)]);
        assert_eq!(log.size(), 4);
        assert_eq!(log.unstable(), 4);
        assert_eq!(log.stable(), 0);
    }

    #[test]
    fn is_past() {
        let mut log = Log::<Ops>::new();
        assert!(!log.is_past(0, 0));
        assert!(!log.is_past(1, 0));
        assert!(!log.is_past(0, 1));
        assert!(!log.is_past(1, 1));
        assert!(!log.is_past(10, 0));
        assert!(!log.is_past(0, 10));
        assert!(!log.is_past(10, 10));

        log.append(0, Ops::Inc(3));
        assert!(log.is_past(0, 0));
        assert!(!log.is_past(1, 0));
        assert!(!log.is_past(0, 1));
        assert!(!log.is_past(1, 1));

        log.append(0, Ops::Inc(3));
        assert!(log.is_past(0, 0));
        assert!(!log.is_past(1, 0));
        assert!(log.is_past(0, 1));
        assert!(!log.is_past(1, 1));

        log.append(1, Ops::Inc(3));
        assert!(log.is_past(0, 0));
        assert!(log.is_past(0, 1));
        assert!(log.is_past(1, 0));
        assert!(log.is_past(1, 2));
        assert!(!log.is_past(1, 3));
    }

    #[test]
    fn execute() {
        let counter = RwLock::new(Counter { v: 0 });
        let mut log = Log::<Ops>::new();
        assert_eq!(log.size(), 0);
        assert_eq!(log.unstable(), 0);
        assert_eq!(log.stable(), 0);

        log.append_batch(
            0,
            vec![
                Ops::Get,
                Ops::Inc(3),
                Ops::Get,
                Ops::AtomicInc(1, 3),
                Ops::AtomicInc(1, 3),
                Ops::Inc(2),
                Ops::Get,
            ],
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 7);
        assert_eq!(log.stable(), 0);

        let results = RwLock::new(std::collections::BTreeMap::new());
        let op_fn = |idx, op| {
            results
                .write()
                .unwrap()
                .insert(idx, counter.write().unwrap().execute(op));
        };

        log.execute(1, op_fn);
        assert_eq!(
            vec![0],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 6);
        assert_eq!(log.stable(), 1);

        log.execute(1, op_fn);
        assert_eq!(
            vec![0],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 6);
        assert_eq!(log.stable(), 1);

        log.execute(2, op_fn);
        assert_eq!(
            vec![0, 3],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 5);
        assert_eq!(log.stable(), 2);

        log.execute(5, op_fn);
        assert_eq!(
            vec![0, 3, 3, 4, 4,],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 2);
        assert_eq!(log.stable(), 5);

        log.execute(7, op_fn);
        assert_eq!(
            vec![0, 3, 3, 4, 4, 6, 6],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 0);
        assert_eq!(log.stable(), 7);

        log.execute(7, op_fn);
        assert_eq!(
            vec![0, 3, 3, 4, 4, 6, 6],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 0);
        assert_eq!(log.stable(), 7);

        log.execute(8, op_fn);
        assert_eq!(
            vec![0, 3, 3, 4, 4, 6, 6],
            results
                .read()
                .unwrap()
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<i32>>()
        );
        assert_eq!(log.size(), 7);
        assert_eq!(log.unstable(), 0);
        assert_eq!(log.stable(), 7);
    }

    #[test]
    fn append_entries() {
        let mut log = Log::<Ops>::new();
        log.append(1, Ops::Inc(3));
        assert_eq!(log.size(), 1);

        // simple addition
        assert!(log.append_entries(
            1,
            1,
            vec![
                LogEntry::new(2, 2, Ops::Get),
                LogEntry::new(2, 3, Ops::Inc(3)),
                LogEntry::new(2, 4, Ops::Get),
            ]
        ));
        assert_eq!(log.size(), 4);

        // indempotence
        assert!(log.append_entries(
            1,
            1,
            vec![
                LogEntry::new(2, 2, Ops::Get),
                LogEntry::new(2, 3, Ops::Inc(3)),
                LogEntry::new(2, 4, Ops::Get),
            ]
        ));
        assert_eq!(log.size(), 4);

        // error -> 2, 5 is not found
        assert!(!log.append_entries(
            2,
            5,
            vec![
                LogEntry::new(2, 2, Ops::Get),
                LogEntry::new(2, 3, Ops::Inc(3)),
                LogEntry::new(2, 4, Ops::Get),
            ]
        ));
        assert_eq!(log.size(), 4);

        // no entries means true
        assert!(log.append_entries(2, 4, vec![]));
        assert_eq!(log.size(), 4);

        // truncating in action
        assert!(log.append_entries(
            1,
            1,
            vec![LogEntry::new(3, 2, Ops::Get), LogEntry::new(3, 3, Ops::Get),]
        ));
        assert_eq!(log.size(), 3);
    }
}
