use crate::log::{Log, LogEntry};
use crate::result_map::ResultMap;
use crate::state_machine::StateMachine;

use async_trait::async_trait;
use futures::select;
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

//use chashmap::CHashMap;
use dashmap::DashMap;
use std::collections::HashMap;

/// Timeout required to start a new election
const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(200);

/// Error types for RAFT
///
/// UnavailableQuorum: it was impossible to contact a quorum (the protocol is unavailable at the moment);
/// ForwardingError: This server forwarded the request to the leader but there was an error;
/// ElectionTimeout: There was an election timeout while an election was occurring. At the moment,
/// nested elections are not supported.
#[derive(Debug)]
pub enum Error<CE: std::error::Error> {
    UnavailableQuorum {
        available_nodes: usize,
        quorum_sz: usize,
        conn_errors: Vec<CE>,
    },
    ForwardingError {
        operation: String,
        conn_error: CE,
    },
    ElectionTimeout,
}

impl<CE> Error<CE>
where
    CE: std::error::Error,
{
    fn unavailable(available_nodes: usize, quorum_sz: usize, conn_errors: Vec<CE>) -> Self {
        Self::UnavailableQuorum {
            available_nodes,
            quorum_sz,
            conn_errors,
        }
    }

    fn forwarding_error(operation: String, conn_error: CE) -> Self {
        Self::ForwardingError {
            operation,
            conn_error,
        }
    }
}

impl<CE> std::error::Error for Error<CE>
where
    CE: std::error::Error,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::UnavailableQuorum { .. } => None,
            Error::ForwardingError {
                operation: _,
                conn_error,
            } => Err(conn_error).ok()?,
            Error::ElectionTimeout => None,
        }
    }
}

impl<CE> std::fmt::Display for Error<CE>
where
    CE: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::UnavailableQuorum {
                available_nodes,
                quorum_sz,
                conn_errors,
            } => {
                let mut backtrace = conn_errors
                    .iter()
                    .fold(String::new(), |a, b| a + &format!("\n{}", b));
                if conn_errors.len() > 0 {
                    backtrace = "\nBacktrace:".to_string() + &backtrace;
                }
                write!(
                    f,
                    "quorum is unavailable: {} online nodes vs. {} required nodes{}",
                    available_nodes, quorum_sz, backtrace
                )
            }

            Error::ForwardingError { operation, .. } => {
                write!(f, "failed to forward operation: {}", operation)
            }
            Error::ElectionTimeout => write!(f, "[unimplemented]: timeout inside an election"),
        }
    }
}

/// Connection abstraction
///
/// Is essentially a very thin shim over the local operations required for server-server communication
/// Every assotiated function returns a result, between the operation result and a connection error
#[async_trait]
pub trait Conn: Send {
    type Op: std::fmt::Debug + Clone;
    type Result;
    type Error: std::error::Error;

    /// id of the replica
    ///
    fn id(&self) -> u64;

    /// Forward an operation
    ///
    async fn forward_op(
        &mut self,
        term: u64,
        leader: u64,
        op: Self::Op,
    ) -> Result<(Self::Result, u64, u64), Self::Error>;

    /// Request a vote from a peer
    /// Returns Ok when the vote was granted
    /// Returns Err when the vote was not granted, carrying information about the
    /// current term and leader
    ///
    async fn request_vote(
        &mut self,
        term: u64,
        candidate: u64,
        last_log_term: u64,
        last_log_idx: usize,
    ) -> Result<Result<(), (u64, Option<u64>)>, Self::Error>;

    /// Append entries to follower log
    /// Returns Ok when the append was successful, with the next entry index to place on that log
    /// Returns Err when there was an inconsistency, including the current term and leader
    ///
    async fn append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_term: u64,
        prev_log_idx: usize,
        leader_commit_idx: usize,
        entries: Vec<LogEntry<Self::Op>>,
    ) -> Result<Result<usize, (u64, Option<u64>)>, Self::Error>;

    /// Whether the connection is healthy or not
    ///
    fn is_connected(&self) -> bool {
        true
    }
}

/// Roles within the RAFT algorithm
#[derive(PartialEq)]
enum Role {
    Leader,
    Follower,
    Candidate,
}

/// RAFT implementation
///
/// This struct holds all the logic of the RAFT algorithm.
/// There are 3 positions in the RAFT algorithm for a replica to assume:
///  - The ``server'' in the replica-client communication
///  - The ``server'' in the replica-replica communication
///  - The ``client'' in the replica-replica communication
pub struct Replica<C, S>
where
    C: Conn,
    S: StateMachine<Op = C::Op, Result = C::Result>,
    S::Result: Clone,
{
    /// Current role of the server
    role: RwLock<Role>,

    /// Id of the server
    id: u64,

    /// Fault tolerance parameter
    f: usize,

    /// Current term (as far as this replica knows)
    current_term: RwLock<u64>,

    /// Person voted for in this term
    voted_for: Mutex<Option<u64>>,

    /// Connections to other replicas
    conns: DashMap<u64, C>,

    /// Ids of all connections
    conn_ids: RwLock<Vec<u64>>,

    /// Leader id
    leader: RwLock<Option<u64>>,

    /// State machine
    state_machine: RwLock<S>,

    /// Log of messages
    log: RwLock<Log<S::Op>>,

    /// Results produced
    results: ResultMap<usize, S::Result>,

    /// Timestamp of the last message from leader
    last_message_ts: RwLock<Instant>,

    /// Index of the next log entry to send to a replica
    next_idx: RwLock<HashMap<u64, usize>>,

    /// Index of the highest log entry known to be replicated for each replica
    match_idx: RwLock<HashMap<u64, usize>>,
}

/// Status struct
/// Information about the current status of this replica (for debug purposes)
///
#[derive(Clone, Debug)]
pub struct Status {
    /// Id of the replica
    pub id: u64,

    /// Number of available replicas
    pub available: usize,

    /// Size of the quorum
    pub quorum_sz: usize,

    /// Term
    pub term: u64,

    /// Leader in this term
    pub leader: Option<u64>,

    /// Voted for
    pub voted_for: Option<u64>,

    /// Last term in log
    pub last_term: u64,

    /// Last index in log
    pub last_idx: usize,

    /// Committed index
    pub committed_idx: usize,

    /// Entries in the log
    pub entries: usize,

    /// Unstable entries in the log
    pub unstable: usize,
}

impl<C, S> Replica<C, S>
where
    C: Conn,
    S: StateMachine<Op = C::Op, Result = C::Result>,
    S::Result: Clone,
{
    // utility functions
    //

    /// Create a new server from a set of connections and a state machine
    /// The id must be unique among the consensus group
    ///
    pub async fn new(conns: Vec<C>, f: usize, id: u64, state_machine: S) -> Self {
        let repl = Replica {
            role: RwLock::new(Role::Follower),
            id,
            f,
            current_term: RwLock::new(0),
            voted_for: Mutex::new(None),
            conns: DashMap::with_capacity(conns.len()),
            conn_ids: RwLock::new(Vec::with_capacity(conns.len())),
            leader: RwLock::new(None),
            state_machine: RwLock::new(state_machine),
            log: RwLock::new(Log::new()),
            results: ResultMap::new(),
            last_message_ts: RwLock::new(Instant::now()),
            next_idx: RwLock::new(HashMap::new()),
            match_idx: RwLock::new(HashMap::new()),
        };
        let mut conn_ids = repl.conn_ids.write().await;
        for c in conns {
            conn_ids.push(c.id());
            repl.conns.insert(c.id(), c);
        }
        drop(conn_ids);
        repl
    }

    pub async fn status(&self) -> Status {
        let available = 1 + self
            .conn_ids
            .read()
            .await
            .iter()
            .filter(|id| {
                self.conns
                    .get(id)
                    .and_then(|c| if c.is_connected() { Some(()) } else { None })
                    .is_some()
            })
            .count();
        let log = self.log.read().await;
        Status {
            id: self.id(),

            available,

            quorum_sz: self.quorum_sz(),

            /// Term
            term: self.current_term().await,

            /// Leader in this term
            leader: *self.leader.read().await,

            /// Voted for
            voted_for: {
                let voted_for = *self.voted_for.lock().await;
                voted_for
            },

            /// Last term in log
            last_term: log.last_term(),

            /// Last index in log
            last_idx: log.last_index(),

            /// Committed index
            committed_idx: log.commit_idx(),

            /// Entries in the log
            entries: log.size(),

            /// Unstable entries in the log
            unstable: log.size(),
        }
    }

    pub async fn push_conn(&self, conn: C) {
        self.conn_ids.write().await.push(conn.id());
        self.conns.insert(conn.id(), conn);
    }

    pub async fn append_conns(&self, conns: Vec<C>) {
        let mut conn_ids = self.conn_ids.write().await;
        conn_ids.reserve(conns.len());
        for c in conns {
            conn_ids.push(c.id());
            self.conns.insert(c.id(), c);
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    // replica server functions: when a replica receives an RPC from another replica
    //

    /// Handle a forwarded operation from another replica
    ///
    pub async fn handle_forwarded(
        &self,
        term: u64,
        leader: u64,
        op: S::Op,
    ) -> Result<(S::Result, u64, u64), Error<C::Error>> {
        self.process_term(term, leader).await;
        let res = self.handle_op(op).await;

        let term = self.current_term().await;
        let leader = {
            let guard = self.leader.read().await;
            guard.unwrap().clone()
        };

        res.map(|res| (res, term, leader))
    }

    /// Grant a vote to a candidate (in response to a RequestVote RPC)
    /// Returns a Result with the following information on error:
    ///  - the current term for this replica
    ///  - the current leader for this replica
    ///
    pub async fn grant_vote(
        &self,
        term: u64,
        candidate_id: u64,
        last_log_term: u64,
        last_log_idx: usize,
    ) -> Result<(), (u64, Option<u64>)> {
        let mut voted_for = self.voted_for.lock().await;
        let cur_term = self.current_term().await;
        let cur_leader = *self.leader.read().await;
        if !(term < cur_term)
            && (voted_for.is_none() || voted_for.unwrap() == candidate_id)
            && !self.log.read().await.is_past(last_log_term, last_log_idx)
        {
            *voted_for = Some(candidate_id);
            Ok(())
        } else {
            Err((cur_term, cur_leader))
        }
    }

    /// Append entries to the log. May refuse if the "leader" is out of date
    /// Returns a Result.
    /// On ok it sends the next index to insert in this replica's log
    /// On error it sends:
    ///  - the current term for this replica
    ///  - the current leader for this replica
    ///
    /// This may also execute a series of operations, if the committed index advances
    ///
    pub async fn append_entries(
        &self,
        term: u64,
        leader: u64,
        prev_log_term: u64,
        prev_log_idx: usize,
        leader_commit_idx: usize,
        entries: Vec<LogEntry<S::Op>>,
    ) -> Result<usize, (u64, Option<u64>)> {
        self.process_term(term, leader).await;
        let cur_term = self.current_term().await;
        if term < cur_term {
            return Err((cur_term, *self.leader.read().await));
        }

        {
            let mut guard = self.log.write().await;
            if !guard.append_entries(prev_log_term, prev_log_idx, entries) {
                return Err((cur_term, *self.leader.read().await));
            }
            self.execute_log(guard, leader_commit_idx).await;
        }
        *self.last_message_ts.write().await = Instant::now();
        Ok(self.log.read().await.last_index() + 1)
    }

    // replica client functions: when a replica receives an RPC from a client
    //

    /// Handle an operation from a client.
    /// If this replica is the leader, it will do it itself,
    /// otherwise it will forward to the leader (possibly having an election before).
    ///
    pub async fn handle_op(&self, op: S::Op) -> Result<S::Result, Error<C::Error>> {
        let leader = self.get_leader().await?;
        if leader == self.id {
            self.do_op(op).await
        } else {
            self.forward_op(leader, op).await
        }
    }

    // workhorses of the protocol
    // the following do most of the heavy lifting of Raft: electing leaders
    // and performing operations
    //

    /// Get the current leader
    /// If there is no leader or there hasn't been an election in some time, an election is
    /// triggered
    ///
    async fn get_leader(&self) -> Result<u64, Error<C::Error>> {
        if self.leader.read().await.is_some()
            && (self.is_leader().await || !self.election_timed_out().await)
        {
            return Ok(self.leader.read().await.unwrap());
        }

        let new_term = {
            let mut role_guard = self.role.write().await;
            *role_guard = Role::Candidate;
            let mut guard = self.current_term.write().await;
            *guard += 1;
            *self.last_message_ts.write().await = Instant::now();
            *guard
        };

        let (last_term, last_idx) = self.log.read().await.last_term_and_index();

        let conn_ids = self.conn_ids.read().await;
        let mut replies = Vec::with_capacity(conn_ids.len());
        let mut errors = Vec::new();
        let mut futs: FuturesUnordered<_> = conn_ids
            .iter()
            .cloned()
            .filter(|id| {
                self.conns
                    .get(id)
                    .and_then(|c| if c.is_connected() { Some(()) } else { None })
                    .is_some()
            })
            .map(|id| self.send_request_vote(id, new_term, last_term, last_idx))
            .collect();

        let available = futs.len() + 1;

        if available < self.quorum_sz() {
            return Err(Error::unavailable(available, self.quorum_sz(), errors));
        }

        replies.push(
            self.grant_vote(new_term, self.id(), last_term, last_idx)
                .await,
        );

        while self.is_candidate().await && !self.election_timed_out().await {
            select! {
                res = futs.select_next_some() => {
                    match res {
                        Ok(v) => replies.push(v),
                        Err(e) => errors.push(e),
                    }

                    if replies.len() >= self.quorum_sz() {
                        break;
                    }
                    else if replies.len() + errors.len() > self.n() - self.f {
                        return Err(Error::unavailable(available, self.quorum_sz(), errors));
                    }
                    else if self.quorum_sz() > available - errors.len() {
                        return Err(Error::unavailable(available, self.quorum_sz(), errors));
                    }
                }

                complete => break
            }
        }

        if replies.len() < self.quorum_sz() {
            return Err(Error::unavailable(available, self.quorum_sz(), errors));
        }
        let positive_votes = replies.iter().filter(|v| v.is_ok()).count();

        let mut role_guard = self.role.write().await;
        if !self.election_timed_out().await {
            if *role_guard == Role::Candidate && positive_votes >= self.quorum_sz() {
                *role_guard = Role::Leader;
                *self.leader.write().await = Some(self.id);
                *self.voted_for.lock().await = None;

                let mut next_idx = self.next_idx.write().await;
                let mut match_idx = self.match_idx.write().await;
                let conn_ids: std::collections::HashSet<_> =
                    self.conn_ids.read().await.iter().cloned().collect();

                next_idx.retain(|k, _| !conn_ids.contains(k));
                match_idx.retain(|k, _| !conn_ids.contains(k));

                let last_idx = self.log.read().await.last_index();
                for id in self.conn_ids.read().await.iter() {
                    next_idx.insert(*id, last_idx + 1);
                    match_idx.insert(*id, 0);
                }
                Ok(self.id)
            } else {
                *role_guard = Role::Follower;
                let mut term_guard = self.current_term.write().await;
                let mut leader_guard = self.leader.write().await;
                let term_leader = replies
                    .iter()
                    .filter(|x| x.is_err())
                    .map(|x| x.unwrap_err())
                    .max_by_key(|(t, _)| t.clone())
                    .unwrap_or((*term_guard, *leader_guard));
                if term_leader.0 != *term_guard {
                    *term_guard = term_leader.0;
                    *self.voted_for.lock().await = None;
                }
                *leader_guard = term_leader.1;
                Ok(leader_guard.unwrap())
            }
        } else {
            Err(Error::ElectionTimeout)
        }
    }

    /// Sends a vote request to a particular replica
    ///
    async fn send_request_vote(
        &self,
        id: u64,
        new_term: u64,
        last_term: u64,
        last_idx: usize,
    ) -> Result<Result<(), (u64, Option<u64>)>, C::Error> {
        self.conns
            .get_mut(&id)
            .unwrap()
            .request_vote(new_term, self.id(), last_term, last_idx)
            .await
    }

    /// Follower forwarding to the leader
    ///
    async fn forward_op(&self, leader: u64, op: S::Op) -> Result<S::Result, Error<C::Error>> {
        let operation = format!("{:?}", op);
        let term = self.current_term().await;

        let op_res = self
            .conns
            .get_mut(&leader)
            .unwrap()
            .forward_op(term, leader, op)
            .await;

        if let Ok((_, term, leader)) = op_res {
            self.process_term(term, leader).await;
        }
        op_res
            .map(|(res, _, _)| res)
            .map_err(|e| Error::forwarding_error(operation, e))
    }

    /// Leader performing an operation.
    /// Sends append_entries requests to everyone and waits for a quorum
    ///
    async fn do_op(&self, op: S::Op) -> Result<S::Result, Error<C::Error>> {
        let entry_idx = self
            .log
            .write()
            .await
            .append(self.current_term().await, op.clone());
        self.advance_log().await; // required because if there are no replicas, the existance of the entry in the log is sufficient

        {
            let term = self.current_term().await;
            let mut replies = 1; // self reply
            let mut errors = Vec::new();
            let mut futs: FuturesUnordered<_> = self
                .conn_ids
                .read()
                .await
                .iter()
                .cloned()
                .filter(|id| {
                    self.conns
                        .get_mut(id)
                        .and_then(|c| if c.is_connected() { Some(()) } else { None })
                        .is_some()
                })
                .map(|id| self.send_append_entries(id, term))
                .collect();

            let available = futs.len() + 1;

            if available < self.quorum_sz() {
                return Err(Error::unavailable(available, self.quorum_sz(), errors));
            }

            'outer: while replies < self.quorum_sz() {
                select! {
                    (id, res) = futs.select_next_some() => {
                        match res {
                            Ok(Ok(next_idx)) => {
                                if let Some(idx) = self.match_idx.write().await.get_mut(&id) {
                                    *idx = std::cmp::max(*idx, entry_idx);
                                }
                                self.next_idx.write().await.insert(id, next_idx);
                                self.advance_log().await;
                                replies += 1;
                            },
                            Ok(Err((term, leader))) => {
                                if let Some(leader) = leader {
                                    self.process_term(term, leader).await;
                                }
                                let leader = self.get_leader().await?;
                                if leader == self.id() {
                                    if let Some(idx) = self.next_idx.write().await.get_mut(&id) {
                                        *idx -= 1;
                                    }
                                    futs.push(self.send_append_entries(id, term));
                                } else {
                                    return self.forward_op(leader, op).await;
                                }
                            }
                            Err(e) => errors.push(e),
                        }

                        if replies >= self.quorum_sz() {
                            break 'outer;
                        }
                        else if replies + errors.len() > self.n() - self.f {
                            return Err(Error::unavailable(available, self.quorum_sz(), errors));
                        }
                        else if self.quorum_sz() > available - errors.len() {
                            return Err(Error::unavailable(available, self.quorum_sz(), errors));
                        }
                    }

                    complete => break 'outer,
                }
            }
        }

        Ok(self.results.pop(entry_idx).into().await)
    }

    /// Sends an append entries request to a particular replica
    ///
    async fn send_append_entries(
        &self,
        id: u64,
        term: u64,
    ) -> (u64, Result<Result<usize, (u64, Option<u64>)>, C::Error>) {
        let (prev_term, prev_idx, entries) = self.get_entries_for(id).await;
        let commit_idx = self.log.read().await.commit_idx();

        let result = self
            .conns
            .get_mut(&id)
            .unwrap()
            .append_entries(term, self.id(), prev_term, prev_idx, commit_idx, entries)
            .await;

        (id, result)
    }

    /// Get the required entries to send to a replica
    /// Also sends the previous log term and index
    ///
    async fn get_entries_for(&self, id: u64) -> (u64, usize, Vec<LogEntry<C::Op>>) {
        self.log
            .read()
            .await
            .get_entries_after(*self.next_idx.read().await.get(&id).unwrap())
    }

    /// Tries to advance the local commit index (thus advancing the log)
    /// This function should be called **only by the leader**
    /// (the replicas advance their log based on the leader commit index on append_entries
    /// requests)
    ///
    async fn advance_log(&self) {
        let votes = {
            let conn_ids = self.conn_ids.read().await;
            let mut votes = HashMap::with_capacity(conn_ids.len());
            votes.insert(self.log.read().await.last_index(), 1); // self index

            let match_guard = self.match_idx.read().await;
            for index in conn_ids.iter().map(|idx| *match_guard.get(idx).unwrap()) {
                match votes.get_mut(&index) {
                    Some(r) => *r += 1,
                    None => {
                        votes.insert(index, 2);
                    } // count the first vote and the leader's vote
                }
            }

            for index in conn_ids.iter().map(|idx| *match_guard.get(idx).unwrap()) {
                votes
                    .iter_mut()
                    .filter(|(k, _)| &&index >= k)
                    .for_each(|pair| *pair.1 += 1);
            }
            votes
        };

        let commit_idx = self.log.read().await.commit_idx();
        let current_term = *self.current_term.read().await;
        let log_guard = self.log.read().await;
        if let Some(index) = votes
            .iter()
            .filter(|(k, v)| {
                if let Some(log_entry) = log_guard.find_entry(**k) {
                    k > &&commit_idx && v >= &&self.quorum_sz() && log_entry.term == current_term
                } else {
                    false
                }
            })
            .map(|(k, _)| k)
            .max()
        {
            drop(log_guard);
            let guard = self.log.write().await;
            self.execute_log(guard, *index).await;
        }
    }

    /// Execute the log up until a given index
    ///
    async fn execute_log(
        &self,
        mut guard: tokio::sync::RwLockWriteGuard<'_, Log<S::Op>>,
        leader_commit_idx: usize,
    ) {
        let mut machine_guard = self.state_machine.write().await;
        guard.execute(leader_commit_idx, |idx, o| {
            self.results.insert(idx, machine_guard.execute(o));
        });
    }

    // utility functions
    //

    /// Whether this replica is the leader
    ///
    async fn is_leader(&self) -> bool {
        *self.role.read().await == Role::Leader
    }

    /// Whether this replica is a candidate
    ///
    async fn is_candidate(&self) -> bool {
        *self.role.read().await == Role::Candidate
    }

    /// Whether an election has timed out
    ///
    async fn election_timed_out(&self) -> bool {
        self.last_message_ts.read().await.elapsed() > RAFT_ELECTION_TIMEOUT
    }

    /// Number of replicas in the consensus group
    ///
    fn n(&self) -> usize {
        2 * self.f + 1
    }

    /// Size of a quorum
    ///
    fn quorum_sz(&self) -> usize {
        self.f + 1
    }

    /// Current term
    ///
    async fn current_term(&self) -> u64 {
        *self.current_term.read().await
    }

    /// Process a (term, leader) pair from an RPC request or response
    ///
    async fn process_term(&self, term: u64, leader: u64) {
        let mut cur_term = self.current_term.write().await;
        if *cur_term < term {
            *cur_term = term;
            *self.leader.write().await = Some(leader);
            *self.role.write().await = Role::Follower;
            *self.voted_for.lock().await = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    enum CtrOp {
        Inc,
        Get,
    }

    struct Ctr(usize);
    impl StateMachine for Ctr {
        type Op = CtrOp;
        type Result = usize;

        fn execute(&mut self, op: Self::Op) -> usize {
            match op {
                CtrOp::Inc => self.0 += 1,
                _ => {}
            }
            self.0
        }
    }

    #[derive(Debug)]
    struct ConnError;
    impl std::error::Error for ConnError {}
    impl std::fmt::Display for ConnError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "connection error")
        }
    }

    struct DummyConn {
        r: Arc<Replica<DummyConn, Ctr>>,
    }

    #[async_trait]
    impl Conn for DummyConn {
        type Op = CtrOp;
        type Result = usize;
        type Error = ConnError;

        fn id(&self) -> u64 {
            self.r.id
        }

        async fn forward_op(
            &mut self,
            term: u64,
            leader: u64,
            op: Self::Op,
        ) -> Result<(Self::Result, u64, u64), Self::Error> {
            let res = self.r.handle_forwarded(term, leader, op).await;
            res.map_err(|_| ConnError {})
        }

        async fn request_vote(
            &mut self,
            term: u64,
            candidate: u64,
            last_log_term: u64,
            last_log_idx: usize,
        ) -> Result<Result<(), (u64, Option<u64>)>, Self::Error> {
            Ok(self
                .r
                .grant_vote(term, candidate, last_log_term, last_log_idx)
                .await)
        }

        async fn append_entries(
            &mut self,
            term: u64,
            leader: u64,
            prev_log_term: u64,
            prev_log_idx: usize,
            leader_commit_idx: usize,
            entries: Vec<LogEntry<Self::Op>>,
        ) -> Result<Result<usize, (u64, Option<u64>)>, Self::Error> {
            Ok(self
                .r
                .append_entries(
                    term,
                    leader,
                    prev_log_term,
                    prev_log_idx,
                    leader_commit_idx,
                    entries,
                )
                .await)
        }
    }

    async fn get_replicas(f: usize) -> Vec<Arc<Replica<DummyConn, Ctr>>> {
        let n = 2 * f + 1;
        let replicas = {
            let mut v = Vec::with_capacity(n);
            for i in 0..n {
                v.push(Arc::new(
                    Replica::new(vec![], f, i.try_into().unwrap(), Ctr(0)).await,
                ));
            }
            v
        };

        for i in 0..n {
            let conns = replicas
                .iter()
                .filter(|r| r.id() != i.try_into().unwrap())
                .map(|r| DummyConn { r: r.clone() })
                .collect();
            replicas[i as usize].append_conns(conns).await;
        }

        replicas
    }

    #[tokio::test]
    async fn create() {
        let f = 2;

        let replicas = get_replicas(f).await;
        let repl = &replicas[0];

        assert_eq!(repl.n(), 5);
        assert_eq!(repl.quorum_sz(), 3);

        assert!(!repl.is_leader().await);
        assert!(!repl.is_candidate().await);
    }

    #[tokio::test]
    async fn single_replica() {
        // what happens when the consensus group has one replica
        let f = 0;
        let replicas = get_replicas(f).await;
        let repl = &replicas[0];

        assert_eq!(repl.n(), 1);
        assert_eq!(repl.quorum_sz(), 1);

        assert!(!repl.is_leader().await);
        assert!(!repl.is_candidate().await);

        assert_eq!(repl.get_leader().await.unwrap(), repl.id());
        assert!(repl.is_leader().await);
        assert!(!repl.is_candidate().await);
        assert_eq!(repl.handle_op(CtrOp::Get).await.unwrap(), 0);
        assert_eq!(repl.handle_op(CtrOp::Inc).await.unwrap(), 1);
        assert_eq!(repl.handle_op(CtrOp::Get).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn multiple_replicas() {
        // what happens when the consensus group has three replica
        let f = 1;
        let replicas = get_replicas(f).await;
        let repl = &replicas[0];

        assert_eq!(repl.n(), 3);
        assert_eq!(repl.quorum_sz(), 2);

        assert!(!repl.is_leader().await);
        assert!(!repl.is_candidate().await);

        assert_eq!(repl.get_leader().await.unwrap(), repl.id());
        assert_eq!(repl.handle_op(CtrOp::Get).await.unwrap(), 0);
        assert_eq!(repl.handle_op(CtrOp::Inc).await.unwrap(), 1);
        assert_eq!(repl.handle_op(CtrOp::Get).await.unwrap(), 1);
    }
}
