use counter_server_protocol::raft_client::RaftClient;
use counter_server_protocol::*;
use raft::log::LogEntry;
use raft::replica::Conn;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};

use crate::machine::CtrOp;

use async_trait::async_trait;

use tracing::{event, Level};

pub struct GrpcConn {
    client: RaftClient<Channel>,
    id: u64,
}

impl GrpcConn {
    pub fn new(id: u64, uri: Uri) -> Result<Self, eyre::Report> {
        Ok(GrpcConn {
            client: RaftClient::new(Channel::builder(uri).connect_lazy()?),
            id,
        })
    }
}

fn convert_op(op: CtrOp) -> Option<Operation> {
    Some(Operation {
        op: Some(match op {
            CtrOp::Get => operation::Op::Get(true),
            CtrOp::Incr { step } => operation::Op::Incr(step),
            CtrOp::Decr { step } => operation::Op::Decr(step),
            CtrOp::AtomicIncr { before, step } => operation::Op::AtomicIncr(Step { before, step }),
            CtrOp::AtomicDecr { before, step } => operation::Op::AtomicDecr(Step { before, step }),
        }),
    })
}

#[async_trait]
impl Conn for GrpcConn {
    type Op = CtrOp;
    type Result = (i64, bool);
    type Error = Status;

    /// id of the replica
    ///
    fn id(&self) -> u64 {
        self.id
    }

    /// Forward an operation
    ///
    async fn forward_op(
        &mut self,
        term: u64,
        leader: u64,
        op: Self::Op,
    ) -> Result<(Self::Result, u64, u64), Self::Error> {
        let res = self
            .client
            .forward_request(Request::new(ForwardReq {
                op: convert_op(op),
                term,
                leader,
            }))
            .await?;
        event!(Level::INFO, "Forwarding a request");
        Ok((
            res.get_ref()
                .result
                .as_ref()
                .map(|res| (res.cur, res.success))
                .unwrap(),
            res.get_ref().term,
            res.get_ref().leader,
        ))
    }

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
    ) -> Result<Result<(), (u64, Option<u64>)>, Self::Error> {
        event!(Level::INFO, "Requesting a vote");
        let res = self
            .client
            .request_vote(Request::new(VoteReq {
                term,
                candidate_id: candidate,
                last_log_term,
                last_log_idx: last_log_idx as u64,
            }))
            .await?;

        Ok(if res.get_ref().success {
            Ok(())
        } else {
            Err((
                res.get_ref().term,
                res.get_ref().leader.as_ref().map(|l| l.id),
            ))
        })
    }

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
    ) -> Result<Result<usize, (u64, Option<u64>)>, Self::Error> {
        event!(Level::INFO, "Sending entries to append");
        let res = self
            .client
            .append_entries(Request::new(AppendEntriesReq {
                term,
                leader: leader_id,
                prev_log_term,
                prev_log_idx: prev_log_idx as u64,
                leader_commit_idx: leader_commit_idx as u64,
                entries: entries
                    .into_iter()
                    .map(|entry| counter_server_protocol::LogEntry {
                        term: entry.term,
                        idx: entry.idx as u64,
                        op: convert_op(entry.op),
                    })
                    .collect(),
            }))
            .await?;

        Ok(if res.get_ref().success {
            Ok(res.get_ref().next_entry as usize)
        } else {
            Err((
                res.get_ref().term,
                res.get_ref().leader.as_ref().map(|l| l.id),
            ))
        })
    }
}
