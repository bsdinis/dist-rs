use crate::conn::GrpcConn;
use crate::machine::{Counter, CtrOp};

use counter_client_protocol::counter_server::Counter as ProtoCounter;
use counter_client_protocol::*;
use counter_server_protocol::raft_server::Raft;
use counter_server_protocol::*;

use tonic::{Request, Response, Status};

use raft::replica::{Conn, Error, Replica};

use tracing::{event, Level};

pub struct CounterService {
    replica: std::sync::Arc<Replica<GrpcConn, Counter>>,
}

impl CounterService {
    pub fn new(replica: std::sync::Arc<Replica<GrpcConn, Counter>>) -> Self {
        CounterService { replica }
    }
}

fn err_to_status(err: Error<<GrpcConn as Conn>::Error>) -> Status {
    match err {
        Error::ElectionTimeout => {
            Status::deadline_exceeded("Could not perform operation due to an election timeout.")
        }
        Error::ForwardingError {
            operation,
            conn_error,
        } => Status::aborted(format!(
            "Could not forward operation {:?} due to a connection error: {:?}.",
            operation, conn_error
        )),
        Error::UnavailableQuorum {
            available_nodes,
            quorum_sz,
            conn_errors,
        } => Status::failed_precondition(format!(
            "Not enough living nodes ({}, when the quorum size is {}). Connection errors: {:?}",
            available_nodes, quorum_sz, conn_errors
        )),
    }
}

fn convert_op(op: Option<Operation>) -> CtrOp {
    match op.unwrap().op.unwrap() {
        operation::Op::Get(_) => CtrOp::Get,
        operation::Op::Incr(step) => CtrOp::Incr { step },
        operation::Op::Decr(step) => CtrOp::Decr { step },
        operation::Op::AtomicIncr(Step { before, step }) => CtrOp::AtomicIncr { before, step },
        operation::Op::AtomicDecr(Step { before, step }) => CtrOp::AtomicDecr { before, step },
    }
}

#[tonic::async_trait]
impl ProtoCounter for CounterService {
    async fn get(
        &self,
        _request: Request<GetCounterReq>,
    ) -> Result<Response<GetCounterResp>, Status> {
        event!(Level::INFO, "Executing get");
        self.replica
            .handle_op(CtrOp::Get)
            .await
            .map(|(cur, _)| Response::new(GetCounterResp { cur }))
            .map_err(err_to_status)
    }
    async fn incr(
        &self,
        request: Request<IncrCounterReq>,
    ) -> Result<Response<IncrCounterResp>, Status> {
        event!(Level::INFO, "Executing incr");
        self.replica
            .handle_op(CtrOp::Incr {
                step: request.get_ref().step,
            })
            .await
            .map(|(cur, _)| Response::new(IncrCounterResp { cur }))
            .map_err(err_to_status)
    }
    async fn decr(
        &self,
        request: Request<DecrCounterReq>,
    ) -> Result<Response<DecrCounterResp>, Status> {
        event!(Level::INFO, "Executing decr");
        self.replica
            .handle_op(CtrOp::Decr {
                step: request.get_ref().step,
            })
            .await
            .map(|(cur, _)| Response::new(DecrCounterResp { cur }))
            .map_err(err_to_status)
    }
    async fn atomic_incr(
        &self,
        request: Request<AtomicIncrCounterReq>,
    ) -> Result<Response<AtomicIncrCounterResp>, Status> {
        event!(Level::INFO, "Executing atomic incr");
        let before = request.get_ref().before;
        let step = request.get_ref().step;
        self.replica
            .handle_op(CtrOp::AtomicIncr { before, step })
            .await
            .map(|(cur, success)| Response::new(AtomicIncrCounterResp { success, cur }))
            .map_err(err_to_status)
    }
    async fn atomic_decr(
        &self,
        request: Request<AtomicDecrCounterReq>,
    ) -> Result<Response<AtomicDecrCounterResp>, Status> {
        event!(Level::INFO, "Executing atomic decr");
        let before = request.get_ref().before;
        let step = request.get_ref().step;
        self.replica
            .handle_op(CtrOp::AtomicDecr { before, step })
            .await
            .map(|(cur, success)| Response::new(AtomicDecrCounterResp { success, cur }))
            .map_err(err_to_status)
    }

    async fn status(&self, _: Request<StatusReq>) -> Result<Response<StatusResp>, Status> {
        let status = self.replica.status().await;
        Ok(Response::new(StatusResp {
            id: status.id,
            available: status.available as u64,
            quorum_sz: status.quorum_sz as u64,
            term: status.term,
            leader: status.leader.map(|val| Optional { val }),
            voted_for: status.voted_for.map(|val| Optional { val }),
            last_term: status.last_term,
            last_idx: status.last_idx as u64,
            committed_idx: status.committed_idx as u64,
            entries: status.entries as u64,
            unstable: status.unstable as u64,
        }))
    }
}

#[tonic::async_trait]
impl Raft for CounterService {
    async fn request_vote(&self, request: Request<VoteReq>) -> Result<Response<VoteResp>, Status> {
        event!(Level::INFO, "Executing grant vote");
        let candidate = request.get_ref().candidate_id;
        Ok(self
            .replica
            .grant_vote(
                request.get_ref().term,
                candidate,
                request.get_ref().last_log_term,
                request.get_ref().last_log_idx as usize,
            )
            .await
            .map(|_| {
                Response::new(VoteResp {
                    success: true,
                    term: 0,
                    leader: Some(OptionalLeader { id: 0 }),
                })
            })
            .map(|x| {
                event!(Level::INFO, "vote was granted to {}", candidate);
                x
            })
            .map_err(|x| {
                event!(Level::INFO, "vote was *not* granted to {}", candidate);
                x
            })
            .unwrap_or_else(|(term, leader)| {
                Response::new(VoteResp {
                    success: false,
                    term,
                    leader: leader.map(|id| OptionalLeader { id }),
                })
            }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesReq>,
    ) -> Result<Response<AppendEntriesResp>, Status> {
        event!(Level::INFO, "Executing append entries");
        Ok(self
            .replica
            .append_entries(
                request.get_ref().term,
                request.get_ref().leader,
                request.get_ref().prev_log_term,
                request.get_ref().prev_log_idx as usize,
                request.get_ref().leader_commit_idx as usize,
                request
                    .into_inner()
                    .entries
                    .into_iter()
                    .map(|entry| raft::log::LogEntry {
                        term: entry.term,
                        idx: entry.idx as usize,
                        op: convert_op(entry.op),
                    })
                    .collect(),
            )
            .await
            .map(|next_entry| {
                event!(Level::INFO, "Successfully appended entries");
                Response::new(AppendEntriesResp {
                    success: true,
                    next_entry: next_entry as u64,
                    term: 0,
                    leader: Some(OptionalLeader { id: 0 }),
                })
            })
            .unwrap_or_else(|(term, leader)| {
                event!(Level::INFO, "Failed to append entries");
                Response::new(AppendEntriesResp {
                    success: false,
                    next_entry: 0,
                    term,
                    leader: leader.map(|id| OptionalLeader { id }),
                })
            }))
    }

    async fn forward_request(
        &self,
        request: Request<ForwardReq>,
    ) -> Result<Response<ForwardResp>, Status> {
        event!(Level::INFO, "Executing forwarded request");
        self.replica
            .handle_forwarded(
                request.get_ref().term,
                request.get_ref().leader,
                convert_op(request.into_inner().op),
            )
            .await
            .map(|((cur, success), term, leader)| {
                Response::new(ForwardResp {
                    result: Some(OperationResult { cur, success }),
                    term,
                    leader,
                })
            })
            .map_err(err_to_status)
    }
}
