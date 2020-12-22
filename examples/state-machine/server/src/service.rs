use counter_protocol::counter_server::Counter as ProtoCounter;
use counter_protocol::*;
use tonic::{Request, Response, Status};

use std::sync::RwLock;

use state_machine::StateMachine;

use tracing::{event, Level};

struct Counter {
    ctr: u64,
}

enum CtrOp {
    Incr(u64),
    Decr(u64),
    AtomicIncr { current: u64, step: u64 },
    AtomicDecr { current: u64, step: u64 },
}

enum CtrReadOnlyOp {
    Get,
}

impl StateMachine for Counter {
    type Op = CtrOp;
    type ReadOnlyOp = CtrReadOnlyOp;
    type Result = Result<u64, u64>;

    fn execute(&mut self, op: CtrOp) -> Self::Result {
        match op {
            CtrOp::Incr(n) => {
                event!(
                    Level::INFO,
                    "Counter updated: {} -> {}",
                    self.ctr,
                    self.ctr.saturating_add(n)
                );
                self.ctr = self.ctr.saturating_add(n);
                Ok(self.ctr)
            }
            CtrOp::Decr(n) => {
                event!(
                    Level::INFO,
                    "Counter updated: {} -> {}",
                    self.ctr,
                    self.ctr.saturating_sub(n)
                );
                self.ctr = self.ctr.saturating_sub(n);
                Ok(self.ctr)
            }
            CtrOp::AtomicIncr { current: cur, step } => {
                if cur == self.ctr {
                    event!(
                        Level::INFO,
                        "Counter updated: {} -> {}",
                        self.ctr,
                        self.ctr.saturating_add(step)
                    );
                    self.ctr = self.ctr.saturating_add(step);
                    Ok(self.ctr)
                } else {
                    event!(
                        Level::INFO,
                        "Counter did not update: current value = {} vs. expected value {}",
                        self.ctr,
                        cur
                    );
                    Err(self.ctr)
                }
            }
            CtrOp::AtomicDecr { current: cur, step } => {
                if cur == self.ctr {
                    event!(
                        Level::INFO,
                        "Counter updated: {} -> {}",
                        self.ctr,
                        self.ctr.saturating_sub(step)
                    );
                    self.ctr = self.ctr.saturating_sub(step);
                    Ok(self.ctr)
                } else {
                    event!(
                        Level::INFO,
                        "Counter did not update: current value = {} vs. expected value {}",
                        self.ctr,
                        cur
                    );
                    Err(self.ctr)
                }
            }
        }
    }

    fn execute_ro(&self, op: CtrReadOnlyOp) -> Self::Result {
        Ok(match op {
            CtrReadOnlyOp::Get => {
                event!(Level::INFO, "Counter value: {}", self.ctr);
                self.ctr
            }
        })
    }
}

pub struct CounterService {
    ctr: RwLock<Counter>,
}

impl CounterService {
    pub fn new(init: u64) -> Self {
        CounterService {
            ctr: RwLock::new(Counter { ctr: init }),
        }
    }

    pub fn incr(&self, step: u64) -> u64 {
        self.ctr
            .write()
            .unwrap()
            .execute(CtrOp::Incr(step))
            .unwrap()
    }
    pub fn decr(&self, step: u64) -> u64 {
        self.ctr
            .write()
            .unwrap()
            .execute(CtrOp::Decr(step))
            .unwrap()
    }
    pub fn atomic_incr(&self, current: u64, step: u64) -> Result<u64, u64> {
        self.ctr
            .write()
            .unwrap()
            .execute(CtrOp::AtomicIncr { current, step })
    }
    pub fn atomic_decr(&self, current: u64, step: u64) -> Result<u64, u64> {
        self.ctr
            .write()
            .unwrap()
            .execute(CtrOp::AtomicDecr { current, step })
    }
    pub fn get(&self) -> u64 {
        self.ctr
            .read()
            .unwrap()
            .execute_ro(CtrReadOnlyOp::Get)
            .unwrap()
    }
}

#[tonic::async_trait]
impl ProtoCounter for CounterService {
    async fn get(
        &self,
        _request: Request<GetCounterReq>,
    ) -> Result<Response<GetCounterResp>, Status> {
        Ok(Response::new(GetCounterResp { cur: self.get() }))
    }
    async fn incr(
        &self,
        request: Request<IncrCounterReq>,
    ) -> Result<Response<IncrCounterResp>, Status> {
        Ok(Response::new(IncrCounterResp {
            cur: self.incr(request.get_ref().step),
        }))
    }
    async fn decr(
        &self,
        request: Request<DecrCounterReq>,
    ) -> Result<Response<DecrCounterResp>, Status> {
        Ok(Response::new(DecrCounterResp {
            cur: self.decr(request.get_ref().step),
        }))
    }
    async fn atomic_incr(
        &self,
        request: Request<AtomicIncrCounterReq>,
    ) -> Result<Response<AtomicIncrCounterResp>, Status> {
        match self.atomic_incr(request.get_ref().before, request.get_ref().step) {
            Ok(cur) => Ok(Response::new(AtomicIncrCounterResp { cur, success: true })),
            Err(cur) => Ok(Response::new(AtomicIncrCounterResp {
                cur,
                success: false,
            })),
        }
    }
    async fn atomic_decr(
        &self,
        request: Request<AtomicDecrCounterReq>,
    ) -> Result<Response<AtomicDecrCounterResp>, Status> {
        match self.atomic_decr(request.get_ref().before, request.get_ref().step) {
            Ok(cur) => Ok(Response::new(AtomicDecrCounterResp { cur, success: true })),
            Err(cur) => Ok(Response::new(AtomicDecrCounterResp {
                cur,
                success: false,
            })),
        }
    }
}
