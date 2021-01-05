use raft::state_machine::StateMachine;

pub struct Counter(pub i64);

#[derive(Debug, Clone, Copy)]
pub enum CtrOp {
    Get,
    Incr { step: i64 },
    Decr { step: i64 },
    AtomicIncr { before: i64, step: i64 },
    AtomicDecr { before: i64, step: i64 },
}

impl StateMachine for Counter {
    type Op = CtrOp;
    type Result = (i64, bool);

    fn execute(&mut self, op: Self::Op) -> Self::Result {
        let success = match op {
            CtrOp::Incr { step } => {
                self.0 += step;
                true
            }
            CtrOp::Decr { step } => {
                self.0 -= step;
                true
            }
            CtrOp::AtomicIncr { before, step } => {
                if self.0 == before {
                    self.0 += step;
                    true
                } else {
                    false
                }
            }
            CtrOp::AtomicDecr { before, step } => {
                if self.0 == before {
                    self.0 -= step;
                    true
                } else {
                    false
                }
            }
            _ => true,
        };
        (self.0, success)
    }
}
