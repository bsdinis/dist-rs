#![no_std]
#![cfg_attr(not(test), no_std)]

/// Very basic State Machine Abstraction
///
/// It provides an `execute` interface, which can be used to change states in the machine
/// By separating the construction of the operations and their executions, storing
/// the operations (eg: in a log) and then executing them becomes trivial. It also
/// allows the API to be generic. Otherwise the function call would be the API.
///
/// It differenciates from readonly operations and mutating operations, since it is
/// a significant distinction to be made and is the basis of many optimizations.
pub trait StateMachine {
    type Op;
    type ReadOnlyOp;
    type Result;
    fn execute(&mut self, op: Self::Op) -> Self::Result;
    fn execute_ro(&self, read_only_op: Self::ReadOnlyOp) -> Self::Result;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Counter {
        ctr: u64
    }

    enum CtrOp {
        Incr(u64),
        Dec(u64),
    }

    enum CtrReadOnlyOp {
        Get
    }

    impl StateMachine for Counter {
        type Op = CtrOp;
        type ReadOnlyOp = CtrReadOnlyOp;
        type Result = Result<u64, ()>;

        fn execute(&mut self, op: CtrOp) -> Self::Result {
            match op {
                CtrOp::Incr(n) => self.ctr += n,
                CtrOp::Dec(n) => self.ctr -= n,
            }
            Ok(self.ctr)
        }

        fn execute_ro(&self, op: CtrReadOnlyOp) -> Self::Result {
            Ok(match op {
                CtrReadOnlyOp::Get => self.ctr
            })
        }
    }
    #[test]
    fn basic_test() {
        let mut sm = Counter { ctr: 0 };
        assert_eq!(Ok(5), sm.execute(CtrOp::Incr(5)));
        assert_eq!(Ok(3), sm.execute(CtrOp::Dec(2)));
        assert_eq!(Ok(3), sm.execute_ro(CtrReadOnlyOp::Get));
    }
}
