/// Very basic State Machine Abstraction
///
/// It provides an `execute` interface, which can be used to change states in the machine
/// By separating the construction of the operations and their executions, storing
/// the operations (eg: in a log) and then executing them becomes trivial. It also
/// allows the API to be generic. Otherwise the function call would be the API.
///
pub trait StateMachine {
    type Op;
    type Result;
    fn execute(&mut self, op: Self::Op) -> Self::Result;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Counter {
        ctr: u64,
    }

    enum CtrOp {
        Get,
        Incr(u64),
        Dec(u64),
    }

    impl StateMachine for Counter {
        type Op = CtrOp;
        type Result = Result<u64, ()>;

        fn execute(&mut self, op: CtrOp) -> Self::Result {
            match op {
                CtrOp::Incr(n) => self.ctr += n,
                CtrOp::Dec(n) => self.ctr -= n,
                _ => (),
            }
            Ok(self.ctr)
        }
    }
    #[test]
    fn basic_test() {
        let mut sm = Counter { ctr: 0 };
        assert_eq!(Ok(5), sm.execute(CtrOp::Incr(5)));
        assert_eq!(Ok(3), sm.execute(CtrOp::Dec(2)));
        assert_eq!(Ok(3), sm.execute(CtrOp::Get));
    }
}
