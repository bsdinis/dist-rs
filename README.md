# repl-rs

Generic replication protocol implementation, written in Rust.

## Motivation

Distributed protocols, be it distributed register, state-machine replication, etc. are notoriously hard to get right. However, they are still re-implemented multiple times, mainly because it seems a difficult solution to generalize. This repository contains _generic_ implementations of
common distributed protocols, along with examples (written in Rust with gRPC as the communication protocol).

## State Machine

There are several protocols which are modelled in literature as a set of nodes doing operations on a state machine --- aka State Machine Replication. The crate `state-machine` offers a generic abstraction over these, with the discriminating read-only operations, as those can be sped-up, in certain scenarios.

## ABD

The ABD (Attiya, Bar-Noy, Dolev) protocol models a distributed register _x_, to which one can set a value or get the current value. This protocol is _linearizable_, and can be used to build key-value stores.
The generic implementation requires servers to implement an `abd::store::LocalStore`, while clients need to implement a `abd::Conn`, which models the connection to each server.


### Todo

 - implement asymmetric quorums

## Future Work

There are other interesting protocols to implement:
 - permissioned ABD (ABD with permissions: this requires the servers to check a permission based on the client id, shifting the onus of coordinating the protocol to a server)
 - Paxos
 - Raft

Moreover, BFT services are also important:
 - PBFT
 - Byzantine Quorums
