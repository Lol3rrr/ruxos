//! TODOS:
//! * [Vertical Paxos](https://lamport.azurewebsites.net/pubs/vertical-paxos.pdf)
//!
//! # Testing under Maelstrom
//! `lein run -- test --workload {} --rate {} --time-limit {} --bin {}`

#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]

pub mod retry;

pub mod basic;

pub mod caspaxos;

pub mod epaxos;

#[cfg(test)]
pub mod tests;
