//! TODOS:
//! * [Vertical Paxos](https://lamport.azurewebsites.net/pubs/vertical-paxos.pdf)

#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]

pub mod basic;

pub mod caspaxos;

#[cfg(test)]
pub mod tests;
