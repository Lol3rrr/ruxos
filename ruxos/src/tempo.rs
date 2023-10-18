//! # References
//! * [Paper](https://hdl.handle.net/1822/81307)

mod client;
mod failuredetector;
mod ipc;
pub mod msgs;
pub mod promises;
pub mod replica;

pub use client::Handle;

pub trait Operation<T> {
    type Result;

    fn apply(&self, state: &mut T) -> Self::Result;
}

#[cfg(test)]
impl Operation<()> for () {
    type Result = ();

    fn apply(&self, state: &mut ()) -> Self::Result {
        ()
    }
}
