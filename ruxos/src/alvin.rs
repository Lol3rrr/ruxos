//! # Structure
//! *
//!
//! # References
//! * [Paper (P.33)](https://link.springer.com/chapter/10.1007/978-3-319-14472-6_3)

pub mod pob {
    pub struct POBLayer {}

    impl POBLayer {
        pub fn broadcast() {}

        pub fn deliver() {}
    }
}

pub mod pcc {}

pub trait Transaction {}
