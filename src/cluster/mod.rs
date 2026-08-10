//! Hosting actors across several nodes.
//!
//! Ownership is *decided* by the placement layer and *enforced* by the epoch
//! every journal write carries: deciding can be briefly wrong, the fence cannot.

mod delivery;

pub use delivery::Dedup;
