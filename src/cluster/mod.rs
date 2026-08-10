//! Hosting actors across several nodes.
//!
//! Ownership is *decided* by the placement layer and *enforced* by the epoch
//! every journal write carries: deciding can be briefly wrong, the fence cannot.

mod delivery;
mod placement;

pub use delivery::Dedup;
pub use placement::{Assignment, InstanceKey, PlacementCommand, PlacementEffect, PlacementTable};
