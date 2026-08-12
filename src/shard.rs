//! Shard actors: the roots at which an actor tree becomes clustered.
//!
//! An actor tree is node-local — a parent and its children are always on the
//! same machine — and clustering happens only at the roots. A shard type is
//! registered once per node with that node's own wiring; everything below a
//! shard root is an ordinary local child created by its parent. So there is no
//! such thing as a clustered child, and the question of who builds one on its
//! owning node never arises.
//!
//! This is Akka's arrangement, where entities are children of a shard, which is
//! a child of a region, all on one host. Letting a tree span machines is what
//! makes supervision a distributed lifetime problem, `ctx.parent()` a possible
//! network hop, and "stop everything under here" a cluster-wide operation.

use crate::path::ActorPath;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The first segment of every shard address, keeping them out of the way of the
/// tree an application builds for itself.
const SYSTEM: &str = "system";
/// The second. `/system/shard/...` reads as what it is.
const SHARD: &str = "shard";

/// An actor type the cluster places, and can build from an address alone.
///
/// A node can only run an actor it can construct, and an actor is live state — a
/// pool, a client, an open connection — so it cannot be shipped from whoever
/// wanted it. Every node registers its own recipe, closing over its own wiring.
///
/// Deliberately **not** a subtrait of [`Actor`]: an event-sourced type is not an
/// `Actor` — [`Persistent`] is — and requiring it would make this
/// unimplementable for exactly the types worth sharding.
///
/// [`Persistent`]: crate::Persistent
pub trait Shard: Send + Sized + 'static {
    /// Messages instances of this type accept.
    ///
    /// The bounds are the compile-time guarantee: a command that could not
    /// survive a hop between hosts is a type error rather than a runtime
    /// surprise on the first send.
    type Command: Send + Serialize + DeserializeOwned + 'static;

    /// Stable name for this type, and the third segment of every instance's
    /// address. Unique across the cluster, and the same in every build — it
    /// travels, and a node that reads a name it does not know cannot build.
    const TYPE: &'static str;

    /// Which actor this command is for.
    fn entity_id(cmd: &Self::Command) -> String;

    /// Which shard this command belongs to, and so which node.
    ///
    /// This *is* the placement policy. Return the entity id for one shard per
    /// actor, or something coarser — an account, a tenant — to put a group on
    /// one machine. Every command for one entity must agree, or that entity has
    /// two homes.
    fn shard_id(cmd: &Self::Command) -> String;
}

/// `/system/shard/<type>` — where a type's actors live, collectively.
#[must_use]
pub fn region_of(type_name: &str) -> ActorPath {
    ActorPath::root()
        .child(SYSTEM)
        .child(SHARD)
        .child(type_name)
}

/// `/system/shard/<type>/<shard>` — the unit placement is decided over.
#[must_use]
pub fn shard_of(type_name: &str, shard_id: &str) -> ActorPath {
    region_of(type_name).child(shard_id)
}

/// `/system/shard/<type>/<shard>/<entity>` — one actor.
#[must_use]
pub fn entity_of(type_name: &str, shard_id: &str, entity_id: &str) -> ActorPath {
    shard_of(type_name, shard_id).child(entity_id)
}

/// The type name in a shard address, if this is one.
///
/// How a node that has only a path finds the recipe for what belongs there.
#[must_use]
pub fn type_in(path: &ActorPath) -> Option<&str> {
    match path.segments() {
        [system, shard, type_name, _, _] if system == SYSTEM && shard == SHARD => {
            Some(type_name.as_str())
        }
        _ => None,
    }
}

/// The shard an address belongs to — its placement key — if it is a shard
/// address.
#[must_use]
pub fn shard_in(path: &ActorPath) -> Option<ActorPath> {
    type_in(path).and(path.parent())
}

/// Where an actor of type `S` handling `cmd` lives, and which shard it is in.
pub(crate) fn address_for<S: Shard>(cmd: &S::Command) -> (ActorPath, ActorPath) {
    let shard_id = S::shard_id(cmd);
    let entity = entity_of(S::TYPE, &shard_id, &S::entity_id(cmd));
    (entity, shard_of(S::TYPE, &shard_id))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_shard_address_is_type_shard_entity() {
        let path = entity_of("session", "17", "sess-abc");
        assert_eq!(path.to_string(), "/system/shard/session/17/sess-abc");
        assert_eq!(type_in(&path), Some("session"));
        assert_eq!(
            shard_in(&path).unwrap().to_string(),
            "/system/shard/session/17"
        );
    }

    /// An application's own tree is not a shard address, so nothing tries to
    /// build one from a recipe.
    #[test]
    fn an_ordinary_address_is_not_a_shard_address() {
        let path = ActorPath::root().child("acct-7").child("session-3");
        assert_eq!(type_in(&path), None);
        assert_eq!(shard_in(&path), None);
    }

    /// A child of a shard actor is local to it, and is not itself addressed as
    /// a shard — otherwise it would be placed independently of its parent.
    #[test]
    fn a_child_of_a_shard_actor_is_not_a_shard_address() {
        let child = entity_of("session", "17", "sess-abc").child("agent-main");
        assert_eq!(type_in(&child), None);
    }
}
