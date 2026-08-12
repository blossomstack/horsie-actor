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
use std::fmt::Display;

/// The first segment of every shard address, keeping them out of the way of the
/// tree an application builds for itself.
const SYSTEM: &str = "system";
/// The second. `/system/shard/...` reads as what it is.
const SHARD: &str = "shard";

/// An actor type the cluster places, and can build from a command alone.
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

    /// How one actor of this type is named.
    ///
    /// A value everywhere it matters. [`Display`] is only how it is spelled into
    /// an address, and an address is a registry key — every node that needs this
    /// id gets it from the command, so nothing ever reads one back out of a
    /// path. That is what lets it carry structure a segment could not: a tenant
    /// alongside a name, say, which is how a multi-tenant deployment reaches an
    /// account's services before the actor has recovered a byte of its history.
    type EntityId: Display + Send + 'static;

    /// How one shard of this type is named.
    ///
    /// A hash bucket is the usual choice, and a `Bucket(u16)` is a better thing
    /// to hand a recipe than the segment it is spelled as.
    type ShardId: Display + Send + 'static;

    /// Stable name for this type, and the third segment of every instance's
    /// address. Unique across the cluster, and the same in every build — it
    /// travels, and a node that reads a name it does not know cannot build.
    const TYPE: &'static str;

    /// Which actor this command is for.
    fn entity_id(cmd: &Self::Command) -> Self::EntityId;

    /// Which shard this command belongs to, and so which node.
    ///
    /// This *is* the placement policy. Return the entity id for one shard per
    /// actor, or something coarser — an account, a tenant, a hash bucket — to
    /// put a group on one machine. Every command for one entity must agree, or
    /// that entity has two homes.
    fn shard_id(cmd: &Self::Command) -> Self::ShardId;
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
pub fn shard_of(type_name: &str, shard_id: impl Display) -> ActorPath {
    region_of(type_name).child(&shard_id.to_string())
}

/// `/system/shard/<type>/<shard>/<entity>` — one actor.
#[must_use]
pub fn entity_of(type_name: &str, shard_id: impl Display, entity_id: impl Display) -> ActorPath {
    shard_of(type_name, shard_id).child(&entity_id.to_string())
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

/// Which actor a recipe is being asked to build.
///
/// Both ids as the extractors returned them, off the command that is about to
/// be delivered. A recipe therefore reads fields rather than segments, and the
/// address grammar stays the framework's business.
pub struct EntityContext<S: Shard> {
    /// Which actor of this type. What an event-sourced one derives its
    /// persistence id from, since that id is asked for at construction and is
    /// how recovery finds the log.
    pub entity_id: S::EntityId,
    /// Which shard placed it here.
    pub shard_id: S::ShardId,
    /// Its full address.
    ///
    /// Spelled from the two ids above, and here because a recipe runs before
    /// the actor exists and so before there is an [`ActorContext`] to ask —
    /// which is where a running actor gets its own path from. Reassembling it
    /// would mean knowing the address grammar, which is the one thing an
    /// application is not supposed to need.
    ///
    /// [`ActorContext`]: crate::ActorContext::path
    pub path: ActorPath,
}

impl<S: Shard> EntityContext<S> {
    /// The shard this actor sits in — what placement is decided over.
    ///
    /// Derived rather than carried. It is the address above with its last
    /// segment removed, only the cluster ever asks for it, and a value holding
    /// two paths that differ by one segment tells them apart by position alone.
    pub(crate) fn shard_path(&self) -> ActorPath {
        shard_of(S::TYPE, &self.shard_id)
    }
}

/// Which actor `cmd` is for.
///
/// The single source of identity, in both directions: a send that starts here
/// calls it with the command in hand, and one that arrives from another node
/// calls it with the command it has just decoded. The address falls out of the
/// ids rather than the ids out of the address, so there is no second encoding
/// of who an actor is and nothing to disagree about.
pub(crate) fn context_of<S: Shard>(cmd: &S::Command) -> EntityContext<S> {
    let entity_id = S::entity_id(cmd);
    let shard_id = S::shard_id(cmd);
    let path = entity_of(S::TYPE, &shard_id, &entity_id);
    EntityContext {
        entity_id,
        shard_id,
        path,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// A session of an account: the shape [`Shard::EntityId`] exists for, since
    /// a session cannot be built without knowing whose it is.
    #[derive(Debug, PartialEq, Eq)]
    struct Tenanted {
        account: String,
        session: String,
    }

    impl Display for Tenanted {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}|{}", self.account, self.session)
        }
    }

    /// The placement bucket a hashed policy produces.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Bucket(u8);

    impl Display for Bucket {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// Both ids come off the command, so a test can state exactly what the
    /// extractors return and check where that puts the actor.
    struct Session;

    struct Open {
        at: Bucket,
        id: Tenanted,
    }

    impl serde::Serialize for Open {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_unit()
        }
    }

    impl<'de> serde::Deserialize<'de> for Open {
        fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            unreachable!("these tests never put a command on a wire")
        }
    }

    impl Shard for Session {
        type Command = Open;
        type EntityId = Tenanted;
        type ShardId = Bucket;
        const TYPE: &'static str = "session";

        fn entity_id(cmd: &Open) -> Tenanted {
            Tenanted {
                account: cmd.id.account.clone(),
                session: cmd.id.session.clone(),
            }
        }
        fn shard_id(cmd: &Open) -> Bucket {
            cmd.at
        }
    }

    #[test]
    fn a_shard_address_is_type_shard_entity() {
        let path = entity_of("session", "17", "sess-abc");
        assert_eq!(path.to_string(), "/system/shard/session/17/sess-abc");
        assert_eq!(type_in(&path), Some("session"));
        assert_eq!(
            path.parent().unwrap().to_string(),
            "/system/shard/session/17",
            "the entity does not sit under the shard placement is decided over"
        );
    }

    /// An application's own tree is not a shard address, so nothing tries to
    /// build one from a recipe.
    #[test]
    fn an_ordinary_address_is_not_a_shard_address() {
        let path = ActorPath::root().child("acct-7").child("session-3");
        assert_eq!(type_in(&path), None);
    }

    /// A child of a shard actor is local to it, and is not itself addressed as
    /// a shard — otherwise it would be placed independently of its parent.
    #[test]
    fn a_child_of_a_shard_actor_is_not_a_shard_address() {
        let child = entity_of("session", "17", "sess-abc").child("agent-main");
        assert_eq!(type_in(&child), None);
    }

    /// The extractors decide both halves of the address, and the entity sits
    /// under the shard placement is decided over rather than beside it.
    #[test]
    fn a_command_decides_where_its_actor_lives() {
        let cmd = Open {
            at: Bucket(17),
            id: Tenanted {
                account: "acct-7".into(),
                session: "sess-3".into(),
            },
        };
        let entity = context_of::<Session>(&cmd);

        assert_eq!(
            entity.path.to_string(),
            "/system/shard/session/17/acct-7|sess-3"
        );
        assert_eq!(entity.shard_path().to_string(), "/system/shard/session/17");
        assert_eq!(entity.shard_id, Bucket(17));
        assert_eq!(entity.entity_id.account, "acct-7");
    }

    /// Two commands naming one entity land on one address, and the ids the
    /// context carries are the extractors' own values — not something spelled
    /// out and read back, which is what would let the two disagree.
    #[test]
    fn one_entity_is_one_address() {
        let open = |account: &str| Open {
            at: Bucket(3),
            id: Tenanted {
                account: account.into(),
                session: "sess-3".into(),
            },
        };
        let first = context_of::<Session>(&open("acct-7"));
        let again = context_of::<Session>(&open("acct-7"));
        let other = context_of::<Session>(&open("acct-9"));

        assert_eq!(first.path, again.path);
        assert_ne!(first.path, other.path);
    }
}
