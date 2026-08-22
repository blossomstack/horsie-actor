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
///
/// What a shard reference names, and the prefix that sweeps every actor of a
/// type this node hosts.
#[must_use]
pub fn region_of(type_name: &str) -> ActorPath {
    ActorPath::root()
        .child(SYSTEM)
        .child(SHARD)
        .child(type_name)
}

/// `/system/shard/<type>/<shard>/<entity>` — where one actor is filed.
///
/// Where the grammar is written, and it is called at exactly the two points
/// where an actor is about to be looked up or created on this node. Everything
/// upstream of those — which node hosts this, what crosses the wire — works in
/// ids, because that is what it is about.
///
/// An address is therefore a local registry key and nothing else. It is not
/// sent, and it is not what placement decides over. It is read back in one
/// place — [`shard_root_of`], for the sweep that stops a shard this node no
/// longer hosts — and that reader is next door rather than anywhere else, so
/// the grammar still has one home.
pub(crate) fn address_of<S: Shard>(entity: &EntityContext<S>) -> ActorPath {
    region_of(entity.type_name)
        .child(&entity.shard_id.to_string())
        .child(&entity.entity_id.to_string())
}

/// A shard this node is running, read back off one of its actors' addresses.
///
/// The two ids as text, which is exactly what placement takes: `owner_of`
/// decides over a type name and a shard id, and both reached the address as
/// [`Display`] output on the way in. Nothing here reconstructs an
/// [`S::ShardId`](Shard::ShardId) or an [`S::EntityId`](Shard::EntityId) — an
/// entity's identity still comes off its command, and this cannot see one.
///
/// Owned, and ordered: a scan of the registry produces one of these per actor
/// and wants the shards, so they are collected into a set that dedups them and
/// gives the sweep behind it a fixed order to work and log in.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PlacedShard {
    /// [`Shard::TYPE`], as it was spelled into the address.
    pub type_name: String,
    /// The shard, as [`Shard::shard_id`]'s answer was spelled into the address.
    pub shard_id: String,
}

impl PlacedShard {
    /// `/system/shard/<type>/<shard>` — the subtree this shard's actors occupy,
    /// and so the path that stops all of them at once.
    pub(crate) fn root(&self) -> ActorPath {
        region_of(&self.type_name).child(&self.shard_id)
    }
}

/// Which shard `path` belongs to, if it is a shard address at all.
///
/// The one reader of the grammar in this direction, kept beside the one writer
/// of it. Every path from `/system/shard/<type>/<shard>` down — the entity, and
/// any child the entity created — answers with the same pair, which is what
/// makes a scan of the registry a scan of the shards this node is running.
///
/// It reads the *placement* segments and only those. That is the line: which
/// node hosts a shard is a question about the pair below, and the pair below is
/// what every node computed the address from in the first place. Which actor a
/// command is for is a different question, and it is still answered by the
/// extractors and never by a segment.
pub(crate) fn shard_root_of(path: &ActorPath) -> Option<PlacedShard> {
    match path.segments() {
        [system, shard, type_name, shard_id, ..] if system == SYSTEM && shard == SHARD => {
            Some(PlacedShard {
                type_name: type_name.clone(),
                shard_id: shard_id.clone(),
            })
        }
        _ => None,
    }
}

/// Which actor a recipe is being asked to build.
///
/// The extractors' own outputs, and nothing this crate worked out from them.
/// Every node that needs to know which actor a command is for gets it from the
/// command, so there is one encoding of identity and nothing to disagree with.
pub struct EntityContext<S: Shard> {
    /// The shard type these ids belong to — [`Shard::TYPE`].
    ///
    /// A field rather than something a reader looks up, because the code that
    /// hands this to placement has gone through a type-erased hop and no longer
    /// names `S`.
    pub type_name: &'static str,
    /// Which shard, and so which node. What placement is decided over.
    pub shard_id: S::ShardId,
    /// Which actor of this type. What an event-sourced one derives its
    /// persistence id from, since that id is asked for at construction and is
    /// how recovery finds the log.
    pub entity_id: S::EntityId,
}

/// Which actor `cmd` is for, and which shard it belongs to.
///
/// The single source of identity, in both directions: a send that starts here
/// calls it with the command in hand, and one that arrives from another node
/// calls it with the command it has just decoded. Nothing reads either id back
/// out of anywhere, so the two directions cannot come to different conclusions.
pub(crate) fn context_of<S: Shard>(cmd: &S::Command) -> EntityContext<S> {
    EntityContext {
        type_name: S::TYPE,
        shard_id: S::shard_id(cmd),
        entity_id: S::entity_id(cmd),
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

    /// A region is where a type's actors live collectively, and one actor is
    /// filed two segments under it: the shard it was placed in, then itself.
    #[test]
    fn an_address_is_region_shard_entity() {
        let entity = context_of::<Session>(&Open {
            at: Bucket(17),
            id: Tenanted {
                account: "acct-7".into(),
                session: "sess-3".into(),
            },
        });

        assert_eq!(
            address_of(&entity).to_string(),
            "/system/shard/session/17/acct-7|sess-3"
        );
        assert!(address_of(&entity).starts_with(&region_of("session")));
    }

    /// A child of a shard actor is local to it and sits below its address, so
    /// nothing places it independently of its parent.
    #[test]
    fn a_child_of_a_shard_actor_lives_under_it() {
        let entity = context_of::<Session>(&Open {
            at: Bucket(17),
            id: Tenanted {
                account: "acct-7".into(),
                session: "sess-3".into(),
            },
        });
        let address = address_of(&entity);
        let child = address.child("agent-main");

        assert_eq!(child.parent().as_ref(), Some(&address));
    }

    /// The extractors decide both ids, and the context carries their answers
    /// rather than anything worked out from them.
    #[test]
    fn a_command_decides_which_actor_it_is_for() {
        let entity = context_of::<Session>(&Open {
            at: Bucket(17),
            id: Tenanted {
                account: "acct-7".into(),
                session: "sess-3".into(),
            },
        });

        assert_eq!(entity.type_name, "session");
        assert_eq!(entity.shard_id, Bucket(17));
        assert_eq!(entity.entity_id.account, "acct-7");
        assert_eq!(entity.entity_id.session, "sess-3");
    }

    /// The grammar reads back the same way it was written, from the entity and
    /// from anything the entity created. A sweep looks at the registry, which
    /// holds a row per actor rather than a row per shard, so a child has to
    /// answer with its shard and not with itself.
    #[test]
    fn a_shard_address_says_which_shard_it_is_in() {
        let entity = context_of::<Session>(&Open {
            at: Bucket(17),
            id: Tenanted {
                account: "acct-7".into(),
                session: "sess-3".into(),
            },
        });
        let address = address_of(&entity);

        for path in [address.clone(), address.child("agent-main")] {
            let shard = shard_root_of(&path).expect("a shard address must be read back");
            assert_eq!(shard.type_name, "session");
            assert_eq!(shard.shard_id, "17");
            assert_eq!(shard.root().to_string(), "/system/shard/session/17");
        }
    }

    /// The tree an application builds for itself is not swept. `/system/shard`
    /// is the prefix that keeps the two apart, and a path that merely starts
    /// with something similar is not in it.
    #[test]
    fn an_ordinary_address_is_not_a_shard_address() {
        for path in [
            ActorPath::root(),
            ActorPath::root().child("acct-7").child("session-3"),
            // The prefix, with nothing placed under it: a region, and a region
            // is every shard of a type rather than one of them.
            region_of("session"),
            // Right shape, wrong prefix.
            ActorPath::root()
                .child("system")
                .child("shards")
                .child("session")
                .child("17"),
        ] {
            assert!(
                shard_root_of(&path).is_none(),
                "{path} was read as a shard address"
            );
        }
    }

    /// Two commands naming one entity are filed under one key, and one naming
    /// another is not.
    #[test]
    fn one_entity_is_one_address() {
        let open = |account: &str| Open {
            at: Bucket(3),
            id: Tenanted {
                account: account.into(),
                session: "sess-3".into(),
            },
        };
        let first = address_of(&context_of::<Session>(&open("acct-7")));
        let again = address_of(&context_of::<Session>(&open("acct-7")));
        let other = address_of(&context_of::<Session>(&open("acct-9")));

        assert_eq!(first, again);
        assert_ne!(first, other);
    }
}
