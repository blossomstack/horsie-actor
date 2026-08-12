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
use std::str::FromStr;
use thiserror::Error;

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

    /// How one actor of this type is named.
    ///
    /// A value in the program and a string in an address: [`Display`] writes the
    /// last segment, [`FromStr`] recovers it on a node that was handed only a
    /// path. The two have to agree, because an id that does not survive that
    /// round trip names a different actor after a failover than it did before.
    ///
    /// A type rather than a `String` so that an id carrying structure — a tenant
    /// alongside a name, which is how a multi-tenant deployment keeps the tenant
    /// derivable from the address — is taken apart here, once, instead of by
    /// every recipe doing surgery on a path.
    type EntityId: FromStr + Display + Send + 'static;

    /// How one shard of this type is named.
    ///
    /// Written into the address and read back out of it the same way an
    /// [`EntityId`](Self::EntityId) is. A hash bucket is the usual choice, and
    /// a `Bucket(u16)` whose [`FromStr`] refuses anything outside the range is
    /// a better thing to hand a recipe than a segment it has to take on trust.
    type ShardId: FromStr + Display + Send + 'static;

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

/// The shard id in an address of type `S`, read back as an id.
///
/// # Errors
/// If `path` is not an address of this type, or its shard segment is not
/// something [`Shard::ShardId`] can be read from.
pub fn shard_in<S: Shard>(path: &ActorPath) -> Result<S::ShardId, UnreadableAddress> {
    read_id(path, S::TYPE, AddressPart::Shard)
}

/// The entity id in an address of type `S`, read back as an id.
///
/// The counterpart of [`type_in`]: that one tells a node holding only a path
/// which recipe to run, and this one tells the recipe which actor it is being
/// asked for. Both directions of the address exist because a path is all a node
/// gets when a message arrives from somewhere else.
///
/// # Errors
/// If `path` is not an address of this type, or its last segment is not
/// something [`Shard::EntityId`] can be read from. Refused rather than
/// substituted: an actor standing at an address under some other id would
/// answer for whoever the address named, and — event-sourced — write to their
/// journal.
pub fn entity_in<S: Shard>(path: &ActorPath) -> Result<S::EntityId, UnreadableAddress> {
    read_id(path, S::TYPE, AddressPart::Entity)
}

/// One id out of an address of `type_name`.
///
/// Both halves come out through here, so the grammar is matched once and the
/// two cannot drift into disagreeing about which segment is which.
fn read_id<T: FromStr>(
    path: &ActorPath,
    type_name: &'static str,
    part: AddressPart,
) -> Result<T, UnreadableAddress> {
    let refuse = || UnreadableAddress {
        path: path.clone(),
        type_name,
        part,
    };
    match path.segments() {
        [system, shard, claimed, shard_id, entity]
            if system == SYSTEM && shard == SHARD && claimed == type_name =>
        {
            match part {
                AddressPart::Shard => shard_id,
                AddressPart::Entity => entity,
            }
            .parse()
            .map_err(|_| refuse())
        }
        _ => Err(refuse()),
    }
}

/// An address a shard type was asked to claim and cannot read.
///
/// Two nodes on different builds, disagreeing about the shape of an id, is the
/// cause worth suspecting: the address was written by whoever sent the command
/// and read by whoever came to own it. Which half failed narrows that down,
/// since the two are usually minted by different code.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("'{path}' does not name {part} of shard type '{type_name}'")]
pub struct UnreadableAddress {
    /// The address as it arrived.
    pub path: ActorPath,
    /// The type that was asked to read it.
    pub type_name: &'static str,
    /// Which of the two ids it could not read.
    pub part: AddressPart,
}

/// One of the two ids an address carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressPart {
    /// Where the actor was placed.
    Shard,
    /// Which actor it is.
    Entity,
}

impl Display for AddressPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Shard => "a shard",
            Self::Entity => "an entity",
        })
    }
}

/// Which actor a recipe is being asked to build.
///
/// Everything the address holds, taken apart once — so a recipe reads fields
/// rather than segments, and a change to the address grammar is a change here
/// instead of in every application that has ever registered a type.
pub struct EntityContext<S: Shard> {
    /// Which actor of this type. What an event-sourced one derives its
    /// persistence id from, since that id is asked for at construction and is
    /// how recovery finds the log.
    pub entity_id: S::EntityId,
    /// Which shard placed it here.
    pub shard_id: S::ShardId,
    /// Its full address.
    ///
    /// Derivable from the two ids above, and here because a recipe runs before
    /// the actor exists and so before there is an [`ActorContext`] to ask —
    /// which is where a running actor gets its own path from. Reassembling it
    /// would mean knowing the address grammar, which is the one thing an
    /// application is not supposed to need.
    ///
    /// [`ActorContext`]: crate::ActorContext::path
    pub path: ActorPath,
}

/// Where an actor of type `S` handling `cmd` lives, and which shard it is in.
pub(crate) fn address_for<S: Shard>(cmd: &S::Command) -> (ActorPath, ActorPath) {
    let shard_id = S::shard_id(cmd).to_string();
    let entity = entity_of(S::TYPE, &shard_id, S::entity_id(cmd));
    (entity, shard_of(S::TYPE, &shard_id))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// A session of an account, which is the shape [`Shard::EntityId`] exists
    /// for: the account has to come back out of the address, because a session
    /// cannot be built without it.
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

    impl FromStr for Tenanted {
        type Err = ();

        fn from_str(text: &str) -> Result<Self, ()> {
            let (account, session) = text.split_once('|').ok_or(())?;
            Ok(Self {
                account: account.to_owned(),
                session: session.to_owned(),
            })
        }
    }

    /// A placement bucket, which is the shard id a hashed policy produces: a
    /// number spelled as a segment. `u8` is doing the work here — a bucket out
    /// of range is exactly what a segment cannot express and a type can.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Bucket(u8);

    impl Display for Bucket {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl FromStr for Bucket {
        type Err = ();

        fn from_str(text: &str) -> Result<Self, ()> {
            text.parse().map(Bucket).map_err(|_| ())
        }
    }

    struct Session;

    impl Shard for Session {
        type Command = ();
        type EntityId = Tenanted;
        type ShardId = Bucket;
        const TYPE: &'static str = "session";

        fn entity_id(_cmd: &()) -> Tenanted {
            unreachable!("these tests address a session by path, never by command")
        }
        fn shard_id(_cmd: &()) -> Bucket {
            unreachable!("these tests address a session by path, never by command")
        }
    }

    /// A different type, to prove an address is read by the type that claims it
    /// rather than by its shape alone.
    struct Supervisor;

    impl Shard for Supervisor {
        type Command = ();
        type EntityId = String;
        type ShardId = String;
        const TYPE: &'static str = "supervisor";

        fn entity_id(_cmd: &()) -> String {
            unreachable!("these tests address a supervisor by path, never by command")
        }
        fn shard_id(_cmd: &()) -> String {
            unreachable!("these tests address a supervisor by path, never by command")
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
        assert!(shard_in::<Session>(&child).is_err());
    }

    /// The round trip the whole design rests on: both ids written into an
    /// address on one node come back out of it, whole, on another.
    #[test]
    fn both_ids_survive_their_address() {
        let id = Tenanted {
            account: "acct-7".into(),
            session: "sess-3".into(),
        };
        let path = entity_of(Session::TYPE, Bucket(17), &id);

        assert_eq!(path.to_string(), "/system/shard/session/17/acct-7|sess-3");
        assert_eq!(entity_in::<Session>(&path), Ok(id));
        assert_eq!(shard_in::<Session>(&path), Ok(Bucket(17)));
    }

    /// A segment that is not an id is refused, and the error says which half of
    /// the address failed — the two are minted by different code, so which one
    /// it was is most of the diagnosis.
    #[test]
    fn an_unreadable_entity_segment_is_refused() {
        let path = entity_of(Session::TYPE, Bucket(17), "no-account-here");
        let refused = entity_in::<Session>(&path).unwrap_err();
        assert_eq!(refused.type_name, "session");
        assert_eq!(refused.part, AddressPart::Entity);
        assert_eq!(refused.path, path);
    }

    /// A bucket outside the range is unreadable in a way a segment could not
    /// have been, which is the whole argument for typing this one too.
    #[test]
    fn a_shard_segment_out_of_range_is_refused() {
        let path = ActorPath::parse("/system/shard/session/999/acct-7|sess-3").unwrap();
        let refused = shard_in::<Session>(&path).unwrap_err();
        assert_eq!(refused.part, AddressPart::Shard);
        assert_eq!(
            entity_in::<Session>(&path),
            Ok(Tenanted {
                account: "acct-7".into(),
                session: "sess-3".into(),
            }),
            "the entity half of the same address is still readable"
        );
    }

    /// An address is read by the type that claims it. Two types whose ids
    /// happen to look alike still cannot answer for each other's actors.
    #[test]
    fn one_types_address_is_not_anothers() {
        let path = entity_of(Supervisor::TYPE, "17", "acct-7|sess-3");
        assert!(entity_in::<Session>(&path).is_err());
        assert_eq!(
            entity_in::<Supervisor>(&path),
            Ok("acct-7|sess-3".to_owned())
        );
    }
}
