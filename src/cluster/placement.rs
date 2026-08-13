use crate::envelope::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Who hosts what.
///
/// **A pure function of a shard's identity and the live member set**, and
/// nothing else. That is the whole type: hand two nodes the same members and
/// they answer identically for every shard, without asking each other and
/// without remembering anything.
///
/// It used to remember. A node recorded each shard it had hosted and answered
/// from that record before it hashed — which meant a node that had hosted
/// something and a node that had not gave different answers from the same live
/// set. Two hosts for one shard follows directly, and the write fence can only
/// reject the second one's *writes*: both go on serving reads from state the
/// other cannot see. The record was there for stickiness, and stickiness only
/// works if it is agreed. This one was node-local.
///
/// Rendezvous hashing gives most of what that was for anyway. Losing a member
/// moves only the shards that were on it, so a shard does not move because some
/// unrelated node came or went. What it does not give is "keep a shard where it
/// is even though the membership changed", and that is a deliberate omission:
/// buying it back means replicating the assignments through consensus — a Raft
/// write on the first message to every entity — which is a separate decision
/// that should arrive with numbers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementTable {
    members: BTreeSet<NodeId>,
}

impl PlacementTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the members placement may use.
    ///
    /// Driven only by the agreed live set. There is deliberately no way to mark
    /// one node up or down on its own: a node forming its own opinion about who
    /// is alive is what produced split brain before consensus decided it.
    pub fn set_members(&mut self, members: impl IntoIterator<Item = NodeId>) {
        self.members = members.into_iter().collect();
    }

    /// Live members.
    #[must_use]
    pub fn members(&self) -> &BTreeSet<NodeId> {
        &self.members
    }

    /// Which node hosts the shard `shard_id` of type `type_name`.
    ///
    /// Rendezvous-style: score the shard against each member and take the
    /// highest, so one shard lands on one node from every node's point of view,
    /// and losing a member moves only what was on it. `None` when nothing is
    /// alive — which is the state a stood-down node is in.
    ///
    /// Takes the two ids rather than a formatted address, because placement
    /// happens *before* there is an address: a command names a shard, the answer
    /// says which host, and only the host that turns out to be this one goes on
    /// to file an actor under a key.
    #[must_use]
    pub fn owner_of(&self, type_name: &str, shard_id: &str) -> Option<NodeId> {
        self.members
            .iter()
            .max_by_key(|node| score(type_name, shard_id, **node))
            .copied()
    }
}

/// How well one member suits one shard. Highest wins.
///
/// FNV-1a, spelled out here rather than taken from [`DefaultHasher`], whose
/// output is explicitly not stable across Rust releases. Every node has to
/// reach the same answer or one shard has two live hosts, so this must not
/// depend on the compiler that built the node asking.
///
/// [`DefaultHasher`]: std::collections::hash_map::DefaultHasher
fn score(type_name: &str, shard_id: &str, node: NodeId) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    /// Between the fields, so that a type and shard whose text runs together
    /// one way cannot score as another pair that runs together the same way.
    /// `0xff` is never a byte of UTF-8, so no id can contain one.
    const SEPARATOR: u8 = 0xff;

    let mut hash = OFFSET;
    let mut eat = |byte: u8| {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    };
    for byte in type_name.bytes() {
        eat(byte);
    }
    eat(SEPARATOR);
    for byte in shard_id.bytes() {
        eat(byte);
    }
    eat(SEPARATOR);
    for byte in node.0.to_be_bytes() {
        eat(byte);
    }
    hash
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]
mod tests {
    use super::*;

    fn table_with(nodes: &[u64]) -> PlacementTable {
        let mut t = PlacementTable::new();
        t.set_members(nodes.iter().map(|n| NodeId(*n)));
        t
    }

    /// Every shard here is of one type, so a test reads as the shard ids a
    /// placement policy would hand out.
    const TYPE: &str = "counter";

    fn shards(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("c{i}")).collect()
    }

    fn owner(t: &PlacementTable, shard: &str) -> Option<NodeId> {
        t.owner_of(TYPE, shard)
    }

    /// The invariant the whole type exists to hold, and the one a node-local
    /// memory of what it had hosted used to break: two nodes with the same live
    /// set answer the same for every shard. Nothing else is an input, so there
    /// is nothing else they can differ by.
    #[tokio::test]
    async fn two_nodes_with_the_same_live_set_agree_on_every_shard() {
        let a = table_with(&[1, 2, 3]);
        let b = table_with(&[3, 1, 2]); // same members, arriving in another order
        let all = shards(200);
        let disagreed: Vec<&String> = all
            .iter()
            .filter(|shard| owner(&a, shard) != owner(&b, shard))
            .collect();
        assert!(disagreed.is_empty(), "nodes disagreed about {disagreed:?}");
    }

    /// **The mapping itself, pinned.** Two nodes only agree if they compute the
    /// same numbers, so what this scheme answers is part of the wire contract
    /// even though nothing about it is written down anywhere else.
    ///
    /// It was previously `DefaultHasher`, whose output the standard library
    /// explicitly does not promise across releases — so two nodes built by
    /// different toolchains could each believe they owned one shard. A literal
    /// expectation is the only thing that catches that class of change, because
    /// every other property here holds just as well for a hash that has
    /// silently become a different hash.
    #[tokio::test]
    async fn the_mapping_is_fixed() {
        let three = table_with(&[1, 2, 3]);
        for (shard, expected) in [("c0", 3), ("c1", 1), ("c2", 1), ("c3", 2), ("c4", 3)] {
            assert_eq!(
                owner(&three, shard),
                Some(NodeId(expected)),
                "the placement of {shard} moved"
            );
        }

        // The type is part of the key, so two types sharing a shard id are
        // placed independently rather than dragged onto one node together.
        let five = table_with(&[1, 2, 3, 4, 5]);
        for (type_name, expected) in [("counter", 1), ("session", 5), ("supervisor", 5)] {
            assert_eq!(
                five.owner_of(type_name, "7"),
                Some(NodeId(expected)),
                "the placement of {type_name}/7 moved"
            );
        }
    }

    /// Losing a member moves only what was on it. A scheme that reshuffled
    /// everything would make one node's death a cluster-wide migration — and it
    /// is most of what the assignment record was there to avoid.
    #[tokio::test]
    async fn losing_a_member_only_moves_its_own_shards() {
        let mut t = table_with(&[1, 2, 3]);
        let all = shards(200);
        let placed: Vec<_> = all
            .iter()
            .map(|shard| (shard, owner(&t, shard).unwrap()))
            .collect();

        t.set_members([NodeId(1), NodeId(2)]);

        let moved = placed
            .iter()
            .filter(|(shard, was)| owner(&t, shard) != Some(*was))
            .count();
        let were_on_three = placed.iter().filter(|(_, was)| *was == NodeId(3)).count();
        assert_eq!(
            moved, were_on_three,
            "only shards hosted on the lost node should move"
        );
    }

    /// Gaining one is the mirror, and it is the case the assignment record was
    /// hiding: a shard does move when the cluster grows, and every node has to
    /// see it move at the same moment.
    #[tokio::test]
    async fn gaining_a_member_moves_only_what_lands_on_it() {
        let mut t = table_with(&[1, 2, 3]);
        let all = shards(200);
        let placed: Vec<_> = all
            .iter()
            .map(|shard| (shard, owner(&t, shard).unwrap()))
            .collect();

        t.set_members([NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);

        let moved: Vec<_> = placed
            .iter()
            .filter(|(shard, was)| owner(&t, shard) != Some(*was))
            .collect();
        assert!(
            moved
                .iter()
                .all(|(shard, _)| owner(&t, shard) == Some(NodeId(4))),
            "a shard moved somewhere other than the node that joined"
        );
        assert!(!moved.is_empty(), "a new member took nothing at all");
    }

    /// A stood-down node has no members it may use, and says so rather than
    /// naming itself.
    #[tokio::test]
    async fn nothing_is_hosted_when_nothing_is_live() {
        let t = PlacementTable::new();
        assert_eq!(owner(&t, "c1"), None);
    }

    /// Shards spread over the cluster rather than piling onto whichever node
    /// hashes highest overall — the property that makes this a placement
    /// strategy rather than an elaborate way to pick one node.
    #[tokio::test]
    async fn shards_spread_across_the_members() {
        let t = table_with(&[1, 2, 3]);
        for node in [1, 2, 3] {
            let mine = shards(300)
                .iter()
                .filter(|shard| owner(&t, shard) == Some(NodeId(node)))
                .count();
            assert!(mine > 50, "node {node} was given only {mine} of 300");
        }
    }

    /// The separator between the fields. Without one, a type and shard whose
    /// text runs together the same way would be one key — so `ab`/`c` and
    /// `a`/`bc` would be placed as though they were the same shard.
    #[tokio::test]
    async fn the_fields_cannot_run_together() {
        let t = table_with(&[1, 2, 3, 4, 5, 6, 7]);
        assert_ne!(t.owner_of("ab", "c"), t.owner_of("a", "bc"));
    }
}
