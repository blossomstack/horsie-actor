use crate::envelope::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Who hosts what.
///
/// **A pure function of an address and the live member set**, and nothing else.
/// That is the whole type: hand two nodes the same members and they answer
/// identically for every address, without asking each other and without
/// remembering anything.
///
/// It used to remember. A node recorded each address it had hosted and answered
/// from that record before it hashed — which meant a node that had hosted
/// something and a node that had not gave different answers from the same live
/// set. Two hosts for one instance follows directly, and the write fence can
/// only reject the second one's *writes*: both go on serving reads from state
/// the other cannot see. The record was there for stickiness, and stickiness
/// only works if it is agreed. This one was node-local.
///
/// Rendezvous hashing gives most of what that was for anyway. Losing a member
/// moves only the addresses that were on it, so an instance does not move
/// because some unrelated node came or went. What it does not give is "keep an
/// instance where it is even though the membership changed", and that is a
/// deliberate omission: buying it back means replicating the assignments through
/// consensus — a Raft write on the first message to every entity — which is a
/// separate decision that should arrive with numbers.
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

    /// Which node hosts `path`.
    ///
    /// Rendezvous-style: hash the address against each member and take the
    /// highest, so the same address lands on the same node from every node's
    /// point of view, and losing a member moves only what was on it. `None` when
    /// nothing is alive — which is the state a stood-down node is in.
    #[must_use]
    pub fn owner_of(&self, path: &str) -> Option<NodeId> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        self.members
            .iter()
            .max_by_key(|node| {
                let mut h = DefaultHasher::new();
                path.hash(&mut h);
                node.hash(&mut h);
                h.finish()
            })
            .copied()
    }
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

    /// Instances live at `/counter/<id>` throughout, so a test reads as the
    /// addresses the rest of the system uses.
    fn at(id: &str) -> String {
        format!("/counter/{id}")
    }

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("c{i}")).collect()
    }

    /// The invariant the whole type exists to hold, and the one a node-local
    /// memory of what it had hosted used to break: two nodes with the same live
    /// set answer the same for every address. Nothing else is an input, so there
    /// is nothing else they can differ by.
    #[tokio::test]
    async fn two_nodes_with_the_same_live_set_agree_on_every_address() {
        let a = table_with(&[1, 2, 3]);
        let b = table_with(&[3, 1, 2]); // same members, arriving in another order
        let addresses = ids(200);
        let disagreed: Vec<&String> = addresses
            .iter()
            .filter(|id| a.owner_of(&at(id)) != b.owner_of(&at(id)))
            .collect();
        assert!(disagreed.is_empty(), "nodes disagreed about {disagreed:?}");
    }

    /// Losing a member moves only what was on it. A scheme that reshuffled
    /// everything would make one node's death a cluster-wide migration — and it
    /// is most of what the assignment record was there to avoid.
    #[tokio::test]
    async fn losing_a_member_only_moves_its_own_instances() {
        let mut t = table_with(&[1, 2, 3]);
        let paths: Vec<String> = ids(200).iter().map(|id| at(id)).collect();
        let placed: Vec<_> = paths
            .iter()
            .map(|path| (path, t.owner_of(path).unwrap()))
            .collect();

        t.set_members([NodeId(1), NodeId(2)]);

        let moved = placed
            .iter()
            .filter(|(path, was)| t.owner_of(path) != Some(*was))
            .count();
        let were_on_three = placed.iter().filter(|(_, was)| *was == NodeId(3)).count();
        assert_eq!(
            moved, were_on_three,
            "only instances hosted on the lost node should move"
        );
    }

    /// Gaining one is the mirror, and it is the case the assignment record was
    /// hiding: a shard does move when the cluster grows, and every node has to
    /// see it move at the same moment.
    #[tokio::test]
    async fn gaining_a_member_moves_only_what_lands_on_it() {
        let mut t = table_with(&[1, 2, 3]);
        let paths: Vec<String> = ids(200).iter().map(|id| at(id)).collect();
        let placed: Vec<_> = paths
            .iter()
            .map(|path| (path, t.owner_of(path).unwrap()))
            .collect();

        t.set_members([NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);

        let moved: Vec<_> = placed
            .iter()
            .filter(|(path, was)| t.owner_of(path) != Some(*was))
            .collect();
        assert!(
            moved
                .iter()
                .all(|(path, _)| t.owner_of(path) == Some(NodeId(4))),
            "an instance moved somewhere other than the node that joined"
        );
        assert!(!moved.is_empty(), "a new member took nothing at all");
    }

    /// A stood-down node has no members it may use, and says so rather than
    /// naming itself.
    #[tokio::test]
    async fn nothing_is_hosted_when_nothing_is_live() {
        let t = PlacementTable::new();
        assert_eq!(t.owner_of(&at("c1")), None);
    }

    /// Members spread over the cluster rather than piling onto whichever node
    /// hashes highest overall — the property that makes this a placement
    /// strategy rather than an elaborate way to pick one node.
    #[tokio::test]
    async fn addresses_spread_across_the_members() {
        let t = table_with(&[1, 2, 3]);
        for node in [1, 2, 3] {
            let mine = ids(300)
                .iter()
                .filter(|id| t.owner_of(&at(id)) == Some(NodeId(node)))
                .count();
            assert!(mine > 50, "node {node} was given only {mine} of 300");
        }
    }
}
