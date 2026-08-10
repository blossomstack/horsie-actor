use crate::envelope::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Which node an instance is assigned to.
///
/// Deliberately does *not* carry an epoch. The epoch is minted by the journal
/// when the assigned node actually claims the log, because that is the only
/// place it can be both durable and monotonic across a total restart. An
/// assignment is a decision about who should try; the claim is what makes it
/// true.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub node: NodeId,
}

/// One instance's identity in the table: actor kind plus instance id.
pub type InstanceKey = (String, String);

/// A decision to record. Applying these in the same order on every node is what
/// makes the table agree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementCommand {
    /// A node joins.
    NodeUp { node: NodeId },
    /// Assign an unassigned instance. Deliberately a no-op when it is already
    /// assigned to a live node, so a race between two nodes proposing the same
    /// assignment resolves to whichever the log ordered first rather than
    /// flapping.
    Assign {
        kind: String,
        id: String,
        node: NodeId,
    },
    /// Give up an instance — a graceful handover, or an idle unload.
    Release { kind: String, id: String },
    /// A node is gone. Releases everything it owned, so the next message to any
    /// of those instances reassigns it somewhere alive.
    NodeDown { node: NodeId },
}

/// What applying a command actually changed.
///
/// Returned rather than inferred so a caller can tell "I got the assignment"
/// from "somebody else already had it" without re-reading the table and racing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementEffect {
    /// The instance is now assigned to this node.
    Assigned(NodeId),
    /// It was already assigned to this node, and still is.
    AlreadyAssigned(NodeId),
    /// The command changed nothing worth reporting.
    NoChange,
    /// A node's assignments were released.
    Released(usize),
}

/// Who owns what, and who is alive.
///
/// A plain data structure with no consensus in it. Replicating it is somebody
/// else's job; this type only has to be deterministic, so that applying the same
/// commands in the same order anywhere produces the same table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementTable {
    assignments: BTreeMap<InstanceKey, Assignment>,
    members: BTreeSet<NodeId>,
}

impl PlacementTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one decision.
    pub fn apply(&mut self, cmd: PlacementCommand) -> PlacementEffect {
        match cmd {
            PlacementCommand::NodeUp { node } => {
                // Idempotent: a node re-announcing itself is normal after a
                // reconnect, and rejoining must not disturb what it already
                // holds.
                self.members.insert(node);
                PlacementEffect::NoChange
            }
            PlacementCommand::Assign { kind, id, node } => {
                let key = (kind, id);
                match self.assignments.get(&key) {
                    // Held by a node that is still alive: leave it. Reassigning
                    // a live instance would give two hosts a reason to claim the
                    // same log, and the fence would then lock one of them out
                    // mid-turn for no benefit.
                    Some(existing) if self.members.contains(&existing.node) => {
                        PlacementEffect::AlreadyAssigned(existing.node)
                    }
                    _ => {
                        self.assignments.insert(key, Assignment { node });
                        PlacementEffect::Assigned(node)
                    }
                }
            }
            PlacementCommand::Release { kind, id } => {
                if self.assignments.remove(&(kind, id)).is_some() {
                    PlacementEffect::Released(1)
                } else {
                    PlacementEffect::NoChange
                }
            }
            PlacementCommand::NodeDown { node } => {
                self.members.remove(&node);
                let doomed: Vec<InstanceKey> = self
                    .assignments
                    .iter()
                    .filter(|(_, a)| a.node == node)
                    .map(|(k, _)| k.clone())
                    .collect();
                let count = doomed.len();
                for key in doomed {
                    self.assignments.remove(&key);
                }
                PlacementEffect::Released(count)
            }
        }
    }

    /// The node assigned to this instance, if any.
    #[must_use]
    pub fn owner(&self, kind: &str, id: &str) -> Option<NodeId> {
        self.assignments
            .get(&(kind.to_owned(), id.to_owned()))
            .map(|a| a.node)
    }

    /// Live members.
    #[must_use]
    pub fn members(&self) -> &BTreeSet<NodeId> {
        &self.members
    }

    /// Everything assigned to `node`.
    #[must_use]
    pub fn assigned_to(&self, node: NodeId) -> Vec<InstanceKey> {
        self.assignments
            .iter()
            .filter(|(_, a)| a.node == node)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Pick a node to host a new instance.
    ///
    /// Rendezvous-style: hash the key across the live members so the same
    /// instance lands on the same node from every node's point of view, and so
    /// losing a member only moves the instances that were on it. Returns `None`
    /// when nothing is alive.
    #[must_use]
    pub fn candidate(&self, kind: &str, id: &str) -> Option<NodeId> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        self.members
            .iter()
            .max_by_key(|node| {
                let mut h = DefaultHasher::new();
                kind.hash(&mut h);
                id.hash(&mut h);
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
        for n in nodes {
            t.apply(PlacementCommand::NodeUp { node: NodeId(*n) });
        }
        t
    }

    fn assign(t: &mut PlacementTable, id: &str, node: u64) -> PlacementEffect {
        t.apply(PlacementCommand::Assign {
            kind: "counter".into(),
            id: id.into(),
            node: NodeId(node),
        })
    }

    #[tokio::test]
    async fn an_unassigned_instance_is_assigned() {
        let mut t = table_with(&[1, 2]);
        assert_eq!(
            assign(&mut t, "c1", 1),
            PlacementEffect::Assigned(NodeId(1))
        );
        assert_eq!(t.owner("counter", "c1"), Some(NodeId(1)));
    }

    /// An instance already held by a live node stays put. Reassigning it would
    /// give two hosts reason to claim one log, and the fence would then lock one
    /// out mid-turn for no benefit — so the second proposer is told who has it
    /// rather than taking it.
    #[tokio::test]
    async fn an_instance_held_by_a_live_node_is_not_reassigned() {
        let mut t = table_with(&[1, 2]);
        assign(&mut t, "c1", 1);
        assert_eq!(
            assign(&mut t, "c1", 2),
            PlacementEffect::AlreadyAssigned(NodeId(1))
        );
        assert_eq!(t.owner("counter", "c1"), Some(NodeId(1)));
    }

    /// An instance stranded on a node nobody has declared down yet is still
    /// takeable — otherwise a missed `NodeDown` would strand it forever.
    #[tokio::test]
    async fn an_instance_on_a_non_member_can_be_taken_over() {
        let mut t = table_with(&[1]);
        assign(&mut t, "c1", 1);
        // Node 2 was never a member, and node 1 leaves without releasing.
        t.members.remove(&NodeId(1));
        assert_eq!(
            assign(&mut t, "c1", 2),
            PlacementEffect::Assigned(NodeId(2))
        );
    }

    #[tokio::test]
    async fn node_down_releases_everything_that_node_held() {
        let mut t = table_with(&[1, 2]);
        assign(&mut t, "c1", 1);
        assign(&mut t, "c2", 1);
        assign(&mut t, "c3", 2);

        assert_eq!(
            t.apply(PlacementCommand::NodeDown { node: NodeId(1) }),
            PlacementEffect::Released(2)
        );
        assert_eq!(t.owner("counter", "c1"), None);
        assert_eq!(t.owner("counter", "c2"), None);
        // Untouched: it was somebody else's.
        assert_eq!(t.owner("counter", "c3"), Some(NodeId(2)));
        assert!(!t.members().contains(&NodeId(1)));
    }

    #[tokio::test]
    async fn release_frees_one_instance() {
        let mut t = table_with(&[1]);
        assign(&mut t, "c1", 1);
        assert_eq!(
            t.apply(PlacementCommand::Release {
                kind: "counter".into(),
                id: "c1".into()
            }),
            PlacementEffect::Released(1)
        );
        assert_eq!(t.owner("counter", "c1"), None);
    }

    /// Determinism is the whole contract: replicating this type means replaying
    /// commands, so the same sequence must produce the same table anywhere.
    #[tokio::test]
    async fn applying_the_same_commands_produces_the_same_table() {
        let cmds = vec![
            PlacementCommand::NodeUp { node: NodeId(1) },
            PlacementCommand::NodeUp { node: NodeId(2) },
            PlacementCommand::Assign {
                kind: "counter".into(),
                id: "c1".into(),
                node: NodeId(1),
            },
            PlacementCommand::Assign {
                kind: "counter".into(),
                id: "c2".into(),
                node: NodeId(2),
            },
            PlacementCommand::NodeDown { node: NodeId(1) },
        ];
        let mut a = PlacementTable::new();
        let mut b = PlacementTable::new();
        for c in cmds {
            a.apply(c.clone());
            b.apply(c);
        }
        assert_eq!(a, b);
    }

    /// Every node picks the same host for an instance, without talking to each
    /// other — which is what keeps them from proposing conflicting assignments
    /// in the common case.
    #[tokio::test]
    async fn candidate_selection_agrees_across_nodes() {
        let a = table_with(&[1, 2, 3]);
        let b = table_with(&[3, 1, 2]); // same members, different insert order
        for id in ["c1", "c2", "c3", "c4", "c5"] {
            assert_eq!(a.candidate("counter", id), b.candidate("counter", id));
        }
    }

    /// Losing a member moves only what was on it. A scheme that reshuffled
    /// everything would make one node's death a cluster-wide migration.
    #[tokio::test]
    async fn losing_a_member_only_moves_its_own_instances() {
        let mut before = table_with(&[1, 2, 3]);
        let ids: Vec<String> = (0..200).map(|i| format!("c{i}")).collect();
        let placed: Vec<_> = ids
            .iter()
            .map(|id| (id, before.candidate("counter", id).unwrap()))
            .collect();

        before.apply(PlacementCommand::NodeDown { node: NodeId(3) });

        let moved = placed
            .iter()
            .filter(|(id, was)| before.candidate("counter", id) != Some(*was))
            .count();
        let were_on_three = placed.iter().filter(|(_, was)| *was == NodeId(3)).count();
        assert_eq!(
            moved, were_on_three,
            "only instances hosted on the lost node should move"
        );
    }

    #[tokio::test]
    async fn candidate_is_none_with_no_members() {
        let t = PlacementTable::new();
        assert_eq!(t.candidate("counter", "c1"), None);
    }
}
