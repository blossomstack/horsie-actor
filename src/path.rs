use std::fmt;
use std::sync::Arc;

/// Where an actor is in the tree, and therefore what it *is*.
///
/// `/` is the root. A top-level actor is a child of root, created by the system;
/// every other actor is created by its parent, under its parent's path. So
/// `/acct-7/session-3/agent-main` names one actor for as long as that actor
/// exists — across a restart, a reactivation after an idle offload, and (later)
/// a move to another host.
///
/// That is the whole reason this type exists. An [`ActorRef`] used to be a
/// mailbox handle, which meant it named an *instance* and died with it. It now
/// names a path, and the mailbox is a cache.
///
/// Segments are derived by whoever creates the actor — a session is its uuid, an
/// agent is `main` or its own uuid — never allocated. A name that changed across
/// a reload would break every held reference on the first recreate.
///
/// [`ActorRef`]: crate::ActorRef
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActorPath {
    /// Root is the empty slice. `Arc` so cloning a path — which every ref does,
    /// and every send reads — costs a refcount rather than the segments.
    segments: Arc<[String]>,
}

impl ActorPath {
    /// The root path, `/`.
    #[must_use]
    pub fn root() -> Self {
        Self {
            segments: Arc::from(Vec::new()),
        }
    }

    /// The path of a child of this one named `name`.
    ///
    /// `name` is expected to be a valid segment; [`is_valid_name`] is what
    /// checks that, and every entry point that takes a name from a caller runs
    /// it first.
    #[must_use]
    pub fn child(&self, name: &str) -> Self {
        let mut segments = Vec::with_capacity(self.segments.len() + 1);
        segments.extend_from_slice(&self.segments);
        segments.push(name.to_owned());
        Self {
            segments: Arc::from(segments),
        }
    }

    /// This path's parent, or `None` at the root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        let (_, head) = self.segments.split_last()?;
        Some(Self {
            segments: Arc::from(head),
        })
    }

    /// Whether this is the root path.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    /// The last segment — the actor's own name — or `None` at the root.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    /// Every segment, root-first.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Whether `prefix` is this path or one of its ancestors.
    ///
    /// What "stop everything under `/acct-7/session-3`" is a scan for, and what
    /// finding the deepest clustered ancestor of a path will be.
    #[must_use]
    pub fn starts_with(&self, prefix: &Self) -> bool {
        prefix.segments.len() <= self.segments.len()
            && prefix
                .segments
                .iter()
                .zip(self.segments.iter())
                .all(|(a, b)| a == b)
    }
}

/// Whether `name` can be one segment of a path.
///
/// Empty names and names containing the separator are rejected, so that a path
/// has exactly one reading: `/a/b` is two segments and can never also be one
/// segment called `a/b`.
#[must_use]
pub fn is_valid_name(name: &str) -> bool {
    !name.is_empty() && !name.contains('/')
}

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            return f.write_str("/");
        }
        for segment in self.segments.iter() {
            write!(f, "/{segment}")?;
        }
        Ok(())
    }
}

// A path is its display form. The derived `Debug` would print the segment
// vector, which is the same information spelled less usefully in every log line
// and assertion failure.
impl fmt::Debug for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn root_displays_as_a_single_slash() {
        assert_eq!(ActorPath::root().to_string(), "/");
        assert!(ActorPath::root().is_root());
        assert_eq!(ActorPath::root().name(), None);
        assert_eq!(ActorPath::root().parent(), None);
    }

    #[test]
    fn a_path_displays_as_its_segments() {
        let path = ActorPath::root().child("acct-7").child("session-3");
        assert_eq!(path.to_string(), "/acct-7/session-3");
        assert_eq!(path.name(), Some("session-3"));
        assert_eq!(path.segments(), ["acct-7", "session-3"]);
    }

    #[test]
    fn parent_walks_back_up_to_root() {
        let path = ActorPath::root().child("a").child("b");
        let parent = path.parent().unwrap();
        assert_eq!(parent.to_string(), "/a");
        assert_eq!(parent.parent().unwrap(), ActorPath::root());
    }

    /// The same name under different parents is a different actor. This is what
    /// makes a name only have to be unique among its siblings.
    #[test]
    fn the_same_name_under_different_parents_differs() {
        let a = ActorPath::root().child("owner-a").child("worker");
        let b = ActorPath::root().child("owner-b").child("worker");
        assert_ne!(a, b);
    }

    #[test]
    fn a_prefix_is_the_path_or_an_ancestor() {
        let path = ActorPath::root().child("a").child("b").child("c");
        assert!(path.starts_with(&ActorPath::root()));
        assert!(path.starts_with(&ActorPath::root().child("a")));
        assert!(path.starts_with(&path));
        assert!(!path.starts_with(&ActorPath::root().child("b")));
        // Longer than the path it is being tested against.
        assert!(!ActorPath::root().child("a").starts_with(&path));
    }

    /// A segment-wise prefix, not a string one: `/ab` does not live under `/a`,
    /// though `"/a"` is a prefix of `"/ab"`.
    #[test]
    fn a_prefix_is_matched_by_segment_not_by_string() {
        let path = ActorPath::root().child("ab");
        assert!(!path.starts_with(&ActorPath::root().child("a")));
    }

    #[test]
    fn a_name_may_not_be_empty_or_contain_a_separator() {
        assert!(is_valid_name("session-3"));
        assert!(!is_valid_name(""));
        assert!(!is_valid_name("a/b"));
    }
}
