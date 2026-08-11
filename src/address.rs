//! Settings per address: which paths are clustered, and why.
//!
//! Matched on the **address**, not on the actor's type, because resolution
//! happens before the actor exists. A node asked for `/acct-7/session-3` has to
//! decide whether that path is clustered and which host owns it with nothing at
//! the path to ask. Keyed by type that is undecidable without a path-to-type
//! registry — a second record of a fact this design keeps in one place. Matched
//! on the path it is a pure function of the string.
//!
//! It also expresses a case a type cannot: one actor type can run both as a
//! session's child and standalone. One type, two places in the tree, and only
//! one of them should ever be clustered.

use crate::path::ActorPath;
use std::cmp::Ordering;
use std::fmt;
use thiserror::Error;

/// One segment of a pattern.
#[derive(Clone, PartialEq, Eq)]
enum Segment {
    /// Matches this name and no other.
    Literal(String),
    /// Matches any one name. Deliberately *one*: `/*` is every top-level actor
    /// and `/*/*` is every child of one, so the two never overlap and a pattern
    /// says at what depth it applies.
    Any,
}

/// A set of addresses, written as a path with `*` for "any one name".
///
/// `/*` is every top-level actor; `/acct-7/*` is every child of that one
/// account; `/*/*` is every child of every top-level actor.
#[derive(Clone, PartialEq, Eq)]
pub struct AddressPattern {
    segments: Vec<Segment>,
}

/// Why a pattern could not be read.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PatternError {
    /// Patterns are absolute, like the paths they match.
    #[error("a pattern must start with '/': '{0}'")]
    NotAbsolute(String),

    /// `//` or a trailing `/`. An empty segment cannot match a name, and
    /// silently dropping it would make two different-looking patterns the same.
    #[error("'{0}' has an empty segment")]
    EmptySegment(String),
}

impl AddressPattern {
    /// Read a pattern from `/a/*/c`. `/` is the root, and matches nothing else.
    ///
    /// # Errors
    /// If it is not absolute, or has an empty segment.
    pub fn parse(text: &str) -> Result<Self, PatternError> {
        let Some(rest) = text.strip_prefix('/') else {
            return Err(PatternError::NotAbsolute(text.to_owned()));
        };
        if rest.is_empty() {
            return Ok(Self {
                segments: Vec::new(),
            });
        }
        let mut segments = Vec::new();
        for part in rest.split('/') {
            if part.is_empty() {
                return Err(PatternError::EmptySegment(text.to_owned()));
            }
            segments.push(if part == "*" {
                Segment::Any
            } else {
                Segment::Literal(part.to_owned())
            });
        }
        Ok(Self { segments })
    }

    /// Whether `path` is one of the addresses this describes.
    ///
    /// Depth is part of the match: a pattern matches paths of its own length and
    /// no others.
    #[must_use]
    pub fn matches(&self, path: &ActorPath) -> bool {
        let names = path.segments();
        self.segments.len() == names.len()
            && self
                .segments
                .iter()
                .zip(names)
                .all(|(segment, name)| match segment {
                    Segment::Literal(literal) => literal == name,
                    Segment::Any => true,
                })
    }

    /// Which of two patterns is more specific.
    ///
    /// A literal beats a wildcard at the first position where they differ, so
    /// `/acct-7/*` is more specific than `/*/session-3`: naming the account
    /// narrows the address sooner. Only ever asked of two patterns that match
    /// the same path, which makes them the same length — the length comparison
    /// is a total-order backstop, not a rule anyone should rely on.
    ///
    /// `Equal` only for patterns that are the same shape, which a table refuses
    /// to hold twice. That is what keeps declaration order out of the answer.
    fn specificity(&self, other: &Self) -> Ordering {
        for (a, b) in self.segments.iter().zip(&other.segments) {
            match (a, b) {
                (Segment::Literal(_), Segment::Any) => return Ordering::Greater,
                (Segment::Any, Segment::Literal(_)) => return Ordering::Less,
                _ => {}
            }
        }
        self.segments.len().cmp(&other.segments.len())
    }
}

impl fmt::Display for AddressPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            return f.write_str("/");
        }
        for segment in &self.segments {
            match segment {
                Segment::Literal(literal) => write!(f, "/{literal}")?,
                Segment::Any => f.write_str("/*")?,
            }
        }
        Ok(())
    }
}

impl fmt::Debug for AddressPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// What an entry says about the addresses it matches.
///
/// Every field is optional, and that is the whole merge rule: an entry that does
/// not mention a setting leaves whatever a broader entry said about it alone.
/// Clustering is *one setting*, not the shape of this type — the next one lands
/// beside it rather than replacing the structure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActorSettings {
    /// Whether actors at these addresses are cluster singletons: placed by the
    /// cluster, and reachable from any node. Unset means local.
    pub clustered: Option<bool>,
}

impl ActorSettings {
    /// Cluster the addresses this is attached to.
    #[must_use]
    pub fn clustered() -> Self {
        Self {
            clustered: Some(true),
        }
    }

    /// Keep the addresses this is attached to local, overriding a broader entry
    /// that clustered them.
    #[must_use]
    pub fn local() -> Self {
        Self {
            clustered: Some(false),
        }
    }
}

/// One setting's value, and which entry decided it.
///
/// The provenance is not a nicety. Patterns compose invisibly, so the first
/// surprising configuration is unanswerable without it — and config that cannot
/// be explained gets worked around rather than fixed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decided<T> {
    /// What applies.
    pub value: T,
    /// The pattern that set it, or `None` if nothing matched and this is the
    /// default.
    pub set_by: Option<AddressPattern>,
}

/// Everything that applies at one address, and where each part came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Whether this address is a cluster singleton.
    pub clustered: Decided<bool>,
}

/// Two entries share a pattern, so which of them wins would come down to the
/// order they were written in.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("'{0}' is configured twice")]
pub struct DuplicatePattern(pub AddressPattern);

/// The configured patterns, read once at startup.
///
/// An empty table is the single-node case: nothing matches, every address takes
/// the default, and the default is local. So a deployment that never mentions
/// clustering behaves exactly as it did before any of this existed, and the same
/// binary serves both.
#[derive(Debug, Clone, Default)]
pub struct SettingsTable {
    entries: Vec<(AddressPattern, ActorSettings)>,
}

impl SettingsTable {
    /// A table with nothing in it: every address local.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an entry.
    ///
    /// # Errors
    /// If the same pattern is already configured. Merging two entries of equal
    /// specificity would have to fall back on declaration order, and an order
    /// that decides anything is what turns a config file into an argument.
    pub fn with(
        mut self,
        pattern: AddressPattern,
        settings: ActorSettings,
    ) -> Result<Self, DuplicatePattern> {
        if self.entries.iter().any(|(p, _)| *p == pattern) {
            return Err(DuplicatePattern(pattern));
        }
        self.entries.push((pattern, settings));
        Ok(self)
    }

    /// What applies at `path`, and which entry set each part of it.
    ///
    /// Every matching entry is merged **per field**, most specific last, so a
    /// narrow pattern overrides the fields it names and leaves the rest alone.
    /// Matching an entry does not discard what a broader one already said.
    #[must_use]
    pub fn at(&self, path: &ActorPath) -> Settings {
        let mut matching: Vec<&(AddressPattern, ActorSettings)> = self
            .entries
            .iter()
            .filter(|(pattern, _)| pattern.matches(path))
            .collect();
        matching.sort_by(|(a, _), (b, _)| a.specificity(b));

        let mut clustered = Decided {
            value: false,
            set_by: None,
        };
        for (pattern, settings) in matching {
            if let Some(value) = settings.clustered {
                clustered = Decided {
                    value,
                    set_by: Some(pattern.clone()),
                };
            }
        }
        Settings { clustered }
    }

    /// Whether anything is configured at all — the single-node shortcut.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn pattern(text: &str) -> AddressPattern {
        AddressPattern::parse(text).unwrap()
    }

    fn path(segments: &[&str]) -> ActorPath {
        segments
            .iter()
            .fold(ActorPath::root(), |path, name| path.child(name))
    }

    #[test]
    fn a_pattern_must_be_absolute_and_have_no_empty_segments() {
        assert_eq!(
            AddressPattern::parse("acct-7"),
            Err(PatternError::NotAbsolute("acct-7".to_owned()))
        );
        assert_eq!(
            AddressPattern::parse("/a//b"),
            Err(PatternError::EmptySegment("/a//b".to_owned()))
        );
        assert_eq!(
            AddressPattern::parse("/a/"),
            Err(PatternError::EmptySegment("/a/".to_owned()))
        );
    }

    #[test]
    fn a_pattern_round_trips_through_its_display() {
        for text in ["/", "/acct-7", "/*", "/*/*", "/acct-7/*/agent-main"] {
            assert_eq!(pattern(text).to_string(), text);
        }
    }

    /// A wildcard is one name, so depth is part of the match. This is what lets
    /// `/*` and `/*/*` say different things without overlapping.
    #[test]
    fn a_wildcard_matches_exactly_one_segment() {
        assert!(pattern("/*").matches(&path(&["acct-7"])));
        assert!(!pattern("/*").matches(&path(&["acct-7", "session-3"])));
        assert!(pattern("/*/*").matches(&path(&["acct-7", "session-3"])));
        assert!(!pattern("/*/*").matches(&path(&["acct-7"])));
    }

    #[test]
    fn a_literal_matches_only_itself() {
        assert!(pattern("/acct-7/*").matches(&path(&["acct-7", "session-3"])));
        assert!(!pattern("/acct-7/*").matches(&path(&["acct-8", "session-3"])));
    }

    #[test]
    fn the_root_pattern_matches_only_the_root() {
        assert!(pattern("/").matches(&ActorPath::root()));
        assert!(!pattern("/").matches(&path(&["acct-7"])));
    }

    /// Nothing configured means every address takes the default, and the default
    /// is local. This is the single-node deployment, and it mentions none of
    /// this.
    #[test]
    fn an_empty_table_leaves_everything_local() {
        let table = SettingsTable::new();
        let settings = table.at(&path(&["acct-7"]));
        assert!(!settings.clustered.value);
        assert_eq!(settings.clustered.set_by, None);
    }

    #[test]
    fn a_matching_entry_applies_and_says_so() {
        let table = SettingsTable::new()
            .with(pattern("/*"), ActorSettings::clustered())
            .unwrap();
        let settings = table.at(&path(&["acct-7"]));
        assert!(settings.clustered.value);
        assert_eq!(settings.clustered.set_by, Some(pattern("/*")));
    }

    /// The more specific entry wins, whichever order the two were written in.
    /// Declaration order deciding anything is what makes a config file an
    /// argument.
    #[test]
    fn the_most_specific_pattern_wins_in_either_order() {
        let broad = (pattern("/*"), ActorSettings::clustered());
        let narrow = (pattern("/acct-7"), ActorSettings::local());

        for entries in [
            vec![broad.clone(), narrow.clone()],
            vec![narrow.clone(), broad.clone()],
        ] {
            let table = entries
                .into_iter()
                .try_fold(SettingsTable::new(), |t, (p, s)| t.with(p, s))
                .unwrap();
            let settings = table.at(&path(&["acct-7"]));
            assert!(!settings.clustered.value);
            assert_eq!(settings.clustered.set_by, Some(pattern("/acct-7")));
            // The broad entry still applies everywhere it was not overridden.
            assert!(table.at(&path(&["acct-8"])).clustered.value);
        }
    }

    /// A literal beats a wildcard at the first position they differ, so two
    /// patterns of the same shape still have an answer that is not "whichever
    /// was written last".
    #[test]
    fn an_earlier_literal_is_more_specific() {
        let table = SettingsTable::new()
            .with(pattern("/acct-7/*"), ActorSettings::clustered())
            .unwrap()
            .with(pattern("/*/session-3"), ActorSettings::local())
            .unwrap();
        let settings = table.at(&path(&["acct-7", "session-3"]));
        assert!(settings.clustered.value);
        assert_eq!(settings.clustered.set_by, Some(pattern("/acct-7/*")));
    }

    /// One pattern twice would put declaration order back in charge, so a table
    /// refuses to hold it.
    #[test]
    fn a_pattern_cannot_be_configured_twice() {
        let err = SettingsTable::new()
            .with(pattern("/*"), ActorSettings::clustered())
            .unwrap()
            .with(pattern("/*"), ActorSettings::local())
            .unwrap_err();
        assert_eq!(err, DuplicatePattern(pattern("/*")));
    }
}
