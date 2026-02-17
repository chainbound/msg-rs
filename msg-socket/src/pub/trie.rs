use std::collections::hash_map::Entry;

use rustc_hash::FxHashMap;

/// A node in the prefix trie.
struct Node {
    children: FxHashMap<String, Node>,
    catch_all: bool,
    topic_end: bool,
}

impl Node {
    fn new() -> Self {
        Self { children: FxHashMap::default(), catch_all: false, topic_end: false }
    }
}

/// A prefix trie for matching topics.
///
/// This trie is used to match topics in a NATS-like system. It supports wildcards:
/// - `*` matches a single token.
/// - `>` matches one or more tokens.
pub(super) struct PrefixTrie {
    root: Node,
}

impl PrefixTrie {
    /// Creates a new, empty prefix trie.
    pub fn new() -> Self {
        Self { root: Node::new() }
    }

    /// Insert a topic into the trie. The topic follows the conventions of a NATS subject:
    /// - A period-separated string.
    /// - A wildcard can be used at any point in the topic:
    ///  - `foo.*` matches `foo.bar` but not `foo.bar.baz`.
    ///  - `foo.>` matches `foo.bar` and `foo.bar.baz`.
    pub fn insert(&mut self, topic: &str) {
        let mut node = &mut self.root;
        for token in topic.split('.') {
            node = node.children.entry(token.to_string()).or_insert(Node::new());
            // Check if this is a catch-all wildcard. If so, we mark it as such and break.
            if token == ">" {
                node.catch_all = true;
                break;
            }
        }
        node.topic_end = true;
    }

    /// Remove a topic from the trie.
    pub fn remove(&mut self, topic: &str) {
        Self::inner_remove(&mut self.root, &mut topic.split('.').collect());
    }

    fn inner_remove(current: &mut Node, tokens: &mut Vec<&str>) -> bool {
        if tokens.is_empty() {
            if current.topic_end {
                current.topic_end = false;
                return current.children.is_empty();
            }
            return false;
        }

        let token = tokens.remove(0);
        if let Entry::Occupied(mut entry) = current.children.entry(token.to_string()) &&
            Self::inner_remove(entry.get_mut(), tokens)
        {
            entry.remove_entry();
            return current.children.is_empty() && !current.topic_end;
        }

        false
    }

    /// Check if the trie contains the current topic, accounting for wildcards.
    pub fn contains(&self, topic: &str) -> bool {
        let mut current = &self.root;
        for token in topic.split('.') {
            if let Some(node) = current.children.get(token) {
                current = node;
            } else if current.children.contains_key("*") {
                current = &current.children["*"];
            } else {
                return current.catch_all;
            }
        }
        current.topic_end || current.catch_all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie_simple() {
        let mut trie = PrefixTrie::new();

        trie.insert("foo.bar.baz");
        trie.insert("foo.*.baz");

        assert!(trie.contains("foo.bar.baz"));
        assert!(trie.contains("foo.anything.baz"));
        assert!(trie.contains("foo.*.baz"));
        assert!(!trie.contains("foo.baz.bar"));
    }

    #[test]
    fn trie_remove() {
        let mut trie = PrefixTrie::new();

        trie.insert("foo.bar.baz");
        trie.insert("foo.*.baz");

        assert!(trie.contains("foo.bar.baz"));
        assert!(trie.contains("foo.anything.baz"));
        assert!(trie.contains("foo.*.baz"));
        assert!(!trie.contains("foo.baz.bar"));

        trie.remove("foo.*.baz");
        assert!(!trie.contains("foo.anything.baz"));
        assert!(!trie.contains("foo.*.baz"));

        trie.remove("foo.bar.baz");
        assert!(!trie.contains("foo.bar.baz"));
    }
}
