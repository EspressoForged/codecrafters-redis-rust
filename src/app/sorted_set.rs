use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

/// Represents an item within the BTreeSet for sorting.
/// Implements Ord to sort by score first, then lexicographically by member.
#[derive(Debug, Clone, Eq)]
pub struct SortedSetItem {
    score: ordered_float::OrderedFloat<f64>,
    member: Bytes,
}

impl PartialEq for SortedSetItem {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.member == other.member
    }
}

impl PartialOrd for SortedSetItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedSetItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

/// Represents a Redis Sorted Set data structure.
/// This struct itself is not thread-safe and must be wrapped in a lock.
#[derive(Debug, Default)]
pub struct SortedSet {
    scores: HashMap<Bytes, f64>,
    sorted_items: BTreeSet<SortedSetItem>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or updates a member in the sorted set.
    /// Returns 1 if a new member was added, 0 if an existing member was updated.
    pub fn add(&mut self, score: f64, member: Bytes) -> usize {
        // If the member exists, remove its old entry from the BTreeSet first.
        if let Some(old_score) = self.scores.get(&member) {
            self.sorted_items.remove(&SortedSetItem {
                score: ordered_float::OrderedFloat(*old_score),
                member: member.clone(),
            });
        }

        let new_item = SortedSetItem {
            score: ordered_float::OrderedFloat(score),
            member: member.clone(),
        };

        // Insert the new score into the HashMap and the new item into the BTreeSet.
        let is_new = self.scores.insert(member, score).is_none();
        self.sorted_items.insert(new_item);

        if is_new {
            1
        } else {
            0
        }
    }

    /// Helper to get all members for iteration in GEOSEARCH.
    pub fn get_all_members(&self) -> Vec<Bytes> {
        self.scores.iter().map(|entry| entry.0.clone()).collect()
    }

    /// Returns the 0-based rank of a member.
    pub fn rank_of(&self, member: &Bytes) -> Option<usize> {
        let score = self.scores.get(member)?;
        let item = SortedSetItem {
            score: ordered_float::OrderedFloat(*score),
            member: member.clone(),
        };
        self.sorted_items.iter().position(|i| i == &item)
    }

    /// Returns a range of members by their 0-based rank.
    pub fn get_range(&self, start: usize, stop: usize) -> Vec<Bytes> {
        self.sorted_items
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|item| item.member.clone())
            .collect()
    }

    /// Returns the score of a specific member.
    pub fn score_of(&self, member: &Bytes) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// Removes a member from the sorted set.
    /// Returns 1 if the member was removed, 0 otherwise.
    pub fn remove(&mut self, member: &Bytes) -> usize {
        if let Some(score) = self.scores.remove(member) {
            self.sorted_items.remove(&SortedSetItem {
                score: ordered_float::OrderedFloat(score),
                member: member.clone(),
            });
            1
        } else {
            0
        }
    }

    /// Returns the number of members in the sorted set.
    pub fn cardinality(&self) -> usize {
        self.scores.len()
    }
}
