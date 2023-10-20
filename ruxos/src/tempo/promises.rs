use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
};

use super::replica::OpId;

mod rangelist;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PromiseValue {
    Single { timestamp: u64 },
    Ranged { start: u64, end: u64 },
}

impl PartialOrd for PromiseValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self {
            Self::Single { timestamp: t1 } => match other {
                Self::Single { timestamp: t2 } => t1.partial_cmp(t2),
                Self::Ranged { start, end } => {
                    if t1 < start {
                        Some(std::cmp::Ordering::Less)
                    } else if t1 > end {
                        Some(std::cmp::Ordering::Greater)
                    } else {
                        None
                    }
                }
            },
            Self::Ranged { start: s1, end: e1 } => match other {
                Self::Single { timestamp: t2 } => {
                    if s1 < t2 {
                        Some(std::cmp::Ordering::Less)
                    } else if e1 > t2 {
                        Some(std::cmp::Ordering::Greater)
                    } else {
                        None
                    }
                }
                Self::Ranged {
                    start: s2,
                    end: _e2,
                } => s1.partial_cmp(s2),
            },
        }
    }
}

impl PromiseValue {
    pub fn can_merge(&self, other: &Self) -> bool {
        match self {
            Self::Single { timestamp: t1 } => match other {
                Self::Single { timestamp: t2 } => t1 == t2 || *t1 + 1 == *t2,
                Self::Ranged { start, end } => {
                    t1 == start || *t1 + 1 == *start || (t1 > start && t1 <= end)
                }
            },
            Self::Ranged { start, end } => match other {
                Self::Single { timestamp } => {
                    timestamp == start
                        || *timestamp + 1 == *start
                        || (timestamp > start && timestamp <= end)
                }
                Self::Ranged { start: s2, end: e2 } => start.max(s2) <= end.min(e2),
            },
        }
    }

    pub fn merge(&mut self, other: Self) {
        match self {
            Self::Single { timestamp: t1 } => match other {
                Self::Single { timestamp: t2 } => {
                    if *t1 == t2 {
                    } else {
                        let start = (*t1).min(t2);
                        let end = (*t1).max(t2);
                        *self = Self::Ranged { start, end };
                    }
                }
                Self::Ranged { start, end } => {
                    let n_start = start.min(*t1);
                    let n_end = end.max(*t1);
                    *self = Self::Ranged {
                        start: n_start,
                        end: n_end,
                    };
                }
            },
            Self::Ranged { start, end } => match other {
                Self::Single { timestamp } => {
                    *start = (*start).min(timestamp);
                    *end = (*end).max(timestamp);
                }
                Self::Ranged { start: s2, end: e2 } => {
                    *start = (*start).min(s2);
                    *end = (*end).max(e2);
                }
            },
        };
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DetachedPromises<NodeId> {
    node: NodeId,
    values: rangelist::RangeList,
}

impl<NodeId> DetachedPromises<NodeId> {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            values: rangelist::RangeList::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Adds all the timestamps t `clock + 1 <= t <= timestamp`
    pub fn add(&mut self, range: RangeInclusive<u64>) {
        if range.is_empty() {
            return;
        }

        self.values.insert(range);
    }

    pub fn iter(&self) -> impl Iterator<Item = &RangeInclusive<u64>> + '_ {
        self.values.iter()
    }

    pub fn filtered(&self, hc: &BTreeMap<NodeId, u64>) -> Self
    where
        NodeId: Clone,
    {
        let smallest = hc.values().min().copied().unwrap_or(0);

        let values = self.values.after_iter(smallest);

        let mut n_values = rangelist::RangeList::new();
        for val in values {
            n_values.insert(val.clone());
        }

        Self {
            node: self.node.clone(),
            values: n_values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AllPromises<NodeId>
where
    NodeId: Ord,
{
    nodes: BTreeMap<NodeId, rangelist::RangeList>,
}

impl<NodeId> AllPromises<NodeId>
where
    NodeId: Ord,
{
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }

    pub fn union_detached(&mut self, detached: DetachedPromises<NodeId>) {
        let promises = self
            .nodes
            .entry(detached.node)
            .or_insert_with(|| rangelist::RangeList::new());

        for promise in detached.values.iter() {
            let range = promise.clone();

            promises.insert(range);
        }
    }

    pub fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (NodeId, u64)>,
    {
        for (node, timestamp) in iter {
            let promises = self
                .nodes
                .entry(node)
                .or_insert_with(|| rangelist::RangeList::new());
            promises.insert(timestamp..=timestamp);
        }
    }

    pub fn highest_contiguous(&self) -> HighestContinuousPromise<NodeId>
    where
        NodeId: Clone,
    {
        let tmp: BTreeMap<_, _> = self
            .nodes
            .iter()
            .filter_map(|(key, node_proms)| {
                let value = node_proms.first().map(|r| *r.end())?;

                Some((key.clone(), value))
            })
            .collect();

        HighestContinuousPromise { nodes: tmp }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AttachedPromises<NodeId>
where
    NodeId: Ord,
{
    node: NodeId,
    promises: BTreeSet<(u64, OpId<NodeId>)>,
    #[cfg_attr(feature = "serde", serde(skip, default = "empty_option"))]
    last_lowest_elem: Option<(u64, OpId<NodeId>)>,
}

fn empty_option<T>() -> Option<T> {
    None
}

impl<NodeId> AttachedPromises<NodeId>
where
    NodeId: Ord + Clone,
{
    pub fn new(id: NodeId) -> Self {
        Self {
            node: id,
            promises: BTreeSet::new(),
            last_lowest_elem: None,
        }
    }

    pub fn len(&self) -> usize {
        self.promises.len()
    }

    pub fn attach(&mut self, op: OpId<NodeId>, timestamp: u64) {
        self.promises.insert((timestamp, op));
    }

    pub fn node(&self) -> &NodeId {
        &self.node
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, &OpId<NodeId>)> + '_ {
        self.promises
            .iter()
            .map(|(timestamp, opid)| (*timestamp, opid))
    }

    pub fn filtered(&mut self, hc: &BTreeMap<NodeId, u64>) -> Self
    where
        NodeId: Clone,
    {
        // Altough this is O(n), n in this case is only the number of Nodes in the system, which is
        // pretty small and should not change often so this acts more as a constant factor
        let smallest = hc.values().min().copied().unwrap_or(0);

        let n_promises: BTreeSet<_> = if let Some(prev_first) = self.last_lowest_elem.as_ref() {
            self.promises
                .range(prev_first..)
                .filter(|(key, _)| *key > smallest)
                .cloned()
                .collect()
        } else {
            self.promises
                .iter()
                .filter(|(key, _)| *key > smallest)
                .cloned()
                .collect()
        };

        self.last_lowest_elem = n_promises.first().cloned();

        Self {
            node: self.node.clone(),
            promises: n_promises,
            last_lowest_elem: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HighestContinuousPromise<NodeId> {
    nodes: BTreeMap<NodeId, u64>,
}

impl<NodeId> HighestContinuousPromise<NodeId>
where
    NodeId: Ord,
{
    pub fn new(ids: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            nodes: ids.into_iter().map(|id| (id, 0)).collect(),
        }
    }

    pub fn sorted(&self) -> Vec<u64> {
        let mut tmp: Vec<_> = self.nodes.values().copied().collect();
        tmp.sort_unstable();
        tmp
    }

    pub fn get(&self, node: &NodeId) -> u64 {
        self.nodes.get(node).copied().unwrap_or(0)
    }

    pub fn values(&self) -> impl Iterator<Item = &u64> + '_ {
        self.nodes.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pvalue_cmp() {
        assert!(PromiseValue::Single { timestamp: 0 } < PromiseValue::Single { timestamp: 1 });
        assert!(PromiseValue::Single { timestamp: 0 } < PromiseValue::Ranged { start: 1, end: 2 });
        assert!(PromiseValue::Single { timestamp: 3 } > PromiseValue::Ranged { start: 1, end: 2 });
        assert!(
            !(PromiseValue::Single { timestamp: 1 } < PromiseValue::Ranged { start: 1, end: 2 })
        );

        assert!(PromiseValue::Ranged { start: 0, end: 2 } < PromiseValue::Single { timestamp: 3 });
        assert!(
            PromiseValue::Ranged { start: 0, end: 2 } < PromiseValue::Ranged { start: 3, end: 5 }
        );
    }

    #[test]
    fn pvalue_can_merge() {
        assert!(
            PromiseValue::Single { timestamp: 1 }.can_merge(&PromiseValue::Single { timestamp: 1 })
        );
        assert!(
            PromiseValue::Single { timestamp: 1 }.can_merge(&PromiseValue::Single { timestamp: 2 })
        );
        assert!(PromiseValue::Single { timestamp: 1 }
            .can_merge(&PromiseValue::Ranged { start: 1, end: 3 }));
        assert!(PromiseValue::Single { timestamp: 1 }
            .can_merge(&PromiseValue::Ranged { start: 2, end: 3 }));

        assert!(PromiseValue::Ranged { start: 1, end: 3 }
            .can_merge(&PromiseValue::Single { timestamp: 1 }));
        assert!(PromiseValue::Ranged { start: 1, end: 3 }
            .can_merge(&PromiseValue::Single { timestamp: 2 }));
        assert!(PromiseValue::Ranged { start: 1, end: 3 }
            .can_merge(&PromiseValue::Single { timestamp: 3 }));
        assert!(PromiseValue::Ranged { start: 1, end: 3 }
            .can_merge(&PromiseValue::Ranged { start: 1, end: 4 }));
        assert!(PromiseValue::Ranged { start: 1, end: 3 }
            .can_merge(&PromiseValue::Ranged { start: 3, end: 5 }));
    }

    #[test]
    fn all_promises() {
        let mut all_proms = AllPromises::new();

        all_proms.extend([(0i32, 1), (0i32, 2), (0i32, 3), (0i32, 5)]);

        let hc = all_proms.highest_contiguous();
        assert_eq!(vec![3], hc.sorted());
    }
}
