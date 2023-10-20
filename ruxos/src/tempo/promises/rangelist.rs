use std::ops::RangeInclusive;

/// A RangeList is a relatively simple data structure that allows you to store multiple Ranges of
/// u64's and merges these ranges together if possible
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RangeList {
    ranges: Vec<RangeInclusive<u64>>,
}

impl RangeList {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    fn raw_insert(&mut self, range: RangeInclusive<u64>) {
        if self.ranges.is_empty() {
            self.ranges.push(range);
            return;
        }

        for (idx, prom) in self.ranges.iter_mut().enumerate().rev() {
            // We need to insert the range here and dont modify anything else
            if prom.end() < range.start() {
                self.ranges.insert(idx + 1, range);
                return;
            }

            if prom.contains(range.end()) {
                let nrange = core::cmp::min(*prom.start(), *range.start())..=*prom.end();

                match idx.checked_sub(1) {
                    Some(pidx) => {
                        let previous_prom = self.ranges.get_mut(pidx).expect("");

                        if previous_prom.contains(nrange.start()) {
                            *previous_prom = *previous_prom.start()..=*nrange.end();
                            self.ranges.remove(idx);
                            return;
                        } else if previous_prom.end() < nrange.start() {
                            self.ranges[idx] = nrange;
                            return;
                        } else {
                            let nrange = core::cmp::min(*nrange.start(), *previous_prom.start())
                                ..=*nrange.end();

                            let break_idx = (&self.ranges[..pidx])
                                .iter()
                                .enumerate()
                                .rev()
                                .find(|(_, r)| r.end() < nrange.start());

                            match break_idx {
                                Some((bidx, _)) => {
                                    self.ranges[bidx + 1] = nrange;
                                    self.ranges.drain(bidx + 2..=idx);

                                    return;
                                }
                                None => match self.ranges.first() {
                                    Some(first_elem) => {
                                        let nrange = *first_elem.start()..=*nrange.end();

                                        self.ranges[0] = nrange;
                                        self.ranges.drain(1..=idx);
                                        return;
                                    }
                                    None => {
                                        self.ranges[0] = nrange;
                                        self.ranges.drain(1..=idx);
                                        return;
                                    }
                                },
                            };
                        }
                    }
                    None => {
                        self.ranges[idx] = nrange;
                        return;
                    }
                };
            }

            if prom.contains(range.start()) {
                let nrange = *prom.start()..=core::cmp::max(*range.end(), *prom.end());
                self.ranges[idx] = nrange;

                return;
            }
        }
    }

    /// Inserts the given Range into the list of ranges
    pub fn insert(&mut self, range: RangeInclusive<u64>) {
        self.raw_insert(range);

        // We iterate backwards to reduce the number of moves that need to be done
        let mut idx = self.ranges.len() - 1;
        for idx in (1..self.ranges.len()).rev() {
            let first = &self.ranges[idx - 1];
            let second = &self.ranges[idx];

            if first.end() + 1 > second.start() - 1 {
                let nrange = *first.start()..=*second.end();

                self.ranges[idx - 1] = nrange;
                self.ranges.remove(idx);
            }
        }
    }

    /// Gets the first Range in the list
    pub fn first(&self) -> Option<&RangeInclusive<u64>> {
        self.ranges.first()
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RangeInclusive<u64>> + '_ {
        self.ranges.iter()
    }

    /// Returns an iterator over all the ranges that contain the point or are after the point
    pub fn after_iter(&self, point: u64) -> impl Iterator<Item = &RangeInclusive<u64>> + '_ {
        let mut left = 0;
        let mut right = self.ranges.len();

        let mut idx = (right - left) / 2 + left;
        while left < right {
            idx = (right - left) / 2 + left;
            let entry = &self.ranges[idx];
            if entry.contains(&point) {
                break;
            }

            if left == right - 1 {
                break;
            }

            if *entry.start() > point {
                right = idx;
            } else if *entry.end() < point {
                left = idx;
            }
        }

        if *self.ranges[idx].end() < point {
            idx += 1;
        }

        self.ranges[idx..].into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_new_ends() {
        let mut list = RangeList::new();

        list.insert(0..=2);
        assert_eq!(vec![0..=2], list.ranges);

        list.insert(4..=10);
        assert_eq!(vec![0..=2, 4..=10], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![0..=2, 4..=10, 20..=30], list.ranges);
    }

    #[test]
    fn add_between() {
        let mut list = RangeList::new();

        list.insert(0..=2);
        assert_eq!(vec![0..=2], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![0..=2, 20..=30], list.ranges);

        list.insert(4..=10);
        assert_eq!(vec![0..=2, 4..=10, 20..=30], list.ranges);
    }

    #[test]
    fn add_end_overlapping() {
        let mut list = RangeList::new();

        list.insert(0..=2);
        assert_eq!(vec![0..=2], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![0..=2, 20..=30], list.ranges);

        list.insert(15..=20);
        assert_eq!(vec![0..=2, 15..=30], list.ranges);
    }

    #[test]
    fn add_end_overlapping_one_elem() {
        let mut list = RangeList::new();

        list.insert(4..=10);
        assert_eq!(vec![4..=10], list.ranges);

        list.insert(1..=4);
        assert_eq!(vec![1..=10], list.ranges);
    }

    #[test]
    fn add_start_overlapping() {
        let mut list = RangeList::new();

        list.insert(0..=2);
        assert_eq!(vec![0..=2], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![0..=2, 20..=30], list.ranges);

        list.insert(2..=10);
        assert_eq!(vec![0..=10, 20..=30], list.ranges);
    }

    #[test]
    fn add_start_end_overlapping() {
        let mut list = RangeList::new();

        list.insert(0..=2);
        assert_eq!(vec![0..=2], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![0..=2, 20..=30], list.ranges);

        list.insert(2..=20);
        assert_eq!(vec![0..=30], list.ranges);
    }

    #[test]
    fn add_start_end_overlapping_multiple() {
        let mut list = RangeList::new();

        list.insert(0..=2);
        assert_eq!(vec![0..=2], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![0..=2, 20..=30], list.ranges);

        list.insert(10..=15);
        assert_eq!(vec![0..=2, 10..=15, 20..=30], list.ranges);

        list.insert(2..=20);
        assert_eq!(vec![0..=30], list.ranges);
    }

    #[test]
    fn add_start_end_overlapping_multiple_2() {
        let mut list = RangeList::new();

        list.insert(4..=8);
        assert_eq!(vec![4..=8], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![4..=8, 20..=30], list.ranges);

        list.insert(10..=15);
        assert_eq!(vec![4..=8, 10..=15, 20..=30], list.ranges);

        list.insert(2..=20);
        assert_eq!(vec![4..=30], list.ranges);
    }

    #[test]
    fn add_start_end_overlapping_multiple_3() {
        let mut list = RangeList::new();

        list.insert(2..=4);
        assert_eq!(vec![2..=4], list.ranges);

        list.insert(20..=30);
        assert_eq!(vec![2..=4, 20..=30], list.ranges);

        list.insert(10..=15);
        assert_eq!(vec![2..=4, 10..=15, 20..=30], list.ranges);

        list.insert(35..=40);
        assert_eq!(vec![2..=4, 10..=15, 20..=30, 35..=40], list.ranges);

        list.insert(12..=36);
        assert_eq!(vec![2..=4, 12..=40], list.ranges);
    }

    #[test]
    fn add_merged() {
        let mut list = RangeList::new();

        list.insert(2..=4);
        assert_eq!(vec![2..=4], list.ranges);

        list.insert(5..=10);
        assert_eq!(vec![2..=10], list.ranges);

        list.insert(12..=20);
        assert_eq!(vec![2..=10, 12..=20], list.ranges);
    }

    #[test]
    fn after_iter() {
        let mut list = RangeList::new();

        list.insert(2..=8);
        list.insert(12..=18);
        list.insert(22..=28);

        let mut iter_0 = list.after_iter(0);
        assert_eq!(Some(&(2..=8)), iter_0.next());
        assert_eq!(Some(&(12..=18)), iter_0.next());
        assert_eq!(Some(&(22..=28)), iter_0.next());
        assert_eq!(None, iter_0.next());

        let mut iter_5 = list.after_iter(5);
        assert_eq!(Some(&(2..=8)), iter_5.next());
        assert_eq!(Some(&(12..=18)), iter_5.next());
        assert_eq!(Some(&(22..=28)), iter_5.next());
        assert_eq!(None, iter_5.next());

        let mut iter_10 = list.after_iter(10);
        assert_eq!(Some(&(12..=18)), iter_10.next());
        assert_eq!(Some(&(22..=28)), iter_10.next());
        assert_eq!(None, iter_10.next());

        let mut iter_30 = list.after_iter(30);
        assert_eq!(None, iter_30.next());
    }
}
