#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub(super) struct Interference<Id> {
    pub(super) node: Id,
    pub(super) instance: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub(super) struct Dependencies<Id> {
    inner: Vec<Interference<Id>>,
}

impl<Id> Dependencies<Id> {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    pub fn from_raw(inner: Vec<Interference<Id>>) -> Self {
        Self { inner }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Interference<Id>> + '_ {
        self.inner.iter()
    }
}

impl<Id> Dependencies<Id>
where
    Id: PartialOrd + Ord + PartialEq,
{
    pub fn union_mut(&mut self, items: impl IntoIterator<Item = Interference<Id>>) {
        self.inner.extend(items);
        self.inner.sort_unstable();
        self.inner.dedup();
    }
}

impl<Id> IntoIterator for Dependencies<Id> {
    type Item = Interference<Id>;
    type IntoIter = <std::vec::Vec<Interference<Id>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}
