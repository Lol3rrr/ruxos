use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use super::{listener::CmdOp, Operation};

pub struct CmdLog<Id, O, T>
where
    O: Operation<T>,
{
    inner: HashMap<Id, BTreeMap<u64, CmdOp<Id, O, T>>>,
}

impl<Id, O, T> CmdLog<Id, O, T>
where
    O: Operation<T>,
{
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl<Id, O, T> CmdLog<Id, O, T>
where
    O: Operation<T>,
    Id: Hash + Eq,
{
    pub fn get(&self, node: &Id, instance: &u64) -> Option<&'_ CmdOp<Id, O, T>> {
        self.inner.get(node).map(|n| n.get(instance)).flatten()
    }

    pub fn get_mut(&mut self, node: &Id, instance: &u64) -> Option<&'_ mut CmdOp<Id, O, T>> {
        self.inner
            .get_mut(node)
            .map(|n| n.get_mut(instance))
            .flatten()
    }

    pub fn insert(
        &mut self,
        node: Id,
        instance: u64,
        op: CmdOp<Id, O, T>,
    ) -> Option<CmdOp<Id, O, T>> {
        let node_cmds = self.inner.entry(node).or_default();
        node_cmds.insert(instance, op)
    }
}
