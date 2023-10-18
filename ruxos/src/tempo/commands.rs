use std::{collections::HashMap, hash::Hash};

use super::replica::{Command, OpId};

pub struct CommandStorage<NodeId, O, V>
where
    NodeId: Ord,
{
    cmds: HashMap<OpId<NodeId>, Command<O, NodeId, V>>,
}

impl<NodeId, O, V> CommandStorage<NodeId, O, V>
where
    NodeId: Ord + Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            cmds: HashMap::new(),
        }
    }

    pub fn get(&self, oid: &OpId<NodeId>) -> Option<&Command<O, NodeId, V>> {
        self.cmds.get(oid)
    }
    pub fn get_mut(&mut self, oid: &OpId<NodeId>) -> Option<&mut Command<O, NodeId, V>> {
        self.cmds.get_mut(oid)
    }

    pub fn insert(
        &mut self,
        oid: OpId<NodeId>,
        cmd: Command<O, NodeId, V>,
    ) -> Option<Command<O, NodeId, V>> {
        self.cmds.insert(oid, cmd)
    }
}
