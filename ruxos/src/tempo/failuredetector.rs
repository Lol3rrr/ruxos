//! Based on the Sigma in https://dl.acm.org/doi/pdf/10.1145/234533.234549

pub struct Detector<NodeId> {
    correct: NodeId,
}

impl<NodeId> Detector<NodeId> {
    pub fn new(correct: NodeId) -> Self {
        Self { correct }
    }
}
impl<NodeId> Detector<NodeId>
where
    NodeId: PartialEq,
{
    pub fn is_leader(&self, node: &NodeId) -> bool {
        // TODO
        return true;
    }
}
