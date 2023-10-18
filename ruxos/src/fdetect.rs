use std::collections::HashMap;

pub struct FDetector {}

struct Sender<N> {
    node: N,
    k: u64,
    graph: Graph<N>,
}

struct SenderMessage<N> {
    src: N,
    graph: Graph<N>,
}

#[derive(Debug, PartialEq, Clone)]
struct GNode<N> {
    node: N,
    failed: N,
    k: u64,
}

#[derive(Debug, PartialEq, Clone)]
struct GEdge {
    from: usize,
    to: usize,
}

#[derive(Debug, Clone)]
struct Graph<N> {
    nodes: Vec<GNode<N>>,
    edges: Vec<GEdge>,
}

impl<N> Sender<N> {
    fn new(node: N) -> Self {
        Sender {
            node,
            k: 0,
            graph: Graph {
                nodes: Vec::new(),
                edges: Vec::new(),
            },
        }
    }
}

impl<N> Sender<N>
where
    N: Clone + PartialEq,
{
    fn send(&mut self, failed: N, message: SenderMessage<N>) -> SenderMessage<N> {
        self.k = self.k + 1;

        // Set the new Graph as the union of our Graph and the graph of the message
        self.graph.merge(message.graph);

        // Add [p, d_p, k_p] to the Graph
        let node_idx = self.graph.nodes.len();
        self.graph.nodes.push(GNode {
            node: self.node.clone(),
            failed,
            k: self.k,
        });

        // Add Edges from all other vertices of the graph to [p, d_p, k_p] to the Graph
        for idx in 0..self.graph.nodes.len().saturating_sub(1) {
            self.graph.edges.push(GEdge {
                from: idx,
                to: node_idx,
            });
        }

        // TODO
        // Calculate the output

        // Send the Graphs to the other Nodes
        SenderMessage {
            src: self.node.clone(),
            graph: self.graph.clone(),
        }
    }

    fn compute(&mut self, nodes: &[N]) {
        for i in 0..nodes.len() {
            // TODO
        }
    }
}

impl<N> Graph<N>
where
    N: PartialEq,
{
    fn merge(&mut self, other: Self) {
        let mut idx_mapping = HashMap::new();

        for (node_idx, node) in other.nodes.into_iter().enumerate() {
            match self
                .nodes
                .iter()
                .enumerate()
                .find(|(_, n)| *n == &node)
                .map(|(i, _)| i)
            {
                Some(idx) => {
                    idx_mapping.insert(node_idx, idx);
                }
                None => {
                    let n_idx = self.nodes.len();
                    self.nodes.push(node);
                    idx_mapping.insert(node_idx, n_idx);
                }
            };
        }

        for mut edge in other.edges {
            edge.from = idx_mapping.get(&edge.from).copied().unwrap();
            edge.to = idx_mapping.get(&edge.to).copied().unwrap();

            if !self.edges.contains(&edge) {
                self.edges.push(edge);
            }
        }
    }
}
