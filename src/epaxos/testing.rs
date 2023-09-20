use super::{listener::ListenerHandle, msgs, Cluster, Operation, ResponseReceiver};

pub struct LocalCluster<Id, O, T>
where
    O: Operation<T>,
{
    nodes: Vec<(Id, ListenerHandle<Id, O, T>)>,
}

impl<Id, O, T> LocalCluster<Id, O, T>
where
    O: Operation<T>,
{
    pub fn new(items: impl IntoIterator<Item = (Id, ListenerHandle<Id, O, T>)>) -> Self {
        Self {
            nodes: items.into_iter().collect(),
        }
    }
}

pub struct LocalClusterResponse<Id, O> {
    nodes: Vec<tokio::sync::oneshot::Receiver<msgs::Response<Id, O>>>,
}

impl<Id, O, T> Cluster<Id, O> for LocalCluster<Id, O, T>
where
    O: Operation<T> + Clone,
    Id: PartialEq + Clone,
{
    type Error = ();
    type Receiver<'r> = LocalClusterResponse<Id, O> where Self: 'r;

    fn size(&self) -> usize {
        self.nodes.len()
    }

    async fn send<'s, 'r>(
        &'s mut self,
        msg: super::msgs::Request<Id, O>,
        count: usize,
        local: &Id,
    ) -> Result<Self::Receiver<'r>, Self::Error>
    where
        's: 'r,
    {
        let mut receivers = Vec::with_capacity(count);

        for (_, node_tx) in self.nodes.iter().filter(|(n, _)| n != local) {
            if receivers.len() >= count {
                break;
            }

            let (tx, rx) = tokio::sync::oneshot::channel();

            match node_tx.raw_feed(msg.clone(), tx) {
                Ok(_) => {}
                Err(_) => continue,
            };

            receivers.push(rx);
        }

        Ok(LocalClusterResponse { nodes: receivers })
    }
}

impl<Id, O> ResponseReceiver<Id, O> for LocalClusterResponse<Id, O> {
    async fn recv(&mut self) -> Result<msgs::Response<Id, O>, ()> {
        let last = self.nodes.pop().ok_or(())?;

        last.await.map_err(|e| ())
    }
}
