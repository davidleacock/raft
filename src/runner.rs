use crate::envelope::Envelope;
use crate::node::{Node, NodeError};
use crate::node_id::NodeId;
use crate::node_state::NodeState;
use crate::raft_message::RaftMessage;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use uuid::Uuid;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, thiserror::Error)]
pub enum NodeRunnerError {
    #[error("node logic error: {0}")]
    Logic(#[from] NodeError),
}

async fn dispatch(
    outbound: Vec<(NodeId, RaftMessage)>,
    from: &NodeId,
    routing_table: &HashMap<NodeId, mpsc::Sender<Envelope>>,
) {
    for (to, msg) in outbound {
        if let Some(sender) = routing_table.get(&to) {
            let _ = sender
                .send(Envelope {
                    from: from.clone(),
                    to: to.clone(),
                    message: msg,
                })
                .await;
        }
    }
}

pub async fn run_node(
    mut node: Node,
    mut inbox: mpsc::Receiver<Envelope>,
    routing_table: HashMap<NodeId, mpsc::Sender<Envelope>>,
    election_timeout: Duration,
    state_tx: watch::Sender<NodeState>,
) -> Result<(), NodeRunnerError> {
    // Sleep is a future that is a value on the stack, the select! loop
    // needs to poll it and thus needs a stable address, so pinning it.
    let sleep = tokio::time::sleep(election_timeout);
    tokio::pin!(sleep);

    let heartbeat = tokio::time::sleep(HEARTBEAT_INTERVAL);
    tokio::pin!(heartbeat);

    loop {
        tokio::select! {
            envelope = inbox.recv() => {
                match envelope {
                    None => return Ok(()),
                    Some(envelope) => {
                        let outbound = node.handle_message(envelope)
                            .map_err(NodeRunnerError::Logic)?;
                        dispatch(outbound, node.id(), &routing_table).await;
                        sleep.as_mut().reset(Instant::now() + election_timeout);
                        let _ = state_tx.send(node.state().clone());
                    }
                }
            }
            _ = &mut sleep => {
                let outbound = node.start_election();
                dispatch(outbound, node.id(), &routing_table).await;
                sleep.as_mut().reset(Instant::now() + election_timeout);
                let _ = state_tx.send(node.state().clone());

            }
            _ = &mut heartbeat => {
                let outbound = node.send_heartbeats();
                dispatch(outbound, node.id(), &routing_table).await;
                heartbeat.as_mut().reset(Instant::now() + HEARTBEAT_INTERVAL);
                let _ = state_tx.send(node.state().clone());
            }
        }
    }
}
