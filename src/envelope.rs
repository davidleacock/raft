use crate::node_id::NodeId;
use crate::raft_message::RaftMessage;

#[derive(Debug)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: RaftMessage,
}
