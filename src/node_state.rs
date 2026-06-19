use crate::node_id::NodeId;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub enum NodeState {
    Follower {
        current_leader: Option<NodeId>,
        voted_for: Option<NodeId>,
    },
    Candidate {
        votes_received: HashSet<NodeId>,
    },
    Leader {
        // next log index to send to each follower
        next_log_index: HashMap<NodeId, usize>,
        // highest log index confirmed replicated on each follower
        confirmed_log_index: HashMap<NodeId, usize>,
    },
}