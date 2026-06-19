use crate::log_entry::{Command, LogEntry};
use crate::node_id::NodeId;

#[derive(Debug)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: usize,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        success: bool,
    },
    AppendEntries {
        leader_id: NodeId,
        prev_log_index: usize,
        term: u64,
        prev_log_term: u64,
        leader_commit: usize,
        entries: Vec<LogEntry>,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        last_log_index: usize,
    },
    ClientRequest {
        command: Command,
    },
}
