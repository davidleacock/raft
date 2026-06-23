use crate::envelope::Envelope;
use crate::log_entry::{Command, LogEntry};
use crate::node_id::NodeId;
use crate::node_state::NodeState;
use crate::raft_message::RaftMessage;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Node {
    node_id: NodeId,
    current_term: u64,
    log: Vec<LogEntry>,
    peers: Vec<NodeId>,
    state: NodeState,
    commit_index: usize,
    last_applied: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("unexpected message: {message}")]
    UnexpectedMessage { message: String },
}

impl Node {
    pub fn new() -> Node {
        Node {
            node_id: NodeId::from_uuid(Uuid::new_v4()),
            current_term: 0,
            log: vec![],
            peers: vec![],
            state: NodeState::Follower {
                current_leader: None,
                voted_for: None,
            },
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn handle_message(
        &mut self,
        envelope: Envelope,
    ) -> Result<Vec<(NodeId, RaftMessage)>, NodeError> {
        let from = envelope.from;
        let old_state = std::mem::replace(
            &mut self.state,
            NodeState::Follower {
                current_leader: None,
                voted_for: None,
            },
        );

        match (old_state, envelope.message) {
            (
                NodeState::Follower {
                    mut current_leader,
                    voted_for,
                },
                RaftMessage::AppendEntries {
                    leader_id,
                    prev_log_index,
                    term,
                    prev_log_term,
                    leader_commit,
                    entries,
                },
            ) => {
                let result = Node::handle_append_entries(
                    &mut self.current_term,
                    &mut self.log,
                    &mut self.commit_index,
                    &mut current_leader,
                    from,
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    leader_commit,
                    entries,
                );

                self.state = NodeState::Follower {
                    current_leader,
                    voted_for,
                };
                Ok(result)
            }
            (
                NodeState::Follower {
                    mut voted_for,
                    current_leader,
                },
                RaftMessage::RequestVote {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                },
            ) => {
                let result = Node::handle_request_vote(
                    &mut self.current_term,
                    &self.log,
                    &mut voted_for,
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                );
                self.state = NodeState::Follower {
                    current_leader,
                    voted_for,
                };
                Ok(result)
            }
            (
                NodeState::Candidate { votes_received },
                RaftMessage::RequestVoteResponse { term, success },
            ) => {
                let (result, state) = Node::handle_candidate_vote_response(
                    &mut self.current_term,
                    &self.peers,
                    &self.log,
                    from,
                    votes_received,
                    term,
                    success,
                );

                self.state = state;

                Ok(result)
            }
            (
                NodeState::Candidate { .. },
                RaftMessage::AppendEntries {
                    leader_id,
                    prev_log_index,
                    term,
                    prev_log_term,
                    leader_commit,
                    entries,
                },
            ) => {
                let mut current_leader: Option<NodeId> = None;
                let voted_for: Option<NodeId> = None;

                let result = Node::handle_append_entries(
                    &mut self.current_term,
                    &mut self.log,
                    &mut self.commit_index,
                    &mut current_leader,
                    from,
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    leader_commit,
                    entries,
                );

                self.state = NodeState::Follower {
                    current_leader,
                    voted_for,
                };
                Ok(result)
            }
            (
                NodeState::Leader {
                    next_log_index,
                    confirmed_log_index,
                },
                RaftMessage::AppendEntriesResponse {
                    term,
                    last_log_index,
                    success,
                },
            ) => {
                let (result, state) = Node::handle_append_entries_response(
                    term,
                    &mut self.current_term,
                    success,
                    confirmed_log_index,
                    next_log_index,
                    last_log_index,
                    &mut self.commit_index,
                    from,
                    &self.log,
                    &self.peers,
                );

                self.state = state;

                Ok(result)
            }
            (
                NodeState::Leader {
                    next_log_index: next_index,
                    confirmed_log_index: match_index,
                },
                RaftMessage::ClientRequest { command },
            ) => {
                let result = Node::handle_client_request(
                    &self.node_id,
                    &self.peers,
                    self.current_term,
                    &mut self.log,
                    command,
                    self.commit_index,
                );

                self.state = NodeState::Leader {
                    next_log_index: next_index,
                    confirmed_log_index: match_index,
                };

                Ok(result)
            }
            (_, _) => Err(NodeError::UnexpectedMessage {
                message: "invalid command sent for current state".to_string(),
            }),
        }
    }

    fn handle_append_entries_response(
        term: u64,
        current_term: &mut u64,
        success: bool,
        mut confirmed_log_index: HashMap<NodeId, usize>,
        mut next_log_index: HashMap<NodeId, usize>,
        last_log_index: usize,
        commit_index: &mut usize,
        from: NodeId,
        log: &[LogEntry],
        peers: &[NodeId],
    ) -> (Vec<(NodeId, RaftMessage)>, NodeState) {
        // An election happened and this node is no longer the leader
        if term > *current_term {
            *current_term = term;
            let state = NodeState::Follower {
                current_leader: None,
                voted_for: None,
            };
            return (vec![], state);
        }

        if success {
            confirmed_log_index.insert(from.clone(), last_log_index);
            next_log_index.insert(from.clone(), last_log_index + 1);

            // Determine highest index and number of nodes that have it
            let mut indices: Vec<usize> = confirmed_log_index.values().copied().collect();
            indices.push(log.len().saturating_sub(1));
            indices.sort_unstable_by(|a, b| b.cmp(a));

            let majority = (peers.len() + 2) / 2;

            if let Some(&majority_commit_index) = indices.get(majority - 1) {
                if majority_commit_index > *commit_index {
                    if let Some(log_entry) = log.get(majority_commit_index) {
                        if log_entry.term == *current_term {
                            *commit_index = majority_commit_index;
                        }
                    }
                }
            }

            let state = NodeState::Leader {
                next_log_index,
                confirmed_log_index,
            };

            (vec![], state)
        } else {
            // We must roll back the next log to attempt at this node by 1
            let entry = next_log_index
                .get_mut(&from)
                .expect("AppendEntriesResponse from peer not in next_log_index");
            *entry = entry.saturating_sub(1);

            let state = NodeState::Leader {
                next_log_index,
                confirmed_log_index,
            };

            (vec![], state)
        }
    }

    fn handle_client_request(
        node_id: &NodeId,
        peers: &[NodeId],
        current_term: u64,
        logs: &mut Vec<LogEntry>,
        command: Command,
        commit_index: usize,
    ) -> Vec<(NodeId, RaftMessage)> {
        let (prev_log_index, prev_log_term) = match logs.last() {
            None => (0, 0),
            Some(entry) => (entry.index, entry.term),
        };

        let new_log_entry = LogEntry {
            term: prev_log_term,
            index: logs.len(),
            command,
        };
        logs.push(new_log_entry.clone());

        peers
            .iter()
            .map(|peer| {
                (
                    peer.clone(),
                    RaftMessage::AppendEntries {
                        leader_id: node_id.clone(),
                        prev_log_index,
                        term: current_term,
                        prev_log_term,
                        leader_commit: commit_index,
                        entries: vec![new_log_entry.clone()],
                    },
                )
            })
            .collect()
    }

    fn handle_candidate_vote_response(
        current_term: &mut u64,
        peers: &[NodeId],
        log: &[LogEntry],
        from: NodeId,
        mut votes_received: HashSet<NodeId>,
        sender_term: u64,
        success: bool,
    ) -> (Vec<(NodeId, RaftMessage)>, NodeState) {
        let majority = 1 + (peers.len() + 1) / 2;

        if *current_term < sender_term {
            let state = NodeState::Follower {
                current_leader: None,
                voted_for: None,
            };

            *current_term = sender_term;

            return (vec![], state);
        }

        if success {
            votes_received.insert(from);
            if votes_received.len() + 1 >= majority {
                let mut next_index = HashMap::new();
                let mut match_index = HashMap::new();
                for node_id in peers.iter() {
                    next_index.insert(node_id.clone(), log.len());
                    match_index.insert(node_id.clone(), 0);
                }
                let state = NodeState::Leader {
                    next_log_index: next_index,
                    confirmed_log_index: match_index,
                };
                (vec![], state)
            } else {
                (vec![], NodeState::Candidate { votes_received })
            }
        } else {
            (vec![], NodeState::Candidate { votes_received })
        }
    }

    fn handle_request_vote(
        current_term: &mut u64,
        log: &[LogEntry],
        voted_for: &mut Option<NodeId>,
        candidate_term: u64,
        candidate_id: NodeId,
        last_log_index: usize,
        last_log_term: u64,
    ) -> Vec<(NodeId, RaftMessage)> {
        if *current_term <= candidate_term
            && voted_for.is_none()
            && Node::is_log_behind(log, last_log_index, last_log_term)
        {
            *voted_for = Some(candidate_id.clone());
            *current_term = candidate_term;
            vec![(
                candidate_id,
                RaftMessage::RequestVoteResponse {
                    term: *current_term,
                    success: true,
                },
            )]
        } else {
            vec![(
                candidate_id,
                RaftMessage::RequestVoteResponse {
                    term: *current_term,
                    success: false,
                },
            )]
        }
    }

    fn handle_append_entries(
        node_current_term: &mut u64,
        log: &mut Vec<LogEntry>,
        commit_index: &mut usize,
        current_leader: &mut Option<NodeId>,
        from: NodeId,
        source_term: u64,
        leader_id: NodeId,
        prev_log_index: usize,
        prev_log_term: u64,
        leader_commit: usize,
        entries: Vec<LogEntry>,
    ) -> Vec<(NodeId, RaftMessage)> {
        if source_term < *node_current_term {
            return vec![(
                from,
                RaftMessage::AppendEntriesResponse {
                    term: *node_current_term,
                    success: false,
                    last_log_index: log.len() - 1,
                },
            )];
        }

        *node_current_term = source_term;
        *current_leader = Some(leader_id);

        // prev_log_index > 0 means "the entry just before these new ones should
        // exist in your log at position prev_log_index - 1 with prev_log_term"
        if prev_log_index > 0 {
            let consistent = log
                .get(prev_log_index - 1)
                .map(|entry| entry.term == prev_log_term)
                .unwrap_or(false);

            if !consistent {
                return vec![(
                    from,
                    RaftMessage::AppendEntriesResponse {
                        term: *node_current_term,
                        success: false,
                        last_log_index: log.len() - 1,
                    },
                )];
            }
        }

        for new_entry in entries {
            match log.get(new_entry.index) {
                Some(existing) if existing.term != new_entry.term => {
                    // Conflicting entry — discard it and everything after, then replace
                    log.truncate(new_entry.index);
                    log.push(new_entry);
                }
                None => log.push(new_entry),
                _ => {} // already have this exact entry, skip
            }
        }

        if leader_commit > *commit_index {
            // Select the smaller of (leader_commit, log.len - 1)
            *commit_index = leader_commit.min(log.len().saturating_sub(1));
        }

        vec![(
            from,
            RaftMessage::AppendEntriesResponse {
                term: *node_current_term,
                success: true,
                last_log_index: log.len() - 1,
            },
        )]
    }

    fn is_log_behind(log: &[LogEntry], last_log_index: usize, last_log_term: u64) -> bool {
        match log.last() {
            None => true,
            Some(entry) => entry.term < last_log_term || entry.index < last_log_index,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    pub fn state(&self) -> &NodeState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_entry::Command::Delete;
    use uuid::Uuid;

    fn make_node_id() -> NodeId {
        NodeId::from_uuid(Uuid::new_v4())
    }

    fn create_request_vote_envelope(from: NodeId, to: NodeId) -> Envelope {
        Envelope {
            from: from.clone(),
            to: to.clone(),
            message: RaftMessage::RequestVote {
                term: 1,
                candidate_id: from,
                last_log_index: 0,
                last_log_term: 0,
            },
        }
    }

    fn create_request_vote_response_envelope(
        from: NodeId,
        to: NodeId,
        term: u64,
        success: bool,
    ) -> Envelope {
        Envelope {
            from: from.clone(),
            to: to.clone(),
            message: RaftMessage::RequestVoteResponse { term, success },
        }
    }

    fn create_append_entries_request_envelope(
        from: NodeId,
        to: NodeId,
        leader_id: NodeId,
        term: u64,
        prev_log_term: u64,
        leader_commit: usize,
        prev_log_index: usize,
        entries: Vec<LogEntry>,
    ) -> Envelope {
        Envelope {
            from: from.clone(),
            to: to.clone(),
            message: RaftMessage::AppendEntries {
                leader_id,
                prev_log_index,
                term,
                prev_log_term,
                leader_commit,
                entries,
            },
        }
    }

    #[test]
    fn follower_grants_vote_when_all_conditions_met() {
        let mut node = Node::new();
        let candidate_id = make_node_id();

        let envelope = create_request_vote_envelope(candidate_id.clone(), node.node_id.clone());

        let result = node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &candidate_id);
        assert!(matches!(
            msg,
            RaftMessage::RequestVoteResponse { success: true, .. }
        ));
    }

    #[test]
    fn follower_denies_vote_when_nodes_term_is_higher() {
        let mut node = Node::new();
        let candidate_id = make_node_id();
        node.current_term = 2;

        let envelope = create_request_vote_envelope(candidate_id.clone(), node.node_id.clone());

        let result = node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &candidate_id);
        assert!(matches!(
            msg,
            RaftMessage::RequestVoteResponse { success: false, .. }
        ));
    }

    #[test]
    fn follower_denies_vote_when_node_has_already_voted() {
        let mut node = Node::new();
        let candidate_id = make_node_id();

        node.state = NodeState::Follower {
            current_leader: None,
            voted_for: Some(make_node_id()),
        };

        let envelope = create_request_vote_envelope(candidate_id.clone(), node.node_id.clone());

        let result = node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &candidate_id);
        assert!(matches!(
            msg,
            RaftMessage::RequestVoteResponse { success: false, .. }
        ));
    }

    #[test]
    fn follower_denies_vote_when_node_log_is_ahead() {
        let mut node = Node::new();
        let candidate_id = make_node_id();

        node.log.push(LogEntry {
            term: 0,
            index: 0,
            command: Command::Set {
                key: "".to_string(),
                value: "".to_string(),
            },
        });

        let envelope = create_request_vote_envelope(candidate_id.clone(), node.node_id.clone());

        let result = node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &candidate_id);
        assert!(matches!(
            msg,
            RaftMessage::RequestVoteResponse { success: false, .. }
        ));
    }

    #[test]
    fn candidate_becomes_follower_if_behind() {
        let mut candidate_node = Node::new();
        let vote_response_node = make_node_id();

        candidate_node.state = NodeState::Candidate {
            votes_received: Default::default(),
        };

        candidate_node.current_term = 1;

        let envelope = create_request_vote_response_envelope(
            vote_response_node.clone(),
            candidate_node.node_id.clone(),
            2,
            false,
        );

        let result = candidate_node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 0);
        // Candidate becomes a Follower due to its Vote Request responder having a higher term than it does
        assert!(matches!(
            candidate_node.state,
            NodeState::Follower {
                current_leader: None,
                voted_for: None
            }
        ));
    }

    #[test]
    fn candidate_stays_candidate_if_terms_equal_votes_unchanged() {
        let mut candidate_node = Node::new();
        let vote_response_node = make_node_id();

        candidate_node.state = NodeState::Candidate {
            votes_received: Default::default(),
        };

        candidate_node.current_term = 1;

        let envelope = create_request_vote_response_envelope(
            vote_response_node.clone(),
            candidate_node.node_id.clone(),
            1,
            false,
        );

        let result = candidate_node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 0);
        // Candidate becomes a Follower due to its Vote Request responder having a higher term than it does
        assert!(matches!(candidate_node.state, NodeState::Candidate { .. }));
        if let NodeState::Candidate { votes_received } = &candidate_node.state {
            assert!(votes_received.is_empty());
        } else {
            panic!("expected Candidate state");
        }
    }

    #[test]
    fn candidate_becomes_leader_once_majority_votes() {
        let mut candidate_node = Node::new();
        let node_1 = make_node_id();
        let node_2 = make_node_id();
        let node_3 = make_node_id();

        candidate_node.state = NodeState::Candidate {
            votes_received: Default::default(),
        };

        candidate_node.log.push(LogEntry {
            term: 0,
            index: 0,
            command: Command::Delete {
                key: "".to_string(),
            },
        });

        // 4 node cluster, the candidate votes for itself so needs two more votes to obtain majority
        candidate_node.peers.push(node_1.clone());
        candidate_node.peers.push(node_2.clone());
        candidate_node.peers.push(node_3.clone());

        // Candidate will be a higher term that its peers
        candidate_node.current_term = 2;

        let envelope_1 = create_request_vote_response_envelope(
            node_1.clone(),
            candidate_node.node_id.clone(),
            1,
            true,
        );

        candidate_node
            .handle_message(envelope_1)
            .expect("handle_message failed");

        assert!(matches!(candidate_node.state, NodeState::Candidate { .. }));

        let envelope_2 = create_request_vote_response_envelope(
            node_2.clone(),
            candidate_node.node_id.clone(),
            1,
            true,
        );

        candidate_node
            .handle_message(envelope_2)
            .expect("handle_message failed");

        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for peer in candidate_node.peers.iter() {
            next_index.insert(peer.clone(), candidate_node.log.len());
            match_index.insert(peer.clone(), 0);
        }

        assert!(matches!(
            candidate_node.state,
            NodeState::Leader {
                next_log_index: _next_index,
                confirmed_log_index: _match_index
            }
        ));
    }

    #[test]
    fn handle_append_entries_sender_term_is_behind_node_term_reject() {
        // node has a higher term than the sender
        let mut follower_node = Node::new();
        follower_node.current_term = 2;
        follower_node.log.push(LogEntry {
            term: 2,
            index: 1,
            command: Delete {
                key: "".to_string(),
            },
        });

        let sender_node = make_node_id();
        let sender_term = 1;

        let envelope = create_append_entries_request_envelope(
            sender_node.clone(),
            follower_node.node_id.clone(),
            sender_node.clone(),
            sender_term,
            1,
            1,
            1,
            vec![],
        );

        let result = follower_node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &sender_node);
        assert!(matches!(
            msg,
            RaftMessage::AppendEntriesResponse {
                term: 2,
                success: false,
                last_log_index: 0
            }
        ));
    }

    #[test]
    fn handle_append_entries_logs_are_not_consistent_reject() {
        let mut follower_node = Node::new();
        follower_node.current_term = 1;

        follower_node.log.push(LogEntry {
            term: 1,
            index: 0,
            command: Delete {
                key: "".to_string(),
            },
        });

        let sender_node = make_node_id();
        let sender_term = 2;

        // The prev_log_term and prev_log_index are not consistent with
        // follower node current values. Disregard the empty vec, not needed in this
        // test.
        let envelope = create_append_entries_request_envelope(
            sender_node.clone(),
            follower_node.node_id.clone(),
            sender_node.clone(),
            sender_term,
            2,
            2,
            2,
            vec![],
        );

        let result = follower_node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &sender_node);

        // The followers always adopts a higher term
        assert!(matches!(
            msg,
            RaftMessage::AppendEntriesResponse {
                term: 2,
                success: false,
                last_log_index: 0
            }
        ));
    }

    #[test]
    fn handle_append_entries_happy_path() {
        let mut follower_node = Node::new();
        follower_node.current_term = 2;
        follower_node.commit_index = 1;

        follower_node.log.push(LogEntry {
            term: 1,
            index: 0,
            command: Command::Set {
                key: "".to_string(),
                value: "".to_string(),
            },
        });

        let sender_node = make_node_id();
        let sender_term = 2;

        let new_log = vec![LogEntry {
            term: 2,
            index: 1,
            command: Delete {
                key: "".to_string(),
            },
        }];

        let envelope = create_append_entries_request_envelope(
            sender_node.clone(),
            follower_node.node_id.clone(),
            sender_node.clone(),
            sender_term,
            1,
            2,
            1,
            new_log,
        );

        let result = follower_node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &sender_node);

        // The followers always adopts a higher term
        assert!(matches!(
            msg,
            RaftMessage::AppendEntriesResponse {
                term: 2,
                success: true,
                last_log_index: 1
            }
        ));

        assert_eq!(follower_node.commit_index, 1);
    }

    #[test]
    fn handle_append_entries_happy_path_truncate_log() {
        let mut follower_node = Node::new();
        follower_node.current_term = 2;
        follower_node.commit_index = 1;

        let entry_1 = LogEntry {
            term: 1,
            index: 0,
            command: Command::Set {
                key: "".to_string(),
                value: "".to_string(),
            },
        };
        follower_node.log.push(entry_1);

        let entry_2 = LogEntry {
            term: 1,
            index: 1,
            command: Command::Set {
                key: "".to_string(),
                value: "".to_string(),
            },
        };
        follower_node.log.push(entry_2);

        let sender_node = make_node_id();
        let sender_term = 2;

        let entry_3 = LogEntry {
            term: 2,
            index: 1,
            command: Delete {
                key: "".to_string(),
            },
        };
        let new_log = vec![entry_3];

        let envelope = create_append_entries_request_envelope(
            sender_node.clone(),
            follower_node.node_id.clone(),
            sender_node.clone(),
            sender_term,
            1,
            2,
            1,
            new_log,
        );

        let result = follower_node
            .handle_message(envelope)
            .expect("handle_message failed");

        assert_eq!(result.len(), 1);
        let (to, msg) = &result[0];
        assert_eq!(to, &sender_node);

        // The followers always adopts a higher term
        assert!(matches!(
            msg,
            RaftMessage::AppendEntriesResponse {
                term: 2,
                success: true,
                last_log_index: 1
            }
        ));

        assert_eq!(follower_node.commit_index, 1);

        assert_eq!(follower_node.log.len(), 2);

        // entry at index 0 unchanged
        assert_eq!(follower_node.log[0].term, 1);
        assert_eq!(follower_node.log[0].index, 0);

        // entry at index 1 was truncated and replaced — term changed from 1 to 2
        assert_eq!(follower_node.log[1].term, 2);
        assert_eq!(follower_node.log[1].index, 1);
    }
}
