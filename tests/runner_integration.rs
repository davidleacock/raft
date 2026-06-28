use dist_1::envelope::Envelope;
use dist_1::node::Node;
use dist_1::node_id::NodeId;
use dist_1::node_state::NodeState;
use dist_1::raft_message::RaftMessage;
use dist_1::runner::run_node;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[tokio::test]
async fn node_starts_election_after_timeout() {
    let mut node = Node::new();
    let peer_node = NodeId::from_uuid(Uuid::new_v4());

    // _node_tx kept alive so run_node's inbox doesn't close before the election fires
    let (_node_tx, node_inbox) = mpsc::channel::<Envelope>(32);
    let (peer_tx, mut peer_rx) = mpsc::channel::<Envelope>(32);

    let mut routing_table: HashMap<NodeId, mpsc::Sender<Envelope>> = HashMap::new();
    node.add_peer(peer_node.clone());
    routing_table.insert(peer_node.clone(), peer_tx);

    let (state_tx, _) = watch::channel(NodeState::Follower {
        current_leader: None,
        voted_for: None,
    });

    let election_timeout = Duration::from_millis(50);
    tokio::spawn(run_node(
        node,
        node_inbox,
        routing_table,
        election_timeout,
        state_tx,
    ));

    let envelope = tokio::time::timeout(Duration::from_millis(500), peer_rx.recv())
        .await
        .expect("timed out — election never fired")
        .expect("peer channel closed before message arrived");

    assert!(matches!(
        envelope.message,
        RaftMessage::RequestVote { term: 1, .. }
    ));
}

#[tokio::test]
async fn only_one_leader_per_cluster() {
    let mut node_1 = Node::new();
    let (node_1_tx, mut node_1_rx) = mpsc::channel::<Envelope>(32);
    let (node_1_state_tx, mut node_1_state_rx) = watch::channel(NodeState::Follower {
        current_leader: None,
        voted_for: None,
    });

    let mut node_2 = Node::new();
    let (node_2_tx, mut node_2_rx) = mpsc::channel::<Envelope>(32);
    let (node_2_state_tx, mut node_2_state_rx) = watch::channel(NodeState::Follower {
        current_leader: None,
        voted_for: None,
    });

    let mut node_3 = Node::new();
    let (node_3_tx, mut node_3_rx) = mpsc::channel::<Envelope>(32);
    let (node_3_state_tx, mut node_3_state_rx) = watch::channel(NodeState::Follower {
        current_leader: None,
        voted_for: None,
    });

    let mut routing_table_1: HashMap<NodeId, mpsc::Sender<Envelope>> = HashMap::new();
    node_1.add_peer(node_2.id().clone());
    node_1.add_peer(node_3.id().clone());
    routing_table_1.insert(node_2.id().clone(), node_2_tx.clone());
    routing_table_1.insert(node_3.id().clone(), node_3_tx.clone());

    let mut routing_table_2: HashMap<NodeId, mpsc::Sender<Envelope>> = HashMap::new();
    node_2.add_peer(node_1.id().clone());
    node_2.add_peer(node_3.id().clone());
    routing_table_2.insert(node_1.id().clone(), node_1_tx.clone());
    routing_table_2.insert(node_3.id().clone(), node_3_tx.clone());

    let mut routing_table_3: HashMap<NodeId, mpsc::Sender<Envelope>> = HashMap::new();
    node_3.add_peer(node_1.id().clone());
    node_3.add_peer(node_2.id().clone());
    routing_table_3.insert(node_1.id().clone(), node_1_tx.clone());
    routing_table_3.insert(node_2.id().clone(), node_2_tx.clone());

    tokio::spawn(run_node(
        node_1,
        node_1_rx,
        routing_table_1,
        Duration::from_millis(50),
        node_1_state_tx,
    ));

    tokio::spawn(run_node(
        node_2,
        node_2_rx,
        routing_table_2,
        Duration::from_millis(150),
        node_2_state_tx,
    ));

    tokio::spawn(run_node(
        node_3,
        node_3_rx,
        routing_table_3,
        Duration::from_millis(75),
        node_3_state_tx,
    ));

    // Wait for a Leader to get elected
    tokio::time::timeout(
        Duration::from_secs(2),
        async {
            tokio::select! {
                _ = node_1_state_rx.wait_for(|s| matches!(s, NodeState::Leader {..})) => {}
                _ = node_2_state_rx.wait_for(|s| matches!(s, NodeState::Leader {..})) => {}
                _ = node_3_state_rx.wait_for(|s| matches!(s, NodeState::Leader {..})) => {}
            }
        }
    )
    .await
    .expect("no leader elected");

    // Ensure only one leader exists for the cluster
    let leader_count = [
        matches!(*node_1_state_rx.borrow(), NodeState::Leader { .. }),
        matches!(*node_2_state_rx.borrow(), NodeState::Leader { .. }),
        matches!(*node_3_state_rx.borrow(), NodeState::Leader { .. }),
    ].iter().filter(|&&b| b).count();

    assert_eq!(leader_count, 1);
}
