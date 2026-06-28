use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(Uuid);

impl NodeId {
    pub fn new(id: &str) -> Result<Self, NodeIdError> {
        match Uuid::try_parse(id) {
            Ok(valid_id) => Ok(NodeId(valid_id)),
            Err(err) => Err(NodeIdError { source: err }),
        }
    }

    pub fn from_uuid(id: Uuid) -> NodeId {
        NodeId(id)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid node id: {source}")]
pub struct NodeIdError {
    #[source]
    source: uuid::Error,
}
