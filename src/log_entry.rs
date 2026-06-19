#[derive(Debug, Clone)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: usize,
    pub(crate) command: Command,
}
