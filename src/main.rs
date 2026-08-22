use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast,RwLock};
use uuid::Uuid;
#[derive(Clone)]
pub struct AppState{
    pub clients:Arc<RwLock<HashMap<String, ClientInfo>>>,
    pub broadcast_tx: broadcast::Sender<BroadcastMessage>,
}


pub struct ClientInfo {
    pub id: String,
    pub username: String,
    pub connected_at: std::time::Instant,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct BroadcastMessage {
    pub sender_id: String,
    pub username: String,
    pub content: String,
    pub timestamp: u64,
}
fn main() {
    println!("Hello, WASIFUL");
}
