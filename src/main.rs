use axum::{
    Router,
    extract::ws::{Message, WebSocket},
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;
#[derive(Clone)]
pub struct AppState {
    pub clients: Arc<RwLock<HashMap<String, ClientInfo>>>,
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
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (broadcast_tx, _) = broadcast::channel(100);
    let state = AppState {
        clients: Arc::new(RwLock::new(HashMap::new())),
        broadcast_tx,
    };
    let app = Router::new()
        .route("/", get("Hello"))
        .route("/ws", get(websocket_handler))
        .route("/health", get(|| async { "OK" }))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::info!("Server running on port 3000");
    axum::serve(listener, app).await.unwrap();
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let client_id = Uuid::new_v4().to_string();

    let (mut sender, mut receiver) = socket.split();

    let mut broadcast_rx = state.broadcast_tx.subscribe();

    {
        let mut clients = state.clients.write().await;
        clients.insert(
            client_id.clone(),
            ClientInfo {
                id: client_id.clone(),
                username: format!("user_{}", &client_id[..8]),
                connected_at: std::time::Instant::now(),
            },
        );
    }

    tracing::info!("Client {} connected", client_id);

    let send_task = tokio::spawn(async move {
        while let Ok(msg) = broadcast_rx.recv().await {
            let json = serde_json::to_string(&msg).unwrap();
            if sender.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    let client_id_clone = client_id.clone();
    let broadcast_tx = state.broadcast_tx.clone();

    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            let broadcast_msg = BroadcastMessage {
                sender_id: client_id_clone.clone(),
                username: format!("user_{}", &client_id_clone[..8]),
                content: text,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };
            let _ = broadcast_tx.send(broadcast_msg);
        }
    }

    send_task.abort();
    state.clients.write().await.remove(&client_id);
    tracing::info!("Client {} disconnected", client_id);
}
