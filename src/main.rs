use axum::{
    Router,
    extract::ws::{Message, WebSocket},
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use std::{sync::Arc, vec};
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(90);
#[derive(Clone)]
pub struct AppState {
    pub clients: Arc<RwLock<HashMap<String, ClientInfo>>>,
    pub broadcast_tx: broadcast::Sender<ServerEvent>,
}

pub struct ClientInfo {
    pub id: String,
    pub username: String,
    pub connected_at: std::time::Instant,
    pub last_pong: Arc<RwLock<std::time::Instant>>,
}

// Client -> Server
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
pub enum ClientEvent {
    #[serde(rename = "message")]
    Message { content: String },
    #[serde(rename = "typing")]
    Typing,
    #[serde(rename = "read")]
    Read { message_id: String },
}
// Server -> Client Event
#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "type")]
pub enum ServerEvent {
    #[serde(rename = "message_created")]
    MessageCreated {
        message_id: String,
        sender_id: String,
        username: String,
        content: String,
        timestamp: u64,
    },
    #[serde(rename = "user_typing")]
    UserTyping {
        user_id: String,
        username: String,
    },
    MessageRead {
        message_id: String,
        user_id: String,
    },
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
    let username = format!("user_{}", &client_id[..8]);

    let (sender, mut receiver) = socket.split();
    let mut broadcast_rx = state.broadcast_tx.subscribe();
    // let sender = Arc::new(tokio::sync::Mutex::new(sender));
    // let last_heartbeat = Arc::new(RwLock::new(std::time::Instant::now()));
    let last_pong = Arc::new(RwLock::new(Instant::now()));
    {
        let mut clients = state.clients.write().await;
        clients.insert(
            client_id.clone(),
            ClientInfo {
                id: client_id.clone(),
                username: username.clone(),
                connected_at: Instant::now(),
                last_pong: last_pong.clone(),
            },
        );
    }

    tracing::info!("Client connected: {} ({})", username, client_id);
    let heartbeat_sender = Arc::new(tokio::sync::Mutex::new(sender));
    let heartbeat_sender_clone = heartbeat_sender.clone();
    let heartbeat_time = last_pong.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            interval.tick().await;
            let last = *heartbeat_time.read().await;
            if last.elapsed() > CLIENT_TIMEOUT {
                tracing::warn!("Client timed out");
                break;
            }
            let mut sender = heartbeat_sender_clone.lock().await;
            if sender.send(Message::Ping(vec![1, 2, 3])).await.is_err() {
                break;
            }
        }
    });
    let broadcast_sender = heartbeat_sender.clone();

    let send_task = tokio::spawn(async move {
        while let Ok(event) = broadcast_rx.recv().await {
            let json = match serde_json::to_string(&event) {
                Ok(json) => json,
                Err(err) => {
                    tracing::error!("Failed to serialize broadcast: {}", err);
                    continue;
                }
            };
            let mut sender = broadcast_sender.lock().await;
            if sender.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    while let Some(result) = receiver.next().await {
        let message = match result {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!("WebSocket receive error: {}", error);
                break;
            }
        };

        match message {
            Message::Pong(_) => {
                *last_pong.write().await = Instant::now();
                tracing::debug!("Received pong from {}", username);
            }

            Message::Text(text) => {
                tracing::info!("RAW MESSAGE RECEIVED: {:?}", text);
                let event: ClientEvent = match serde_json::from_str(&text) {
                    Ok(event) => event,

                    // Err(error) => {
                    //     tracing::warn!("Invalid client event: {}", error);

                    //     continue;
                    // }
                    Err(error) => {
                        tracing::warn!("Invalid client event: {} | Received: {:?}", error, text);

                        continue;
                    }
                };
                handle_client_event(event, &client_id, &username, &state).await;
            }

            Message::Close(_) => {
                tracing::info!("Client requested disconnect: {}", username);

                break;
            }

            _ => {}
        }
    }
    heartbeat_task.abort();
    send_task.abort();

    state.clients.write().await.remove(&client_id);

    tracing::info!("Client disconnected: {}", username);
}
async fn handle_client_event(
    event: ClientEvent,
    client_id: &str,
    username: &str,
    state: &AppState,
) {
    match event {
        ClientEvent::Message { content } => {
            let message_id = Uuid::new_v4().to_string();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let event = ServerEvent::MessageCreated {
                message_id,
                sender_id: client_id.to_string(),
                username: username.to_string(),
                content,
                timestamp,
            };

            match state.broadcast_tx.send(event) {
                Ok(receiver_count) => {
                    tracing::info!(
                        "Message broadcast successfully to {} clients",
                        receiver_count
                    );
                }

                Err(error) => {
                    tracing::warn!("Failed to broadcast message: {}", error);
                }
            }
        }
        ClientEvent::Typing => {
            let event = ServerEvent::UserTyping {
                user_id: client_id.to_string(),
                username: username.to_string(),
            };

            let _ = state.broadcast_tx.send(event);
        }
        ClientEvent::Read { message_id } => {
            let event = ServerEvent::MessageRead {
                message_id,
                user_id: client_id.to_string(),
            };
            let _ = state.broadcast_tx.send(event);
        }
    }
}
