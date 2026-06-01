//! HTTP + WebSocket handlers and shared application state.

use std::sync::{Arc, RwLock};

use actix_web::{HttpRequest, HttpResponse, Responder, web};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tracing::{debug, warn};

/// Shared state handed to every request handler.
pub struct AppState {
    /// Most recent valid JPEG frame (empty until the first frame arrives).
    pub latest: Arc<RwLock<Bytes>>,
    /// Broadcast source of live frames; each WebSocket client subscribes its own receiver.
    pub tx: broadcast::Sender<Bytes>,
}

/// `GET /` — minimal HTML viewer that renders the WebSocket stream.
pub async fn index() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("index.html"))
}

/// `GET /latest` — return the most recent JPEG frame.
pub async fn latest(state: web::Data<AppState>) -> impl Responder {
    let frame = state.latest.read().unwrap().clone();
    if frame.is_empty() {
        return HttpResponse::ServiceUnavailable().body("no frame yet");
    }
    HttpResponse::Ok()
        .content_type("image/jpeg")
        .insert_header(("Cache-Control", "no-store"))
        .body(frame)
}

/// `GET /ws` — upgrade to a WebSocket and stream binary JPEG frames in real time.
pub async fn ws(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;
    let mut rx = state.tx.subscribe();
    let snapshot = state.latest.read().unwrap().clone();

    actix_web::rt::spawn(async move {
        // Send the current frame immediately so a new client isn't blank until the next frame.
        if !snapshot.is_empty() && session.binary(snapshot).await.is_err() {
            return;
        }

        loop {
            tokio::select! {
                frame = rx.recv() => match frame {
                    Ok(frame) => {
                        if session.binary(frame).await.is_err() {
                            break; // client gone
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        debug!(skipped, "websocket client lagged, dropping frames");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                msg = msg_stream.next() => match msg {
                    Some(Ok(actix_ws::Message::Ping(p))) => {
                        if session.pong(&p).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(actix_ws::Message::Close(reason))) => {
                        let _ = session.close(reason).await;
                        return;
                    }
                    Some(Ok(_)) => {} // ignore text/binary/pong/continuation from client
                    Some(Err(e)) => {
                        warn!(error = %e, "websocket protocol error");
                        break;
                    }
                    None => break, // stream ended
                },
            }
        }

        let _ = session.close(None).await;
    });

    Ok(res)
}
