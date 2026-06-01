//! actix-web server that reads framed JPEGs from the ESP32-S3-CAM over UART and
//! exposes them over HTTP (`GET /latest`), a live WebSocket (`/ws`), and a
//! browser viewer (`GET /`).

mod reader;
mod web;

use std::sync::{Arc, RwLock};

use actix_web::{App, HttpServer, web as axweb};
use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::web::AppState;

const DEFAULT_PORT: &str = "/dev/ttyACM0";
const DEFAULT_BAUD: u32 = 4_000_000;
const DEFAULT_BIND: &str = "0.0.0.0:8080";
/// Live frames buffered per subscriber before lagging clients start dropping them.
const BROADCAST_CAPACITY: usize = 16;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let port = std::env::var("CAM_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
    let baud = std::env::var("CAM_BAUD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BAUD);
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND.to_string());

    let latest = Arc::new(RwLock::new(Bytes::new()));
    let (tx, _rx) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);

    // Blocking serial reader on its own thread.
    {
        let latest = latest.clone();
        let tx = tx.clone();
        std::thread::spawn(move || reader::run(port, baud, latest, tx));
    }

    let state = axweb::Data::new(AppState { latest, tx });

    info!(%bind, "starting HTTP server");
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/", axweb::get().to(web::index))
            .route("/latest", axweb::get().to(web::latest))
            .route("/ws", axweb::get().to(web::ws))
    })
    .bind(&bind)?
    .run()
    .await
}
