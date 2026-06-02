//! actix-web server that reads framed JPEGs from the ESP32-S3-CAM over UART or
//! USB-OTG CDC and exposes them over HTTP (`GET /latest`), a live WebSocket
//! (`/ws`), and a browser viewer (`GET /`).

mod reader;
mod web;

use std::sync::{Arc, RwLock};

use actix_web::{App, HttpServer, web as axweb};
use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::web::AppState;

const DEFAULT_PORT: &str = "/dev/ttyACM0";
/// UART (CH343) default baud — must match the flashed firmware.
const DEFAULT_BAUD: u32 = 4_000_000;
/// USB-OTG CDC ignores baud rate; we open with a harmless nominal value.
const OTG_NOMINAL_BAUD: u32 = 115_200;
const DEFAULT_BIND: &str = "0.0.0.0:8080";
/// Live frames buffered per subscriber before lagging clients start dropping them.
const BROADCAST_CAPACITY: usize = 16;

/// Physical link the firmware streams over.
#[derive(Debug, Clone, Copy)]
enum Transport {
    /// Native USB-OTG CDC — a true USB device; baud is irrelevant.
    Otg,
    /// CH343 UART — real serial; baud must match the firmware.
    Uart,
}

impl Transport {
    /// Parse `CAM_TRANSPORT` (case-insensitive). Unknown/empty → default `Otg` with a warning.
    fn from_env() -> Self {
        match std::env::var("CAM_TRANSPORT") {
            Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
                "otg" => Transport::Otg,
                "uart" => Transport::Uart,
                other => {
                    warn!(value = other, "unknown CAM_TRANSPORT, defaulting to otg");
                    Transport::Otg
                }
            },
            Err(_) => Transport::Otg,
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let transport = Transport::from_env();
    let port = std::env::var("CAM_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
    let cam_baud = std::env::var("CAM_BAUD").ok().and_then(|s| s.parse::<u32>().ok());
    let baud = match transport {
        Transport::Uart => cam_baud.unwrap_or(DEFAULT_BAUD),
        Transport::Otg => {
            if cam_baud.is_some() {
                warn!("CAM_BAUD is ignored in OTG transport (USB CDC has no baud rate)");
            }
            OTG_NOMINAL_BAUD
        }
    };
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND.to_string());

    let latest = Arc::new(RwLock::new(Bytes::new()));
    let (tx, _rx) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);

    // Blocking serial reader on its own thread.
    info!(?transport, %port, baud, "starting serial reader");
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
