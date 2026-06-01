//! Blocking serial reader that decodes the ESP32-S3-CAM framed-JPEG stream.
//!
//! Wire format (repeated per frame), produced by the firmware:
//!     [ MAGIC: AA 55 AA 55 ][ len: u32 little-endian ][ JPEG payload ]
//!
//! Runs on a dedicated std thread (serialport I/O is blocking), publishing each
//! valid frame to a shared `latest` snapshot and a broadcast channel for live
//! WebSocket fan-out.

use std::io::{BufReader, Read};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

const MAGIC: [u8; 4] = [0xAA, 0x55, 0xAA, 0x55];
const MAX_FRAME: u32 = 4 * 1024 * 1024; // 4 MB sanity cap to reject bad lengths
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Run the reader loop forever: (re)open the port and decode frames. On any
/// serial error (unplug, reflash, short read) it logs and retries after a delay
/// so the HTTP server keeps running.
pub fn run(port: String, baud: u32, latest: Arc<RwLock<Bytes>>, tx: broadcast::Sender<Bytes>) {
    loop {
        info!(%port, baud, "opening serial port");
        match serialport::new(&port, baud)
            .timeout(Duration::from_secs(1))
            .open()
        {
            Ok(sp) => {
                let mut reader = BufReader::new(sp);
                if let Err(e) = read_frames(&mut reader, &latest, &tx) {
                    error!(error = %e, "serial read loop ended, reconnecting");
                }
            }
            Err(e) => {
                error!(error = %e, "failed to open serial port, retrying");
            }
        }
        std::thread::sleep(RECONNECT_DELAY);
    }
}

/// Decode frames until an I/O error occurs (which bubbles up to trigger a reconnect).
fn read_frames<R: Read>(
    reader: &mut R,
    latest: &Arc<RwLock<Bytes>>,
    tx: &broadcast::Sender<Bytes>,
) -> std::io::Result<()> {
    loop {
        sync_to_magic(reader)?;

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let length = u32::from_le_bytes(len_buf);
        if length == 0 || length > MAX_FRAME {
            warn!(length, "bogus frame length, resyncing");
            continue;
        }

        let mut payload = vec![0u8; length as usize];
        reader.read_exact(&mut payload)?;

        // Validate JPEG markers so the browser never receives garbage.
        let valid = payload.len() >= 4
            && payload[..2] == [0xFF, 0xD8]
            && payload[payload.len() - 2..] == [0xFF, 0xD9];
        if !valid {
            debug!(length, "invalid JPEG markers, skipping frame");
            continue;
        }

        let frame = Bytes::from(payload);
        *latest.write().unwrap() = frame.clone();
        // Err only means there are currently no WebSocket subscribers — fine.
        let _ = tx.send(frame);
        debug!(length, subscribers = tx.receiver_count(), "frame published");
    }
}

/// Slide a 4-byte window one byte at a time until it matches MAGIC.
fn sync_to_magic<R: Read>(reader: &mut R) -> std::io::Result<()> {
    let mut window = [0u8; 4];
    let mut filled = 0usize;
    let mut byte = [0u8; 1];
    loop {
        reader.read_exact(&mut byte)?;
        if filled < 4 {
            window[filled] = byte[0];
            filled += 1;
        } else {
            window.rotate_left(1);
            window[3] = byte[0];
        }
        if filled == 4 && window == MAGIC {
            return Ok(());
        }
    }
}
