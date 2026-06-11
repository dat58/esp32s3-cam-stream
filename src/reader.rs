//! Blocking serial reader that decodes the ESP32-S3-CAM framed-JPEG stream.
//!
//! Wire format (repeated per frame), produced by the firmware:
//!     [ MAGIC: AA 55 AA 55 ][ len: u32 little-endian ][ JPEG payload ]
//!
//! Runs on a dedicated std thread (serialport I/O is blocking), publishing each
//! valid frame to a shared `latest` snapshot and a broadcast channel for live
//! WebSocket fan-out.

use std::io::{BufReader, ErrorKind, Read};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

const MAGIC: [u8; 4] = [0xAA, 0x55, 0xAA, 0x55];
const MAX_FRAME: u32 = 4 * 1024 * 1024; // 4 MB sanity cap to reject bad lengths
/// Poll granularity for reads. A timeout is *not* an error — it just means no
/// bytes arrived in this window, so we keep waiting (see `read_full`).
const READ_TIMEOUT: Duration = Duration::from_secs(1);
/// Reconnect backoff bounds. We start short so a genuine blip recovers fast, but
/// back off exponentially so a persistently-absent device isn't hammered.
const RECONNECT_MIN_DELAY: Duration = Duration::from_millis(500);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
/// A connection that streamed for at least this long is considered "stable":
/// the next failure restarts the backoff from the minimum.
const STABLE_CONNECTION: Duration = Duration::from_secs(5);

/// Run the reader loop forever: (re)open the port and decode frames. On a fatal
/// serial error (unplug, reflash, USB hangup) it logs and retries with backoff
/// so the HTTP server keeps running.
pub fn run(port: String, baud: u32, latest: Arc<RwLock<Bytes>>, tx: broadcast::Sender<Bytes>) {
    let mut backoff = RECONNECT_MIN_DELAY;
    loop {
        info!(%port, baud, "opening serial port");
        match serialport::new(&port, baud)
            .timeout(READ_TIMEOUT)
            .open()
        {
            Ok(sp) => {
                let mut reader = BufReader::new(sp);
                let started = Instant::now();
                if let Err(e) = read_frames(&mut reader, &latest, &tx) {
                    error!(error = %e, "serial read loop ended, reconnecting");
                }
                // If we held a working connection for a while, the failure is a
                // fresh event, not a tight loop — reset the backoff.
                backoff = if started.elapsed() >= STABLE_CONNECTION {
                    RECONNECT_MIN_DELAY
                } else {
                    (backoff * 2).min(RECONNECT_MAX_DELAY)
                };
            }
            Err(e) => {
                error!(error = %e, "failed to open serial port, retrying");
                backoff = (backoff * 2).min(RECONNECT_MAX_DELAY);
            }
        }
        std::thread::sleep(backoff);
    }
}

/// Read exactly `buf.len()` bytes, treating a read timeout as "keep waiting"
/// rather than a fatal error. `std::io::Read::read_exact` aborts on the first
/// `TimedOut`, which would tear down the (healthy) connection during any quiet
/// gap between frames; this preserves partial progress across timeouts so frame
/// sync is never lost. Only genuine errors (EOF, Broken pipe) bubble up.
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8], what: &str) -> std::io::Result<()> {
    let total = buf.len();
    let mut filled = 0;
    let mut timeouts = 0u32;
    while filled < total {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "serial port closed (EOF)",
                ));
            }
            Ok(n) => {
                filled += n;
                debug!(what, n, filled, total, "read chunk");
            }
            // No data this window, or an interrupted syscall — keep waiting.
            Err(e) if e.kind() == ErrorKind::TimedOut || e.kind() == ErrorKind::Interrupted => {
                timeouts += 1;
                debug!(what, filled, total, timeouts, "read timed out, waiting for more");
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Decode frames until an I/O error occurs (which bubbles up to trigger a reconnect).
fn read_frames<R: Read>(
    reader: &mut R,
    latest: &Arc<RwLock<Bytes>>,
    tx: &broadcast::Sender<Bytes>,
) -> std::io::Result<()> {
    loop {
        let skipped = sync_to_magic(reader)?;

        let mut len_buf = [0u8; 4];
        read_full(reader, &mut len_buf, "len")?;
        let length = u32::from_le_bytes(len_buf);
        debug!(length, skipped, "frame header: read 4-byte length");
        if length == 0 || length > MAX_FRAME {
            warn!(length, "bogus frame length, resyncing");
            continue;
        }

        let mut payload = vec![0u8; length as usize];
        read_full(reader, &mut payload, "payload")?;
        debug!(length, "frame payload: read full payload");

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

/// Slide a 4-byte window one byte at a time until it matches MAGIC. Returns the
/// number of bytes consumed before the marker was found (0 == perfectly aligned;
/// a large value means we're skipping junk / mid-stream noise to resync).
fn sync_to_magic<R: Read>(reader: &mut R) -> std::io::Result<usize> {
    let mut window = [0u8; 4];
    let mut filled = 0usize;
    let mut byte = [0u8; 1];
    let mut consumed = 0usize;
    loop {
        read_full(reader, &mut byte, "magic")?;
        consumed += 1;
        if filled < 4 {
            window[filled] = byte[0];
            filled += 1;
        } else {
            window.rotate_left(1);
            window[3] = byte[0];
        }
        if filled == 4 && window == MAGIC {
            return Ok(consumed - 4);
        }
    }
}
