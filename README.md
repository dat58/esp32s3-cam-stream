# esp32s3-cam-stream

An [actix-web](https://actix.rs/) server that reads the framed-JPEG stream from an
**ESP32-S3-CAM** over UART and re-exposes it over HTTP:

- **`GET /`** — a browser viewer that renders the live stream into an `<img>`.
- **`GET /latest`** — the most recent JPEG frame (`image/jpeg`), one per request.
- **`/ws`** — a WebSocket that pushes each new JPEG frame in real time (one binary message per frame).

It reads the same wire format the firmware emits (`AA 55 AA 55` marker + 4-byte little-endian
length + JPEG payload).

## Requirements

- **Rust** (edition 2024 — use a recent stable toolchain, 1.85+).
- **Linux build dependencies for `serialport`:** `libudev` and `pkg-config`.
  ```bash
  # Debian/Ubuntu (incl. WSL2)
  sudo apt-get install -y pkg-config libudev-dev
  ```
- An **ESP32-S3-CAM** flashed with the streaming firmware, connected over its UART/CH343 port
  (appears as `/dev/ttyACM0`). The board must be streaming for frames to appear.

## Configuration

Configuration is entirely via environment variables, loaded from a `.env` file at startup (a
`.env` is optional — defaults apply if a variable is unset). Copy the template and edit:

```bash
cp .env.example .env
```

| Variable    | Default          | Description |
|-------------|------------------|-------------|
| `CAM_PORT`  | `/dev/ttyACM0`   | UART/CH343 serial port the firmware streams over. |
| `CAM_BAUD`  | `4000000`        | Baud rate. **Must match the flashed firmware**. |
| `BIND_ADDR` | `0.0.0.0:8080`   | Address:port the HTTP/WebSocket server binds to. |
| `RUST_LOG`  | `info`           | Log filter ([`tracing` EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) syntax), e.g. `debug`, `esp32s3_cam_stream=debug`. |

> The baud **must match the firmware**. If they disagree you'll get no frames or a flood of
> dropped/skipped frames. There is no hardware flow control, so a baud the host can't drain shows
> up as occasional skipped frames (the reader resyncs on the next marker) rather than corruption.

## Running

```bash
cargo run --release
```

Then open the viewer in a browser:

- **http://localhost:8080/**

The page connects to `ws://<host>/ws` automatically and shows the live stream (with an fps
counter and auto-reconnect). The raw endpoints are also available directly:

```bash
# Save the latest single frame
curl -s http://localhost:8080/latest -o frame.jpg
```

### Important: don't share the serial port

The UART port carries the **binary JPEG stream**. Only one program may hold it at a time. Don't
run this server while `espflash monitor` or `cargo run` (flashing) are
using the same port — stop them first.


## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| Server starts but `GET /latest` returns `503 no frame yet` | No frames decoded yet — board not streaming, wrong `CAM_PORT`, or mismatched `CAM_BAUD`. |
| Log: `failed to open serial port, retrying` | Port doesn't exist or is held by another program. Check `CAM_PORT` and that nothing else (monitor/flasher/reader) has it open. The server keeps retrying and recovers once the port is free. |
| Log: `invalid JPEG markers, skipping frame` | Normal resync after dropped bytes / a false marker. Occasional is fine; if frequent, lower `CAM_BAUD`. |
| Permission denied opening the port | Add your user to the `dialout` group: `sudo usermod -aG dialout $USER` (re-login), or adjust the device permissions. |
