# picam-orchestrator

A headless WebP streaming backend for Raspberry Pi camera systems. Receives raw YUV420 video from `picam-raw` and object detection data from `picam-hailo`, then encodes and streams annotated or live video to browsers over HTTP.

## Overview

`picam-orchestrator` sits between the camera/AI pipeline and the viewer:

```
picam-raw  ─────(UDP YUV420)────► picam-orchestrator ──(HTTP, multipart WebP)──► browsers
picam-hailo ────(TCP JSON)──────►        │
                                         ▼
                                   http://<pi>:81
```

Streams can be toggled at runtime between **live** (zero added latency) and **annotated** (delayed by a configurable hold time so detection boxes align with frames). Both modes, OSD overlays, and camera switching are controllable via HTTP endpoints without restarting the service.

## Features

- Dual-resolution streams: full-resolution `main` and low-resolution `lores`, each independently configurable
- Multipart WebP stream over HTTP (`multipart/x-mixed-replace`, same `<img src="/stream">` delivery MJPEG used) with up to 50 concurrent clients
- Direct YUV420→WebP encoding (no RGB conversion) — roughly 25-40% smaller frames than the JPEG this replaced, at equivalent visual quality, for about the same CPU cost (both are intra-only per-frame codecs; the Pi 5 has no hardware encoder for a true inter-frame codec like H.264 anyway)
- Bounding box + label annotation drawn into YUV luma plane
- OSD burn-in: camera ID and wall-clock timestamp from embedded frame timestamps
- Runtime toggles for annotation, OSD, and camera switching—no restart required
- Auto-reconnecting TCP clients for detections and telemetry
- Systemd service with `CAP_NET_BIND_SERVICE` and CPU affinity pinning

Event snapshot files written to disk (by `EventRecorder`, on detection triggers) are still plain JPEG — that's a one-off archival file, not the bandwidth-sensitive live stream, so it wasn't changed.

## Requirements

**Build:**
- CMake ≥ 3.16
- C++17 compiler
- `libjpeg-dev` (event snapshot files)
- `libwebp-dev` (live stream)
- `pkg-config`

**Runtime:**
- `libjpeg` and `libwebp` shared libraries
- `picam-raw` (UDP streams + telemetry + command server)
- `picam-hailo` (detection TCP stream)

## Build

```bash
cmake -B build
cmake --build build -j$(nproc)
```

The binary is `build/picam_orchestrator`.

## Install (Debian package)

```bash
dpkg -i picam-orchestrator_*.deb
systemctl enable --now picam-orchestrator
```

The package creates a `picam-orchestrator` system user, installs the systemd unit, and deploys a default `config.ini` to `/etc/picam-orchestrator/`.

## Usage

```bash
./picam_orchestrator --config config.ini
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config`, `-c` | `config.ini` | Path to configuration file |

The HTTP viewer is available at `http://<pi-ip>:81` once the upstream services are running.

## Configuration

All settings are read from `config.ini` at startup. Edit the file and restart the service to apply changes (runtime toggles for annotation and OSD are available via HTTP).

```ini
[input]
host         = 127.0.0.1
main_port    = 8560        ; UDP port for full-res YUV420 stream
main_width   = 2304
main_height  = 1296
lores_port   = 8561        ; UDP port for low-res stream
lores_width  = 640
lores_height = 360
ping_every   = 5           ; heartbeat interval (seconds)

[detections]
host         = 127.0.0.1
port         = 8558        ; picam-hailo TCP detection stream
tolerance_ms = 150         ; max timestamp delta for frame↔detection match

[telemetry]
host         = 127.0.0.1
port         = 8555        ; lux + active-camera stream
command_port = 8556        ; camera-switch command port

[delay]
delay_ms     = 1000        ; hold time before annotating and sending

[encode]
webp_quality          = 80        ; live stream (/stream)
jpeg_quality          = 80        ; EventRecorder's saved snapshot files only
output_fps_live       = 15
output_fps_annotated  = 30

[annotate]
lores = false              ; start in annotated mode for lores
main  = false              ; start in annotated mode for main

[osd]
camera_id     = false      ; burn camera ID into frame
time          = false      ; burn UTC timestamp into frame
camera_labels = "Front,Back"  ; optional human-readable labels

[output]
http_port      = 81
status_port    = 8091
default_stream = main
```

## HTTP Endpoints

| Endpoint | Description |
|----------|-------------|
| `/stream?stream=main\|lores` | Multipart WebP stream (`multipart/x-mixed-replace`) |
| `/status.json` | Pipeline stats, FPS, client count, telemetry |
| `/annotate?main=true\|false&lores=true\|false` | Toggle delayed+annotated mode per resolution |
| `/osd?camera_id=true\|false&time=true\|false` | Toggle OSD overlays at runtime |
| `/camera?id=N` | Switch active camera (proxied to picam-raw) |

### Examples

```bash
# Enable annotated main stream (frames held delay_ms, boxes drawn)
curl http://<pi-ip>:81/annotate?main=true

# Disable annotation, return to zero-latency live
curl http://<pi-ip>:81/annotate?main=false

# Show timestamp OSD
curl http://<pi-ip>:81/osd?time=true

# Switch to camera 1
curl http://<pi-ip>:81/camera?id=1

# Check pipeline status (plaintext key=value)
echo status | nc <pi-ip> 8091
```

## Architecture

### Threading model

| Thread | Owner | Responsibility |
|--------|-------|----------------|
| UDP receive | `UdpRawReceiver` (×2) | Reassemble chunks into YUV420 frames, invoke callback |
| UDP ping | `UdpRawReceiver` (×2) | Send heartbeat to picam-raw every `ping_every` seconds |
| Detection recv | `DetectionReceiver` | TCP read from picam-hailo, auto-reconnect |
| Telemetry recv | `TelemetryReceiver` | TCP read from picam-raw telemetry, auto-reconnect |
| HTTP accept | `StreamHttpServer` | Accept connections, spawn per-client threads |
| Per-client write | `StreamHttpServer` (×N) | Send WebP frames to one client, keepalive boundary every 10 s |
| Status server | `StatusServer` | One-shot TCP status queries |
| Main encode loop | `main()` | Drain delay buffers, annotate, encode WebP, broadcast |

### Frame path (annotated mode)

```
UDP chunk arrival → reassemble → DelayBuffer::push()
                                      │
                               (delay_ms elapses)
                                      │
Main encode loop ──────► DelayBuffer::pop()
        │                             │
        │              DetectionBuffer::findNearest()
        │                             │
        └──────► annotate YUV ────────┘
                      │
                 OSD burn-in
                      │
                 WebP encode
                      │
              broadcast to clients
```

In **live mode** the delay buffer is still drained (to prevent growth) but the encode loop uses the latest frame mailbox instead, adding zero hold time.

### Key design choices

- **Lock-free client broadcast:** The client list is an `atomic<shared_ptr<vector<>>>` swapped only on connect/disconnect. The hot encode path does a single atomic load—no mutex on frame delivery.
- **Direct YUV→WebP:** `WebPPicture`'s YUV420 fields (`y`/`u`/`v` planes + strides) accept planar YUV directly, bypassing any colorspace conversion — same trick the previous JPEG encoder used via libjpeg's `jpeg_write_raw_data`.
- **WebP over a real inter-frame codec:** WebP (like JPEG) is intra-only per frame — no motion estimation between frames — so it costs about the same CPU as JPEG while still buying ~25-40% smaller frames from its better entropy coding and prediction modes. A real video codec (H.264 etc.) would compress much further, but that gain specifically comes from motion-compensated inter-frame prediction, which is the CPU-expensive part — and the Pi 5 has no hardware H.264 encoder to offload it to.
- **Annotation in luma only:** Bounding boxes and labels are drawn into the Y plane only (luma value 235 for boxes, 16 for backgrounds). No chroma modification needed for visible overlays.
- **50-client cap:** Prevents unbounded thread and memory growth; designed for small viewer counts, not CDN scale.

## Status output

```
$ echo status | nc <pi-ip> 8091
frames_in=1234
frames_out=1230
matched=1229
fps=15.0
delay_buffer_depth=2
clients=3
```

`/status.json` returns the same data as JSON alongside telemetry (lux, active camera, label).

## Systemd service

```bash
systemctl start   picam-orchestrator
systemctl stop    picam-orchestrator
systemctl status  picam-orchestrator
journalctl -u picam-orchestrator -f
```

The unit runs as an unprivileged user with `CAP_NET_BIND_SERVICE` (for port 81), pinned to CPU core 2, and restarts automatically after 3 seconds on failure.
