/**
 * picam-orchestrator
 * ============
 * Delays the raw main YUV420 stream from picam-raw by ~1 second, draws
 * detection boxes from picam-hailo onto the delayed frames (matched by
 * timestamp), encodes to WebP, and streams live to any browser as a
 * motion-WebP sequence over plain HTTP. No signaling, no negotiation, no
 * extra protocol — just <img src="/stream"> which every browser has
 * supported natively for decades; only the per-frame image codec inside
 * the multipart response changed (from JPEG to WebP, for smaller frames
 * at equivalent quality — see WebpEncoder below).
 *
 * Event snapshots saved to disk by EventRecorder are still plain JPEG
 * (JpegEncoder, unchanged) — those are one-off files meant for viewing/
 * archival, not a bandwidth-sensitive live stream.
 *
 * Pipeline:
 *
 *   picam-raw (UDP main stream, YUV420, chunked)
 *       │
 *       ▼
 *   UdpRawReceiver  →  DelayBuffer (holds ~1s)
 *       │
 *   picam-hailo (TCP detection stream, JSON)        ─┐
 *       │                                             │
 *       ▼                                             │
 *   DetectionBuffer (timestamp indexed)  ◄─────────────┘
 *       │
 *       ▼  (frame released after delay_ms, nearest detection found)
 *   Annotator — draws boxes into YUV420 Y-plane
 *       │
 *       ▼
 *   WebpEncoder (libwebp)
 *       │
 *       ▼
 *   StreamHttpServer — multipart/x-mixed-replace over HTTP
 *       ▼
 *   Browser  (open http://<pi-ip>:81, or via picam-frontend on port 80)
 *
 * Build:
 *   cmake -B build && cmake --build build -j$(nproc)
 *
 * Run:
 *   ./build/picam_orchestrator --config config.ini
 *   Open http://<pi-ip>:81 in a browser (default port).
 */

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <queue>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "config.hpp"

using Clock = std::chrono::steady_clock;
static std::atomic<bool> g_stop{false};
static void signal_handler(int) { g_stop = true; }

// Global, runtime-toggleable OSD state — camera ID and timestamp burn-in
// are independent flags, each startable from config.ini (osd.camera_id,
// osd.time) but flippable on the fly via GET /osd?camera_id=true|false
// and/or &time=true|false, without restarting the service. Affects every
// connected viewer and every stream (Main/Lores/Annotated) on this Pi —
// there is no per-client override by design.
static std::atomic<bool> g_osd_camera_id_enabled{false};
static std::atomic<bool> g_osd_time_enabled{false};

// Global, per-resolution annotation toggle. When ON for a resolution,
// that resolution's stream is routed through the delay buffer + lores
// detection matching (1s delay, boxes drawn from picam-hailo) instead
// of being sent live. When OFF, frames go straight to the encoder with
// zero added latency. This is a server-wide setting per resolution —
// there is no per-client override (every Lores viewer sees the same
// thing; same for Main), matching how the OSD toggle already works.
static std::atomic<bool> g_lores_annotated{false};
static std::atomic<bool> g_main_annotated{false};

// picam-raw's address for camera-switch commands. Set once in main()
// from config (telemetry.host / telemetry.command_port) before
// StreamHttpServer starts — read-only after that, so no synchronization
// is needed despite being read from request-handling threads.
static std::string g_picamraw_host = "127.0.0.1";
static int         g_picamraw_cmd_port = 8556;

// ─────────────────────────────────────────────────────────────────────────────
// Chunk header — matches picam-raw exactly
// ─────────────────────────────────────────────────────────────────────────────
static constexpr size_t kHeaderSize       = 8;
static constexpr size_t kChunk0HeaderSize = 32;
static constexpr size_t kLabelSize        = 15;

struct ChunkHeader {
    uint32_t frameSeq    = 0;
    uint16_t chunkSeq    = 0;
    uint16_t totalChunks = 0;
    int64_t  timestampUs = 0;
    uint8_t  cameraIndex = 0;
    char     cameraLabel[kLabelSize + 1] = {};
};

static ChunkHeader parseHeader(const uint8_t* buf, size_t len) {
    ChunkHeader h;
    h.frameSeq    = (uint32_t(buf[0])<<24)|(uint32_t(buf[1])<<16)|
                    (uint32_t(buf[2])<<8) | uint32_t(buf[3]);
    h.chunkSeq    = (uint16_t(buf[4])<<8) | uint16_t(buf[5]);
    h.totalChunks = (uint16_t(buf[6])<<8) | uint16_t(buf[7]);
    if (h.chunkSeq == 0 && len >= kChunk0HeaderSize) {
        h.timestampUs =
            (int64_t(buf[ 8])<<56)|(int64_t(buf[ 9])<<48)|
            (int64_t(buf[10])<<40)|(int64_t(buf[11])<<32)|
            (int64_t(buf[12])<<24)|(int64_t(buf[13])<<16)|
            (int64_t(buf[14])<<8) | int64_t(buf[15]);
        h.cameraIndex = buf[16];
        std::memcpy(h.cameraLabel, &buf[17], kLabelSize);
        h.cameraLabel[kLabelSize] = '\0';
    }
    return h;
}

// ─────────────────────────────────────────────────────────────────────────────
// RawFrame
// ─────────────────────────────────────────────────────────────────────────────
struct RawFrame {
    std::vector<uint8_t> data;
    int      width  = 0;
    int      height = 0;
    int64_t  timestampUs = 0;
    uint8_t  cameraIndex = 0;
    Clock::time_point arrival;  // local monotonic arrival time, for delay timing
};

// ─────────────────────────────────────────────────────────────────────────────
// Detection types (from picam-hailo JSON)
// ─────────────────────────────────────────────────────────────────────────────
struct Detection {
    std::string cls;
    float conf, x0, y0, x1, y1;  // normalised [0,1] relative to model input frame
};
struct DetectionEvent {
    int64_t  ts_us     = 0;
    uint32_t frame_seq = 0;
    std::vector<Detection> dets;
};

// ─────────────────────────────────────────────────────────────────────────────
// UdpRawReceiver — same chunk reassembly protocol as picam-recorder/picam-hailo
// ─────────────────────────────────────────────────────────────────────────────
class UdpRawReceiver {
public:
    using FrameCallback = std::function<void(RawFrame)>;

    UdpRawReceiver(std::string host, int port, int width, int height,
                   int pingEverySecs, FrameCallback cb)
        : host_(std::move(host)), port_(port)
        , frameBytes_(static_cast<size_t>(width * height * 3 / 2))
        , width_(width), height_(height)
        , pingEvery_(pingEverySecs), cb_(std::move(cb))
    {}

    ~UdpRawReceiver() {
        if (sock_ >= 0) ::close(sock_);
        if (recvThread_.joinable()) recvThread_.join();
        if (pingThread_.joinable()) pingThread_.join();
    }

    void start() {
        sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sock_ < 0) throw std::runtime_error("UdpRawReceiver: socket() failed");
        int rcvbuf = 64 * 1024 * 1024;
        ::setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        // Without a receive timeout, recv() in recvLoop() blocks
        // indefinitely whenever picam-raw goes quiet. close()ing the
        // socket from the destructor is NOT reliable at unblocking a
        // thread already parked inside recv() on a UDP socket — confirmed
        // via gdb: ~UdpRawReceiver()'s join() hung forever on exactly this.
        // A periodic timeout lets recvLoop's while(!g_stop) check re-run
        // regularly, the same fix already applied to DetectionReceiver
        // and TelemetryReceiver.
        struct timeval tv{1, 0};
        ::setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        sockaddr_in local{};
        local.sin_family      = AF_INET;
        local.sin_addr.s_addr = INADDR_ANY;
        local.sin_port        = 0;
        ::bind(sock_, reinterpret_cast<sockaddr*>(&local), sizeof(local));
        std::memset(&server_, 0, sizeof(server_));
        server_.sin_family = AF_INET;
        server_.sin_port   = htons(static_cast<uint16_t>(port_));
        ::inet_pton(AF_INET, host_.c_str(), &server_.sin_addr);
        pingThread_ = std::thread(&UdpRawReceiver::pingLoop, this);
        recvThread_ = std::thread(&UdpRawReceiver::recvLoop, this);
        std::cerr << "[UDP] Receiving " << width_ << "x" << height_
                  << " from " << host_ << ":" << port_ << "\n";
    }

    bool streamReady() const { return framesReceived_.load() > 0; }
    bool waitForStream(int timeoutSecs) {
        auto deadline = Clock::now() + std::chrono::seconds(timeoutSecs);
        while (Clock::now() < deadline && !g_stop) {
            if (streamReady()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return streamReady();
    }

private:
    void pingLoop() {
        while (!g_stop) {
            ::sendto(sock_, "HELLO", 5, 0,
                     reinterpret_cast<sockaddr*>(&server_), sizeof(server_));
            for (int i = 0; i < pingEvery_ * 10 && !g_stop; ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    void recvLoop() {
        std::vector<uint8_t> buf(kChunk0HeaderSize + 65536);
        std::map<uint32_t, std::map<uint16_t, std::vector<uint8_t>>> partial;
        std::map<uint32_t, ChunkHeader> frameMeta;

        while (!g_stop) {
            ssize_t n = ::recv(sock_, buf.data(), buf.size(), 0);
            if (n < 0) { if (errno == EINTR || g_stop) break; continue; }
            if (static_cast<size_t>(n) < kHeaderSize) continue;

            ChunkHeader hdr = parseHeader(buf.data(), static_cast<size_t>(n));
            if (hdr.totalChunks == 0) continue;
            size_t hdrSize = (hdr.chunkSeq == 0) ? kChunk0HeaderSize : kHeaderSize;
            if (static_cast<size_t>(n) <= hdrSize) continue;

            if (partial.size() > 32) {
                auto it = partial.begin();
                frameMeta.erase(it->first);
                partial.erase(it);
            }
            if (hdr.chunkSeq == 0) frameMeta[hdr.frameSeq] = hdr;

            partial[hdr.frameSeq][hdr.chunkSeq].assign(
                buf.begin() + hdrSize, buf.begin() + n);
            auto& chunks = partial[hdr.frameSeq];
            if (static_cast<uint16_t>(chunks.size()) != hdr.totalChunks) continue;

            std::vector<uint8_t> frameData;
            frameData.reserve(frameBytes_);
            bool ok = true;
            for (uint16_t i = 0; i < hdr.totalChunks; ++i) {
                auto it = chunks.find(i);
                if (it == chunks.end()) { ok = false; break; }
                frameData.insert(frameData.end(), it->second.begin(), it->second.end());
            }
            partial.erase(hdr.frameSeq);
            if (!ok || frameData.size() < frameBytes_) { frameMeta.erase(hdr.frameSeq); continue; }
            frameData.resize(frameBytes_);

            RawFrame rf;
            rf.data    = std::move(frameData);
            rf.width   = width_;
            rf.height  = height_;
            rf.arrival = Clock::now();
            auto mit = frameMeta.find(hdr.frameSeq);
            if (mit != frameMeta.end()) {
                rf.timestampUs = mit->second.timestampUs;
                rf.cameraIndex = mit->second.cameraIndex;
                frameMeta.erase(mit);
            }
            cb_(std::move(rf));
            ++framesReceived_;
        }
    }

    std::string   host_;
    int           port_;
    size_t        frameBytes_;
    int           width_, height_, pingEvery_;
    FrameCallback cb_;
    int           sock_ = -1;
    sockaddr_in   server_{};
    std::thread   recvThread_, pingThread_;
    std::atomic<int> framesReceived_{0};
};

// ─────────────────────────────────────────────────────────────────────────────
// DelayBuffer — holds frames until delay_ms has elapsed since arrival,
// then releases them in order via a pull interface.
// ─────────────────────────────────────────────────────────────────────────────
class DelayBuffer {
public:
    explicit DelayBuffer(int delayMs) : delayMs_(delayMs) {}

    void push(RawFrame frame) {
        std::lock_guard<std::mutex> lk(mu_);
        q_.push_back(std::move(frame));
        cv_.notify_one();
    }

    // Blocks until a frame has aged past delayMs_, then returns it.
    bool pop(RawFrame& out, int waitMs = 200) {
        std::unique_lock<std::mutex> lk(mu_);
        auto now = Clock::now();
        if (q_.empty() ||
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now - q_.front().arrival).count() < delayMs_) {
            cv_.wait_for(lk, std::chrono::milliseconds(waitMs));
            return false;
        }
        out = std::move(q_.front());
        q_.pop_front();
        return true;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return q_.size();
    }

private:
    int                       delayMs_;
    std::deque<RawFrame>      q_;
    mutable std::mutex        mu_;
    std::condition_variable   cv_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Minimal JSON field extraction (no JSON library dependency)
// Sufficient for the flat structure picam-hailo emits.
// ─────────────────────────────────────────────────────────────────────────────
namespace minijson {

static int64_t extract_int64(const std::string& s, const std::string& key) {
    auto pos = s.find("\"" + key + "\":");
    if (pos == std::string::npos) return 0;
    pos += key.size() + 3;
    return std::strtoll(s.c_str() + pos, nullptr, 10);
}

static float extract_float(const std::string& s, size_t fromPos, const std::string& key) {
    auto pos = s.find("\"" + key + "\":", fromPos);
    if (pos == std::string::npos) return 0.0f;
    pos += key.size() + 3;
    return std::strtof(s.c_str() + pos, nullptr);
}

static std::string extract_string(const std::string& s, size_t fromPos, const std::string& key) {
    auto pos = s.find("\"" + key + "\":\"", fromPos);
    if (pos == std::string::npos) return "";
    pos += key.size() + 4;
    auto end = s.find('"', pos);
    if (end == std::string::npos) return "";
    return s.substr(pos, end - pos);
}

// Parse a picam-hailo detection JSON line into a DetectionEvent.
// Skips frame_jpeg entirely — we only need ts_us and the box list.
static bool parse_event(const std::string& line, DetectionEvent& out) {
    auto detPos = line.find("\"detections\":[");
    if (detPos == std::string::npos) return false;

    out.ts_us     = extract_int64(line, "ts_us");
    out.frame_seq = static_cast<uint32_t>(extract_int64(line, "frame_seq"));
    out.dets.clear();

    size_t pos = detPos + 14;
    while (pos < line.size() && line[pos] != ']') {
        auto objStart = line.find('{', pos);
        if (objStart == std::string::npos) break;
        auto objEnd = line.find('}', objStart);
        // box is a nested object, so find the SECOND '}' (closes "box", then closes the det)
        auto boxClose = line.find('}', objEnd + 1);
        if (boxClose == std::string::npos) break;
        std::string obj = line.substr(objStart, boxClose - objStart + 1);

        Detection d;
        d.cls  = extract_string(obj, 0, "class");
        d.conf = extract_float(obj, 0, "conf");
        d.x0   = extract_float(obj, 0, "x0");
        d.y0   = extract_float(obj, 0, "y0");
        d.x1   = extract_float(obj, 0, "x1");
        d.y1   = extract_float(obj, 0, "y1");
        out.dets.push_back(d);

        pos = boxClose + 1;
        if (pos < line.size() && line[pos] == ',') ++pos;
    }
    return true;
}

} // namespace minijson

// ─────────────────────────────────────────────────────────────────────────────
// sendCameraSwitchCommand — talks to picam-raw's plain-text CommandServer
// (default TCP port 8556) to switch the active camera. picam-raw already
// implements this protocol: send "switch<N>\n", get back a JSON line like
// {"ok":true,"active_camera":1,"label":"..."}. This is a short, one-shot
// connect/send/recv/close — not a persistent connection like
// TelemetryReceiver/DetectionReceiver, since camera switches are rare,
// user-triggered events, not a continuous stream.
//
// Returns true and fills outResponse with picam-raw's raw response line on
// success. Returns false (outResponse holds an error message instead) if
// the connection or the round-trip fails for any reason — unreachable
// host, connect timeout, or picam-raw not running.
static bool sendCameraSwitchCommand(const std::string& host, int port, int cameraId,
                                    std::string& outResponse, int timeoutMs = 3000) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { outResponse = "{\"ok\":false,\"error\":\"socket() failed\"}"; return false; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        outResponse = "{\"ok\":false,\"error\":\"invalid host\"}";
        return false;
    }

    // Non-blocking connect with a bounded timeout — this is a real network
    // call to a separate process (possibly even a separate machine in some
    // deployments), so unlike picam-orchestrator's other loopback-only sockets,
    // a hung or unreachable picam-raw must not be able to block this
    // thread indefinitely.
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        ::close(fd);
        outResponse = "{\"ok\":false,\"error\":\"connect failed\"}";
        return false;
    }
    if (rc < 0) {
        fd_set wfds; FD_ZERO(&wfds); FD_SET(fd, &wfds);
        timeval tv{timeoutMs / 1000, (timeoutMs % 1000) * 1000};
        rc = ::select(fd + 1, nullptr, &wfds, nullptr, &tv);
        if (rc <= 0) {
            ::close(fd);
            outResponse = "{\"ok\":false,\"error\":\"connect timed out\"}";
            return false;
        }
        int err = 0; socklen_t elen = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
        if (err != 0) {
            ::close(fd);
            outResponse = "{\"ok\":false,\"error\":\"connect failed\"}";
            return false;
        }
    }
    fcntl(fd, F_SETFL, flags);  // back to blocking for the send/recv below

    std::string cmd = "switch" + std::to_string(cameraId) + "\n";
    if (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) < 0) {
        ::close(fd);
        outResponse = "{\"ok\":false,\"error\":\"send failed\"}";
        return false;
    }

    struct timeval rtv{2, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv));
    char buf[512];
    ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
    ::close(fd);

    if (n <= 0) {
        outResponse = "{\"ok\":false,\"error\":\"no response from picam-raw\"}";
        return false;
    }
    buf[n] = '\0';
    outResponse.assign(buf, static_cast<size_t>(n));
    // Trim trailing newline picam-raw always sends, for a cleaner embed
    // into picam-orchestrator's own JSON response below.
    while (!outResponse.empty() &&
          (outResponse.back() == '\n' || outResponse.back() == '\r'))
        outResponse.pop_back();
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// DetectionBuffer — thread-safe, timestamp-indexed ring of recent detections.
// Old entries beyond maxAgeMs are pruned on insert.
// ─────────────────────────────────────────────────────────────────────────────
class DetectionBuffer {
public:
    explicit DetectionBuffer(int maxAgeMs) : maxAgeMs_(maxAgeMs) {}

    void push(DetectionEvent evt) {
        std::lock_guard<std::mutex> lk(mu_);
        events_.push_back(std::move(evt));
        // Prune anything older than maxAgeMs relative to the newest event
        int64_t newest = events_.back().ts_us;
        while (!events_.empty() &&
               (newest - events_.front().ts_us) > maxAgeMs_ * 1000LL)
            events_.pop_front();
    }

    // Find the detection event closest to target_ts_us within tolerance.
    // Returns true and fills `out` if found.
    bool findNearest(int64_t target_ts_us, int64_t toleranceUs,
                     DetectionEvent& out) const {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t bestDiff = INT64_MAX;
        bool found = false;
        for (const auto& e : events_) {
            int64_t diff = std::abs(e.ts_us - target_ts_us);
            if (diff < bestDiff) { bestDiff = diff; out = e; found = true; }
        }
        return found && bestDiff <= toleranceUs;
    }

    size_t size() const { std::lock_guard<std::mutex> lk(mu_); return events_.size(); }

private:
    int maxAgeMs_;
    std::deque<DetectionEvent> events_;
    mutable std::mutex mu_;
};

// ─────────────────────────────────────────────────────────────────────────────
// DetectionReceiver — TCP client connecting to picam-hailo's detection port.
// Reconnects automatically on disconnect. Parses newline-delimited JSON.
// ─────────────────────────────────────────────────────────────────────────────
class DetectionReceiver {
public:
    using OnEvent = std::function<void(const DetectionEvent&)>;

    DetectionReceiver(std::string host, int port, DetectionBuffer& buf,
                      OnEvent onEvent = nullptr)
        : host_(std::move(host)), port_(port), buf_(buf)
        , onEvent_(std::move(onEvent)) {}

    ~DetectionReceiver() {
        if (thread_.joinable()) thread_.join();
    }

    void start() {
        thread_ = std::thread(&DetectionReceiver::loop, this);
    }

private:
    void loop() {
        while (!g_stop) {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons(static_cast<uint16_t>(port_));
            ::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);

            if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
                ::close(fd);
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }
            std::cerr << "[Detections] Connected to " << host_ << ":" << port_ << "\n";

            // Without a receive timeout, recv() blocks indefinitely whenever
            // picam-hailo goes quiet (which is normal — detections only fire
            // when something is in frame). That left this thread unable to
            // notice g_stop, so the destructor's join() would hang forever
            // and the whole process needed `kill -9` to exit. A periodic
            // timeout lets the while(!g_stop) check re-run regularly.
            struct timeval tv{1, 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            std::string lineBuf;
            char chunk[8192];
            while (!g_stop) {
                ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) continue;  // timeout, loop and recheck g_stop
                    break;
                }
                if (n == 0) break;  // peer closed
                lineBuf.append(chunk, static_cast<size_t>(n));

                size_t nl;
                while ((nl = lineBuf.find('\n')) != std::string::npos) {
                    std::string line = lineBuf.substr(0, nl);
                    lineBuf.erase(0, nl + 1);
                    DetectionEvent evt;
                    if (minijson::parse_event(line, evt)) {
                        if (onEvent_) onEvent_(evt);
                        buf_.push(std::move(evt));
                    }
                }
            }
            ::close(fd);
            std::cerr << "[Detections] Disconnected, retrying...\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    std::string      host_;
    int              port_;
    DetectionBuffer& buf_;
    OnEvent          onEvent_;
    std::thread      thread_;
};

// ─────────────────────────────────────────────────────────────────────────────
// UUID generator — tries /proc/sys/kernel/random/uuid first (Linux), then
// falls back to reading 16 bytes from /dev/urandom and formatting as v4.
// ─────────────────────────────────────────────────────────────────────────────
static std::string generateUUID() {
    {
        std::ifstream f("/proc/sys/kernel/random/uuid");
        if (f) {
            std::string s;
            std::getline(f, s);
            while (!s.empty() && (s.back() == '\n' || s.back() == '\r' || s.back() == ' '))
                s.pop_back();
            if (s.size() == 36) return s;
        }
    }
    uint8_t b[16] = {};
    { std::ifstream u("/dev/urandom", std::ios::binary); u.read(reinterpret_cast<char*>(b), 16); }
    b[6] = (b[6] & 0x0F) | 0x40;
    b[8] = (b[8] & 0x3F) | 0x80;
    char buf[37];
    std::snprintf(buf, sizeof(buf),
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        b[0],b[1],b[2],b[3], b[4],b[5], b[6],b[7],
        b[8],b[9], b[10],b[11],b[12],b[13],b[14],b[15]);
    return buf;
}

// ─────────────────────────────────────────────────────────────────────────────
// Recorder TCP helpers — one-shot connect/send/recv/close per command.
// picam-recorder's control protocol: plain-text "start <name>\n", "stop\n".
// Response: key=value lines terminated by a blank line.
// ─────────────────────────────────────────────────────────────────────────────
static std::string recorderCommand(const std::string& host, int port,
                                   const std::string& cmd) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return "";

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd); return "";
    }

    // 3-second non-blocking connect
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    ::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    fd_set wfds; FD_ZERO(&wfds); FD_SET(fd, &wfds);
    timeval tv{3, 0};
    if (::select(fd + 1, nullptr, &wfds, nullptr, &tv) <= 0) {
        ::close(fd); return "";
    }
    ::fcntl(fd, F_SETFL, flags);

    std::string line = cmd + "\n";
    if (::send(fd, line.data(), line.size(), MSG_NOSIGNAL) < 0) {
        ::close(fd); return "";
    }

    // Read until blank line (\n\n) or timeout
    timeval rtv{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv));
    std::string resp;
    char buf[512];
    while (resp.find("\n\n") == std::string::npos) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        resp.append(buf, static_cast<size_t>(n));
    }
    ::close(fd);
    return resp;
}

// Returns the file path reported by the recorder, or empty on error.
static std::string recorderStart(const std::string& host, int port,
                                 const std::string& name) {
    std::string resp = recorderCommand(host, port, "start " + name);
    auto pos = resp.find("file=");
    if (pos == std::string::npos) return "";
    auto end = resp.find('\n', pos + 5);
    return resp.substr(pos + 5,
                       end == std::string::npos ? std::string::npos : end - (pos + 5));
}

static void recorderStop(const std::string& host, int port) {
    recorderCommand(host, port, "stop");
}

// ─────────────────────────────────────────────────────────────────────────────
// EventRecorder — triggers picam-recorder when detections are active, collects
// the detection events, and writes them as <recording>.events.json when
// recording ends (after idle_secs of silence).
// ─────────────────────────────────────────────────────────────────────────────
class EventRecorder {
public:
    // Called with the triggering DetectionEvent; returns JPEG bytes to save,
    // or empty to skip the snapshot.
    using SnapshotFn = std::function<std::vector<uint8_t>(const DetectionEvent&)>;

    EventRecorder(std::string host, int port, SnapshotFn snapshotFn = nullptr)
        : host_(std::move(host)), port_(port), snapshotFn_(std::move(snapshotFn))
    {
        thread_ = std::thread([this]{ loop(); });
    }

    ~EventRecorder() {
        shutdown_ = true;
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    // Called from any thread for every detection event, empty or not.
    // Non-empty dets start/continue a recording; empty dets stop it immediately.
    void notify(const DetectionEvent& evt) {
        std::lock_guard<std::mutex> lk(mu_);
        if (evt.dets.empty()) {
            if (recording_) {
                stopRequested_ = true;
                cv_.notify_all();
            }
            return;
        }
        haveEvents_ = true;
        accumulated_.push_back(evt);
        cv_.notify_all();
    }

private:
    static int64_t nowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void loop() {
        while (!shutdown_) {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait_for(lk, std::chrono::seconds(1));
            if (shutdown_) break;

            if (haveEvents_ && !recording_) {
                std::string uuid = generateUUID();
                int64_t startedUs = nowUs();
                // Grab the trigger event before releasing the lock for I/O below.
                DetectionEvent triggerEvt = accumulated_.empty()
                    ? DetectionEvent{} : accumulated_.front();
                lk.unlock();

                std::string file = recorderStart(host_, port_, uuid);

                if (!file.empty() && snapshotFn_ && !triggerEvt.dets.empty()) {
                    auto jpeg = snapshotFn_(triggerEvt);
                    if (!jpeg.empty()) {
                        std::string imgPath = file;
                        auto dot = imgPath.rfind('.');
                        if (dot != std::string::npos) imgPath.resize(dot);
                        imgPath += ".jpg";
                        std::ofstream imgf(imgPath, std::ios::binary);
                        imgf.write(reinterpret_cast<const char*>(jpeg.data()),
                                   static_cast<std::streamsize>(jpeg.size()));
                        std::cerr << "[EventRecorder] Snapshot: " << imgPath << "\n";
                    }
                }

                lk.lock();
                if (!file.empty()) {
                    recording_   = true;
                    currentFile_ = file;
                    startedUs_   = startedUs;
                    std::cerr << "[EventRecorder] Started: " << file << "\n";
                } else {
                    std::cerr << "[EventRecorder] Failed to start recorder\n";
                    accumulated_.clear();
                    haveEvents_ = false;
                }
            }

            if (recording_ && stopRequested_) {
                stopRequested_ = false;
                flush(lk);
            }
        }

        // Drain on shutdown
        std::unique_lock<std::mutex> lk(mu_);
        if (recording_) flush(lk);
    }

    // Must be called with mu_ held; releases and re-acquires it around I/O.
    void flush(std::unique_lock<std::mutex>& lk) {
        std::string file        = currentFile_;
        auto        events      = std::move(accumulated_);
        int64_t     startedUs   = startedUs_;
        recording_   = false;
        currentFile_ = "";
        haveEvents_  = false;
        lk.unlock();

        recorderStop(host_, port_);
        saveEvents(file, events, startedUs);

        lk.lock();
    }

    void saveEvents(const std::string& mp4path,
                    const std::vector<DetectionEvent>& events,
                    int64_t startedUs) {
        std::string jsonPath = mp4path;
        auto dot = jsonPath.rfind('.');
        if (dot != std::string::npos) jsonPath.resize(dot);
        jsonPath += ".events.json";

        auto writeEventLines = [&](std::ostream& f) {
            for (size_t i = 0; i < events.size(); ++i) {
                const auto& e = events[i];
                f << "    {\"ts_us\": " << e.ts_us
                  << ", \"frame_seq\": " << e.frame_seq
                  << ", \"detections\": [";
                for (size_t j = 0; j < e.dets.size(); ++j) {
                    const auto& d = e.dets[j];
                    if (j) f << ", ";
                    f << "{\"class\": \"" << d.cls << "\""
                      << ", \"conf\": " << d.conf
                      << ", \"x0\": " << d.x0
                      << ", \"y0\": " << d.y0
                      << ", \"x1\": " << d.x1
                      << ", \"y1\": " << d.y1 << "}";
                }
                f << "]}";
                if (i + 1 < events.size()) f << ",";
                f << "\n";
            }
        };

        if (!std::filesystem::exists(jsonPath)) {
            std::ofstream f(jsonPath);
            if (!f) {
                std::cerr << "[EventRecorder] Cannot write: " << jsonPath << "\n";
                return;
            }
            f << std::fixed << std::setprecision(4);
            f << "{\n"
              << "  \"recording\": \"" << mp4path << "\",\n"
              << "  \"started_us\": " << startedUs << ",\n"
              << "  \"events\": [\n";
            writeEventLines(f);
            f << "  ]\n}\n";
            std::cerr << "[EventRecorder] Created: " << jsonPath
                      << " (" << events.size() << " events)\n";
            return;
        }

        // File exists — splice new events in before the closing  ]\n}\n.
        //
        // The file always ends with the 6-byte tail "  ]\n}\n". The byte at
        // position -(tailLen+2) from EOF distinguishes two cases:
        //   '}' → existing events in array, need a comma before the new ones.
        //         Truncate 7 bytes (removes the trailing \n too) so the file
        //         ends at the last event's closing }, then write ,\n + new events.
        //   '[' → events array was empty, no comma needed.
        //         Truncate 6 bytes, file ends at the opening [\n, write directly.
        const std::streamoff kTailLen   = 6;  // "  ]\n}\n"
        const std::streamoff kProbeBack = kTailLen + 2;

        auto fileSize = static_cast<std::streamoff>(std::filesystem::file_size(jsonPath));
        if (fileSize < kProbeBack) {
            std::cerr << "[EventRecorder] File too small to append, skipping: " << jsonPath << "\n";
            return;
        }

        char discriminator = '\0';
        {
            std::ifstream probe(jsonPath, std::ios::binary);
            probe.seekg(-kProbeBack, std::ios::end);
            probe.get(discriminator);
        }

        bool needComma = (discriminator != '[');
        std::error_code ec;
        std::filesystem::resize_file(
            jsonPath,
            static_cast<std::uintmax_t>(fileSize - kTailLen - (needComma ? 1 : 0)),
            ec);
        if (ec) {
            std::cerr << "[EventRecorder] resize_file failed: " << ec.message() << "\n";
            return;
        }

        std::ofstream f(jsonPath, std::ios::app);
        if (!f) {
            std::cerr << "[EventRecorder] Cannot append: " << jsonPath << "\n";
            return;
        }
        f << std::fixed << std::setprecision(4);
        if (needComma) f << ",\n";
        writeEventLines(f);
        f << "  ]\n}\n";
        std::cerr << "[EventRecorder] Appended: " << jsonPath
                  << " (" << events.size() << " events)\n";
    }

    std::string  host_;
    int          port_;
    SnapshotFn   snapshotFn_;

    std::mutex              mu_;
    std::condition_variable cv_;
    std::thread             thread_;
    std::atomic<bool>       shutdown_{false};

    bool                         recording_     = false;
    bool                         haveEvents_    = false;
    bool                         stopRequested_ = false;
    std::string                  currentFile_;
    std::vector<DetectionEvent>  accumulated_;
    int64_t                      startedUs_     = 0;
};

// ─────────────────────────────────────────────────────────────────────────────
// TelemetryState — latest lux/active-camera reading from picam-raw's
// TelemetryServer (port 8555 by default). Read by /status.json so the
// sidebar can show current light level without polling picam-raw directly.
// ─────────────────────────────────────────────────────────────────────────────
struct TelemetryState {
    std::mutex  mu;
    float       lux           = -1.0f;
    int         activeCamera  = 0;
    std::string cameraLabel;
    bool        connected     = false;
};
static TelemetryState g_telemetry;

class TelemetryReceiver {
public:
    TelemetryReceiver(std::string host, int port)
        : host_(std::move(host)), port_(port) {}

    ~TelemetryReceiver() { if (thread_.joinable()) thread_.join(); }

    void start() { thread_ = std::thread(&TelemetryReceiver::loop, this); }

private:
    void loop() {
        while (!g_stop) {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons(static_cast<uint16_t>(port_));
            ::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);

            if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
                ::close(fd);
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }
            std::cerr << "[Telemetry] Connected to " << host_ << ":" << port_ << "\n";
            { std::lock_guard<std::mutex> lk(g_telemetry.mu); g_telemetry.connected = true; }

            // See DetectionReceiver::loop for why this timeout exists —
            // without it, a quiet telemetry stream blocks this thread in
            // recv() forever, preventing clean shutdown.
            struct timeval tv{1, 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            std::string lineBuf;
            char chunk[4096];
            while (!g_stop) {
                ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                    break;
                }
                if (n == 0) break;
                lineBuf.append(chunk, static_cast<size_t>(n));

                size_t nl;
                while ((nl = lineBuf.find('\n')) != std::string::npos) {
                    std::string line = lineBuf.substr(0, nl);
                    lineBuf.erase(0, nl + 1);
                    parseLine(line);
                }
            }
            ::close(fd);
            { std::lock_guard<std::mutex> lk(g_telemetry.mu); g_telemetry.connected = false; }
            std::cerr << "[Telemetry] Disconnected, retrying...\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    // Telemetry JSON is flat: {"lux":N.N,"active_camera":N,"camera_label":"..."}
    static void parseLine(const std::string& line) {
        float lux  = minijson::extract_float(line, 0, "lux");
        int   cam  = static_cast<int>(minijson::extract_int64(line, "active_camera"));
        std::string label = minijson::extract_string(line, 0, "camera_label");

        std::lock_guard<std::mutex> lk(g_telemetry.mu);
        g_telemetry.lux          = lux;
        g_telemetry.activeCamera = cam;
        if (!label.empty()) g_telemetry.cameraLabel = label;
    }

    std::string host_;
    int         port_;
    std::thread thread_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Annotator — draws bounding boxes + labels directly into a YUV420 Y-plane.
// Cheaper than converting to RGB first; we only need luma contrast for boxes.
// ─────────────────────────────────────────────────────────────────────────────
namespace annot {

static const uint8_t kFont5x7[][5] = {
    {0x00,0x00,0x00,0x00,0x00},{0x00,0x00,0x5F,0x00,0x00},
    {0x00,0x07,0x00,0x07,0x00},{0x14,0x7F,0x14,0x7F,0x14},
    {0x24,0x2A,0x7F,0x2A,0x12},{0x23,0x13,0x08,0x64,0x62},
    {0x36,0x49,0x55,0x22,0x50},{0x00,0x05,0x03,0x00,0x00},
    {0x00,0x1C,0x22,0x41,0x00},{0x00,0x41,0x22,0x1C,0x00},
    {0x08,0x2A,0x1C,0x2A,0x08},{0x08,0x08,0x3E,0x08,0x08},
    {0x00,0x50,0x30,0x00,0x00},{0x08,0x08,0x08,0x08,0x08},
    {0x00,0x60,0x60,0x00,0x00},{0x20,0x10,0x08,0x04,0x02},
    {0x3E,0x51,0x49,0x45,0x3E},{0x00,0x42,0x7F,0x40,0x00},
    {0x42,0x61,0x51,0x49,0x46},{0x21,0x41,0x45,0x4B,0x31},
    {0x18,0x14,0x12,0x7F,0x10},{0x27,0x45,0x45,0x45,0x39},
    {0x3C,0x4A,0x49,0x49,0x30},{0x01,0x71,0x09,0x05,0x03},
    {0x36,0x49,0x49,0x49,0x36},{0x06,0x49,0x49,0x29,0x1E},
    {0x00,0x36,0x36,0x00,0x00},{0x00,0x56,0x36,0x00,0x00},
    {0x00,0x08,0x14,0x22,0x41},{0x14,0x14,0x14,0x14,0x14},
    {0x41,0x22,0x14,0x08,0x00},{0x02,0x01,0x51,0x09,0x06},
    {0x32,0x49,0x79,0x41,0x3E},{0x7E,0x11,0x11,0x11,0x7E},
    {0x7F,0x49,0x49,0x49,0x36},{0x3E,0x41,0x41,0x41,0x22},
    {0x7F,0x41,0x41,0x22,0x1C},{0x7F,0x49,0x49,0x49,0x41},
    {0x7F,0x09,0x09,0x09,0x01},{0x3E,0x41,0x41,0x49,0x7A},
    {0x7F,0x08,0x08,0x08,0x7F},{0x00,0x41,0x7F,0x41,0x00},
    {0x20,0x40,0x41,0x3F,0x01},{0x7F,0x08,0x14,0x22,0x41},
    {0x7F,0x40,0x40,0x40,0x40},{0x7F,0x02,0x04,0x02,0x7F},
    {0x7F,0x04,0x08,0x10,0x7F},{0x3E,0x41,0x41,0x41,0x3E},
    {0x7F,0x09,0x09,0x09,0x06},{0x3E,0x41,0x51,0x21,0x5E},
    {0x7F,0x09,0x19,0x29,0x46},{0x46,0x49,0x49,0x49,0x31},
    {0x01,0x01,0x7F,0x01,0x01},{0x3F,0x40,0x40,0x40,0x3F},
    {0x1F,0x20,0x40,0x20,0x1F},{0x3F,0x40,0x38,0x40,0x3F},
    {0x63,0x14,0x08,0x14,0x63},{0x07,0x08,0x70,0x08,0x07},
    {0x61,0x51,0x49,0x45,0x43},{0x00,0x7F,0x41,0x41,0x00},
    {0x02,0x04,0x08,0x10,0x20},{0x00,0x41,0x41,0x7F,0x00},
    {0x04,0x02,0x01,0x02,0x04},{0x40,0x40,0x40,0x40,0x40},
};
static const int kScale = 2, kCW = 5, kCH = 7;

static void setY(uint8_t* y, int w, int h, int x, int yy, uint8_t v) {
    if (x >= 0 && x < w && yy >= 0 && yy < h) y[yy * w + x] = v;
}

static void hline(uint8_t* y, int w, int h, int x0, int x1, int yy, uint8_t v, int t = 2) {
    for (int k = 0; k < t; ++k) for (int x = x0; x <= x1; ++x) setY(y, w, h, x, yy + k, v);
}
static void vline(uint8_t* y, int w, int h, int x, int y0, int y1, uint8_t v, int t = 2) {
    for (int k = 0; k < t; ++k) for (int yy = y0; yy <= y1; ++yy) setY(y, w, h, x + k, yy, v);
}
static void rect(uint8_t* y, int w, int h, int x0, int y0, int x1, int y1, uint8_t v) {
    hline(y, w, h, x0, x1, y0, v);
    hline(y, w, h, x0, x1, y1 - 1, v);
    vline(y, w, h, x0, y0, y1, v);
    vline(y, w, h, x1 - 1, y0, y1, v);
}

static void drawChar(uint8_t* y, int w, int h, int px, int py, char ch, uint8_t fg, uint8_t bg) {
    if (ch >= 'a' && ch <= 'z') ch -= 32;
    int idx = static_cast<uint8_t>(ch) - 0x20;
    if (idx < 0 || idx >= (int)(sizeof(kFont5x7)/sizeof(kFont5x7[0]))) idx = 0;
    const uint8_t* g = kFont5x7[idx];
    for (int col = 0; col < kCW; ++col)
        for (int row = 0; row < kCH; ++row) {
            bool lit = (g[col] >> row) & 1;
            uint8_t v = lit ? fg : bg;
            for (int sy = 0; sy < kScale; ++sy)
                for (int sx = 0; sx < kScale; ++sx)
                    setY(y, w, h, px + col*kScale + sx, py + row*kScale + sy, v);
        }
}

static void drawLabel(uint8_t* y, int w, int h, int x0, int y0, const std::string& text) {
    int tw = static_cast<int>(text.size()) * (kCW + 1) * kScale;
    int th = kCH * kScale + 4;
    int ly = y0 - th; if (ly < 0) ly = y0 + 2;
    for (int yy = ly; yy < ly + th && yy < h; ++yy)
        for (int x = x0; x < x0 + tw + 4 && x < w; ++x)
            setY(y, w, h, x, yy, 16);
    int cx = x0 + 2;
    for (char c : text) { drawChar(y, w, h, cx, ly + 2, c, 235, 16); cx += (kCW+1)*kScale; }
}

// Draw all detections (normalised coords) onto a YUV420 Y-plane.
void annotate(uint8_t* yPlane, int w, int h, const std::vector<Detection>& dets) {
    for (const auto& d : dets) {
        int x0 = static_cast<int>(d.x0 * w);
        int y0 = static_cast<int>(d.y0 * h);
        int x1 = static_cast<int>(d.x1 * w);
        int y1 = static_cast<int>(d.y1 * h);
        rect(yPlane, w, h, x0, y0, x1, y1, 235);
        char label[64];
        snprintf(label, sizeof(label), "%s %.0f%%", d.cls.c_str(), d.conf * 100.0f);
        drawLabel(yPlane, w, h, x0, y0, label);
    }
}

} // namespace annot

// ─────────────────────────────────────────────────────────────────────────────
// OSD — burns camera ID (bottom-left) and a wall-clock timestamp
// (bottom-right) directly into a frame's YUV420 Y-plane. Reuses annot's
// font table and pixel-drawing primitives (setY/drawChar) rather than
// duplicating the bitmap font a third time in this codebase — this is
// the same visual style as picam-recorder's OSD, ported here so the live
// streams can carry the same camera ID + timestamp burn-in.
// Off by default; enabled via [osd] in config.ini.
// ─────────────────────────────────────────────────────────────────────────────
namespace osd {

using annot::setY;
using annot::drawChar;
constexpr int kScale = annot::kScale;
constexpr int kCW    = annot::kCW;
constexpr int kCH    = annot::kCH;
constexpr int kPad   = 4;

static int textWidth(const std::string& s) {
    return static_cast<int>(s.size()) * (kCW + 1) * kScale;
}
static int textHeight() { return kCH * kScale; }

static void drawBg(uint8_t* y, int w, int h, int px, int py, int bw, int bh,
                   uint8_t bgY = 16) {
    for (int yy = py; yy < py + bh && yy < h; ++yy)
        for (int x = px; x < px + bw && x < w; ++x)
            setY(y, w, h, x, yy, bgY);
}

static void drawString(uint8_t* y, int w, int h, int px, int py,
                       const std::string& text, uint8_t fg = 235, uint8_t bg = 16) {
    int cx = px;
    for (char c : text) {
        drawChar(y, w, h, cx, py, c, fg, bg);
        cx += (kCW + 1) * kScale;
    }
}

// Burns:
//   bottom-left  : "cam: <id>"      — only if showCameraId is true
//   bottom-right : "YYYY-MM-DD HH:MM:SS" (UTC, derived from timestampUs)
//                                     — only if showTime is true
void burn(uint8_t* yPlane, int width, int height,
         int64_t timestampUs, const std::string& cameraId,
         bool showCameraId, bool showTime) {
    if (!showCameraId && !showTime) return;

    int th      = textHeight();
    int bottomY = height - th - kPad;

    if (showCameraId) {
        std::string camStr = "cam: " + cameraId;
        int camW = textWidth(camStr);
        drawBg(yPlane, width, height, kPad - 2, bottomY - 2, camW + 4, th + 4);
        drawString(yPlane, width, height, kPad, bottomY, camStr);
    }

    if (showTime) {
        time_t sec = static_cast<time_t>(timestampUs / 1'000'000LL);
        std::tm tm{};
        gmtime_r(&sec, &tm);
        char tsBuf[32];
        std::strftime(tsBuf, sizeof(tsBuf), "%Y-%m-%d %H:%M:%S", &tm);
        std::string tsStr(tsBuf);

        int tsW = textWidth(tsStr);
        int tsX = width - tsW - kPad;
        drawBg(yPlane, width, height, tsX - 2, bottomY - 2, tsW + 4, th + 4);
        drawString(yPlane, width, height, tsX, bottomY, tsStr);
    }
}

} // namespace osd


// ─────────────────────────────────────────────────────────────────────────────
// JpegEncoder — libjpeg, encodes packed YUV420 directly via the raw-data
// API (jpeg_write_raw_data). This skips the YUV→RGB conversion entirely —
// libjpeg's internal DCT pipeline already operates in YCbCr space, so
// converting to RGB first and then having libjpeg convert back internally
// was pure wasted work. This is the single biggest CPU win available
// without touching resolution or framerate.
// ─────────────────────────────────────────────────────────────────────────────
#include <cstdio>
#include <cstddef>
#include <jpeglib.h>

class JpegEncoder {
public:
    explicit JpegEncoder(int quality) : quality_(quality) {}

    // Encode a packed YUV420 frame (Y plane, then U, then V) to JPEG bytes.
    std::vector<uint8_t> encode(const uint8_t* yuv, int w, int h) {
        // libjpeg's raw-data path requires dimensions that are multiples
        // of 16 (2 MCU rows of 8 for the chroma-subsampled planes). Pad
        // up internally if needed — common camera resolutions like
        // 2304x1296 and 640x360 already satisfy this, but guard anyway.
        const int w16 = (w + 15) & ~15;
        const int h16 = (h + 15) & ~15;

        struct jpeg_compress_struct cinfo{};
        struct jpeg_error_mgr       jerr{};
        cinfo.err = jpeg_std_error(&jerr);
        jpeg_create_compress(&cinfo);

        uint8_t*      outbuf = nullptr;
        unsigned long outlen = 0;
        jpeg_mem_dest(&cinfo, &outbuf, &outlen);

        cinfo.image_width      = static_cast<JDIMENSION>(w);
        cinfo.image_height     = static_cast<JDIMENSION>(h);
        cinfo.input_components = 3;
        cinfo.in_color_space   = JCS_YCbCr;
        jpeg_set_defaults(&cinfo);
        jpeg_set_quality(&cinfo, quality_, TRUE);

        // 4:2:0 subsampling to match our YUV420 input exactly —
        // no resampling needed, direct pass-through.
        cinfo.comp_info[0].h_samp_factor = 2;
        cinfo.comp_info[0].v_samp_factor = 2;
        cinfo.comp_info[1].h_samp_factor = 1;
        cinfo.comp_info[1].v_samp_factor = 1;
        cinfo.comp_info[2].h_samp_factor = 1;
        cinfo.comp_info[2].v_samp_factor = 1;
        cinfo.raw_data_in = TRUE;

        jpeg_start_compress(&cinfo, TRUE);

        const int uvW16 = w16 / 2, uvH16 = h16 / 2;
        const size_t yBytes  = static_cast<size_t>(w) * h;
        const size_t uvBytes = static_cast<size_t>(w/2) * (h/2);
        const uint8_t* yPlane = yuv;
        const uint8_t* uPlane = yuv + yBytes;
        const uint8_t* vPlane = uPlane + uvBytes;

        // Build row pointer arrays. libjpeg processes 16 Y rows / 8 UV
        // rows per call (one MCU row for 4:2:0). We feed it directly from
        // the source buffer where possible, padding the last partial MCU
        // row by clamping to the last valid row (no separate copy needed
        // for the common case where h is already a multiple of 16).
        std::vector<JSAMPROW> yRows(16), uRows(8), vRows(8);

        for (int mcuRow = 0; mcuRow * 16 < h16; ++mcuRow) {
            for (int i = 0; i < 16; ++i) {
                int row = mcuRow * 16 + i;
                int clamped = std::min(row, h - 1);
                yRows[static_cast<size_t>(i)] =
                    const_cast<JSAMPROW>(yPlane + static_cast<size_t>(clamped) * w);
            }
            for (int i = 0; i < 8; ++i) {
                int row = mcuRow * 8 + i;
                int clamped = std::min(row, (h/2) - 1);
                uRows[static_cast<size_t>(i)] =
                    const_cast<JSAMPROW>(uPlane + static_cast<size_t>(clamped) * (w/2));
                vRows[static_cast<size_t>(i)] =
                    const_cast<JSAMPROW>(vPlane + static_cast<size_t>(clamped) * (w/2));
            }
            JSAMPARRAY planes[3] = { yRows.data(), uRows.data(), vRows.data() };
            jpeg_write_raw_data(&cinfo, planes, 16);
        }
        (void)uvW16; (void)uvH16;

        jpeg_finish_compress(&cinfo);
        jpeg_destroy_compress(&cinfo);

        std::vector<uint8_t> result(outbuf, outbuf + outlen);
        free(outbuf);
        return result;
    }

private:
    int                   quality_;
};

// ─────────────────────────────────────────────────────────────────────────────
// WebpEncoder — libwebp, encodes packed YUV420 directly (no RGB round
// trip, same trick as JpegEncoder above) for the live browser stream.
// WebP is intra-only per frame just like JPEG — no motion estimation
// between frames, so CPU cost stays comparable — but its extra
// prediction modes and better entropy coding buy roughly 25-40% smaller
// frames at equivalent visual quality. That's the real-world bandwidth
// win available without paying for a genuine inter-frame video codec
// (H.264 etc.), which the Pi 5 has no hardware encoder for anyway and
// which would cost meaningfully more CPU for motion estimation.
// ─────────────────────────────────────────────────────────────────────────────
#include <webp/encode.h>

class WebpEncoder {
public:
    explicit WebpEncoder(int quality) : quality_(quality) {}

    // Encode a packed YUV420 frame (Y plane, then U, then V) to WebP bytes.
    std::vector<uint8_t> encode(const uint8_t* yuv, int w, int h) {
        WebPConfig config;
        WebPConfigInit(&config);
        WebPConfigPreset(&config, WEBP_PRESET_DEFAULT, static_cast<float>(quality_));
        config.method = 0;  // fastest — this is a real-time stream, not archival quality

        WebPPicture picture;
        WebPPictureInit(&picture);
        picture.width      = w;
        picture.height     = h;
        picture.use_argb   = 0;
        picture.colorspace = WEBP_YUV420;
        if (!WebPPictureAlloc(&picture)) return {};

        // No MCU-alignment padding needed here (unlike JpegEncoder's
        // raw-data path above) — WebPPictureAlloc handles arbitrary
        // width/height and rounds the chroma planes internally.
        const size_t   yBytes  = static_cast<size_t>(w) * static_cast<size_t>(h);
        const int      uvW     = w / 2, uvH = h / 2;
        const size_t   uvBytes = static_cast<size_t>(uvW) * static_cast<size_t>(uvH);
        const uint8_t* yPlane  = yuv;
        const uint8_t* uPlane  = yuv + yBytes;
        const uint8_t* vPlane  = uPlane + uvBytes;

        for (int row = 0; row < h; ++row)
            std::memcpy(picture.y + row * picture.y_stride,
                        yPlane + static_cast<size_t>(row) * static_cast<size_t>(w),
                        static_cast<size_t>(w));
        for (int row = 0; row < uvH; ++row) {
            std::memcpy(picture.u + row * picture.uv_stride,
                        uPlane + static_cast<size_t>(row) * static_cast<size_t>(uvW),
                        static_cast<size_t>(uvW));
            std::memcpy(picture.v + row * picture.uv_stride,
                        vPlane + static_cast<size_t>(row) * static_cast<size_t>(uvW),
                        static_cast<size_t>(uvW));
        }

        WebPMemoryWriter writer;
        WebPMemoryWriterInit(&writer);
        picture.writer     = WebPMemoryWrite;
        picture.custom_ptr = &writer;

        std::vector<uint8_t> result;
        if (WebPEncode(&config, &picture))
            result.assign(writer.mem, writer.mem + writer.size);
        WebPMemoryWriterClear(&writer);
        WebPPictureFree(&picture);
        return result;
    }

private:
    int quality_;
};

// ─────────────────────────────────────────────────────────────────────────────
// StreamHttpServer
//
// No HTML page is served here (see the /stream handler's 404 fallback
// below) — the web UI lives in picam-frontend, which proxies /stream and
// /status.json. This server just does the multipart/x-mixed-replace
// WebP stream at "/stream": every connected browser tab is its own
// client thread, and sendFrameForSource() pushes the latest frame to
// all clients watching that resolution. No JS, no signaling, no
// negotiation on this server's side — <img src="/stream"> is a format
// browsers have supported natively since the late 1990s; only the
// per-frame image codec inside changed (JPEG → WebP).
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// StreamSelector — shared atomic state for which source feeds the encoder.
// ─────────────────────────────────────────────────────────────────────────────
// Annotation is no longer a separate stream — it's an orthogonal,
// globally-toggleable overlay on whichever resolution the client is
// watching (see g_lores_annotated / g_main_annotated below). A client
// only ever selects a resolution; what they see for that resolution
// (live vs delayed+detection-boxed) follows the resolution's own
// global annotation flag.
enum class StreamSource { Lores, Main };

// Stream selection is per-client (stored in StreamClient::selected).
// There is no global selection state.

static const char* streamName(StreamSource s) {
    switch (s) {
        case StreamSource::Main:  return "main";
        case StreamSource::Lores: return "lores";
    }
    return "unknown";
}

static StreamSource parseStreamName(const std::string& s) {
    if (s == "lores") return StreamSource::Lores;
    return StreamSource::Main;  // default/fallback
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamHttpServer
//
// Architecture: one encode per frame, broadcast to N clients.
//
// Each browser connection that hits /stream gets its own StreamClient
// object with a bounded single-slot mailbox (always holds the LATEST
// frame, drops the previous one if the client hasn't sent it yet) and
// its own write thread. sendFrame() atomically swaps the latest frame
// into every client's mailbox and signals their write thread — no I/O
// under the broadcast lock, so a slow or stalled client cannot block
// the encoder or any other client.
//
// The write thread sends the multipart opening header immediately on
// connect (before the first frame arrives), so the browser has a valid
// HTTP response right away and starts rendering as soon as the first
// WebP frame lands.
// ─────────────────────────────────────────────────────────────────────────────
using EncodedFrame = std::shared_ptr<const std::vector<uint8_t>>;

struct StreamClient {
    int        fd;
    StreamSource selected;           // per-client stream selection
    std::mutex              mu;
    std::condition_variable cv;
    EncodedFrame             latest;
    bool                     closed = false;
    // Set false by the write-loop thread right before it exits. Lets the
    // lock-free client list (see StreamHttpServer) prune dead entries
    // without needing weak_ptr/expired() checks, since the list now holds
    // plain shared_ptr in an immutable, atomically-swapped snapshot.
    std::atomic<bool>       alive{true};

    StreamClient(int fd_, StreamSource sel) : fd(fd_), selected(sel) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// PipelineStatus — defined here (rather than near StatusServer further
// down) because StreamHttpServer's /status.json handler needs to read
// latest_frame_ts_us from it.
// ─────────────────────────────────────────────────────────────────────────────
struct PipelineStatus {
    std::mutex  mu;
    uint64_t    frames_in   = 0;
    uint64_t    frames_out  = 0;
    uint64_t    matched     = 0;   // frames that found a detection within tolerance
    float       fps         = 0.0f;
    int         delay_buffer_depth = 0;
    int         clients     = 0;
    // The embedded timestamp (microseconds, Unix epoch) from the most
    // recently encoded frame, regardless of which stream it came from.
    // This is picam-raw's own clock, carried in every UDP chunk-0 header
    // — NOT the time on whatever machine is running picam-orchestrator or
    // viewing the web UI. Exposed via /status.json so the frontend's
    // sidebar clock reflects the camera feed's actual time rather than
    // the viewer's browser clock.
    int64_t     latest_frame_ts_us = 0;
};
static PipelineStatus g_pstatus;

class StreamHttpServer {
public:
    StreamHttpServer(int port, StreamSource defaultStream)
        : port_(port), defaultStream_(defaultStream) {}

    ~StreamHttpServer() {
        running_ = false;
        // close() alone does not reliably unblock another thread that's
        // blocked inside accept() on this fd. shutdown() first forces
        // accept() to actually return with an error before the fd closes
        // — without this, acceptThread_.join() below can hang forever.
        if (fd_ >= 0) {
            ::shutdown(fd_, SHUT_RDWR);
            ::close(fd_);
        }
        if (acceptThread_.joinable()) acceptThread_.join();
        // No lock needed to read the snapshot — just load it.
        auto snapshot = std::atomic_load(&clients_);
        for (auto& sp : *snapshot) {
            std::lock_guard<std::mutex> clk(sp->mu);
            sp->closed = true;
            sp->cv.notify_all();
        }
    }

    void start() {
        running_ = true;
        std::atomic_store(&clients_, std::make_shared<const ClientList>());  // start with an empty snapshot
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));
        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            // An unchecked bind() failure here used to fall through to
            // listen() on an unbound socket, which Linux silently
            // auto-binds to a random ephemeral port — the process kept
            // running and logged a port number it was NOT actually
            // listening on. Fail loudly instead, especially since this
            // is the port picam-frontend (or any direct viewer) expects
            // to reach this backend on.
            std::cerr << "[Stream] FATAL: bind() failed on port " << port_
                      << ": " << std::strerror(errno) << "\n"
                      << "[Stream] Is something else already using this port? "
                      << "Check: sudo ss -tlnp | grep ':" << port_ << " '\n";
            ::close(fd_);
            fd_ = -1;
            running_ = false;
            std::exit(1);
        }
        ::listen(fd_, 16);
        acceptThread_ = std::thread(&StreamHttpServer::acceptLoop, this);
        std::cerr << "[Stream] Viewer: http://0.0.0.0:" << port_ << "\n";
    }

    // Deliver a frame to every client currently watching `source`.
    // Lock-free read: just load the current snapshot. No mutex, ever,
    // on this path — which runs many times per second from the main
    // pipeline loop and was previously the dominant source of lock
    // contention severe enough to look like a deadlock under load.
    void sendFrameForSource(StreamSource source, const std::vector<uint8_t>& webp) {
        auto frame = std::make_shared<const std::vector<uint8_t>>(webp);
        auto snapshot = std::atomic_load(&clients_);
        for (auto& sp : *snapshot) {
            if (!sp->alive.load()) continue;
            if (sp->selected != source) continue;
            std::lock_guard<std::mutex> clk(sp->mu);
            sp->latest = frame;
            sp->cv.notify_one();
        }
    }

    int clientCount() const {
        auto snapshot = std::atomic_load(&clients_);
        int n = 0;
        for (auto& sp : *snapshot) if (sp->alive.load()) ++n;
        return n;
    }

    // Count clients watching a specific stream
    int clientCountForSource(StreamSource src) const {
        auto snapshot = std::atomic_load(&clients_);
        int n = 0;
        for (auto& sp : *snapshot) if (sp->alive.load() && sp->selected == src) ++n;
        return n;
    }

    // Snapshot all counts (total + one per resolution) in a single
    // lock-free pass over the current client-list snapshot.
    struct Counts { int total = 0, main = 0, lores = 0; };
    Counts snapshotCounts() const {
        auto snapshot = std::atomic_load(&clients_);
        Counts c;
        for (auto& sp : *snapshot) {
            if (!sp->alive.load()) continue;
            ++c.total;
            switch (sp->selected) {
                case StreamSource::Main:  ++c.main;  break;
                case StreamSource::Lores: ++c.lores; break;
            }
        }
        return c;
    }

private:
    using ClientList = std::vector<std::shared_ptr<StreamClient>>;

    // The only mutation point for the client list. Builds a brand new
    // vector (copy of the current snapshot, pruned of dead entries, plus
    // the new client if provided) and atomically swaps it in. This is the
    // ONLY place that takes registerMu_ — and it only runs on connect,
    // which is rare compared to the encode loop's per-frame reads above.
    void registerClient(std::shared_ptr<StreamClient> newClient) {
        std::lock_guard<std::mutex> lk(registerMu_);
        auto old = std::atomic_load(&clients_);
        auto fresh = std::make_shared<ClientList>();
        fresh->reserve(old->size() + 1);
        for (auto& sp : *old) {
            if (sp->alive.load()) fresh->push_back(sp);
        }
        if (newClient) fresh->push_back(std::move(newClient));
        std::shared_ptr<const ClientList> immutableFresh = std::move(fresh);
        std::atomic_store(&clients_, std::move(immutableFresh));
    }

private:
    void acceptLoop() {
        while (running_) {
            int cfd = ::accept(fd_, nullptr, nullptr);
            if (cfd < 0) { if (errno == EINTR) continue; break; }
            std::thread([this, cfd]{ handleClient(cfd); }).detach();
        }
    }

    static std::string urlDecode(const std::string& s) {
        std::string out;
        for (size_t i = 0; i < s.size(); ++i) {
            if (s[i] == '%' && i + 2 < s.size() &&
                std::isxdigit(static_cast<unsigned char>(s[i+1])) &&
                std::isxdigit(static_cast<unsigned char>(s[i+2]))) {
                int v = std::stoi(s.substr(i+1, 2), nullptr, 16);
                out += static_cast<char>(v);
                i += 2;
            } else if (s[i] == '+') {
                out += ' ';
            } else {
                out += s[i];
            }
        }
        return out;
    }

    static std::string queryParam(const std::string& path, const std::string& key) {
        auto q = path.find('?');
        if (q == std::string::npos) return "";
        std::string qs = path.substr(q + 1);
        std::istringstream iss(qs);
        std::string pair;
        while (std::getline(iss, pair, '&')) {
            auto eq = pair.find('=');
            if (eq == std::string::npos) continue;
            if (pair.substr(0, eq) == key)
                return urlDecode(pair.substr(eq + 1));
        }
        return "";
    }

    void handleClient(int cfd) {
        struct timeval tv{5, 0};
        ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        std::string req;
        char buf[4096];
        ssize_t n = ::recv(cfd, buf, sizeof(buf), 0);
        if (n > 0) req.assign(buf, static_cast<size_t>(n));

        std::istringstream iss(req);
        std::string method, fullPath, proto;
        iss >> method >> fullPath >> proto;

        std::string path = fullPath.substr(0, fullPath.find('?'));

        // /select is now stream-agnostic at the server level — it targets
        // a specific client identified by a ?client= token embedded in
        // the stream src URL. However for simplicity we accept a plain
        // /select?stream=X POST from the browser JS; the JS on the page
        // reloads the <img src> with ?stream=X to attach to the right feed.
        if (path == "/select") {
            // This endpoint is kept for status/UI sync but the real per-client
            // selection happens via the stream URL parameter below.
            std::string s = queryParam(fullPath, "stream");
            StreamSource src = parseStreamName(s);
            // Update any existing clients from this connection — in practice
            // the browser sends /select then resets <img src="/stream?stream=X">
            sendSimple(cfd, 200, "application/json",
                      std::string("{\"ok\":true,\"stream\":\"") +
                      streamName(src) + "\"}");
            ::close(cfd);
            return;
        }

        if (path == "/osd") {
            // Runtime, on-the-fly toggle for the camera-ID and timestamp
            // burn-ins — independently controllable, no config edit or
            // service restart needed. Global: affects every connected
            // viewer of either resolution (Main/Lores) on this Pi, by
            // design (see the g_osd_*_enabled comment above).
            auto parseBool = [](const std::string& v) {
                return v == "true" || v == "1" || v == "yes";
            };

            std::string camIdVal = queryParam(fullPath, "camera_id");
            std::string timeVal  = queryParam(fullPath, "time");
            // Legacy combined toggle — still accepted for backward
            // compatibility with anything calling the old single-flag
            // API; sets both independent flags to the same value.
            std::string enabledVal = queryParam(fullPath, "enabled");

            if (!enabledVal.empty()) {
                bool enable = parseBool(enabledVal);
                g_osd_camera_id_enabled.store(enable);
                g_osd_time_enabled.store(enable);
                std::cerr << "\n[OSD] Both camera_id and time set to "
                          << (enable ? "enabled" : "disabled")
                          << " via legacy /osd?enabled= param\n";
            }
            if (!camIdVal.empty()) {
                bool enable = parseBool(camIdVal);
                g_osd_camera_id_enabled.store(enable);
                std::cerr << "\n[OSD] camera_id " << (enable ? "enabled" : "disabled")
                          << " via /osd endpoint\n";
            }
            if (!timeVal.empty()) {
                bool enable = parseBool(timeVal);
                g_osd_time_enabled.store(enable);
                std::cerr << "\n[OSD] time " << (enable ? "enabled" : "disabled")
                          << " via /osd endpoint\n";
            }

            sendSimple(cfd, 200, "application/json",
                      std::string("{\"ok\":true")
                      + ",\"camera_id_enabled\":" + (g_osd_camera_id_enabled.load() ? "true" : "false")
                      + ",\"time_enabled\":" + (g_osd_time_enabled.load() ? "true" : "false")
                      + "}");
            ::close(cfd);
            return;
        }

        if (path == "/annotate") {
            // Runtime, on-the-fly toggle for whether a resolution's
            // stream is delayed+detection-annotated or fully live.
            // Independently controllable per resolution, no config edit
            // or service restart needed. Global, same as the OSD toggle
            // — every connected viewer of that resolution sees the same
            // thing (see g_lores_annotated/g_main_annotated's comment).
            auto parseBool = [](const std::string& v) {
                return v == "true" || v == "1" || v == "yes";
            };

            std::string loresVal = queryParam(fullPath, "lores");
            std::string mainVal  = queryParam(fullPath, "main");

            if (!loresVal.empty()) {
                bool enable = parseBool(loresVal);
                g_lores_annotated.store(enable);
                std::cerr << "\n[Annotate] lores " << (enable ? "enabled" : "disabled")
                          << " via /annotate endpoint\n";
            }
            if (!mainVal.empty()) {
                bool enable = parseBool(mainVal);
                g_main_annotated.store(enable);
                std::cerr << "\n[Annotate] main " << (enable ? "enabled" : "disabled")
                          << " via /annotate endpoint\n";
            }

            sendSimple(cfd, 200, "application/json",
                      std::string("{\"ok\":true")
                      + ",\"lores_annotated\":" + (g_lores_annotated.load() ? "true" : "false")
                      + ",\"main_annotated\":" + (g_main_annotated.load() ? "true" : "false")
                      + "}");
            ::close(cfd);
            return;
        }

        if (path == "/camera") {
            // Switches picam-raw's active camera by proxying to its
            // existing plain-text CommandServer ("switch<N>\n" on TCP
            // port 8556 by default — see telemetry.command_port). This
            // is what lets picam-frontend expose a Camera 0 / Camera 1
            // switch button without talking to picam-raw's TCP protocol
            // directly — it only ever speaks HTTP to picam-orchestrator.
            std::string idStr = queryParam(fullPath, "id");
            if (idStr.empty() || idStr.find_first_not_of("0123456789") != std::string::npos) {
                sendSimple(cfd, 400, "application/json",
                          "{\"ok\":false,\"error\":\"missing or invalid ?id=\"}");
                ::close(cfd);
                return;
            }
            int cameraId = std::atoi(idStr.c_str());
            std::string raw_response;
            bool reached = sendCameraSwitchCommand(g_picamraw_host, g_picamraw_cmd_port,
                                                   cameraId, raw_response);
            std::cerr << "\n[Camera] switch" << cameraId << " -> "
                      << (reached ? "ok" : "FAILED") << ": " << raw_response << "\n";
            // picam-raw's response is already a complete JSON object — pass
            // it straight through rather than re-wrapping it, so callers see
            // its actual ok/error/active_camera/label fields directly. Only
            // the HTTP status code distinguishes reach-failures (502) from
            // picam-raw successfully responding (200, even if that response
            // itself says {"ok":false,...} for an invalid camera index).
            sendSimple(cfd, reached ? 200 : 502, "application/json", raw_response);
            ::close(cfd);
            return;
        }

        if (path == "/status.json") {
            float lux; int activeCam; std::string camLabel; bool telConnected;
            {
                std::lock_guard<std::mutex> tlk(g_telemetry.mu);
                lux          = g_telemetry.lux;
                activeCam    = g_telemetry.activeCamera;
                camLabel     = g_telemetry.cameraLabel;
                telConnected = g_telemetry.connected;
            }
            int64_t frameTsUs;
            {
                std::lock_guard<std::mutex> plk(g_pstatus.mu);
                frameTsUs = g_pstatus.latest_frame_ts_us;
            }
            auto counts = snapshotCounts();  // one lock acquisition, not four
            std::ostringstream json;
            json << std::fixed << std::setprecision(1);
            json << "{\"clients\":" << counts.total
                 << ",\"camera_id_enabled\":" << (g_osd_camera_id_enabled.load() ? "true" : "false")
                 << ",\"time_enabled\":" << (g_osd_time_enabled.load() ? "true" : "false")
                 << ",\"lores_annotated\":" << (g_lores_annotated.load() ? "true" : "false")
                 << ",\"main_annotated\":" << (g_main_annotated.load() ? "true" : "false")
                 // Embedded timestamp (Unix microseconds) from picam-raw's
                 // own clock, carried in every UDP frame's header — NOT
                 // this Pi's or the viewer's wall-clock time. 0 if no
                 // frame has been encoded yet since startup.
                 << ",\"frame_ts_us\":" << frameTsUs
                 << ",\"streams\":{"
                 <<   "\"main\":"      << counts.main
                 <<   ",\"lores\":"    << counts.lores
                 << "}"
                 << ",\"telemetry\":{"
                 <<   "\"connected\":" << (telConnected ? "true" : "false")
                 <<   ",\"lux\":" << lux
                 <<   ",\"active_camera\":" << activeCam
                 <<   ",\"camera_label\":\"" << camLabel << "\""
                 << "}}";
            sendSimple(cfd, 200, "application/json", json.str());
            ::close(cfd);
            return;
        }

        if (path == "/stream") {
            // The stream selection is passed as a query param on the src URL:
            // <img src="/stream?stream=annotated">
            // This lets each browser tab have an independent selection without
            // any server-side per-session state beyond the StreamClient itself.
            std::string streamParam = queryParam(fullPath, "stream");
            StreamSource src = streamParam.empty()
                ? defaultStream_
                : parseStreamName(streamParam);

            // Defensive cap, checked BEFORE any response is sent — prevents
            // unbounded growth of the client list if some future bug leaks
            // connections again. Reading clientCount() here is lock-free
            // (just an atomic load), so this check is essentially free.
            if (clientCount() >= 50) {
                std::cerr << "\n[Stream] Rejecting connection — at client cap (50)\n";
                sendSimple(cfd, 503, "text/plain", "Too many connections");
                ::close(cfd);
                return;
            }

            const char* hdr =
                "HTTP/1.1 200 OK\r\n"
                "Cache-Control: no-cache, private\r\n"
                "Pragma: no-cache\r\n"
                "Content-Type: multipart/x-mixed-replace; boundary=frame\r\n"
                "Connection: close\r\n\r\n";
            if (::send(cfd, hdr, strlen(hdr), MSG_NOSIGNAL) < 0) {
                ::close(cfd); return;
            }

            struct timeval notimeout{0, 0};
            ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &notimeout, sizeof(notimeout));

            auto client = std::make_shared<StreamClient>(cfd, src);
            registerClient(client);  // the only place that briefly locks anything
            std::cerr << "\n[Stream] Client connected stream="
                      << streamName(src) << " total=" << clientCount() << "\n";

            while (true) {
                EncodedFrame frame;
                bool needKeepalive = false;
                {
                    std::unique_lock<std::mutex> clk(client->mu);
                    // Bounded wait, not indefinite. Without a timeout here,
                    // a client whose selected stream never produces a frame
                    // (e.g. nobody else triggers Main's encoder, or the
                    // upstream UDP feed stalls) blocks this thread forever.
                    // That leaked the thread AND the open fd on every such
                    // connection, and the resulting pile-up of dead entries
                    // in clients_ created severe lock contention on mu_ via
                    // sendFrameForSource/clientCountForSource, which run on
                    // every encode tick — eventually starving brand new
                    // connections (including unrelated ones, like the status
                    // port) from completing basic socket I/O at all.
                    bool got = client->cv.wait_for(clk, std::chrono::seconds(10),
                        [&]{ return client->latest || client->closed; });
                    if (client->closed) break;
                    if (!got) {
                        needKeepalive = true;
                    } else {
                        frame = std::move(client->latest);
                        client->latest.reset();
                    }
                }
                // CRITICAL: the keepalive send happens OUTSIDE the client->mu
                // lock scope above. ::send() is a blocking syscall — calling
                // it while holding client->mu created a real deadlock: if
                // sendFrameForSource() (which holds the server-wide mu_ for
                // its whole duration) was concurrently trying to acquire
                // this same client's mu_ to deliver a frame, it would block
                // on a lock held by a thread itself blocked inside a slow
                // ::send(). Every other thread needing mu_ then queues up
                // behind that, including the main pipeline thread — which
                // is exactly the deadlock the gdb thread dump showed: the
                // main thread itself stuck on the same futex as everyone
                // else, with nothing actually making progress.
                if (needKeepalive) {
                    bool ok = ::send(cfd, "--frame\r\n\r\n", 11, MSG_NOSIGNAL) > 0;
                    if (!ok) break;
                    continue;
                }
                char partHdr[128];
                int phLen = snprintf(partHdr, sizeof(partHdr),
                    "--frame\r\nContent-Type: image/webp\r\nContent-Length: %zu\r\n\r\n",
                    frame->size());
                bool ok =
                    ::send(cfd, partHdr, (size_t)phLen, MSG_NOSIGNAL) > 0 &&
                    ::send(cfd, frame->data(), frame->size(), MSG_NOSIGNAL) > 0 &&
                    ::send(cfd, "\r\n", 2, MSG_NOSIGNAL) > 0;
                if (!ok) break;
            }

            client->alive.store(false);  // next registerClient() call will prune this entry
            ::close(cfd);
            std::cerr << "\n[Stream] Client disconnected\n";
            return;
        }

        // No HTML page is served here — this is a stream-only backend.
        // The web UI lives in picam-frontend, which proxies /stream and
        // /status.json from one or more of these backends.
        sendSimple(cfd, 404, "text/plain", "Not found");
        ::close(cfd);
    }

    static void sendSimple(int cfd, int code, const std::string& mime,
                           const std::string& body) {
        std::ostringstream resp;
        resp << "HTTP/1.1 " << code << " OK\r\n"
             << "Content-Type: " << mime << "\r\n"
             << "Content-Length: " << body.size() << "\r\n"
             << "Access-Control-Allow-Origin: *\r\n"
             << "Connection: close\r\n\r\n"
             << body;
        std::string s = resp.str();
        ::send(cfd, s.data(), s.size(), MSG_NOSIGNAL);
    }

    int               port_;
    StreamSource      defaultStream_;
    int               fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread       acceptThread_;
    // Lock-free client list: readers (sendFrameForSource, clientCount*,
    // snapshotCounts — all called from the hot per-frame encode loop)
    // just atomic_load this shared_ptr, no mutex involved at all. Writers
    // (registerClient, called only on connect) build a new immutable
    // vector and atomic_store it in. registerMu_ only serializes
    // concurrent connects against each other — it is never touched by
    // the hot read path, so it can never be the source of the
    // sustained contention we were chasing before.
    //
    // NOTE: std::atomic<std::shared_ptr<T>> is a C++20 feature. This
    // project targets C++17, so we use the std::atomic_load/atomic_store
    // free-function overloads instead, which provide the same atomicity
    // guarantee for shared_ptr in C++11 and later (implemented via an
    // internal lock table in libstdc++, much lighter-weight than holding
    // a dedicated mutex for the duration of an iteration).
    std::shared_ptr<const ClientList> clients_;
    std::mutex        registerMu_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Status — for a simple TCP status query, same pattern as the other tools.
// PipelineStatus/g_pstatus are defined earlier in the file (before
// StreamHttpServer) since /status.json's handler needs to read from it too.
// ─────────────────────────────────────────────────────────────────────────────
class StatusServer {
public:
    explicit StatusServer(int port) : port_(port) {}
    void start() { thread_ = std::thread(&StatusServer::loop, this); thread_.detach(); }
private:
    void loop() {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(static_cast<uint16_t>(port_));
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::cerr << "[Status] FATAL: bind() failed on port " << port_
                      << ": " << std::strerror(errno) << "\n";
            ::close(fd);
            return;
        }
        ::listen(fd, 8);
        std::cerr << "[Status] Listening on 0.0.0.0:" << port_ << "\n";
        while (!g_stop) {
            int cfd = ::accept(fd, nullptr, nullptr);
            if (cfd < 0) { if (errno == EINTR) continue; break; }
            std::thread([cfd]{
                struct timeval tv{2, 0};
                ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                char c; std::string line;
                while (::recv(cfd, &c, 1, 0) == 1) { if (c=='\n') break; if (c!='\r') line += c; }
                std::lock_guard<std::mutex> lk(g_pstatus.mu);
                char buf[512];
                snprintf(buf, sizeof(buf),
                    "ok=true\nframes_in=%llu\nframes_out=%llu\nmatched=%llu\n"
                    "fps=%.1f\ndelay_buffer_depth=%d\nclients=%d\n\n",
                    (unsigned long long)g_pstatus.frames_in,
                    (unsigned long long)g_pstatus.frames_out,
                    (unsigned long long)g_pstatus.matched,
                    g_pstatus.fps, g_pstatus.delay_buffer_depth, g_pstatus.clients);
                ::send(cfd, buf, strlen(buf), MSG_NOSIGNAL);
                ::close(cfd);
            }).detach();
        }
        ::close(fd);
    }
    int port_;
    std::thread thread_;
};

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGPIPE, SIG_IGN);

    std::string cfg_path = "config.ini";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if ((a == "--config" || a == "-c") && i + 1 < argc) cfg_path = argv[++i];
    }
    Config cfg(cfg_path);

    const std::string raw_host     = cfg.get_str("input.host", "127.0.0.1");

    // Main stream (live, no delay)
    const int main_port    = cfg.get_int("input.main_port", 8560);
    const int main_width   = cfg.get_int("input.main_width", 2304);
    const int main_height  = cfg.get_int("input.main_height", 1296);

    // Lores stream (live, no delay)
    const int lores_port   = cfg.get_int("input.lores_port", 8561);
    const int lores_width  = cfg.get_int("input.lores_width", 640);
    const int lores_height = cfg.get_int("input.lores_height", 360);

    const int ping_every   = cfg.get_int("input.ping_every", 5);

    const std::string det_host     = cfg.get_str("detections.host", "127.0.0.1");
    const int         det_port     = cfg.get_int("detections.port", 8558);
    const int         tolerance_ms = cfg.get_int("detections.tolerance_ms", 150);

    const std::string rec_host     = cfg.get_str("recorder.host", "127.0.0.1");
    const int         rec_port     = cfg.get_int("recorder.port", 8080);

    const std::string tel_host     = cfg.get_str("telemetry.host", "127.0.0.1");
    const int         tel_port     = cfg.get_int("telemetry.port", 8555);
    // picam-raw's CommandServer — same host as telemetry (both are
    // picam-raw), separate port. Used by the /camera HTTP endpoint to
    // switch the active camera on the fly.
    const int         cmd_port     = cfg.get_int("telemetry.command_port", 8556);
    g_picamraw_host     = tel_host;
    g_picamraw_cmd_port = cmd_port;

    const int delay_ms = cfg.get_int("delay.delay_ms", 1000);

    // WebP for the live stream (see WebpEncoder); JPEG is still used only
    // for EventRecorder's saved snapshot files, not the stream itself.
    const int webp_quality     = cfg.get_int("encode.webp_quality", 80);
    const int jpeg_quality     = cfg.get_int("encode.jpeg_quality", 80);
    const int output_fps_live  = cfg.get_int("encode.output_fps_live", 15);
    const int output_fps_annot = cfg.get_int("encode.output_fps_annotated", 30);

    // OSD: burns camera ID and/or wall-clock timestamp directly into the
    // video frame pixels (bottom-left / bottom-right respectively).
    // Independently controllable — off by default for both. Most viewers
    // already see a timestamp in the web UI sidebar, so this is for cases
    // where the raw stream itself needs to carry that information (e.g.
    // recording the proxied stream downstream, or viewing outside the
    // picam-frontend UI entirely).
    g_osd_camera_id_enabled.store(cfg.get_bool("osd.camera_id", false));
    g_osd_time_enabled.store(cfg.get_bool("osd.time", false));

    // Annotation: whether each resolution's stream is delayed +
    // detection-boxed, or fully live. Off (live) by default for both —
    // same startup-default-only pattern as OSD above; the live value
    // can change at any time via GET /annotate, no restart needed.
    g_lores_annotated.store(cfg.get_bool("annotate.lores", false));
    g_main_annotated.store(cfg.get_bool("annotate.main", false));
    // Optional comma-separated list of labels, one per camera index —
    // e.g. "Front,Back" labels camera 0 as "Front" and camera 1 as
    // "Back" instead of the bare numeric index.
    const std::string osd_labels_raw = cfg.get_str("osd.camera_labels", "");
    std::vector<std::string> osd_labels;
    if (!osd_labels_raw.empty()) {
        std::istringstream iss(osd_labels_raw);
        std::string item;
        while (std::getline(iss, item, ',')) osd_labels.push_back(item);
    }
    auto osd_camera_label = [&osd_labels](uint8_t cameraIndex) -> std::string {
        if (cameraIndex < osd_labels.size() && !osd_labels[cameraIndex].empty())
            return osd_labels[cameraIndex];
        return std::to_string(static_cast<int>(cameraIndex));
    };

    const int         http_port   = cfg.get_int("output.http_port", 81);
    const int         status_port = cfg.get_int("output.status_port", 8091);

    std::string default_stream = cfg.get_str("output.default_stream", "main");
    StreamSource defaultSrc = parseStreamName(default_stream);

    std::cerr << "[Config] main      : udp://" << raw_host << ":" << main_port
              << "  " << main_width << "x" << main_height << "\n"
              << "[Config] lores     : udp://" << raw_host << ":" << lores_port
              << "  " << lores_width << "x" << lores_height << "\n"
              << "[Config] detections: tcp://" << det_host << ":" << det_port
              << "  tolerance=" << tolerance_ms << "ms\n"
              << "[Config] recorder  : tcp://" << rec_host << ":" << rec_port << "\n"
              << "[Config] telemetry : tcp://" << tel_host << ":" << tel_port << "\n"
              << "[Config] delay     : " << delay_ms << "ms (applied to whichever resolution has annotation on)\n"
              << "[Config] encode    : webp quality=" << webp_quality
              << "  (snapshot jpeg quality=" << jpeg_quality << ")"
              << "  live_fps=" << output_fps_live
              << "  annotated_fps=" << output_fps_annot << "\n"
              << "[Config] osd       : camera_id=" << (g_osd_camera_id_enabled.load() ? "enabled" : "disabled")
              << " time=" << (g_osd_time_enabled.load() ? "enabled" : "disabled") << "\n"
              << "[Config] annotate  : lores=" << (g_lores_annotated.load() ? "enabled" : "disabled")
              << " main=" << (g_main_annotated.load() ? "enabled" : "disabled") << "\n"
              << "[Config] default   : " << streamName(defaultSrc) << "\n"
              << "[Config] viewer    : http://0.0.0.0:" << http_port << "\n"
              << "[Config] status    : 0.0.0.0:" << status_port << "\n";

    // ── Three source pipelines. Main UDP registration feeds BOTH the live
    // Two source pipelines (lores, main), each feeding BOTH a live
    // "latest frame" mailbox and its own delay buffer — annotation is
    // now an orthogonal per-resolution toggle (see g_lores_annotated /
    // g_main_annotated), not a separate third stream, so each
    // resolution needs its own delay buffer to support being switched
    // between live and delayed+annotated independently of the other.

    std::mutex   mainMu, loresMu;
    RawFrame     mainLatest, loresLatest;
    bool         mainHas = false, loresHas = false;

    DelayBuffer mainDelayBuf(delay_ms);
    DelayBuffer loresDelayBuf(delay_ms);

    // ── Stream HTTP server — constructed before the receivers below
    // since their frame callbacks reference streamSrv.clientCount() to
    // decide whether to do the latest-frame mailbox copy at all. ────────
    StreamHttpServer streamSrv(http_port, defaultSrc);

    UdpRawReceiver mainReceiver(raw_host, main_port, main_width, main_height, ping_every,
        [&](RawFrame f) {
            {
                std::lock_guard<std::mutex> lk(g_pstatus.mu);
                ++g_pstatus.frames_in;
            }
            // Always keep the main mailbox warm, unconditionally. The
            // previous "only copy if a client is watching" gate called
            // streamSrv.clientCountForSource() from this UDP receive callback,
            // which fires up to 30x/second — adding lock contention to a
            // hot path for a marginal copy-avoidance win. Worse, it created
            // a startup race: a freshly connected client had to wait for
            // BOTH a UDP packet to arrive AND clientCountForSource() to
            // return true at that exact instant, and under lock contention
            // that could be delayed indefinitely. Main frames are simply
            // not very expensive to copy compared to the cost of that
            // race condition.
            {
                std::lock_guard<std::mutex> lk(mainMu);
                mainLatest = f;   // copy — mainDelayBuf needs its own copy below
                mainHas = true;
            }
            // Always feed the delay buffer too, regardless of whether
            // main annotation is currently on — so flipping the toggle
            // on doesn't have to wait for the buffer to refill from
            // empty before the first delayed frame is ready.
            mainDelayBuf.push(std::move(f));
        });

    UdpRawReceiver loresReceiver(raw_host, lores_port, lores_width, lores_height, ping_every,
        [&](RawFrame f) {
            {
                std::lock_guard<std::mutex> lk(loresMu);
                loresLatest = f;   // copy — loresDelayBuf needs its own copy below
                loresHas = true;
            }
            loresDelayBuf.push(std::move(f));
        });

    DetectionBuffer detBuf(delay_ms + tolerance_ms + 2000);
    EventRecorder evtRec(rec_host, rec_port,
        [&](const DetectionEvent& evt) -> std::vector<uint8_t> {
            RawFrame frame;
            {
                std::lock_guard<std::mutex> lk(mainMu);
                if (!mainHas) return {};
                frame = mainLatest;
            }
            annot::annotate(frame.data.data(), frame.width, frame.height, evt.dets);
            JpegEncoder enc(jpeg_quality);
            return enc.encode(frame.data.data(), frame.width, frame.height);
        });
    DetectionReceiver detReceiver(det_host, det_port, detBuf,
        [&evtRec](const DetectionEvent& e){ evtRec.notify(e); });
    TelemetryReceiver telReceiver(tel_host, tel_port);

    StatusServer status_srv(status_port);
    status_srv.start();

    mainReceiver.start();
    loresReceiver.start();
    detReceiver.start();
    telReceiver.start();
    streamSrv.start();

    std::cerr << "[Main] Waiting for raw streams...\n";
    if (!mainReceiver.waitForStream(30))
        std::cerr << "[Main] Warning: no main stream after 30s\n";
    if (!loresReceiver.waitForStream(10))
        std::cerr << "[Main] Warning: no lores stream after 10s\n";

    std::cerr << "[Main] Streams active. Open http://<pi-ip>"
              << (http_port == 80 ? "" : ":" + std::to_string(http_port))
              << " in a browser, or via picam-frontend's proxy.\n";

    // ── Two-resolution parallel encode loop, each independently
    // switchable between live and delayed+annotated ──────────────────────────
    // Lores and Main each have their own fps throttle, their own delay
    // buffer, and their own annotation flag (g_lores_annotated /
    // g_main_annotated). When a resolution's flag is off, frames come
    // straight from the live mailbox at output_fps_live. When on, frames
    // are popped from that resolution's delay buffer (still draining
    // even with the flag off, so it doesn't grow unbounded and so the
    // first delayed frame is ready immediately if the flag flips on),
    // matched against detections, boxed, and sent at output_fps_annot.

    int64_t live_interval_us  = output_fps_live  > 0
        ? static_cast<int64_t>(1e6 / output_fps_live)  : 0;
    int64_t annot_interval_us = output_fps_annot > 0
        ? static_cast<int64_t>(1e6 / output_fps_annot) : 0;
    int64_t tolerance_us = static_cast<int64_t>(tolerance_ms) * 1000;

    auto last_main_time  = Clock::now();
    auto last_lores_time = Clock::now();
    auto t_last          = Clock::now();

    // Two encoders — one per resolution. Each is reused whether that
    // resolution is currently live or annotated, since only one of those
    // two states is ever active for a given resolution at a time.
    WebpEncoder mainEncoder(webp_quality);
    WebpEncoder loresEncoder(webp_quality);

    while (!g_stop) {
        auto now = Clock::now();
        bool did_work = false;
        int64_t newest_ts_us = 0;  // tracks whichever resolution produced the most recent frame this tick
        uint64_t matched_this_tick = 0;

        // Single lock acquisition per iteration instead of one per stream
        // (see snapshotCounts() comment for why this matters).
        auto counts = streamSrv.snapshotCounts();

        // ── Main ──────────────────────────────────────────────────────────────
        bool mainAnnotated = g_main_annotated.load();
        auto main_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            now - last_main_time).count();
        int64_t main_interval = mainAnnotated ? annot_interval_us : live_interval_us;

        // Always drain main's delay buffer regardless of the annotation
        // flag or whether anyone's watching, so it never grows unbounded
        // and is instantly ready if the flag flips on mid-stream.
        RawFrame mainDelayedFrame;
        bool mainPopped = mainDelayBuf.pop(mainDelayedFrame, 0);

        if (counts.main > 0 && main_elapsed >= main_interval) {
            RawFrame frame;
            bool haveFrame = false;

            if (mainAnnotated) {
                if (mainPopped) { frame = std::move(mainDelayedFrame); haveFrame = true; }
            } else {
                std::lock_guard<std::mutex> lk(mainMu);
                if (mainHas) { frame = mainLatest; haveFrame = true; }
            }

            if (haveFrame && !frame.data.empty()) {
                if (mainAnnotated) {
                    DetectionEvent evt;
                    bool matched = detBuf.findNearest(frame.timestampUs, tolerance_us, evt);
                    if (matched) {
                        annot::annotate(frame.data.data(), frame.width, frame.height, evt.dets);
                        ++matched_this_tick;
                    }
                }
                if (g_osd_camera_id_enabled.load() || g_osd_time_enabled.load())
                    osd::burn(frame.data.data(), frame.width, frame.height,
                             frame.timestampUs,
                             osd_camera_label(frame.cameraIndex),
                             g_osd_camera_id_enabled.load(), g_osd_time_enabled.load());
                auto webp = mainEncoder.encode(frame.data.data(), frame.width, frame.height);
                streamSrv.sendFrameForSource(StreamSource::Main, webp);
                last_main_time = now;
                did_work = true;
                newest_ts_us = frame.timestampUs;
            }
        } else if (mainPopped) {
            did_work = true;  // drained the buffer even though we didn't encode this tick
        }

        // ── Lores ─────────────────────────────────────────────────────────────
        bool loresAnnotated = g_lores_annotated.load();
        auto lores_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            now - last_lores_time).count();
        int64_t lores_interval = loresAnnotated ? annot_interval_us : live_interval_us;

        RawFrame loresDelayedFrame;
        bool loresPopped = loresDelayBuf.pop(loresDelayedFrame, 0);

        if (counts.lores > 0 && lores_elapsed >= lores_interval) {
            RawFrame frame;
            bool haveFrame = false;

            if (loresAnnotated) {
                if (loresPopped) { frame = std::move(loresDelayedFrame); haveFrame = true; }
            } else {
                std::lock_guard<std::mutex> lk(loresMu);
                if (loresHas) { frame = loresLatest; haveFrame = true; }
            }

            if (haveFrame && !frame.data.empty()) {
                if (loresAnnotated) {
                    DetectionEvent evt;
                    bool matched = detBuf.findNearest(frame.timestampUs, tolerance_us, evt);
                    if (matched) {
                        annot::annotate(frame.data.data(), frame.width, frame.height, evt.dets);
                        ++matched_this_tick;
                    }
                }
                if (g_osd_camera_id_enabled.load() || g_osd_time_enabled.load())
                    osd::burn(frame.data.data(), frame.width, frame.height,
                             frame.timestampUs,
                             osd_camera_label(frame.cameraIndex),
                             g_osd_camera_id_enabled.load(), g_osd_time_enabled.load());
                auto webp = loresEncoder.encode(frame.data.data(), frame.width, frame.height);
                streamSrv.sendFrameForSource(StreamSource::Lores, webp);
                last_lores_time = now;
                did_work = true;
                newest_ts_us = frame.timestampUs;
            }
        } else if (loresPopped) {
            did_work = true;
        }

        // Always sleep a little, even when work was done. The previous
        // version only slept when did_work was false, meaning the loop
        // ran completely unthrottled — as fast as the CPU allowed — for
        // the entire time any client was connected. Combined with 4-6
        // separate mu_ acquisitions per iteration, that was the real
        // source of the contention severe enough to look like a deadlock.
        // A 1ms floor costs nothing for fps targets in the 10-30 range
        // but caps the iteration rate to something sane.
        std::this_thread::sleep_for(std::chrono::milliseconds(did_work ? 1 : 2));

        {
            std::lock_guard<std::mutex> lk(g_pstatus.mu);
            g_pstatus.delay_buffer_depth = static_cast<int>(mainDelayBuf.size() + loresDelayBuf.size());
            g_pstatus.clients            = counts.total;
            g_pstatus.frames_out        += (newest_ts_us != 0) ? 1 : 0;
            g_pstatus.matched           += matched_this_tick;
            if (newest_ts_us != 0) g_pstatus.latest_frame_ts_us = newest_ts_us;
        }

        // Status log once per second — reuse the same snapshot, no new lock.
        double since = std::chrono::duration<double>(now - t_last).count();
        if (since >= 1.0) {
            std::cerr << "\r[Main]"
                      << " main=" << counts.main << (mainAnnotated ? "(annotated)" : "(live)")
                      << " lores=" << counts.lores << (loresAnnotated ? "(annotated)" : "(live)")
                      << " buf=" << (mainDelayBuf.size() + loresDelayBuf.size()) << "   ";
            std::cerr.flush();
            t_last = now;
        }
    }

    std::cerr << "\n[Main] Shutting down.\n";
    return 0;
}
