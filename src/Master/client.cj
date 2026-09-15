package igsse.Master

import igsse.Commons.*
import igsse.Features.codec.*
import igsse.Features.resume.*
import std.collection.ArrayList
import std.net.*
import std.sync.*
import std.time.*

// =============================================================================
// Minimal SSE client (h1c): GET + Accept: text/event-stream, validate
// 200 + content-type fail-closed, stream-parse, auto-reconnect with the
// Last-Event-ID header and the resume policy's delay.
//
// Single-owner: the thread that creates the client drives nextEvent().
// The heartbeat read deadline IS the socket read timeout — any bytes
// (comments included) keep the connection alive; silence past the window
// drops the connection and takes the reconnect path.
//
// 204 No Content stops reconnection (spec) and fails the call closed.
// =============================================================================

public class SseClient {
    private let host: String
    private let port: UInt16
    private let path: String
    private let heartbeatTimeoutMs: Int64
    private let maxReconnects: Int64
    private let policy: SseResumePolicy
    private var socket: ?TcpSocket
    private var parser: SseStreamParser
    private var pending: ArrayList<SseEvent>
    private var reconnectsUsed: Int64
    private var stopped: Bool

    public init(host!: String, port!: UInt16, path!: String = "/events",
                heartbeatTimeoutMs!: Int64 = 15000, maxReconnects!: Int64 = 5,
                policy!: SseResumePolicy = SseResumePolicy()) {
        if (heartbeatTimeoutMs < 1) {
            throw Exception("heartbeat timeout must be >= 1ms")
        }
        this.host = host
        this.port = port
        this.path = path
        this.heartbeatTimeoutMs = heartbeatTimeoutMs
        this.maxReconnects = maxReconnects
        this.policy = policy
        this.socket = None
        this.parser = SseStreamParser()
        this.pending = ArrayList<SseEvent>()
        this.reconnectsUsed = 0
        this.stopped = false
    }

    /// Connect (or reconnect) and validate the response fail-closed.
    public func open(): Unit {
        let sock = TcpSocket(host, port)
        sock.connect(timeout: Duration.millisecond * heartbeatTimeoutMs)
        sock.readTimeout = Duration.millisecond * heartbeatTimeoutMs
        let req = ArrayList<Byte>()
        appendAscii(req, "GET ${path} HTTP/1.1\r\n")
        appendAscii(req, "Host: ${host}:${port}\r\n")
        appendAscii(req, "Accept: text/event-stream\r\n")
        appendAscii(req, "Cache-Control: no-cache\r\n")
        let id = policy.lastEventId()
        if (id != "") {
            appendAscii(req, "Last-Event-ID: ${id}\r\n")
        }
        appendAscii(req, "\r\n")
        sock.write(req.toArray())
        let resp = readResponseHead(sock, parser)
        if (resp.status == 204) {
            // spec: the server says do not reconnect
            stopped = true
            sock.close()
            throw SseTransportException(detail: "server answered 204 — do not reconnect")
        }
        if (resp.status != 200) {
            sock.close()
            throw SseTransportException(detail: "unexpected status ${resp.status}")
        }
        if (!stringHasPrefixBytes(resp.contentType, "text/event-stream")) {
            sock.close()
            throw SseTransportException(detail: "content-type is not text/event-stream: ${resp.contentType}")
        }
        socket = Some(sock)
        parser = SseStreamParser() // fresh stream; lastEventId lives in the policy
    }

    /// Next event. Returns None when `timeoutMs` elapsed with a healthy
    /// (but idle) connection. Reconnects internally on connection loss;
    /// throws once maxReconnects is exhausted. A retry field on the wire
    /// updates the resume policy automatically.
    public func nextEvent(timeoutMs!: Int64 = 5000): ?SseEvent {
        let deadline = MonoTime.now() + Duration.millisecond * timeoutMs
        let noneEv: ?SseEvent = None
        while (true) {
            if (stopped) {
                return noneEv
            }
            // deliver from the parser first (remove(range) returns Unit —
            // index first, then remove)
            if (pending.size > 0) {
                let e = pending[0]
                pending.remove(0..1)
                policy.onEvent(e.id)
                // F4: the authoritative retry value comes from the parsed
                // event — never from scanning raw bytes (event DATA that
                // merely CONTAINS "retry: N" must not touch the policy)
                match (e.retryMs) {
                    case Some(ms) => policy.onServerRetry(ms)
                    case None => ()
                }
                return Some(e)
            }
            var sock: ?TcpSocket = None
            match (socket) {
                case Some(s) => sock = Some(s)
                case None => ()
            }
            match (sock) {
                case Some(s) =>
                    let remaining = MonoTime.now() + Duration.millisecond * 1
                    let _ = remaining
                    if (MonoTime.now() >= deadline) {
                        return noneEv
                    }
                    // any read failure — hard close or silence past the
                    // heartbeat window — takes the reconnect path
                    var chunk: ?Array<Byte> = None
                    try {
                        let buf = Array<Byte>(4096, repeat: 0)
                        let r = s.read(buf)
                        if (r <= 0) {
                            chunk = None
                        } else {
                            chunk = Some(sliceBytes(buf, 0, r))
                        }
                    } catch (_: Exception) {
                        chunk = None // read timeout or reset
                    }
                    match (chunk) {
                        case Some(bytes) =>
                            let events = parser.feed(bytes)
                            for (e in events) {
                                pending.add(e)
                            }
                            syncParserStateToPolicy()
                            continue
                        case None =>
                            // silence past the heartbeat window OR a hard
                            // close — either way, take the reconnect path
                            dropConnection()
                    }
                case None =>
                    dropConnection()
            }
        }
        throw SseTransportException(detail: "unreachable event loop exit")
    }

    public func close(): Unit {
        stopped = true
        match (socket) {
            case Some(s) => s.close()
            case None => ()
        }
    }

    private func dropConnection(): Unit {
        match (socket) {
            case Some(s) => s.close()
            case None => ()
        }
        socket = None
        parser = SseStreamParser()
        pending = ArrayList<SseEvent>()
        policy.onFailure()
        if (reconnectsUsed >= maxReconnects) {
            throw SseTransportException(detail: "reconnect budget exhausted (${maxReconnects})")
        }
        reconnectsUsed += 1
        let delay = policy.nextDelayMs()
        if (delay > 0) {
            sleep(Duration.millisecond * delay)
        }
        open()
    }

    /// WHATWG: the reconnection time and the last-event-id string
    /// update at PARSE time, not at dispatch — wherever bytes enter the
    /// parser (head leftover OR stream reads), the resume policy must
    /// sync. A retry-only / id-only line with no data still reaches it
    /// (MA3-igSSE).
    private func syncParserStateToPolicy(): Unit {
        match (parser.reconnectionTimeMs()) {
            case Some(ms) => policy.onServerRetry(ms)
            case None => ()
        }
        let lid = parser.lastEventId
        // WHATWG: an explicit empty `id:` field RESETS the last-event-id
        // string, and the parser's buffer already distinguishes that from
        // "no id field seen" (which cannot turn a non-empty id empty).
        // The old non-empty guard swallowed the reset, so the reconnect
        // Last-Event-ID header disagreed with the server's latest cursor
        // (prior hardening round).
        policy.onEvent(lid)
    }

    // ---- HTTP response head ----

    private func readResponseHead(sock: TcpSocket, parser: SseStreamParser): HttpResponseHead {
        let headBuf = ArrayList<Byte>()
        let buf = Array<Byte>(256, repeat: 0)
        var seenEnd = false
        while (!seenEnd && headBuf.size < 8192) {
            var r: Int64 = -1
            try {
                r = sock.read(buf)
            } catch (e: Exception) {
                sock.close()
                throw SseTransportException(detail: "response head read failed: ${e}")
            }
            if (r <= 0) {
                sock.close()
                throw SseTransportException(detail: "response head closed early")
            }
            var i: Int64 = 0
            while (i < r) {
                headBuf.add(buf[i])
                if (headEndsAt(headBuf)) {
                    seenEnd = true
                    // bytes AFTER the head in this same read chunk are
                    // stream body — feed them, never discard
                    let leftover = sliceBytes(buf, i + 1, r - i - 1)
                    if (leftover.size > 0) {
                        let evs = parser.feed(leftover)
                        for (e in evs) {
                            pending.add(e)
                        }
                        syncParserStateToPolicy()
                    }
                    break
                }
                i++
            }
        }
        if (!seenEnd) {
            sock.close()
            throw SseTransportException(detail: "response head too large")
        }
        let bytes = headBuf.toArray()
        let status = parseStatus(bytes)
        let contentType = extractHeader(bytes, "content-type:")
        HttpResponseHead(status, contentType)
    }

    private func headEndsAt(buf: ArrayList<Byte>): Bool {
        let n = buf.size
        if (n >= 4 && buf[n - 4] == 0x0D && buf[n - 3] == 0x0A
            && buf[n - 2] == 0x0D && buf[n - 1] == 0x0A) {
            return true
        }
        if (n >= 2 && buf[n - 2] == 0x0A && buf[n - 1] == 0x0A) {
            return true
        }
        false
    }

    private func parseStatus(head: Array<Byte>): Int64 {
        // "HTTP/1.1 200 OK" → the three bytes after the first space
        var sp: Int64 = -1
        var i: Int64 = 0
        while (i < head.size && head[i] != 0x0A) {
            if (head[i] == 0x20 && sp < 0) {
                sp = i
            }
            i++
        }
        if (sp < 0 || sp + 4 >= head.size) {
            return 0
        }
        var v: Int64 = 0
        var k: Int64 = 0
        while (k < 3) {
            let b = head[sp + 1 + k]
            if (b < 0x30 || b > 0x39) {
                return 0
            }
            v = v * 10 + Int64(b - 0x30)
            k++
        }
        v
    }

    private func extractHeader(head: Array<Byte>, lowerNameWithColon: String): String {
        let prefix = lowerNameWithColon.toArray()
        var lineStart: Int64 = 0
        var i: Int64 = 0
        while (i <= head.size) {
            if (i == head.size || head[i] == 0x0A) {
                var end = i
                if (end > lineStart && head[end - 1] == 0x0D) {
                    end -= 1
                }
                var j = lineStart
                var matched = end - lineStart >= prefix.size
                while (matched && j < lineStart + prefix.size) {
                    var b = head[j]
                    if (b >= 0x41 && b <= 0x5A) {
                        b = b + 0x20
                    }
                    if (b != prefix[j - lineStart]) {
                        matched = false
                    }
                    j++
                }
                if (matched) {
                    var vStart = lineStart + prefix.size
                    while (vStart < end && head[vStart] == 0x20) {
                        vStart += 1
                    }
                    let value = Array<Byte>(end - vStart, repeat: 0)
                    var k: Int64 = 0
                    while (k < end - vStart) {
                        value[k] = head[vStart + k]
                        k++
                    }
                    return String.fromUtf8(value)
                }
                lineStart = i + 1
            }
            i++
        }
        ""
    }

    private func stringHasPrefixBytes(value: String, prefix: String): Bool {
        let sb = value.toArray()
        let pb = prefix.toArray()
        if (sb.size < pb.size) {
            return false
        }
        var i: Int64 = 0
        while (i < pb.size) {
            if (sb[i] != pb[i]) {
                return false
            }
            i++
        }
        true
    }

    private func appendAscii(out: ArrayList<Byte>, text: String): Unit {
        for (b in text.toArray()) {
            out.add(b)
        }
    }

    private func sliceBytes(src: Array<Byte>, offset: Int64, length: Int64): Array<Byte> {
        let safe = if (length < 0) { 0 } else { length }
        let out = Array<Byte>(safe, repeat: 0)
        var i: Int64 = 0
        while (i < safe) {
            if (offset + i >= src.size) {
                break
            }
            out[i] = src[offset + i]
            i++
        }
        out
    }
}

class HttpResponseHead {
    let status: Int64
    let contentType: String

    init(status: Int64, contentType: String) {
        this.status = status
        this.contentType = contentType
    }
}
