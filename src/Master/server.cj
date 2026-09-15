package igsse.Master

import igsse.Commons.*
import igsse.Features.backpressure.*
import igsse.Features.codec.*
import igsse.Features.heartbeat.*
import std.collection.ArrayList
import std.net.*
import std.sync.*
import std.time.*

// =============================================================================
// Minimal HTTP/1.1 SSE server (h1c, no TLS).
//
// Contract: the application implements SseSubscriberFactory — per
// connection the server extracts the request's Last-Event-ID ("" when
// absent) and hands it to the factory; the returned subscription's queue
// is drained to the wire. Replay/resume storage is the application's job;
// backpressure (bounded queue + policy) and heartbeat comment frames are
// the server's job.
//
// Concurrency: one spawned thread per connection under a hard cap
// (over-cap connections are closed immediately and silently — writing a
// 503 from the accept thread to a silent client would block the whole
// accept loop; F3) — the serial-accept lesson
// applied: an SSE connection is long-lived by definition, so a serial
// accept loop could never serve a second client.
// =============================================================================

public interface SseSubscriberFactory {
    func onSubscribe(lastEventId: String): SseSubscription
}

/// Connection admission counter: owns its mutex. Every access to the
/// count happens inside this class's synchronized methods, keeping the
/// mutex-field pairing in one place (cjlint P.02-legible; the previous
/// form updated a bare field through synchronized(srv.lock) closures,
/// which is equally safe but hides the pairing from static analysis).
class ConnectionAdmissionCounter {
    private var count: Int64 = 0
    private let mutex: Mutex = Mutex()

    func tryAcquire(cap: Int64): Bool {
        var ok = false
        synchronized(mutex) {
            if (count < cap) {
                count += 1
                ok = true
            }
        }
        ok
    }

    func release(): Unit {
        synchronized(mutex) {
            count -= 1
        }
    }
}

public class SseServer {
    private let factory: SseSubscriberFactory
    private let heartbeatIntervalMs: Int64
    private let maxConcurrent: Int64
    private var serverSocket: ?TcpServerSocket
    private var running: Bool
    private let admission: ConnectionAdmissionCounter = ConnectionAdmissionCounter()
    private let lock: Mutex = Mutex()

    public init(factory!: SseSubscriberFactory, heartbeatIntervalMs!: Int64 = 15000,
                maxConcurrent!: Int64 = 64) {
        if (heartbeatIntervalMs < 1 || maxConcurrent < 1) {
            throw Exception("invalid server config: heartbeat=${heartbeatIntervalMs} max=${maxConcurrent}")
        }
        this.factory = factory
        this.heartbeatIntervalMs = heartbeatIntervalMs
        this.maxConcurrent = maxConcurrent
        this.serverSocket = None
        this.running = false
    }

    public func start(bindPort: UInt16): UInt16 {
        let s = TcpServerSocket(bindAt: bindPort)
        s.bind()
        let addr = s.localAddress
        let bound: UInt16 = match (addr) {
            case a: IPSocketAddress => a.port
            case _ => 0u16
        }
        serverSocket = Some(s)
        running = true
        let sock = s
        let srv = this
        spawn {
            while (srv.running) {
                var accepted: ?TcpSocket = None
                try {
                    accepted = Some(sock.accept())
                } catch (_: Exception) {
                    break // server closed
                }
                match (accepted) {
                    case Some(sock2) =>
                        if (!srv.admission.tryAcquire(srv.maxConcurrent)) {
                            // F3: close WITHOUT writing — a 503 write on
                            // this (accept) thread to a client that never
                            // reads would block the whole accept loop
                            // forever. The cap is a hard guard.
                            sock2.close()
                            continue
                        }
                        spawn {
                            try {
                                srv.serveConnection(sock2)
                            } catch (_: Exception) {}
                            srv.admission.release()
                        }
                    case None => ()
                }
            }
        }
        bound
    }

    public func stop(): Unit {
        running = false
        match (serverSocket) {
            case Some(s) => s.close()
            case None => ()
        }
    }

    private func serveConnection(sock: TcpSocket): Unit {
        sock.readTimeout = Duration.second * 5
        let parsed = readAndParseRequest(sock)
        match (parsed) {
            case Some(lastEventId) =>
                runStream(sock, lastEventId)
            case None =>
                sock.close()
        }
    }

    private func runStream(sock: TcpSocket, lastEventId: String): Unit {
        let sub = factory.onSubscribe(lastEventId)
        let head = ArrayList<Byte>()
        appendAscii(head, "HTTP/1.1 200 OK\r\n")
        appendAscii(head, "Content-Type: text/event-stream\r\n")
        appendAscii(head, "Cache-Control: no-cache\r\n")
        appendAscii(head, "Connection: keep-alive\r\n")
        appendAscii(head, "\r\n")
        sock.write(head.toArray())
        // stream loop: drain the bounded queue, heartbeat when idle
        var lastWrite = MonoTime.now()
        while (running) {
            if (sub.isDisconnectRequested()) {
                break // slow consumer dropped under backpressure policy
            }
            let events = sub.drain(64)
            if (events.size > 0) {
                for (e in events) {
                    sock.write(SseEncoder.encodeEvent(e))
                }
                lastWrite = MonoTime.now()
                if (sub.isClosed()) {
                    break
                }
                continue
            }
            if (sub.isClosed()) {
                break // drained to completion: end the response
            }
            let idle = Int64((MonoTime.now() - lastWrite) / Duration.millisecond)
            if (idle >= heartbeatIntervalMs) {
                sock.write(heartbeatFrame())
                lastWrite = MonoTime.now()
            }
            sleep(Duration.millisecond * 10)
        }
        sock.close()
    }

    // ---- request head ----

    private func readAndParseRequest(sock: TcpSocket): ?String {
        let headBuf = ArrayList<Byte>()
        let buf = Array<Byte>(256, repeat: 0)
        var seenEnd = false
        while (!seenEnd && headBuf.size < 8192) {
            var r: Int64 = -1
            try {
                r = sock.read(buf)
            } catch (_: Exception) {
                let noneHead: ?String = None
                return noneHead // timeout before a full head
            }
            if (r <= 0) {
                let noneHead: ?String = None
                return noneHead
            }
            var i: Int64 = 0
            while (i < r) {
                headBuf.add(buf[i])
                if (headEndsAt(headBuf)) {
                    seenEnd = true
                    break
                }
                i++
            }
        }
        if (!seenEnd) {
            let noneHead: ?String = None
            return noneHead // head too large — fail the connection
        }
        return Some(extractLastEventId(headBuf.toArray()))
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

    /// Case-insensitive `last-event-id` header extraction. Request heads
    /// are ASCII; everything runs on raw bytes (lowercase A-Z by +0x20).
    private func extractLastEventId(head: Array<Byte>): String {
        let prefix: Array<Byte> = [0x6C, 0x61, 0x73, 0x74, 0x2D, 0x65, 0x76, 0x65, 0x6E, 0x74, 0x2D, 0x69, 0x64, 0x3A]
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
                    // F6: an undecodable header value is treated as absent
                    try {
                        return String.fromUtf8(value)
                    } catch (_: Exception) {
                        return ""
                    }
                }
                lineStart = i + 1
            }
            i++
        }
        ""
    }

    private func appendAscii(out: ArrayList<Byte>, text: String): Unit {
        for (b in text.toArray()) {
            out.add(b)
        }
    }
}
