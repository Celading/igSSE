package igsse.Features.heartbeat

import igsse.Features.codec.*
import std.collection.ArrayList
import std.time.*

// =============================================================================
// Heartbeat: server idle comment frames + client read deadline.
//
// A comment frame is bytes on the wire but never dispatches an event on a
// spec decoder — exactly right for keepalive. Two sides:
// - server: if nothing was written for `intervalMs`, emit a comment frame
//   so intermediate proxies and the client's read deadline stay fed;
// - client: if NO bytes at all (comments count) arrive within
//   `timeoutMs`, the connection is considered dead → reconnect path.
// =============================================================================

public const SSE_HEARTBEAT_FRAME: String = ": ping\n\n"

/// Server-side: the frame to write when idle past the interval.
public func heartbeatFrame(comment!: String = "ping"): Array<Byte> {
    // ":ping\n\n" — built by hand to stay allocation-stable; the encoder
    // path (SseEncoder.encodeComment) is covered by the offline tests.
    let text = comment.toArray()
    let out = ArrayList<Byte>()
    out.add(0x3A)
    for (b in text) {
        out.add(b)
    }
    out.add(0x0A)
    out.add(0x0A)
    out.toArray()
}

/// Shared idle tracker (injectable nowMs for tests).
public class HeartbeatTracker {
    private let intervalMs: Int64
    private var lastActivityMs: Int64
    private let clock: () -> Int64

    public init(intervalMs!: Int64, clock!: () -> Int64) {
        if (intervalMs < 1) {
            throw Exception("heartbeat interval must be >= 1ms")
        }
        this.intervalMs = intervalMs
        this.clock = clock
        this.lastActivityMs = clock()
    }

    public func markActivity(): Unit {
        lastActivityMs = clock()
    }

    public func idleMs(): Int64 {
        clock() - lastActivityMs
    }

    /// True when a heartbeat should be written now (and only then — the
    /// caller is expected to markActivity after writing).
    public func due(): Bool {
        idleMs() >= intervalMs
    }
}
