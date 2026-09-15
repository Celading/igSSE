package igsse.Features.resume

// =============================================================================
// Reconnection policy (§reconnection time + Last-Event-ID).
//
// Spec: a `retry:` field sets the reconnection time in ms; the default is
// implementation-defined ("a few seconds"). Extension beyond spec: when a
// connection fails WITHOUT the server having delivered anything since the
// last attempt, the delay doubles up to a cap so a dead endpoint is not
// hammered; delivering any event resets the backoff. 204 No Content stops
// reconnection entirely (spec-fail-closed).
// =============================================================================

public class SseResumePolicy {
    public let initialRetryMs: Int64
    public let maxRetryMs: Int64
    private var serverRetryMs: Int64    // 0 = server never sent retry
    private var consecutiveFailures: Int64
    private var lastEventIdValue: String

    public init(initialRetryMs!: Int64 = 3000, maxRetryMs!: Int64 = 60000) {
        if (initialRetryMs < 1 || maxRetryMs < initialRetryMs) {
            throw Exception("invalid retry policy: initial=${initialRetryMs} max=${maxRetryMs}")
        }
        this.initialRetryMs = initialRetryMs
        this.maxRetryMs = maxRetryMs
        this.serverRetryMs = 0
        this.consecutiveFailures = 0
        this.lastEventIdValue = ""
    }

    /// A `retry:` field seen on the wire (spec: replaces the timer).
    public func onServerRetry(ms: Int64): Unit {
        if (ms > 0) {
            serverRetryMs = ms
        }
    }

    /// An event was delivered — backoff resets, id advances.
    public func onEvent(id: String): Unit {
        lastEventIdValue = id
        consecutiveFailures = 0
    }

    /// The connection dropped (or a reconnect attempt failed): one more
    /// failure counts.
    public func onFailure(): Unit {
        consecutiveFailures += 1
    }

    /// Delay before the next attempt. Base = server retry when the server
    /// sent one, else the initial value; doubled per consecutive
    /// empty-handed failure, capped at maxRetryMs.
    public func nextDelayMs(): Int64 {
        var base = if (serverRetryMs > 0) { serverRetryMs } else { initialRetryMs }
        var backoff = consecutiveFailures
        if (backoff > 4) {
            backoff = 4 // beyond x16 the cap decides
        }
        var delay = base
        var i: Int64 = 0
        while (i < backoff) {
            delay = delay * 2
            if (delay >= maxRetryMs) {
                delay = maxRetryMs
                break
            }
            i++
        }
        if (delay > maxRetryMs) {
            delay = maxRetryMs
        }
        delay
    }

    public func lastEventId(): String {
        lastEventIdValue
    }

    public func serverRetrySeen(): Bool {
        serverRetryMs > 0
    }
}
