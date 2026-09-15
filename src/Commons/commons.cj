package igsse.Commons

// =============================================================================
// igSSE scope ruling + shared errors.
//
// Member surface: WHATWG server-sent events over HTTP/1.1 (h1c, no TLS) —
// streaming codec (spec-exact), Last-Event-ID resume, heartbeat comment
// frames + client read deadline, bounded-queue backpressure policies.
// Out of scope: TLS/HTTP2 transport, browser EventSource DOM, protoc-style
// codegen, persistent server-side replay storage (the library carries the
// seam via SseSubscriberFactory; storage belongs to the application).
// =============================================================================

public class SseDecodeException <: Exception {
    public init(detail!: String) {
        super("SSE decode: ${detail}")
    }
}

public class SseEncodeException <: Exception {
    public init(detail!: String) {
        super("SSE encode: ${detail}")
    }
}

public class SseTransportException <: Exception {
    public init(detail!: String) {
        super("SSE transport: ${detail}")
    }
}
