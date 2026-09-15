# igsse — Server-Sent Events (WHATWG) codec, resume, heartbeat, backpressure

Pure Cangjie (std only, no stdx) implementation of the WHATWG Server-Sent
Events specification (HTML Standard §server-sent-events) over HTTP/1.1.
Independent implementation; see [NOTICE](NOTICE) for trademark and
conformance wording and [LICENSE](LICENSE) for terms.

Package name: `igsse`; package version: `0.1.0`. Preview describes the
current supported scope, not completion of every future protocol feature.

## Current scope

- streaming codec: incremental parser (BOM, LF/CRLF/CR including
  split-CRLF across chunks, comments, case-sensitive fields, one-space
  rule, multi-line data, id set/reset/NUL-ignore, retry digits-only, EOF
  discards incomplete) + fail-closed encoder + heartbeat comment frames
- resume: Last-Event-ID tracking, reconnect header emission, server
  retry honored, bounded exponential backoff, 204 stops reconnecting
- backpressure: bounded per-subscriber queue — DropOldest / DropNewest /
  BlockWithTimeout / DisconnectSlowConsumer
- HTTP/1.1 server (spawned per connection under a hard cap, heartbeat
  loop) + auto-reconnect client (status/content-type fail-closed)

## Requirements

Cangjie/CJPM 1.1.3 is the current verification toolchain on macOS
arm64. Other platforms require their own validation. The library has
zero third-party runtime dependencies.

## Build

From the package directory:

```sh
cjpm build -j1
```

The source repository root additionally emits the `igsse_test` test
executable, run directly with its exit code.

## Independent use

These instructions use local source or an extracted source package; they
do not claim a registry version is already publicly available. Put the
library package (the directory containing its package `cjpm.toml`, not
the outer workspace) beside a consumer:

```toml
[dependencies]
igsse = { path = "../igsse" }
```

Use this as `src/main.cj` in an executable consumer:

```cangjie
package preview_example
import igsse.Features.codec.*

main(): Int64 {
    let parser = SseStreamParser()
    // WHATWG dispatch: an event completes at its terminating blank line
    let events = parser.feed("event: add\ndata: {\"n\":7}\n\n".toArray())
    if (events.size != 1) { return 2 }
    let ev = events[0]
    if (ev.eventType != "add" || ev.data != "{\"n\":7}") { return 3 }
    println("event ${ev.eventType}: ${ev.data}")
    return 0
}
```

Build with `cjpm build -j1` and run the emitted executable
(`target/release/bin/main` for the verified standalone layout). Expected
output:

```text
event add: {"n":7}
```

## Error handling

The parser follows the WHATWG algorithm: unknown fields are ignored,
invalid `retry:` values discard the field, a NUL in an `id:` field is
ignored, and an incomplete trailing event is never dispatched (EOF
discards it). The encoder fails closed on characters that would corrupt
the wire format.

## Resource and trust boundaries

Feed `SseStreamParser` arbitrary chunk boundaries — lines, CRLFs and the
BOM may split across chunks. Backpressure is explicit: subscriber queues
are bounded with a chosen policy (DropOldest, DropNewest,
BlockWithTimeout, DisconnectSlowConsumer); the server caps concurrent
connections and runs a heartbeat loop so idle proxies do not collect
live streams.

## Limitations

HTTP/1.1 only (no TLS, no HTTP/2 SSE). Server-side replay storage is
application-provided (`SseSubscriberFactory` seam); there is no pub/sub
fan-out tree. The client is single-owner threaded; no EventSource DOM
binding, no CORS machinery.

## Standards and licensing

WHATWG HTML Standard §server-sent-events is the recorded reference.
Independent implementation; no affiliation or standards certification is
implied. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
