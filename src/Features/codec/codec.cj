package igsse.Features.codec

import igsse.Commons.*
import std.collection.ArrayList

// =============================================================================
// SSE wire codec (WHATWG HTML Standard §server-sent-events, fetched
// 2026-09-10). Spec anchors baked into the code:
//
// - stream may start with a U+FEFF BOM (stripped);
// - lines end with LF, CRLF, or CR — a CR at a chunk boundary must WAIT
//   for the next byte (it may be the LF of a CRLF);
// - comment = line whose first byte is ':' — ignored;
// - field = name, or name ":" value; ONE leading U+0020 of the value is
//   removed; names are compared case-sensitively ("Data:" is NOT "data:");
// - "data" accumulates value + '\n'; on dispatch one trailing '\n' is
//   removed and the rest are the payload's embedded newlines;
// - "event" sets the type buffer (default "message" at dispatch);
// - "id": value containing U+0000 is IGNORED; an EMPTY value RESETS the
//   last event id (both per spec §interpreting);
// - "retry": only-ASCII-digits value sets the reconnection time, else the
//   field is ignored;
// - unknown field names are ignored;
// - dispatch happens ONLY on the blank line; an empty data buffer means
//   no event (buffers still reset);
// - EOF discards the incomplete (unterminated) event — never dispatched.
//
// Byte-level discipline: field names/values and id/retry/event checks run
// on raw bytes (UTF-8 is self-synchronizing, so ASCII bytes 0x00/0x0A/
// 0x0D/0x3A can only be those code points themselves).
// =============================================================================

/// One dispatched event. `id` is "" when the event carries no id; the
/// parser's `lastEventId` tracks the spec's "last event ID string"
/// (persists across events, reset by an empty `id:`).
public class SseEvent {
    public let eventType: String
    public let data: String
    public let id: String
    public let retryMs: ?Int64

    public init(eventType!: String, data!: String, id!: String, retryMs!: ?Int64) {
        this.eventType = eventType
        this.data = data
        this.id = id
        this.retryMs = retryMs
    }
}

/// Incremental streaming decoder: feed arbitrary byte chunks, get the
/// events that completed. Chunks may split lines, split CRLFs, and split
/// the BOM. `eof()` discards any unterminated tail (spec: never
/// dispatched) and resets the parser for reuse.
public class SseStreamParser {
    private var bomBuf: ArrayList<Byte>
    private var bomChecking: Bool
    private var lineBuf: ArrayList<Byte>
    private var crPending: Bool
    private var dataBuf: ArrayList<Byte> // byte buffer — a String would
    // make N data lines quadratic (each append re-copies the buffer)
    private var typeBuf: String
    private var idBuf: String
    private var retryBuf: ?Int64
    // WHATWG §9.2.2: the reconnection time updates at FIELD-PARSE time,
    // independent of any event dispatch — a `retry:` line with no data
    // still takes effect. Sticky, unlike the per-event retryBuf.
    private var appliedRetry: ?Int64
    public var lastEventId: String

    public init() {
        bomBuf = ArrayList<Byte>()
        bomChecking = true
        lineBuf = ArrayList<Byte>()
        crPending = false
        dataBuf = ArrayList<Byte>()
        typeBuf = ""
        idBuf = ""
        retryBuf = None<Int64>
        appliedRetry = None<Int64>
        lastEventId = ""
    }

    /// The currently-effective reconnection time (sticky), or None when
    /// the wire never sent a usable `retry:` field.
    public func reconnectionTimeMs(): ?Int64 {
        appliedRetry
    }

    /// Feed the next byte chunk; returns every event completed by it.
    public func feed(data: Array<Byte>): ArrayList<SseEvent> {
        let out = ArrayList<SseEvent>()
        for (b in data) {
            if (bomChecking) {
                bomBuf.add(b)
                if (bomBuf.size < 3) {
                    continue
                }
                // three bytes buffered: drop them iff they are the BOM
                let arr = bomBuf.toArray()
                if (arr[0] == 0xEF && arr[1] == 0xBB && arr[2] == 0xBF) {
                    bomBuf = ArrayList<Byte>()
                    bomChecking = false
                    continue
                }
                // not a BOM: the buffered bytes are stream content
                bomChecking = false
                bomBuf = ArrayList<Byte>()
                for (bb in arr) {
                    feedOne(bb, out)
                }
                continue
            }
            feedOne(b, out)
        }
        out
    }

    private func feedOne(b: Byte, out: ArrayList<SseEvent>): Unit {
        if (crPending) {
            crPending = false
            if (b == 0x0A) {
                return // CRLF: the line already dispatched at the CR
            }
            // CR then something else: the line dispatched; fall through
        }
        if (b == 0x0A) {
            dispatchLine(out)
            return
        }
        if (b == 0x0D) {
            crPending = true
            dispatchLine(out)
            return
        }
        lineBuf.add(b)
    }

    /// EOF: the unterminated tail is DISCARDED (spec: an incomplete event
    /// is never dispatched). The parser resets for stream reuse.
    public func eof(): Unit {
        lineBuf = ArrayList<Byte>()
        crPending = false
        dataBuf = ArrayList<Byte>()
        typeBuf = ""
        idBuf = ""
        retryBuf = None<Int64>
        bomBuf = ArrayList<Byte>()
        bomChecking = true
    }

    private func dispatchLine(out: ArrayList<SseEvent>): Unit {
        let lineBytes = lineBuf.toArray()
        lineBuf = ArrayList<Byte>()
        if (lineBytes.size == 0) {
            // blank line: the dispatch point
            let ev = dispatchEvent()
            match (ev) {
                case Some(e) => out.add(e)
                case None => ()
            }
            return
        }
        // comment: first byte ':' — ignored
        if (lineBytes[0] == 0x3A) {
            return
        }
        // split at the first colon
        var colon: Int64 = -1
        var i: Int64 = 0
        while (i < lineBytes.size) {
            if (lineBytes[i] == 0x3A) {
                colon = i
                break
            }
            i++
        }
        var nameBytes: Array<Byte>
        var valueBytes: Array<Byte>
        if (colon < 0) {
            nameBytes = lineBytes
            valueBytes = Array<Byte>()
        } else {
            nameBytes = sliceBytesC(lineBytes, 0, colon)
            var vStart = colon + 1
            if (vStart < lineBytes.size && lineBytes[vStart] == 0x20) {
                vStart += 1 // exactly ONE leading space is removed
            }
            valueBytes = sliceBytesC(lineBytes, vStart, lineBytes.size - vStart)
        }
        // field names/values are UTF-8; a line that is not valid UTF-8 is
        // ignored (lenient — the spec's stream is assumed valid UTF-8)
        var name = ""
        var value = ""
        try {
            name = String.fromUtf8(nameBytes)
            value = String.fromUtf8(valueBytes)
        } catch (_: Exception) {
            return
        }
        applyField(name, value)
    }

    private func applyField(name: String, value: String): Unit {
        if (name == "event") {
            typeBuf = value
            return
        }
        if (name == "data") {
            for (b in value.toArray()) {
                dataBuf.add(b)
            }
            dataBuf.add(0x0A)
            return
        }
        if (name == "id") {
            // a NULL anywhere in the value makes the whole field ignored
            var hasNul = false
            for (b in value.toArray()) {
                if (b == 0) {
                    hasNul = true
                    break
                }
            }
            if (!hasNul) {
                idBuf = value
            }
            return
        }
        if (name == "retry") {
            // only-ASCII-digits value sets the reconnection time; wider
            // than 10 digits is treated as invalid — a 20-digit hostile
            // value must not overflow the accumulator (F2). The WIDTH
            // check gates the loop itself: accumulating first and deciding
            // later still overflows.
            if (value.size > 0 && value.size <= 10) {
                var digitsOnly = true
                var v: Int64 = 0
                for (b in value.toArray()) {
                    if (b >= 0x30 && b <= 0x39) {
                        v = v * 10 + Int64(b - 0x30)
                    } else {
                        digitsOnly = false
                        break
                    }
                }
                if (digitsOnly) {
                    retryBuf = Some(v)
                    appliedRetry = Some(v)
                }
            }
            return
        }
        // unknown field names are ignored
    }

    /// Blank-line dispatch (§interpreting): empty data buffer → no event;
    /// otherwise strip ONE trailing '\n' and emit. The last event id
    /// advances to the id buffer regardless (spec: set at dispatch).
    private func dispatchEvent(): ?SseEvent {
        lastEventId = idBuf
        if (dataBuf.size == 0) {
            typeBuf = ""
            retryBuf = None<Int64>
            return None<SseEvent>
        }
        // strip exactly one trailing '\n' (the separator added per line)
        var dataBytes = dataBuf.toArray()
        if (dataBytes.size > 0 && dataBytes[dataBytes.size - 1] == 0x0A) {
            dataBytes = sliceBytesC(dataBytes, 0, dataBytes.size - 1)
        }
        let data = String.fromUtf8(dataBytes)
        let evType = if (typeBuf == "") { "message" } else { typeBuf }
        let ev = SseEvent(eventType: evType, data: data, id: idBuf, retryMs: retryBuf)
        typeBuf = ""
        dataBuf = ArrayList<Byte>()
        retryBuf = None<Int64>
        Some(ev)
    }
}

func sliceBytesC(src: Array<Byte>, offset: Int64, length: Int64): Array<Byte> {
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

/// Wire encoder. Field order: id, event, retry, data lines, blank
/// terminator. Validation fail-closed: CR/LF/NUL cannot appear in id,
/// event type, or comment text; CR/NUL cannot appear inside data (LF
/// splits the payload into multiple data lines, which the decoder joins
/// back with '\n' — byte-safe for multi-byte UTF-8 content because the
/// split runs on raw bytes).
public class SseEncoder {
    private static func bytesContain(values: Array<Byte>, needle: Byte): Bool {
        for (b in values) {
            if (b == needle) {
                return true
            }
        }
        false
    }

    private static func validateFieldBytes(value: String, field: String): Unit {
        let bytes = value.toArray()
        if (bytesContain(bytes, 0x0D) || bytesContain(bytes, 0x0A)) {
            throw SseEncodeException(detail: "${field} contains a line break")
        }
        if (bytesContain(bytes, 0x00)) {
            throw SseEncodeException(detail: "${field} contains NUL")
        }
    }

    public static func encodeEvent(e: SseEvent): Array<Byte> {
        validateFieldBytes(e.id, "id")
        validateFieldBytes(e.eventType, "event type")
        match (e.retryMs) {
            case Some(ms) =>
                if (ms < 0) {
                    throw SseEncodeException(detail: "retry must be non-negative: ${ms}")
                }
            case None => ()
        }
        let out = ArrayList<Byte>()
        if (e.id != "") {
            appendField(out, "id", e.id)
        }
        if (e.eventType != "message") {
            appendField(out, "event", e.eventType)
        }
        match (e.retryMs) {
            case Some(ms) => appendField(out, "retry", intToString(ms))
            case None => ()
        }
        // data: split the payload on LF into data lines (raw bytes —
        // multi-byte UTF-8 content never contains 0x0A inside a sequence)
        let dataBytes = e.data.toArray()
        var start: Int64 = 0
        var i: Int64 = 0
        while (i < dataBytes.size) {
            if (dataBytes[i] == 0x0A) {
                let lineBytes = sliceBytesC(dataBytes, start, i - start)
                if (bytesContain(lineBytes, 0x0D) || bytesContain(lineBytes, 0x00)) {
                    throw SseEncodeException(detail: "data line contains CR or NUL")
                }
                appendField(out, "data", String.fromUtf8(lineBytes))
                start = i + 1
            }
            i++
        }
        let tail = sliceBytesC(dataBytes, start, dataBytes.size - start)
        if (bytesContain(tail, 0x0D) || bytesContain(tail, 0x00)) {
            throw SseEncodeException(detail: "data line contains CR or NUL")
        }
        appendField(out, "data", String.fromUtf8(tail))
        out.add(0x0A) // blank-line terminator
        out.toArray()
    }

    /// Comment frame (heartbeat). Comments never dispatch an event on a
    /// spec-conformant decoder but ARE bytes on the wire — liveness.
    public static func encodeComment(text: String): Array<Byte> {
        validateFieldBytes(text, "comment")
        let out = ArrayList<Byte>()
        out.add(0x3A) // ':'
        for (b in text.toArray()) {
            out.add(b)
        }
        out.add(0x0A)
        out.add(0x0A)
        out.toArray()
    }

    private static func appendField(out: ArrayList<Byte>, name: String, value: String): Unit {
        for (b in name.toArray()) {
            out.add(b)
        }
        out.add(0x3A) // ':'
        for (b in value.toArray()) {
            out.add(b)
        }
        out.add(0x0A)
    }

    private static func intToString(v: Int64): String {
        if (v == 0) {
            return "0"
        }
        var x = v
        let digits = ArrayList<Byte>()
        while (x > 0) {
            digits.add(UInt8(0x30 + x % 10))
            x = x / 10
        }
        let arr = digits.toArray()
        let out = ArrayList<Byte>()
        var i = arr.size - 1
        while (i >= 0) {
            out.add(arr[i])
            i--
        }
        String.fromUtf8(out.toArray())
    }
}
