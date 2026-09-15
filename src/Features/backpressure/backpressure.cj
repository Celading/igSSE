package igsse.Features.backpressure

import igsse.Features.codec.*
import std.collection.ArrayList
import std.sync.*
import std.time.*

// =============================================================================
// Backpressure: bounded per-subscriber queue with explicit policies.
//
// The queue is the seam between the application publisher and the
// connection writer. publish() NEVER grows the queue unboundedly; when
// full, the configured policy applies:
//   DropOldest              — shed the oldest queued event (telemetry)
//   DropNewest              — shed the incoming event (audit trails)
//   BlockWithTimeout        — poll-wait up to timeoutMs for space, then
//                             shed the incoming event
//   DisconnectSlowConsumer  — signal the server to drop the connection
//
// Locking discipline (Cangjie trap from igCoAP/igMQTT): synchronized()
// takes a lambda, so a bare `return` inside it returns from the LAMBDA,
// not the function. Every synchronized block therefore evaluates to an
// Int64 outcome code; the function maps codes to the enum outside.
// =============================================================================

public enum BackpressurePolicy {
    | DropOldest
    | DropNewest
    | BlockWithTimeout
    | DisconnectSlowConsumer
}

public enum PublishOutcome {
    | Delivered
    | ShedOldest
    | ShedIncoming
    | PublishTimedOut
    | SlowConsumerDisconnected
}

public class SseSubscription {
    private let capacity: Int64
    private let policy: BackpressurePolicy
    private let blockTimeoutMs: Int64
    private var queue: ArrayList<SseEvent>
    private var closedFlag: Bool
    private var disconnectFlag: Bool
    private var shedTotal: Int64
    private let epoch: MonoTime
    private let lock: Mutex = Mutex()

    public init(capacity!: Int64 = 1024, policy!: BackpressurePolicy = BackpressurePolicy.DropOldest,
                blockTimeoutMs!: Int64 = 1000) {
        if (capacity < 1) {
            throw Exception("queue capacity must be >= 1")
        }
        this.capacity = capacity
        this.policy = policy
        this.blockTimeoutMs = blockTimeoutMs
        this.queue = ArrayList<SseEvent>()
        this.closedFlag = false
        this.disconnectFlag = false
        this.shedTotal = 0
        this.epoch = MonoTime.now()
    }

    private func nowMs(): Int64 {
        Int64((MonoTime.now() - epoch) / Duration.millisecond)
    }

    public func close(): Unit {
        synchronized(lock) {
            closedFlag = true
        }
    }

    public func isClosed(): Bool {
        synchronized(lock) {
            if (closedFlag) { 1 } else { 0 }
        } == 1
    }

    /// The server drops a slow consumer's connection when this is set.
    public func isDisconnectRequested(): Bool {
        synchronized(lock) {
            if (disconnectFlag) { 1 } else { 0 }
        } == 1
    }

    public func shedTotalCount(): Int64 {
        synchronized(lock) {
            shedTotal
        }
    }

    public func size(): Int64 {
        synchronized(lock) {
            queue.size
        }
    }

    /// Publisher side. Blocking only under BlockWithTimeout, and even
    /// then bounded by blockTimeoutMs.
    public func publish(e: SseEvent): PublishOutcome {
        // fast path: space available (match — enums have no ==)
        let fast = synchronized(lock) {
            if (closedFlag || disconnectFlag) {
                4 // SlowConsumerDisconnected
            } else if (queue.size < capacity) {
                queue.add(e)
                0 // Delivered
            } else {
                match (policy) {
                    case DropOldest =>
                        shedTotal += 1
                        let shed = queue[0]
                        queue.remove(0..1)
                        queue.add(e)
                        1 // ShedOldest
                    case DropNewest =>
                        shedTotal += 1
                        2 // ShedIncoming
                    case DisconnectSlowConsumer =>
                        disconnectFlag = true
                        4 // SlowConsumerDisconnected
                    case BlockWithTimeout =>
                        5 // fall through to the bounded wait
                }
            }
        }
        if (fast != 5) {
            return codeToOutcome(fast)
        }
        // BlockWithTimeout: poll for space outside the lock, bounded
        let deadline = nowMs() + blockTimeoutMs
        while (nowMs() < deadline) {
            sleep(Duration.millisecond * 5)
            let outcome = synchronized(lock) {
                if (closedFlag || disconnectFlag) {
                    4 // SlowConsumerDisconnected
                } else if (queue.size < capacity) {
                    queue.add(e)
                    0 // Delivered
                } else {
                    3 // PublishTimedOut (so far)
                }
            }
            if (outcome != 3) {
                return codeToOutcome(outcome)
            }
        }
        synchronized(lock) {
            shedTotal += 1
        }
        PublishOutcome.ShedIncoming
    }

    private func codeToOutcome(code: Int64): PublishOutcome {
        match (code) {
            case 0 => PublishOutcome.Delivered
            case 1 => PublishOutcome.ShedOldest
            case 2 => PublishOutcome.ShedIncoming
            case 4 => PublishOutcome.SlowConsumerDisconnected
            case _ => PublishOutcome.PublishTimedOut
        }
    }

    /// Server side: take up to max events (oldest first).
    public func drain(max: Int64): ArrayList<SseEvent> {
        let out = ArrayList<SseEvent>()
        synchronized(lock) {
            while (out.size < max && queue.size > 0) {
                let e = queue[0]
                queue.remove(0..1)
                out.add(e)
            }
        }
        out
    }
}
