import assert from "node:assert/strict"
import { test } from "node:test"
import { loadClasses } from "./load-typescript.mjs"

function setup() {
    let now = 0
    const timers = new Map(), channels = [], failures = []
    class Packet { constructor(inner) { Object.assign(this, inner) } }
    const { WebRtcControlStream, WebRTCTransport } = loadClasses("../web/stream/transport/webrtc.ts", ["WebRtcControlStream", "WebRTCTransport"], {
        performance: { now: () => now },
        document: { visibilityState: "visible" },
        InputBatcher: class { batchInput(event) { return [event] } removeBatchedInputs() { return [] } },
        ClientInputEvent_Tags: new Proxy({}, { get: (_, key) => key }),
        ControlPacket: { MouseMoveAbsolute: Packet, MouseMoveRelative: Packet, MouseScroll: Packet, MouseHorizontalScroll: Packet },
        controlPacketSerialize: (_, packet) => packet,
        globalObject: () => ({ setInterval(fn) { timers.set(1, fn); return 1 }, clearInterval(id) { timers.delete(id) } }),
        wait: async () => {},
    })
    function channel(label, options) {
        const listeners = new Map()
        const c = { label, options, readyState: "open", bufferedAmount: 0, sent: [],
            addEventListener(name, fn) { listeners.set(name, fn) },
            removeEventListener(name) { listeners.delete(name) },
            send(packet) { this.sent.push(packet) },
            emit(name) { listeners.get(name)?.() },
        }
        channels.push(c)
        return c
    }
    const control = new WebRtcControlStream({ createDataChannel: channel }, undefined, reason => failures.push(reason))
    const primary = channel("primary")
    control.setChannel(primary, {})
    return { control, primary, channels, timers, failures, WebRTCTransport, tick(ms = 8) { now += ms; timers.get(1)?.() } }
}

test("congested reliable channel preserves quick press/release in order", () => {
    const { control, primary, tick } = setup()
    primary.bufferedAmount = 4097
    control.send({ tag: "MouseButton", inner: { action: "press" } })
    control.send({ tag: "MouseButton", inner: { action: "release" } })
    assert.equal(primary.sent.length, 0)
    assert.equal(control.packetBuffer.length, 2)
    primary.bufferedAmount = 0
    tick()
    assert.deepEqual(primary.sent.map(p => p.inner.action), ["press", "release"])
    assert.equal(control.packetBuffer.length, 0)
})

test("stale queued inputs close the session without replay", () => {
    const { control, primary, tick, failures, timers } = setup()
    primary.bufferedAmount = 4097
    control.send({ tag: "Keyboard", inner: {} })
    primary.bufferedAmount = 0
    tick(1001)
    assert.equal(primary.sent.length, 0)
    assert.match(failures[0], /stalled/)
    assert.equal(timers.size, 0)
    assert.equal(control.packetBuffer.length, 0)
})

test("reliable queue overflow fails and remains bounded", () => {
    const { control, primary, failures } = setup()
    primary.readyState = "connecting"
    for (let i = 0; i < 300; i++) control.sendRaw({ i })
    assert.equal(failures.length, 1)
    assert.equal(control.packetBuffer.length, 0)
})

test("mouse is ordered and expiring; congested positions coalesce to latest", () => {
    const { control, channels, tick } = setup()
    const mouse = channels[0]
    assert.deepEqual(mouse.options, { ordered: true, maxPacketLifeTime: 30 })
    mouse.bufferedAmount = 513
    for (const x of [1, 2, 3]) { control.send({ tag: "MouseMoveAbsolute", inner: { x, y: 1, referenceWidth: 100, referenceHeight: 100 } }); tick() }
    mouse.bufferedAmount = 0
    tick()
    assert.deepEqual(mouse.sent.map(p => p.x), [3])
    tick(100)
    assert.deepEqual(mouse.sent.map(p => p.x), [3, 3])
})

test("rendering pause cannot replay accumulated relative movement", () => {
    const { control, channels, tick } = setup()
    control.send({ tag: "MouseMoveRelative", inner: { deltaX: 40, deltaY: 4 } })
    tick(300)
    assert.equal(channels[1].sent.length, 0)
})

test("control closure and send exceptions report one failure and stop timers", () => {
    const a = setup()
    a.primary.emit("close")
    assert.equal(a.failures.length, 1)
    assert.equal(a.timers.size, 0)
    const b = setup()
    b.primary.send = () => { throw new Error("SCTP failed") }
    b.control.sendRaw({})
    assert.match(b.failures[0], /SCTP failed/)
    assert.equal(b.timers.size, 0)
})

test("codec detection uses the receiving video codec rather than assuming H264", async () => {
    const { WebRTCTransport } = setup()
    const transport = Object.assign(Object.create(WebRTCTransport.prototype), {
        peer: { async getStats() { return new Map([["video", { type: "inbound-rtp", kind: "video", codecId: "codec" }], ["codec", { mimeType: "video/H265" }]]) } },
    })
    assert.equal(await transport.findOutCodec(), "h265")
})

test("codec resolves before any media arrives, without polling statistics", async () => {
    const { WebRTCTransport } = setup()
    let statsCalls = 0
    const transport = Object.assign(Object.create(WebRTCTransport.prototype), {
        videoReceiver: { getParameters: () => ({ codecs: [{ mimeType: "video/H265" }] }) },
        peer: { async getStats() { statsCalls++; return new Map() } },
    })
    // inbound-rtp cannot exist yet: RTP only flows after this resolves and the
    // stream starts. Waiting on statistics here deadlocks every session.
    assert.equal(await transport.findOutCodec(), "h265")
    assert.equal(statsCalls, 0)
})

test("codec falls back to the answer SDP when the receiver reports no codecs", async () => {
    const { WebRTCTransport } = setup()
    const transport = Object.assign(Object.create(WebRTCTransport.prototype), {
        videoReceiver: { getParameters: () => ({ codecs: [] }) },
        peer: {
            remoteDescription: { sdp: "a=rtpmap:98 H265/90000" },
            async getStats() { return new Map() },
        },
    })
    assert.equal(await transport.findOutCodec(), "h265")
})


test("a persistent disconnected peer fails with a specific reason", async () => {
    const { WebRTCTransport, tick } = setup()
    const reasons = []
    const transport = Object.assign(Object.create(WebRTCTransport.prototype), {
        wasConnected: true, closed: false, checkingHealth: false,
        disconnectedSince: null, lastHealthCheck: 0, lastVideoProgress: 0, healthFrames: null,
        peer: { connectionState: "disconnected" },
        async getStats() { return { framesDecoded: 10 } },
        fail(reason) { reasons.push(reason) },
    })
    await transport.checkHealth()
    tick(10001)
    await transport.checkHealth()
    assert.match(reasons[0], /disconnected for ten seconds/)
})

test("unchanging video reports diagnostics without disconnecting an idle desktop", async () => {
    const { WebRTCTransport, tick } = setup()
    const logs = [], failures = []
    const transport = Object.assign(Object.create(WebRTCTransport.prototype), {
        wasConnected: true, closed: false, checkingHealth: false,
        disconnectedSince: null, lastHealthCheck: 0, lastVideoProgress: 0, healthFrames: null,
        peer: { connectionState: "connected" },
        logger: { debug(message) { logs.push(message) } },
        async getStats() { return { framesDecoded: 10 } },
        fail(reason) { failures.push(reason) },
    })
    for (let i = 0; i < 12; i++) { tick(2000); await transport.checkHealth() }
    assert.equal(failures.length, 0)
    assert.equal(logs.filter(message => message.includes("frozen-stream diagnostics")).length, 1)
})

test("an established transport failure is not described as a failed initial connection", async () => {
    const { Stream } = loadClasses("../web/stream/index.ts", ["Stream"], {})
    const logs = []
    const stream = Object.assign(Object.create(Stream.prototype), {
        settings: { dataTransport: "webrtc" }, permissions: {},
        debugLog(message) { logs.push(message) },
        async tryWebRTCTransport() { return "failed" },
    })
    await stream.startConnection()
    assert.match(logs.at(-1), /connected, but the connection was lost/)
})


test("wheel input uses the reliable queue even while mouse movement is congested", () => {
    const { control, primary, channels } = setup()
    channels[0].bufferedAmount = 9999
    control.send({ tag: "MouseScrollVertical", inner: { scrollY: 120 } })
    assert.equal(primary.sent[0].scrollAmount1, 120)
    assert.equal(channels[0].sent.length, 0)
})
