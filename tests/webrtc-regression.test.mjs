import assert from "node:assert/strict"
import { test } from "node:test"
import { loadClasses } from "./load-typescript.mjs"

function setup(fetch = async () => {}) {
    const timers = new Map()
    let sequence = 0
    const classes = loadClasses("../web/stream/transport/webrtc.ts", ["WebRTCTransport", "WebRtcControlStream"], {
        fetchApi: fetch,
        webrtcSessionAnswerParse: () => ({}),
        ClientInputEvent_Tags: new Proxy({}, { get: (_, name) => name }),
        globalObject: () => ({
            setTimeout(callback) { timers.set(++sequence, callback); return sequence },
            clearTimeout(id) { timers.delete(id) },
            requestAnimationFrame() {},
        }),
    })
    const transport = Object.assign(Object.create(classes.WebRTCTransport.prototype), {
        api: {}, location: null, pendingIceCandidates: [], sendingIceCandidates: false,
        closed: false, iceCandidateSendTimer: null,
        peer: { iceGatheringState: "complete", async setRemoteDescription() {}, close() {} },
        controlStream: { close() {} },
    })
    transport.boundSendIceCandidates = transport.sendIceCandidates.bind(transport)
    return { ...classes, transport, timers }
}

test("late SDP answer flushes candidates even after ICE gathering completes", async () => {
    const requests = []
    const { transport, timers } = setup(async (...args) => requests.push(args))
    transport.onIceCandidate({ candidate: { toJSON: () => ({ candidate: "candidate:early" }) } })
    assert.equal(requests.length, 0)
    await transport.setAnswer({ location: "/session/1", answerSdp: "" })
    assert.equal(requests[0][3].trickleIceSdpFrag, "a=candidate:early")
    assert.deepEqual(transport.pendingIceCandidates, [])
    assert.equal(timers.size, 0)
})

test("candidates arriving during a PATCH are sent in the next batch", async () => {
    let release
    const requests = []
    const { transport, timers } = setup(async (...args) => {
        requests.push(args)
        if (requests.length === 1) await new Promise(resolve => release = resolve)
    })
    transport.location = "/session/1"
    transport.pendingIceCandidates.push("first")
    const sending = transport.sendIceCandidates()
    transport.onIceCandidate({ candidate: { toJSON: () => ({ candidate: "second" }) } })
    release()
    await sending
    await [...timers.values()][0]()
    assert.deepEqual(requests.map(args => args[3].trickleIceSdpFrag), ["a=first", "a=second"])
})

test("failed candidate PATCH retries and close cancels retry", async () => {
    const { transport, timers } = setup(async () => { throw new Error("offline") })
    transport.location = "/session/1"
    transport.pendingIceCandidates.push("first")
    await transport.sendIceCandidates()
    assert.deepEqual(transport.pendingIceCandidates, ["first"])
    assert.equal(timers.size, 1)
    await transport.close()
    assert.equal(timers.size, 0)
})

test("gamepad connect, state and disconnect use the native batcher without throwing", () => {
    const { WebRtcControlStream } = setup()
    const events = [], sent = []
    const control = Object.assign(Object.create(WebRtcControlStream.prototype), {
        packetBuffer: [],
        controllerBatcher: {
            removeBatchedInputs() { return [] },
            batchInput(event) {
                events.push(event)
                return [event]
            },
        },
        disposed: false, channel: { readyState: "open" }, controller: {},
        mouseState: { moveX: 0, moveY: 0 }, mouseScrollX: 0, mouseScrollY: 0,
        sendRaw(packet) { sent.push(packet) },
        trySendOn(_, packet) { sent.push(packet) },
        sendKeysCompact() {},
    })
    for (const tag of ["ControllerConnect", "ControllerState", "ControllerDisconnect"]) {
        control.send({ tag, inner: { controllerNumber: 1, leftStickX: 0.5 } })
        control.sendBatchedInputs()
    }
    assert.equal(events.length, 3)
    assert.deepEqual(sent, events)
    assert.equal(sent[1].inner.leftStickX, 0.5)
})
