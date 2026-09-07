import assert from "node:assert/strict"
import { test } from "node:test"
import { loadClasses } from "./load-typescript.mjs"

test("disconnecting one gamepad keeps others updating and reconnect reuses its slot", () => {
    const pads = []
    const { StreamInput } = loadClasses("../web/stream/input.ts", ["StreamInput"], {
        navigator: { getGamepads: () => pads },
        window: { setInterval: () => 1 },
        emptyGamepadState: () => ({}),
        extractGamepadState: pad => ({ index: pad.index }),
        SUPPORTED_BUTTONS: {},
    })
    const added = [], removed = [], updated = []
    const input = Object.assign(Object.create(StreamInput.prototype), {
        connected: true, gamepads: [], gamepadRumbleCurrent: [], gamepadRumbleInterval: null,
        config: { controllerConfig: { sendIntervalOverride: null } },
        collectActuators: () => [],
        sendControllerAdd: id => added.push(id),
        sendControllerRemove: id => removed.push(id),
        sendController: (id, state) => updated.push([id, state.index]),
    })
    pads[3] = { index: 3, mapping: "standard" }
    pads[7] = { index: 7, mapping: "standard" }
    input.onGamepadConnect(pads[3])
    input.onGamepadConnect(pads[7])
    assert.deepEqual(added, [0, 1])
    assert.ok(input.gamepadRumbleCurrent[7])
    input.onGamepadDisconnect({ gamepad: pads[3] })
    pads[3] = null
    input.onGamepadUpdate()
    assert.deepEqual(removed, [0])
    assert.deepEqual(updated, [[1, 7]])
    pads[4] = { index: 4, mapping: "standard" }
    input.onGamepadConnect(pads[4])
    assert.deepEqual(added, [0, 1, 0])
})
