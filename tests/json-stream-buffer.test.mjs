import assert from "node:assert/strict"
import { test } from "node:test"
import { loadClasses } from "./load-typescript.mjs"

const { StreamedJsonResponse } = loadClasses("../web/api.ts", ["StreamedJsonResponse"], {
    window: { addEventListener() {} },
})
const encoder = new TextEncoder()

function parse(chunks) {
    return new StreamedJsonResponse(new ReadableStream({
        start(controller) {
            for (const chunk of chunks) controller.enqueue(chunk)
            controller.close()
        },
    }).getReader())
}

test("pairing completion survives coalesced response records", async () => {
    const parser = parse([encoder.encode('{"pin":"1234"}\n{"paired":true}\n')])
    assert.deepEqual(await parser.next(), { pin: "1234" })
    assert.deepEqual(await parser.next(), { paired: true })
    assert.equal(await parser.next(), null)
})

test("buffered records do not wait for another network read", { timeout: 500 }, async () => {
    let reads = 0
    const parser = new StreamedJsonResponse({ read() {
        reads++
        return reads === 1
            ? Promise.resolve({ done: false, value: encoder.encode('1\n2\n3\n') })
            : new Promise(() => {})
    } })
    assert.equal(await parser.next(), 1)
    assert.equal(await parser.next(), 2)
    assert.equal(await parser.next(), 3)
    assert.equal(reads, 1)
})

test("split UTF-8 and an unterminated final record are preserved", async () => {
    const bytes = encoder.encode('{"name":"电脑 🎮"}\n{"done":true}')
    const parser = parse(Array.from(bytes, byte => Uint8Array.of(byte)))
    assert.deepEqual(await parser.next(), { name: "电脑 🎮" })
    assert.deepEqual(await parser.next(), { done: true })
    assert.equal(await parser.next(), null)
})

test("truncated JSON is reported instead of silently discarded", async () => {
    await assert.rejects(parse([encoder.encode('{"pin":')]).next(), SyntaxError)
})
