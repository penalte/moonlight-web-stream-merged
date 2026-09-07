import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdtemp, readFile, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { once } from "node:events"
import { setTimeout as delay } from "node:timers/promises"

const binary = resolve(process.argv[2] ?? "target/debug/web-server")
const directory = await mkdtemp(join(tmpdir(), "moonlight-server-smoke-"))
const base = "http://127.0.0.1:18089/api"
const server = spawn(binary, ["--bind-address", "127.0.0.1:18089", "--log-file", "server/service.log"], {
    cwd: directory, stdio: ["ignore", "ignore", "pipe"],
})
let errors = ""
server.stderr.on("data", chunk => errors += chunk)
const exited = once(server, "exit")

async function request(path, method = "GET", body, cookie) {
    return fetch(base + path, {
        method, headers: { "Content-Type": "application/json", ...(cookie ? { Cookie: cookie } : {}) },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
    })
}
async function login(name, password) {
    const response = await request("/login", "POST", { name, password })
    assert.equal(response.status, 200)
    return response.headers.get("set-cookie").split(";")[0]
}

try {
    let ready = false
    for (let i = 0; i < 100; i++) {
        if (server.exitCode !== null) throw new Error("server exited: " + errors)
        try { ready = (await request("/authenticate")).status === 401 } catch {}
        if (ready) break
        await delay(100)
    }
    assert.ok(ready, "server must start with a missing log directory")
    assert.ok((await stat(join(directory, "server/service.log"))).isFile())

    const bootstrap = await Promise.all(["first", "second"].map(name => request("/login", "POST", { name, password: "initial password" })))
    assert.equal(bootstrap.filter(response => response.status === 200).length, 1)
    const admin = bootstrap.find(response => response.status === 200).headers.get("set-cookie").split(";")[0]
    const roleResponse = await request("/role", "POST", {
        name: "Smoke", ty: "User", default_settings: {}, permissions: {
            allow_add_hosts: true, maximum_bitrate_kbps: null,
            allow_codec_h264: true, allow_codec_h265: true, allow_codec_av1: true,
            allow_hdr: true, allow_transport_webrtc: true, allow_transport_websockets: true,
        },
    }, admin)
    assert.equal(roleResponse.status, 200)
    const { role } = await roleResponse.json()
    const userResponse = await request("/user", "POST", {
        name: "smoke-user", password: "old password", role_id: role.id, client_unique_id: "smoke-user",
    }, admin)
    assert.equal(userResponse.status, 200)
    const user = await userResponse.json()
    const oldSession = await login("smoke-user", "old password")
    assert.equal((await request("/user", "PATCH", { id: user.id, password: "new password" }, admin)).status, 200)
    assert.equal((await request("/authenticate", "GET", undefined, oldSession)).status, 401)
    const session = await login("smoke-user", "new password")
    for (const [ty, expected] of [["Admin", 200], ["User", 403]]) {
        assert.equal((await request("/role", "PATCH", { id: role.id, ty }, admin)).status, 200)
        assert.equal((await request("/users", "GET", undefined, session)).status, expected)
    }
    assert.equal((await request("/role?id=" + role.id, "DELETE", undefined, admin)).status, 409)
    assert.equal((await request("/user?user_id=" + user.id, "GET", undefined, admin)).status, 200)
    server.kill("SIGINT")
    const [code] = await Promise.race([exited, delay(10000).then(() => { throw new Error("shutdown did not complete") })])
    assert.equal(code, 0)
    const stored = JSON.parse(await readFile(join(directory, "server/data.json"), "utf8"))
    assert.ok(JSON.stringify(stored).includes("smoke-user"))
    console.log("Server smoke passed: fresh log directory, atomic bootstrap, session revocation, role promotion/demotion, safe deletion, shutdown persistence.")
} finally {
    if (server.exitCode === null) server.kill("SIGKILL")
}
