import { Api, fetchApi, WebRTCAnswer } from "../../api"
import { InputBatcher, ClientInputEvent, ClientInputEvent_Tags, ControlPacket, ControlPacketConfig, controlPacketDeserialize, controlPacketSerialize, PacketDirection, VideoFormats, WebRtcSessionAnswer, webrtcSessionAnswerParse, WebRtcSessionOffer, webrtcSessionOfferApply } from "../../uniffi/moonlight_common_bindings"
import { globalObject, wait } from "../../util"
import { AudioPlayer, TrackAudioPlayer } from "../audio/index"
import { Logger } from "../log"
import { DataPipe } from "../pipeline/pipes"
import { StatValue } from "../stats"
import { TrackVideoRenderer, VideoRenderer } from "../video/index"
import { generateControlPacketConfig, IControlStream, Transport, TransportAudioType, TransportConnectData, TransportOptions, TransportShutdown, TransportVideoType } from "./index"

export class WebRTCTransport implements Transport {

    readonly implementationName: string = "webrtc"

    readonly controlStream
    onconnect: ((connectData: TransportConnectData) => void) | null = null
    onclose: ((shutdown: TransportShutdown) => void) | null = null

    private logger?: Logger

    private api: Api

    private peer: RTCPeerConnection
    private location: string | null = null
    private healthTimer: number | null = null
    private checkingHealth = false
    private disconnectedSince: number | null = null
    private lastHealthCheck = 0
    private lastVideoProgress = 0
    private healthFrames: number | null = null
    private reportedVideoStall = false

    constructor(api: Api, configuration: RTCConfiguration, logger?: Logger) {
        this.logger = logger

        this.api = api

        // Create peer
        this.peer = new RTCPeerConnection(configuration)
        this.controlStream = new WebRtcControlStream(this.peer, logger, reason => this.fail(reason))

        this.logger?.debug(`Using ice servers ${JSON.stringify(configuration.iceServers?.flatMap(server => server.urls))}`)

        // Set Event Listeners
        this.peer.addEventListener("connectionstatechange", this.onStateChange.bind(this))
        this.peer.addEventListener("datachannel", this.onDataChannel.bind(this))
        this.peer.addEventListener("track", this.onTrack.bind(this))

        // Ice Gathering
        this.peer.addEventListener("icecandidate", this.onIceCandidate.bind(this))

        // Add Media
        this.peer.addTransceiver("video", { direction: "recvonly" })
        this.peer.addTransceiver("audio", { direction: "recvonly" })

        // Dummy data channel required so that the answerer knows we accept data channels
        this.peer.createDataChannel("dummy")
        this.healthTimer = globalObject().setInterval(() => void this.checkHealth(), 2000)
    }

    private sdpOfferOptions: WebRtcSessionOffer | null = null
    private sdpAnswer: WebRtcSessionAnswer | null = null

    async createOffer(options: TransportOptions): Promise<string> {
        this.logger?.debug("Creating webrtc offer")

        let offer = await this.peer.createOffer()
        if (offer.type != "offer") {
            throw `WHEP offer is of type ${offer.type}`
        }

        this.logger?.debug("Setting webrtc local description")
        await this.peer.setLocalDescription(offer)

        // Insert custom options
        this.sdpOfferOptions = {
            ...options
        }
        const sdp = webrtcSessionOfferApply(offer.sdp ?? "", this.sdpOfferOptions)

        this.logger?.debug(`successfully generated webrtc sdp with options ${JSON.stringify(this.sdpOfferOptions)}`)
        console.debug("Client Sdp", sdp)

        this.logger?.debug(`starting ice candidate sender`)
        this.sendIceCandidates()

        return sdp
    }
    async setAnswer(response: WebRTCAnswer): Promise<void> {
        console.debug("server sdp", JSON.stringify(response))

        this.logger?.debug(`received whep response with location "${response.location}"`)
        // Print ice candidates
        for (const line of response.answerSdp.split("\r\n")) {
            if (line.startsWith("a=candidate")) {
                this.logger?.debug(`received remote ice candidate ${line.substring(2)}`)
            }
        }

        this.location = response.location

        this.sdpAnswer = webrtcSessionAnswerParse(response.answerSdp)
        this.logger?.debug(`Server responded with extensions ${JSON.stringify(this.sdpAnswer)}`)

        await this.peer.setRemoteDescription({
            type: "answer",
            sdp: response.answerSdp,
        })
        await this.sendIceCandidates()
    }

    private connectData: TransportConnectData | null = null
    private async generateConnectData(): Promise<TransportConnectData> {
        if (this.connectData) {
            return this.connectData
        }

        if (!this.videoStream || !this.audioStream) {
            throw `WebRTC WHEP response didn't contain a video and audio stream! Video: ${this.videoStream != null}, Audio: ${this.audioStream != null}`
        }
        const codec = await this.findOutCodec()

        const audioSettings = this.audioStream.getSettings()

        this.connectData = {
            capabilities: {
                touch: false
            },
            videoType: "videotrack",
            videoSetup: {
                // Assume the requested parameters are correct
                width: this.sdpOfferOptions?.width ?? -1,
                height: this.sdpOfferOptions?.height ?? -1,
                fps: this.sdpOfferOptions?.fps ?? -1,
                codec,
            },
            audioType: "audiotrack",
            audioSetup: {
                channels: audioSettings.channelCount ?? 2,
                sampleRate: audioSettings.sampleRate ?? 48000,
                // TODO
                streams: 0,
                coupledStreams: 0,
                samplesPerFrame: 0,
                mapping: []
            },
            appName: this.sdpAnswer?.appName ?? "Unknown"
        }
        return this.connectData
    }

    private async checkHealth() {
        if (this.closed || this.checkingHealth || !this.wasConnected) return
        this.checkingHealth = true
        try {
            const now = performance.now()
            if (this.peer.connectionState == "disconnected") {
                this.disconnectedSince ??= now
                if (now - this.disconnectedSince >= 10000) {
                    this.fail("WebRTC remained disconnected for ten seconds")
                    return
                }
            } else this.disconnectedSince = null
            const stats = await this.getStats()
            if (this.closed) return
            const frames = typeof stats.framesDecoded == "number" ? stats.framesDecoded : null
            // Hidden pages may legitimately stop presenting video. Reset the observation window.
            if (document.visibilityState != "visible" || now - this.lastHealthCheck > 5000 || frames != this.healthFrames) {
                this.lastVideoProgress = now
                this.reportedVideoStall = false
            }
            this.lastHealthCheck = now
            this.healthFrames = frames
            if (!this.reportedVideoStall && now - this.lastVideoProgress >= 15000) {
                this.logger?.debug("WebRTC frozen-stream diagnostics: " + JSON.stringify(stats))
                this.reportedVideoStall = true
                this.logger?.debug("No video decoding progress for fifteen seconds; checking connection health independently")
            }
        } catch (error) {
            this.logger?.debug("WebRTC health statistics unavailable: " + error)
        } finally {
            this.checkingHealth = false
        }
    }

    private fail(reason: string) {
        if (this.closed) return
        this.logger?.debug(reason)
        this.onclose?.(this.wasConnected ? "failed" : "failednoconnect")
        void this.close()
    }
    private wasConnected = false
    private onStateChange() {
        this.logger?.debug(`WebRTC state: peer=${this.peer.connectionState}, ice=${this.peer.iceConnectionState}, sctp=${this.peer.sctp?.state ?? "unavailable"}`)
        if (this.closed) return
        if (this.peer.connectionState == "connected") {
            if (this.wasConnected) return
            this.wasConnected = true

            this.generateConnectData().then(connectData => {
                if (!this.closed && this.onconnect) {
                    this.onconnect(connectData)
                }
            }).catch(error => this.fail("WebRTC setup failed: " + error))
        } else if (this.peer.connectionState == "failed" || this.peer.connectionState == "closed") {
            this.fail("WebRTC connection " + this.peer.connectionState)
        }
    }

    // -- Trickle Ice
    private iceCandidateSendTimer: number | null = null
    private pendingIceCandidates: Array<string> = []
    private sendingIceCandidates = false
    private closed = false
    private onIceCandidate(event: RTCPeerConnectionIceEvent) {
        if (!event.candidate) {
            // Ice Gathering finished
            this.logger?.debug("ice gathering finished")
            return
        }

        const candidate = event.candidate.toJSON().candidate
        if (candidate) {
            this.pendingIceCandidates.push(candidate)
            void this.sendIceCandidates()
        }
    }

    private boundSendIceCandidates = this.sendIceCandidates.bind(this)
    private async sendIceCandidates() {
        if (this.closed || this.sendingIceCandidates || !this.location) {
            return
        }
        if (this.iceCandidateSendTimer != null) {
            globalObject().clearTimeout(this.iceCandidateSendTimer)
            this.iceCandidateSendTimer = null
        }
        if (this.pendingIceCandidates.length == 0) {
            return
        }
        this.sendingIceCandidates = true
        const candidates = this.pendingIceCandidates.splice(0)
        try {
            await fetchApi(this.api, this.location, "PATCH", {
                noUrlModify: true,
                trickleIceSdpFrag: candidates.map(x => "a=" + x).join("\r\n"),
                response: "ignore",
            })
        } catch (error) {
            this.pendingIceCandidates.unshift(...candidates)
            this.logger?.debug("failed to send ice candidates: " + error)
        } finally {
            this.sendingIceCandidates = false
            if (!this.closed && this.pendingIceCandidates.length > 0) {
                this.iceCandidateSendTimer = globalObject().setTimeout(this.boundSendIceCandidates, 200)
            }
        }
    }

    // -- Control Stream / Media
    private onDataChannel(event: RTCDataChannelEvent) {
        const channel = event.channel

        this.logger?.debug(`received data channel with label: ${channel.label}`)

        if (channel.label == "moonlight.control") {
            const config = generateControlPacketConfig()

            this.controlStream.setChannel(channel, config)
        }
    }

    private onTrack(event: RTCTrackEvent) {
        event.receiver.jitterBufferTarget = 0
        if ("playoutDelayHint" in event.receiver) {
            event.receiver.playoutDelayHint = 0
        }
        const track = event.track

        this.logger?.debug(`received track with label: ${track.label}, kind: ${track.kind}`)

        if (track.kind == "video") {
            track.contentHint = "motion"

            this.videoStream = track
        } else if (track.kind == "audio") {
            this.audioStream = track
        }
    }

    // Video
    private videoStream: MediaStreamTrack | null = null

    setVideoPipeline(type: "videotrack", pipeline: (TrackVideoRenderer & VideoRenderer)): Promise<void>;
    setVideoPipeline(type: "data", pipeline: (DataPipe & VideoRenderer)): Promise<void>;
    async setVideoPipeline(type: TransportVideoType, pipeline: unknown): Promise<void> {
        if (!this.videoStream || !this.connectData) {
            throw "the stream must be connected!"
        }

        if (type == "videotrack") {
            const trackPipeline = pipeline as (TrackVideoRenderer & VideoRenderer)

            trackPipeline.setTrack(this.videoStream)
        } else if (type == "data") {
            throw "unimplemented"
        }
    }

    // Audio
    private audioStream: MediaStreamTrack | null = null

    setAudioPipeline(type: "audiotrack", pipeline: (TrackAudioPlayer & AudioPlayer)): Promise<void>
    setAudioPipeline(type: "data", pipeline: (DataPipe & AudioPlayer)): Promise<void>
    async setAudioPipeline(type: TransportAudioType, pipeline: AudioPlayer): Promise<void> {
        if (!this.audioStream || !this.connectData) {
            throw "the stream must be connected!"
        }

        if (type == "audiotrack") {
            const trackPipeline = pipeline as (TrackAudioPlayer & AudioPlayer)

            trackPipeline.setTrack(this.audioStream)
        } else if (type == "data") {
            throw "unimplemented"
        }
    }

    async close(): Promise<void> {
        if (this.closed) return
        this.closed = true
        if (this.healthTimer != null) globalObject().clearInterval(this.healthTimer)
        this.healthTimer = null
        this.controlStream.close()
        // Close the peer
        this.peer.close()

        // Delete the ice candidate send loop
        globalObject().clearTimeout(this.iceCandidateSendTimer)
        this.iceCandidateSendTimer = null

        // Delete our current session on the server
        if (this.location) {
            try {
                await fetchApi(this.api, this.location, "DELETE", {
                    keepalive: true,
                    noUrlModify: true,
                    response: "ignore",
                })
            } catch (e) {
                console.debug("failed to DELETE webrtc session", e)
            }
        }
    }

    private async findOutCodec(): Promise<keyof VideoFormats> {
        let tries = 0

        while (true) {
            const stats = await this.peer.getStats()
            for (const [_key, value] of stats) {
                // Video Stream
                if ("type" in value && "kind" in value
                    && value.type == "inbound-rtp" && value.kind == "video"
                ) {
                    const codec = stats.get(value.codecId)?.mimeType?.toLowerCase()
                    if (codec == "video/h264") return "h264"
                    if (codec == "video/h265") return "h265"
                    if (codec == "video/av1") return "av1Main8"
                }
            }
            tries += 1
            if (tries > 10) {
                throw new Error("No negotiated video codec appeared in WebRTC receiver statistics")
            }

            await wait(100)
        }
    }

    private lastTotalDecodeTime = 0
    private lastFramesDecoded = 0
    async getStats(): Promise<Record<string, StatValue>> {
        const out: Record<string, StatValue> = {}

        Object.assign(out, this.controlStream.getStats())
        out.peerState = this.peer.connectionState
        out.iceState = this.peer.iceConnectionState
        out.sctpState = this.peer.sctp?.state ?? "unavailable"

        const stats = await this.peer.getStats()

        for (const [_key, value] of stats) {
            if (value.type == "candidate-pair" && value.state == "succeeded" && value.nominated) {
                out.connectionRttMs = value.currentRoundTripTime * 1000
                out.localCandidateType = stats.get(value.localCandidateId)?.candidateType
                out.remoteCandidateType = stats.get(value.remoteCandidateId)?.candidateType
            }

            // Video Stream
            if ("type" in value && "kind" in value
                && value.type == "inbound-rtp" && value.kind == "video"
            ) {
                out.resolution = `Width: ${value?.frameWidth}, Height: ${value?.frameHeight}`

                out.framesDecoded = value?.framesDecoded
                out.framesDropped = value?.framesDropped
                out.keyFramesDecoded = value?.keyFramesDecoded

                out.packetsLost = value?.packetsLost
                out.packetsReceived = value?.packetsReceived

                out.nackCount = value?.nackCount
                out.pliCount = value?.pliCount
                out.firCount = value?.firCount

                if ("totalDecodeTime" in value && "framesDecoded" in value) {
                    out.decodeTimePerFrameMs = value.framesDecoded > this.lastFramesDecoded
                        ? (value.totalDecodeTime - this.lastTotalDecodeTime) / (value.framesDecoded - this.lastFramesDecoded) * 1000.0 : 0

                    this.lastFramesDecoded = value.framesDecoded
                    this.lastTotalDecodeTime = value.totalDecodeTime
                }

                out.currentFps = value?.framesPerSecond
            }
            if ("type" in value && "mimeType" in value && typeof value.mimeType == "string"
                && value.type == "codec" && value.mimeType.startsWith("video/")
            ) {
                out.codec = value.mimeType.substring(6)
                out.codecSdpFmtpLine = value?.sdpFmtpLine
            }

            // Audio Stream
        }

        return out
    }
}


class WebRtcControlStream implements IControlStream {
    private config: ControlPacketConfig | null = null
    private channel: RTCDataChannel | null = null
    private mouse: RTCDataChannel
    private mouseAbsolute: RTCDataChannel
    private controllerBatcher = new InputBatcher()
    private disposed = false
    private packetBuffer: Array<{ packet: ControlPacket, queuedAt: number }> = []
    private mouseState: { x: number, y: number, referenceWidth: number, referenceHeight: number } | { moveX: number, moveY: number } = { moveX: 0, moveY: 0 }
    private mouseDirty = false
    private timer: number | null = null
    private blockedSince: number | null = null
    private lastBufferedAmount = 0
    private lastTick = performance.now()
    private lastAbsoluteSend = 0
    onreceive: ((packet: ControlPacket) => void) | null = null

    constructor(peer: RTCPeerConnection, private logger?: Logger, private onFailure: (reason: string) => void = () => {}) {
        // Ordered partial reliability prevents old positions overtaking newer ones.
        this.mouseAbsolute = peer.createDataChannel("moonlight.control.mouseAbsolute", { ordered: true, maxPacketLifeTime: 30 })
        this.mouse = peer.createDataChannel("moonlight.control.mouse", { ordered: true, maxPacketLifeTime: 30 })
        for (const channel of [this.mouseAbsolute, this.mouse]) {
            channel.addEventListener("close", this.boundChannelFailure)
            channel.addEventListener("error", this.boundChannelFailure)
        }
        // Input scheduling must not depend on rendering animation frames.
        this.timer = globalObject().setInterval(() => this.sendBatchedInputs(), 8)
    }
    private boundChannelFailure = () => this.fail("WebRTC control channel closed or failed")
    private fail(reason: string) {
        if (this.disposed) return
        this.logger?.debug(reason)
        this.close()
        this.onFailure(reason)
    }
    getStats(): Record<string, StatValue> {
        return {
            controlState: this.channel?.readyState ?? "waiting",
            controlBufferedBytes: this.channel?.bufferedAmount ?? 0,
            controlPendingPackets: this.packetBuffer.length,
            controlOldestPacketMs: this.packetBuffer.length ? performance.now() - this.packetBuffer[0].queuedAt : 0,
            mouseBufferedBytes: this.mouse.bufferedAmount + this.mouseAbsolute.bufferedAmount,
        }
    }
    setChannel(channel: RTCDataChannel | null, config?: ControlPacketConfig) {
        if (this.channel) {
            this.channel.removeEventListener("open", this.boundFlush)
            this.channel.removeEventListener("bufferedamountlow", this.boundFlush)
            this.channel.removeEventListener("message", this.boundMessage)
            this.channel.removeEventListener("close", this.boundChannelFailure)
            this.channel.removeEventListener("error", this.boundChannelFailure)
        }
        this.channel = channel
        if (!channel || !config) return
        this.config = config
        channel.binaryType = "arraybuffer"
        channel.bufferedAmountLowThreshold = 512
        channel.addEventListener("open", this.boundFlush)
        channel.addEventListener("bufferedamountlow", this.boundFlush)
        channel.addEventListener("message", this.boundMessage)
        channel.addEventListener("close", this.boundChannelFailure)
        channel.addEventListener("error", this.boundChannelFailure)
        this.trySendBufferedPackets()
    }
    private boundMessage = (event: MessageEvent) => {
        if (!this.config || this.disposed) return
        const packet = controlPacketDeserialize(this.config, PacketDirection.ClientBound, event.data)
        if (packet) this.onreceive?.(packet)
    }
    send(input: ClientInputEvent) {
        if (this.disposed) return
        switch (input.tag) {
            case ClientInputEvent_Tags.MouseMoveAbsolute:
                this.mouseState = { ...input.inner }
                this.mouseDirty = true
                return
            case ClientInputEvent_Tags.MouseMoveRelative:
                if (!("moveX" in this.mouseState)) this.mouseState = { moveX: 0, moveY: 0 }
                this.mouseState.moveX += input.inner.deltaX
                this.mouseState.moveY += input.inner.deltaY
                return
            case ClientInputEvent_Tags.MouseScrollVertical:
                this.sendRaw(new ControlPacket.MouseScroll({ scrollAmount1: input.inner.scrollY, scrollAmount2: input.inner.scrollY, zero: 0 }))
                return
            case ClientInputEvent_Tags.MouseScrollHorizontal:
                this.sendRaw(new ControlPacket.MouseHorizontalScroll({ scrollAmount: input.inner.scrollX }))
                return
            default:
                // Preserve every key/button edge on one ordered reliable channel.
                for (const packet of this.controllerBatcher.batchInput(input)) this.sendRaw(packet)
        }
    }
    sendRaw(packet: ControlPacket) {
        if (this.disposed) return
        if (this.packetBuffer.length >= 256) {
            this.fail("WebRTC input queue overflow; refusing delayed input replay")
            return
        }
        this.packetBuffer.push({ packet, queuedAt: performance.now() })
        this.trySendBufferedPackets()
    }
    private boundFlush = () => this.trySendBufferedPackets()
    private trySendBufferedPackets() {
        if (this.disposed) return
        const now = performance.now()
        if (this.packetBuffer.length && now - this.packetBuffer[0].queuedAt > 1000) {
            this.fail("WebRTC input queue stalled for more than one second")
            return
        }
        const buffered = this.channel?.bufferedAmount ?? 0
        if (!buffered || buffered < this.lastBufferedAmount) this.blockedSince = null
        this.lastBufferedAmount = buffered
        if (buffered) {
            this.blockedSince ??= now
            if (now - this.blockedSince > 1000) {
                this.fail("WebRTC control send buffer stopped draining for one second")
                return
            }
        }
        while (this.channel && this.packetBuffer.length) {
            if (!this.trySendOn(this.channel, this.packetBuffer[0].packet)) break
            this.packetBuffer.shift()
        }
    }
    private sendBatchedInputs() {
        if (this.disposed) return
        const now = performance.now()
        if (now - this.lastTick > 250 && "moveX" in this.mouseState) this.mouseState = { moveX: 0, moveY: 0 }
        this.lastTick = now
        this.trySendBufferedPackets()
        if (this.disposed) return
        if ("x" in this.mouseState) {
            if ((this.mouseDirty || now - this.lastAbsoluteSend >= 100) && this.trySendOn(this.mouseAbsolute, new ControlPacket.MouseMoveAbsolute({ ...this.mouseState, unused: 0 }))) {
                this.mouseDirty = false
                this.lastAbsoluteSend = now
            }
        } else {
            if (this.mouseState.moveX || this.mouseState.moveY) this.trySendOn(this.mouse, new ControlPacket.MouseMoveRelative({ deltaX: Math.max(-32768, Math.min(32767, this.mouseState.moveX)), deltaY: Math.max(-32768, Math.min(32767, this.mouseState.moveY)) }))
            this.mouseState = { moveX: 0, moveY: 0 }
        }
    }
    private trySendOn(channel: RTCDataChannel, packet: ControlPacket): boolean {
        if (this.disposed || !this.config || channel.readyState != "open") return false
        if (channel.bufferedAmount > (channel == this.channel ? 4096 : 512)) return false
        const buffer = controlPacketSerialize(this.config, packet)
        if (!buffer) return false
        try {
            channel.send(buffer)
            return true
        } catch (error) {
            this.fail("WebRTC input send failed: " + error)
            return false
        }
    }
    close() {
        this.disposed = true
        if (this.timer != null) globalObject().clearInterval(this.timer)
        this.timer = null
        this.packetBuffer = []
        this.setChannel(null)
        for (const channel of [this.mouseAbsolute, this.mouse]) {
            channel?.removeEventListener("close", this.boundChannelFailure)
            channel?.removeEventListener("error", this.boundChannelFailure)
        }
    }
}
