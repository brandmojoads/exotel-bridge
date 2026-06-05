const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 8080;
const AGENT_ID = process.env.AGENT_ID || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("ElevenLabs-Exotel Bridge v7 Running");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (exotelWs) => {
  console.log("=================================");
  console.log("Exotel connected at " + new Date().toISOString());
  console.log("=================================");

  let streamSid   = null;
  let elevenWs    = null;
  let elevenReady = false;
  let audioQueue  = [];

  // Outbound audio pacer — 160 bytes every 20ms = mulaw 8kHz real-time
  const CHUNK_BYTES = 160;
  const CHUNK_MS    = 20;
  let outBuffer  = Buffer.alloc(0);
  let pacerTimer = null;

  function startPacer() {
    if (pacerTimer) return;
    pacerTimer = setInterval(() => {
      if (outBuffer.length === 0) { stopPacer(); return; }
      const chunk = outBuffer.slice(0, CHUNK_BYTES);
      outBuffer   = outBuffer.slice(CHUNK_BYTES);
      if (exotelWs.readyState === WebSocket.OPEN) {
        exotelWs.send(JSON.stringify({
          event: "media", streamSid,
          media: { payload: chunk.toString("base64") }
        }));
      }
    }, CHUNK_MS);
  }

  function stopPacer() {
    if (pacerTimer) { clearInterval(pacerTimer); pacerTimer = null; }
  }

  function clearOutBuffer() {
    outBuffer = Buffer.alloc(0);
    stopPacer();
    if (exotelWs.readyState === WebSocket.OPEN) {
      exotelWs.send(JSON.stringify({ event: "clear", streamSid }));
    }
  }

  function connectToElevenLabs() {
    const url     = "wss://api.elevenlabs.io/v1/convai/conversation?agent_id=" + AGENT_ID;
    const headers = ELEVENLABS_API_KEY ? { "xi-api-key": ELEVENLABS_API_KEY } : {};
    elevenWs      = new WebSocket(url, { headers });

    elevenWs.on("open", () => {
      console.log("ElevenLabs WebSocket open");
      elevenWs.send(JSON.stringify({
        type: "conversation_initiation_client_data",
        conversation_config_override: {
          agent: { language: "en" },
          tts:   { optimize_streaming_latency: 3 }
        },
        audio: {
          input:  { encoding: "mulaw", sample_rate: 8000 },
          output: { encoding: "mulaw", sample_rate: 8000 }
        }
      }));
      console.log("Sent init — mulaw 8kHz both directions");
    });

    elevenWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        const t   = msg.type;

        if (t === "conversation_initiation_metadata") {
          console.log("ElevenLabs READY:", JSON.stringify(msg));
          elevenReady = true;
          audioQueue.forEach((c) => elevenWs.send(c));
          audioQueue = [];
        }

        if (t === "audio" && msg.audio_event && msg.audio_event.audio_base_64) {
          const raw = Buffer.from(msg.audio_event.audio_base_64, "base64");
          outBuffer = Buffer.concat([outBuffer, raw]);
          startPacer();
        }

        if (t === "interruption") {
          console.log("Interruption — clearing buffer");
          clearOutBuffer();
        }

        if (t === "ping" && msg.ping_event && msg.ping_event.event_id !== undefined) {
          elevenWs.send(JSON.stringify({ type: "pong", event_id: msg.ping_event.event_id }));
          console.log("Pong sent, event_id:", msg.ping_event.event_id);
        }

      } catch (e) {
        outBuffer = Buffer.concat([outBuffer, data]);
        startPacer();
      }
    });

    elevenWs.on("close", (code, reason) => {
      console.log("ElevenLabs disconnected:", code, reason.toString());
      stopPacer();
      if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
    });

    elevenWs.on("error", (err) => {
      console.error("[ElevenLabs ERROR]:", err.message);
      stopPacer();
      if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
    });
  }

  exotelWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.event === "start") {
        streamSid = (msg.start && msg.start.streamSid) ? msg.start.streamSid : "stream1";
        console.log("Stream started, SID:", streamSid);
        connectToElevenLabs();
      }

      if (msg.event === "media" && msg.media && msg.media.payload) {
        const audioMsg = JSON.stringify({ user_audio_chunk: msg.media.payload });
        if (elevenWs && elevenReady && elevenWs.readyState === WebSocket.OPEN) {
          elevenWs.send(audioMsg);
        } else {
          audioQueue.push(audioMsg);
          if (audioQueue.length > 100) audioQueue.shift();
        }
      }

      if (msg.event === "stop") {
        console.log("Exotel stream stopped");
        stopPacer();
        if (elevenWs && elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
      }

    } catch (e) {
      console.log("[Exotel] Non-JSON message");
    }
  });

  exotelWs.on("close", (code) => {
    console.log("Exotel disconnected, code:", code);
    stopPacer();
    if (elevenWs && elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
  });

  exotelWs.on("error", (err) => console.error("[Exotel ERROR]:", err.message));
});

server.listen(PORT, () => {
  console.log("Bridge v7 running on port " + PORT);
  console.log("Agent ID: " + AGENT_ID);
});
