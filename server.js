const WebSocket = require("ws");
const http = require("http");
const url = require("url");

const PORT = process.env.PORT || 8080;
const AGENT_ID = process.env.AGENT_ID || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";
const ELEVENLABS_WS_URL = "wss://api.elevenlabs.io/v1/convai/conversation";

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("ElevenLabs-Exotel Bridge v11 Running");
});

const wss = new WebSocket.Server({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const parsed   = url.parse(req.url, true);
  const pathname = parsed.pathname;

  if (
    pathname === "/v1/convai/conversation/exotel" ||
    pathname === "/" ||
    pathname === ""
  ) {
    const agentId = parsed.query.agent_id || AGENT_ID;
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req, agentId);
    });
  } else {
    console.log("Rejected unknown path:", pathname);
    socket.destroy();
  }
});

wss.on("connection", (exotelWs, req, agentId) => {
  console.log("=== Exotel connected " + new Date().toISOString() + " ===");
  console.log("Agent ID:", agentId);

  let streamSid   = "default-stream";
  let elevenReady = false;
  let inputQueue  = [];

  const CHUNK_BYTES = 160;
  const CHUNK_MS    = 20;
  let outBuffer  = Buffer.alloc(0);
  let pacerTimer = null;

  function startPacer() {
    if (pacerTimer) return;
    console.log("Pacer STARTED, buffer:", outBuffer.length, "bytes");
    pacerTimer = setInterval(() => {
      if (outBuffer.length === 0) { stopPacer(); return; }
      const chunk = outBuffer.slice(0, CHUNK_BYTES);
      outBuffer   = outBuffer.slice(CHUNK_BYTES);
      if (exotelWs.readyState === WebSocket.OPEN) {
        exotelWs.send(JSON.stringify({
          event:     "media",
          streamSid: streamSid,
          media:     { payload: chunk.toString("base64") }
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
    if (exotelWs.readyState === WebSocket.OPEN)
      exotelWs.send(JSON.stringify({ event: "clear", streamSid }));
  }

  const elevenUrl = ELEVENLABS_WS_URL + "?agent_id=" + agentId;
  const elevenWs  = new WebSocket(elevenUrl, {
    headers: ELEVENLABS_API_KEY ? { "xi-api-key": ELEVENLABS_API_KEY } : {}
  });

  elevenWs.on("open", () => {
    console.log("ElevenLabs connected — sending init");
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
  });

  elevenWs.on("message", (data, isBinary) => {
    if (isBinary) {
      outBuffer = Buffer.concat([outBuffer, data]);
      startPacer();
      return;
    }
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === "conversation_initiation_metadata") {
        const meta = msg.conversation_initiation_metadata_event || {};
        console.log("ElevenLabs READY | output_format:", meta.agent_output_audio_format);
        elevenReady = true;
        inputQueue.forEach((c) => elevenWs.send(c));
        inputQueue = [];
      }

      if (msg.type === "audio" && msg.audio_event?.audio_base_64) {
        const raw = Buffer.from(msg.audio_event.audio_base_64, "base64");
        outBuffer = Buffer.concat([outBuffer, raw]);
        startPacer();
      }

      if (msg.type === "interruption") {
        clearOutBuffer();
      }

      if (msg.type === "ping" && msg.ping_event?.event_id !== undefined) {
        elevenWs.send(JSON.stringify({
          type:     "pong",
          event_id: msg.ping_event.event_id
        }));
      }

    } catch (e) {
      console.log("[ElevenLabs] parse error:", e.message);
    }
  });

  elevenWs.on("close", (code, reason) => {
    console.log("ElevenLabs closed:", code, reason.toString());
    stopPacer();
    if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
  });

  elevenWs.on("error", (err) => {
    console.error("[ElevenLabs ERROR]:", err.message);
    stopPacer();
    if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
  });

  exotelWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.event === "start" && msg.start?.streamSid) {
        streamSid = msg.start.streamSid;
        console.log("streamSid:", streamSid);
      }

      if (msg.event === "media" && msg.media?.payload) {
        const chunk = JSON.stringify({ user_audio_chunk: msg.media.payload });
        if (elevenReady && elevenWs.readyState === WebSocket.OPEN) {
          elevenWs.send(chunk);
        } else {
          inputQueue.push(chunk);
          if (inputQueue.length > 100) inputQueue.shift();
        }
      }

      if (msg.event === "stop") {
        console.log("Exotel stop");
        stopPacer();
        if (elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
      }

    } catch (e) {}
  });

  exotelWs.on("close", (code) => {
    console.log("Exotel closed:", code);
    stopPacer();
    if (elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
  });

  exotelWs.on("error", (err) => console.error("[Exotel ERROR]:", err.message));
});

// Keep-alive ping to prevent Render free tier sleep
if (process.env.RENDER_KEEP_ALIVE) {
  const https = require("https");
  setInterval(() => {
    https.get("https://exotel-bridge-1.onrender.com").on("error", () => {});
  }, 14 * 60 * 1000); // ping every 14 minutes
}

server.listen(PORT, () => {
  console.log("Bridge v11 running on port " + PORT);
  console.log("Agent ID:", AGENT_ID);
});
