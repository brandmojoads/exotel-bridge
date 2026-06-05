const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 8080;
const AGENT_ID = process.env.AGENT_ID || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";

function sampleToMulaw(s) {
  const BIAS = 0x84, CLIP = 32635;
  const sign = s < 0 ? 0x80 : 0;
  if (sign) s = -s;
  if (s > CLIP) s = CLIP;
  s += BIAS;
  let exp = 7;
  for (let e = 0, m = 0x4000; e < 8; e++, m >>= 1) {
    if (s & m) { exp = 7 - e; break; }
  }
  const mant = (s >> (exp + 3)) & 0x0f;
  return (~(sign | (exp << 4) | mant)) & 0xff;
}

function pcm16kToMulaw8k(b64) {
  const buf = Buffer.from(b64, "base64");
  const samples = new Int16Array(buf.buffer, buf.byteOffset, buf.length / 2);
  const out = Buffer.alloc(Math.floor(samples.length / 2));
  for (let i = 0; i < out.length; i++) {
    const avg = Math.round((samples[i * 2] + samples[i * 2 + 1]) / 2);
    out[i] = sampleToMulaw(avg);
  }
  return out.toString("base64");
}

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("ElevenLabs-Exotel Bridge v10 Running");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (exotelWs) => {
  console.log("=== Exotel connected " + new Date().toISOString() + " ===");

  let streamSid    = "default-stream";
  let elevenReady  = false;
  let inputQueue   = [];
  let outputFormat = "ulaw_8000";

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
          event: "media", streamSid,
          media: { payload: chunk.toString("base64") }
        }));
      }
    }, CHUNK_MS);
  }

  function stopPacer() {
    if (pacerTimer) { clearInterval(pacerTimer); pacerTimer = null; console.log("Pacer STOPPED"); }
  }

  function clearOutBuffer() {
    outBuffer = Buffer.alloc(0);
    stopPacer();
    if (exotelWs.readyState === WebSocket.OPEN)
      exotelWs.send(JSON.stringify({ event: "clear", streamSid }));
  }

  function handleElevenAudio(b64, format) {
    let mulawBuf;
    if (format === "ulaw_8000" || format === "mulaw_8000") {
      mulawBuf = Buffer.from(b64, "base64");
      console.log("Audio: mulaw8k passthrough", mulawBuf.length, "bytes");
    } else {
      mulawBuf = Buffer.from(pcm16kToMulaw8k(b64), "base64");
      console.log("Audio: pcm16k→mulaw8k converted", mulawBuf.length, "bytes");
    }
    outBuffer = Buffer.concat([outBuffer, mulawBuf]);
    startPacer();
  }

  const elevenWs = new WebSocket(
    "wss://api.elevenlabs.io/v1/convai/conversation?agent_id=" + AGENT_ID,
    { headers: ELEVENLABS_API_KEY ? { "xi-api-key": ELEVENLABS_API_KEY } : {} }
  );

  elevenWs.on("open", () => {
    console.log("ElevenLabs open — sending init");
    elevenWs.send(JSON.stringify({
      type: "conversation_initiation_client_data",
      conversation_config_override: {
        agent: { language: "en" },
        tts:   { optimize_streaming_latency: 0 }
      },
      audio: {
        input:  { encoding: "mulaw", sample_rate: 8000 },
        output: { encoding: "mulaw", sample_rate: 8000 }
      }
    }));
  });

  elevenWs.on("message", (data, isBinary) => {
    if (isBinary) {
  console.log("Ignoring binary frame:", data.length);
  return;
}    
    try {
      const msg = JSON.parse(data.toString());
      const t   = msg.type;
      console.log("[ElevenLabs]:", t);

      if (t === "conversation_initiation_metadata") {
        const meta = msg.conversation_initiation_metadata_event || {};
        outputFormat = meta.agent_output_audio_format || "ulaw_8000";
        console.log("*** Output format confirmed by ElevenLabs:", outputFormat, "***");
        elevenReady = true;
        inputQueue.forEach((c) => elevenWs.send(c));
        inputQueue = [];
      }

      if (t === "audio" && msg.audio_event && msg.audio_event.audio_base_64) {
        const size = Buffer.from(
    msg.audio_event.audio_base_64,
    "base64"
  ).length;

  console.log(
    "AUDIO FRAME:",
    outputFormat,
    "SIZE:",
    size
  );
        handleElevenAudio(
    msg.audio_event.audio_base_64,
    outputFormat
  );
}

      if (t === "interruption") { clearOutBuffer(); }

      if (t === "ping" && msg.ping_event?.event_id !== undefined) {
        elevenWs.send(JSON.stringify({ type: "pong", event_id: msg.ping_event.event_id }));
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
        console.log("[Exotel] streamSid:", streamSid);
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

server.listen(PORT, () => {
  console.log("Bridge v10 running on port " + PORT);
  console.log("Agent ID: " + AGENT_ID);
});
