const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 8080;
const AGENT_ID = process.env.AGENT_ID || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";

// mulaw decode table
const MULAW_DECODE_TABLE = (() => {
  const table = new Int16Array(256);
  for (let i = 0; i < 256; i++) {
    let x = ~i;
    const sign = x & 0x80 ? -1 : 1;
    const exponent = (x >> 4) & 0x07;
    const mantissa = x & 0x0f;
    let magnitude = ((mantissa << 1) + 33) << exponent;
    table[i] = sign * (magnitude - 33);
  }
  return table;
})();

// Convert mulaw 8kHz base64 → PCM 16kHz base64
function mulawToLinear16(mulawBase64) {
  const mulawBytes = Buffer.from(mulawBase64, "base64");
  const pcm8k = new Int16Array(mulawBytes.length);
  for (let i = 0; i < mulawBytes.length; i++) {
    pcm8k[i] = MULAW_DECODE_TABLE[mulawBytes[i]];
  }
  const pcm16k = new Int16Array(pcm8k.length * 2);
  for (let i = 0; i < pcm8k.length - 1; i++) {
    pcm16k[i * 2] = pcm8k[i];
    pcm16k[i * 2 + 1] = Math.round((pcm8k[i] + pcm8k[i + 1]) / 2);
  }
  pcm16k[(pcm8k.length - 1) * 2] = pcm8k[pcm8k.length - 1];
  pcm16k[(pcm8k.length - 1) * 2 + 1] = pcm8k[pcm8k.length - 1];
  return Buffer.from(pcm16k.buffer).toString("base64");
}

// Convert PCM 16kHz base64 → mulaw 8kHz base64
function linearToMulaw(sample) {
  const BIAS = 0x84;
  const CLIP = 32635;
  const sign = sample < 0 ? 0x80 : 0;
  if (sign) sample = -sample;
  if (sample > CLIP) sample = CLIP;
  sample += BIAS;
  let exponent = 7;
  for (let exp = 0, mask = 0x4000; exp < 8; exp++, mask >>= 1) {
    if (sample & mask) { exponent = 7 - exp; break; }
  }
  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  return ~(sign | (exponent << 4) | mantissa) & 0xff;
}

function linear16ToMulaw(pcmBase64) {
  const pcmBytes = Buffer.from(pcmBase64, "base64");
  const samples = new Int16Array(pcmBytes.buffer, pcmBytes.byteOffset, pcmBytes.length / 2);
  const mulaw = Buffer.alloc(Math.floor(samples.length / 2));
  for (let i = 0; i < mulaw.length; i++) {
    mulaw[i] = linearToMulaw(samples[i * 2]);
  }
  return mulaw.toString("base64");
}

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("ElevenLabs-Exotel Bridge Running");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (exotelWs) => {
  console.log("=================================");
  console.log("Exotel connected at " + new Date().toISOString());
  console.log("=================================");

  let streamSid = null;
  let elevenWs = null;
  let elevenReady = false;
  let audioQueue = [];

  function connectToElevenLabs() {
    const url = "wss://api.elevenlabs.io/v1/convai/conversation?agent_id=" + AGENT_ID;
    const headers = ELEVENLABS_API_KEY ? { "xi-api-key": ELEVENLABS_API_KEY } : {};
    elevenWs = new WebSocket(url, { headers });

    elevenWs.on("open", () => {
      console.log("ElevenLabs WebSocket open");
      const initMsg = {
        type: "conversation_initiation_client_data",
        conversation_config_override: {
          agent: { language: "en" },
          tts: { optimize_streaming_latency: 3 }
        }
      };
      elevenWs.send(JSON.stringify(initMsg));
      console.log("Sent init message to ElevenLabs");
    });

    elevenWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.type === "conversation_initiation_metadata") {
          elevenReady = true;
          console.log("ElevenLabs READY - flushing", audioQueue.length, "queued chunks");
          audioQueue.forEach((chunk) => elevenWs.send(chunk));
          audioQueue = [];
        }

        if (msg.type === "audio" && msg.audio_event && msg.audio_event.audio_base_64) {
          if (exotelWs.readyState === WebSocket.OPEN) {
            const mulawAudio = linear16ToMulaw(msg.audio_event.audio_base_64);
            const exotelMsg = {
              event: "media",
              streamSid: streamSid,
              media: { payload: mulawAudio }
            };
            exotelWs.send(JSON.stringify(exotelMsg));
          }
        }

        if (msg.type === "interruption") {
          if (exotelWs.readyState === WebSocket.OPEN) {
            exotelWs.send(JSON.stringify({ event: "clear", streamSid: streamSid }));
          }
        }

        if (msg.type === "ping" && msg.ping_event) {
          elevenWs.send(JSON.stringify({
            type: "pong",
            pong_event: { event_id: msg.ping_event.event_id }
          }));
        }

      } catch (e) {
        if (exotelWs.readyState === WebSocket.OPEN) exotelWs.send(data);
      }
    });

    elevenWs.on("close", (code, reason) => {
      console.log("ElevenLabs disconnected:", code, reason.toString());
      if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
    });

    elevenWs.on("error", (err) => {
      console.error("[ElevenLabs ERROR]:", err.message);
      if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
    });
  }

  exotelWs.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.event === "start") {
        streamSid = msg.start && msg.start.streamSid ? msg.start.streamSid : "stream1";
        console.log("Stream started, SID:", streamSid);
        connectToElevenLabs();
      }

      if (msg.event === "media" && msg.media && msg.media.payload) {
        const pcm16Audio = mulawToLinear16(msg.media.payload);
        const audioMsg = JSON.stringify({ user_audio_chunk: pcm16Audio });

        if (elevenWs && elevenReady && elevenWs.readyState === WebSocket.OPEN) {
          elevenWs.send(audioMsg);
        } else {
          audioQueue.push(audioMsg);
          if (audioQueue.length > 100) audioQueue.shift();
        }
      }

      if (msg.event === "stop") {
        console.log("Exotel stream stopped");
        if (elevenWs && elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
      }

    } catch (e) {
      console.log("[Exotel] Non-JSON message");
    }
  });

  exotelWs.on("close", (code) => {
    console.log("Exotel disconnected, code:", code);
    if (elevenWs && elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
  });

  exotelWs.on("error", (err) => console.error("[Exotel ERROR]:", err.message));
});

server.listen(PORT, () => {
  console.log("Bridge running on port " + PORT);
  console.log("Agent ID: " + AGENT_ID);
});
