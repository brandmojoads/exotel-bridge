const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 8080;
const AGENT_ID = process.env.AGENT_ID || "";
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY || "";

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
          agent: {
            language: "en"
          },
          tts: {
            optimize_streaming_latency: 3
          }
        }
      };
      elevenWs.send(JSON.stringify(initMsg));
      console.log("Sent init message to ElevenLabs");
    });

    elevenWs.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        console.log("[ElevenLabs MSG type]:", msg.type);

        if (msg.type === "conversation_initiation_metadata") {
          elevenReady = true;
          console.log("ElevenLabs READY - flushing queued audio");
          audioQueue.forEach((chunk) => {
            elevenWs.send(chunk);
          });
          audioQueue = [];
        }

        if (msg.type === "audio" && msg.audio_event && msg.audio_event.audio_base_64) {
          if (exotelWs.readyState === WebSocket.OPEN) {
            const exotelMsg = {
              event: "media",
              streamSid: streamSid,
              media: {
                payload: msg.audio_event.audio_base_64
              }
            };
            exotelWs.send(JSON.stringify(exotelMsg));
          }
        }

        if (msg.type === "agent_response_correction" || msg.type === "interruption") {
          if (exotelWs.readyState === WebSocket.OPEN) {
            exotelWs.send(JSON.stringify({ event: "clear", streamSid: streamSid }));
          }
        }

      } catch (e) {
        if (exotelWs.readyState === WebSocket.OPEN) {
          exotelWs.send(data);
        }
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
      console.log("[Exotel MSG event]:", msg.event);

      if (msg.event === "start") {
        streamSid = msg.start && msg.start.streamSid ? msg.start.streamSid : "stream1";
        console.log("Stream started, SID:", streamSid);
        connectToElevenLabs();
      }

      if (msg.event === "media" && msg.media && msg.media.payload) {
        const audioMsg = {
          user_audio_chunk: msg.media.payload
        };
        const audioStr = JSON.stringify(audioMsg);

        if (elevenWs && elevenReady && elevenWs.readyState === WebSocket.OPEN) {
          elevenWs.send(audioStr);
        } else {
          audioQueue.push(audioStr);
          if (audioQueue.length > 50) audioQueue.shift();
        }
      }

      if (msg.event === "stop") {
        console.log("Exotel stream stopped");
        if (elevenWs && elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
      }

    } catch (e) {
      console.log("[Exotel] Non-JSON message received");
    }
  });

  exotelWs.on("close", (code) => {
    console.log("Exotel disconnected, code:", code);
    if (elevenWs && elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
  });

  exotelWs.on("error", (err) => {
    console.error("[Exotel ERROR]:", err.message);
  });
});

server.listen(PORT, () => {
  console.log("Bridge running on port " + PORT);
  console.log("Agent ID: " + AGENT_ID);
});
