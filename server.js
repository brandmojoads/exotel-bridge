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

  const elevenWs = new WebSocket(
    "wss://api.elevenlabs.io/v1/convai/conversation?agent_id=" + AGENT_ID,
    {
      headers: ELEVENLABS_API_KEY ? { "xi-api-key": ELEVENLABS_API_KEY } : {},
    }
  );

  elevenWs.on("open", () => {
    console.log("ElevenLabs connected - bridge is LIVE");
  });

  exotelWs.on("message", (data) => {
    if (elevenWs.readyState === WebSocket.OPEN) {
      elevenWs.send(data);
    }
  });

  elevenWs.on("message", (data) => {
    if (exotelWs.readyState === WebSocket.OPEN) {
      exotelWs.send(data);
    }
  });

  exotelWs.on("close", () => {
    console.log("Exotel disconnected");
    if (elevenWs.readyState === WebSocket.OPEN) elevenWs.close();
  });

  elevenWs.on("close", () => {
    console.log("ElevenLabs disconnected");
    if (exotelWs.readyState === WebSocket.OPEN) exotelWs.close();
  });

  exotelWs.on("error", (err) => console.error("Exotel error:", err.message));
  elevenWs.on("error", (err) => console.error("ElevenLabs error:", err.message));
});

server.listen(PORT, () => {
  console.log("Bridge running on port " + PORT);
  console.log("Agent ID: " + AGENT_ID);
});
