const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 8080;
const AGENT_ID =
process.env.AGENT_ID ||
"agent_8301kswvnygdfp9sg1t5jx5txbp0";

const ELEVENLABS_API_KEY =
process.env.ELEVENLABS_API_KEY || "";

const server = http.createServer((req, res) => {
res.writeHead(200);
res.end("ElevenLabs-Exotel Bridge Running");
});

const wss = new WebSocket.Server({ server });

wss.on("connection", (exotelWs) => {
console.log("=================================");
console.log("Exotel connected");
console.log("=================================");

const elevenWs = new WebSocket(
`wss://api.elevenlabs.io/v1/convai/conversation?agent_id=${AGENT_ID}`,
{
headers: ELEVENLABS_API_KEY
? { "xi-api-key": ELEVENLABS_API_KEY }
: {},
}
);

elevenWs.on("open", () => {
console.log("ElevenLabs connected");
});

// Exotel → ElevenLabs
exotelWs.on("message", (data) => {
try {
console.log("---------- EXOTEL MESSAGE ----------");
console.log(data.toString());
console.log("------------------------------------");
} catch (e) {
console.log("Exotel binary message received");
}

```
// TEMPORARILY DISABLED
// elevenWs.send(data);
```

});

// ElevenLabs → Exotel
elevenWs.on("message", (data) => {
try {
console.log("---------- ELEVEN MESSAGE ----------");
console.log(data.toString());
console.log("------------------------------------");
} catch (e) {
console.log("ElevenLabs binary message received");
}
});

exotelWs.on("close", () => {
console.log("Exotel disconnected");
elevenWs.close();
});

elevenWs.on("close", () => {
console.log("ElevenLabs disconnected");
});

exotelWs.on("error", (err) => {
console.error("Exotel error:", err);
});

elevenWs.on("error", (err) => {
console.error("ElevenLabs error:", err);
});
});

server.listen(PORT, () => {
console.log(`Bridge running on port ${PORT}`);
});
