// server.js - WebSocket server for Zona Cero multiplayer
// Listens on the port provided by the environment (for cloud hosts) or 8765 locally.
// Uses the 'ws' library (npm install ws).

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8765;

// Simple HTTP server to keep the process alive on platforms like Render that expect an HTTP endpoint.
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Zona Cero WebSocket server is running');
});

const wss = new WebSocket.Server({ server });

// Store connected clients
const clients = new Set();

wss.on('connection', (ws) => {
  console.log('New client connected');
  clients.add(ws);

  ws.on('message', (message) => {
    // Broadcast received messages to all other clients
    for (const client of clients) {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

server.listen(PORT, () => {
  console.log(`Servidor LAN activo en puerto ${PORT}`);
});
