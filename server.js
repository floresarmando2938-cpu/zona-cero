// ============================================================
//  ZONA CERO 3D — Servidor Multijugador en la Nube (Render)
//  Tecnología: Node.js + http + ws (WebSocket)
// ============================================================

'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8765;
const MAX_PLAYERS = 8;
const TICK_MS = 50;   // broadcast de world-state cada 50ms

// Servidor HTTP para Render (Health check / keep-alive)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Zona Cero WebSocket server is running');
});

// Servidor WebSocket montado sobre el servidor HTTP
const wss = new WebSocketServer({ server });

/**
 * players: Map<id, { ws, name, x, z, y, yaw, pitch, hp, anim, wpn, ready }>
 */
const players = new Map();
const killedZombies = new Set();
let hostId = null;

function uid() {
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch(_) {}
  }
}

function broadcast(obj, excludeId = null) {
  const msg = JSON.stringify(obj);
  for (const [id, p] of players) {
    if (id !== excludeId && p.ws.readyState === p.ws.OPEN) {
      try { p.ws.send(msg); } catch(_) {}
    }
  }
}

function broadcastAll(obj) {
  broadcast(obj, null);
}

function promoteNextHost() {
  const first = players.keys().next().value;
  if (first && first !== hostId) {
    hostId = first;
    console.log(`[Host] Nuevo host: ${players.get(hostId).name} (${hostId})`);
    broadcastAll({ t: 'host', id: hostId });
  }
}

// ── Tick de broadcast (World State) ─────────────────────────
setInterval(() => {
  if (players.size === 0) return;

  const worldPlayers = {};
  for (const [id, p] of players) {
    worldPlayers[id] = {
      x:     p.x,
      z:     p.z,
      y:     p.y,
      yaw:   p.yaw,
      pitch: p.pitch,
      hp:    p.hp,
      anim:  p.anim,
      wpn:   p.wpn,
      name:  p.name,
    };
  }

  broadcastAll({ t: 'world', players: worldPlayers, ts: Date.now() });
}, TICK_MS);

// ── Conexiones WebSocket ─────────────────────────────────────
wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) {
    send(ws, { t: 'error', msg: 'Servidor lleno (máx. 8 jugadores)' });
    ws.close();
    return;
  }

  const id = uid();
  let playerName = `Jugador_${id.slice(0, 4)}`;

  const playerState = {
    ws,
    name:  playerName,
    x: 0, z: 0, y: 0,
    yaw: 0, pitch: 0,
    hp:   100,
    anim: 'idle',
    wpn:  0,
    ready: false,
  };
  players.set(id, playerState);

  if (players.size === 1) {
    hostId = id;
    console.log(`[Host] Host inicial: (${id})`);
  }

  console.log(`[+] Conexión establecida (${id}) | Total: ${players.size}`);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    switch (msg.t) {
      case 'join': {
        const name = String(msg.name || playerName).slice(0, 32);
        playerState.name = name;
        playerName = name;

        const existingPlayers = [];
        for (const [pid, p] of players) {
          if (pid !== id) {
            existingPlayers.push({ id: pid, name: p.name, hp: p.hp });
          }
        }
        send(ws, {
          t:       'welcome',
          id,
          isHost:  id === hostId,
          players: existingPlayers,
        });

        broadcast({ t: 'join', id, name }, id);
        console.log(`[Join] ${name} (${id}) | Host: ${id === hostId}`);
        break;
      }

      case 'startGame': {
        if (id === hostId) {
          broadcast({ t: 'startGame' }, id);
          console.log(`[StartGame] Host inició la partida`);
        }
        break;
      }

      case 'p': {
        playerState.x     = msg.x     ?? playerState.x;
        playerState.z     = msg.z     ?? playerState.z;
        playerState.y     = msg.y     ?? playerState.y;
        playerState.yaw   = msg.yaw   ?? playerState.yaw;
        playerState.pitch = msg.pitch ?? playerState.pitch;
        playerState.hp    = msg.hp    ?? playerState.hp;
        playerState.anim  = msg.anim  ?? playerState.anim;
        playerState.wpn   = msg.wpn   ?? playerState.wpn;
        break;
      }

      case 'zkill': {
        const zid = msg.zid;
        if (zid === undefined) break;
        if (!killedZombies.has(zid)) {
          killedZombies.add(zid);
          broadcastAll({ t: 'zkill', zid, killerId: id });
          console.log(`[ZKill] Zombie ${zid} eliminado por ${playerName}`);
        }
        break;
      }

      case 'zspawn': {
        if (id !== hostId) break;
        const { zid, type, x, z } = msg;
        broadcast({ t: 'zspawn', zid, type, x, z }, id);
        break;
      }

      case 'wave': {
        if (id !== hostId) break;
        const n = Number(msg.n);
        killedZombies.clear();
        broadcastAll({ t: 'wave', n });
        console.log(`[Wave] Oleada ${n} iniciada`);
        break;
      }

      case 'shoot': {
        broadcast({ t: 'shoot', id, zid: msg.zid, dmg: msg.dmg }, id);
        break;
      }

      case 'chat': {
        const chatMsg = String(msg.msg || '').slice(0, 256);
        broadcastAll({ t: 'chat', id, name: playerName, msg: chatMsg });
        console.log(`[Chat] ${playerName}: ${chatMsg}`);
        break;
      }

      case 'ready': {
        playerState.ready = true;
        let allReady = true;
        for (const [, p] of players) {
          if (!p.ready) { allReady = false; break; }
        }
        if (allReady && players.size > 0) {
          broadcastAll({ t: 'allReady' });
          console.log('[Lobby] Todos listos');
        }
        break;
      }

      default:
        break;
    }
  });

  const handleDisconnect = () => {
    if (!players.has(id)) return;
    players.delete(id);
    console.log(`[-] Desconectado: ${playerName} (${id}) | Restantes: ${players.size}`);
    broadcast({ t: 'leave', id });

    if (id === hostId) {
      hostId = null;
      promoteNextHost();
    }
  };

  ws.on('close', handleDisconnect);
  ws.on('error', (err) => {
    console.error(`[Error] ${playerName} (${id}): ${err.message}`);
    handleDisconnect();
  });
});

server.listen(PORT, () => {
  console.log(`Servidor LAN activo en puerto ${PORT}`);
});
