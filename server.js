// ============================================================
//  ZONA CERO 3D — Servidor Multijugador en la Nube (Render)
//  Tecnología: Node.js + http + ws (WebSocket)
//  Soporte Multi-Salas (Room Codes) + Sistema Revivir Co-Op
// ============================================================

'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8765;
const MAX_PLAYERS_PER_ROOM = 6;
const TICK_MS = 50;   // broadcast de world-state cada 50ms (20fps)

// Estructura de Salas: Map<roomCode, Room>
// Room = { code, hostId, players: Map<id, playerState>, killedZombies: Set, createdAt: number }
const rooms = new Map();
const socketToRoom = new Map(); // ws -> { roomId, playerId }

// Generador de códigos de sala limpios de 4 caracteres alfanuméricos (sin caracteres confusos como 0/O, 1/I)
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function generateRoomCode() {
  let code = '';
  for (let i = 0; i < 4; i++) {
    code += CODE_CHARS.charAt(Math.floor(Math.random() * CODE_CHARS.length));
  }
  return rooms.has(code) ? generateRoomCode() : code;
}

function uid() {
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) { // 1 = WebSocket.OPEN
    try { ws.send(JSON.stringify(obj)); } catch(_) {}
  }
}

function getOrCreateRoom(code) {
  const c = String(code || 'GLOBAL').trim().toUpperCase();
  if (!rooms.has(c)) {
    rooms.set(c, {
      code: c,
      hostId: null,
      players: new Map(),
      killedZombies: new Set(),
      createdAt: Date.now()
    });
    console.log(`[Sala Creada] Código: ${c}`);
  }
  return rooms.get(c);
}

function broadcastRoom(room, obj, excludeId = null) {
  if (!room) return;
  const msg = JSON.stringify(obj);
  for (const [id, p] of room.players) {
    if (id !== excludeId && p.ws.readyState === 1) {
      try { p.ws.send(msg); } catch(_) {}
    }
  }
}

function promoteNextHost(room) {
  if (!room) return;
  const first = room.players.keys().next().value;
  if (first) {
    room.hostId = first;
    const hPlayer = room.players.get(first);
    console.log(`[Host] [${room.code}] Nuevo host asignado: ${hPlayer ? hPlayer.name : ''} (${first})`);
    broadcastRoom(room, { t: 'host', id: first });
  } else {
    room.hostId = null;
    // Si la sala queda vacía y no es la global, eliminarla después de un tiempo
    if (room.code !== 'GLOBAL') {
      rooms.delete(room.code);
      console.log(`[Sala Cerrada] Código: ${room.code} (vacía)`);
    }
  }
}

// Servidor HTTP para Render (Health check / keep-alive)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  let totalPlayers = 0;
  for (const [, r] of rooms) totalPlayers += r.players.size;
  res.end(`Zona Cero WebSocket Server activo.\nSalas activas: ${rooms.size}\nJugadores totales: ${totalPlayers}`);
});

const wss = new WebSocketServer({ server });

// ── Tick de broadcast (World State) por sala ─────────────────
setInterval(() => {
  for (const [code, room] of rooms) {
    if (room.players.size === 0) continue;

    const worldPlayers = {};
    for (const [id, p] of room.players) {
      if (!p.hasJoined) continue;
      worldPlayers[id] = {
        x:       p.x,
        z:       p.z,
        y:       p.y,
        yaw:     p.yaw,
        pitch:   p.pitch,
        hp:      p.hp,
        downed:  !!p.downed,
        anim:    p.anim,
        wpn:     p.wpn,
        name:    p.name,
      };
    }

    broadcastRoom(room, { t: 'world', players: worldPlayers, ts: Date.now() });
  }
}, TICK_MS);

// ── Ping / Keep-alive para evitar desconexiones en móviles ────
const pingInterval = setInterval(() => {
  for (const [, room] of rooms) {
    for (const [id, p] of room.players) {
      if (p.isAlive === false) {
        console.log(`[Heartbeat] Cerrando conexión inactiva: ${p.name} (${id})`);
        p.ws.terminate();
        room.players.delete(id);
        if (id === room.hostId) promoteNextHost(room);
        continue;
      }
      p.isAlive = false;
      try { p.ws.ping(); } catch(_) {}
    }
  }
}, 25000);

wss.on('close', () => clearInterval(pingInterval));

// ── Conexiones WebSocket ──────────────────────────────────────
wss.on('connection', (ws) => {
  const id = uid();
  let currentRoom = null;
  let playerName = `Soldado_${id.slice(0, 4)}`;

  const playerState = {
    ws,
    name:       playerName,
    x: 0, z: 0, y: 0,
    yaw: 0, pitch: 0,
    hp:         100,
    downed:     false,
    anim:       'idle',
    wpn:        0,
    hasJoined:  false,
    isAlive:    true,
  };

  ws.on('pong', () => { playerState.isAlive = true; });

  ws.on('message', (raw) => {
    playerState.isAlive = true;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.t) {
      // Unirse o crear una sala
      case 'join': {
        const name = String(msg.name || playerName).trim().slice(0, 18) || 'Soldado';
        const roomCode = String(msg.room || msg.code || (msg.create ? generateRoomCode() : 'GLOBAL')).trim().toUpperCase();

        currentRoom = getOrCreateRoom(roomCode);

        if (currentRoom.players.size >= MAX_PLAYERS_PER_ROOM) {
          send(ws, { t: 'error', msg: `La sala ${roomCode} está llena (máx. ${MAX_PLAYERS_PER_ROOM} jugadores)` });
          return;
        }

        playerState.name = name;
        playerState.hasJoined = true;
        playerName = name;
        currentRoom.players.set(id, playerState);
        socketToRoom.set(ws, { roomId: currentRoom.code, playerId: id });

        if (!currentRoom.hostId || !currentRoom.players.has(currentRoom.hostId)) {
          currentRoom.hostId = id;
        }

        const existingPlayers = [];
        for (const [pid, p] of currentRoom.players) {
          if (pid !== id && p.hasJoined) {
            existingPlayers.push({ id: pid, name: p.name, hp: p.hp, downed: !!p.downed });
          }
        }

        send(ws, {
          t:        'welcome',
          id,
          room:     currentRoom.code,
          isHost:   id === currentRoom.hostId,
          players:  existingPlayers,
        });

        broadcastRoom(currentRoom, { t: 'join', id, name, room: currentRoom.code }, id);
        console.log(`[Join] [${currentRoom.code}] ${name} (${id}) | Host: ${id === currentRoom.hostId} | Total: ${currentRoom.players.size}`);
        break;
      }

      case 'startGame': {
        if (!currentRoom) return;
        if (!currentRoom.hostId || !currentRoom.players.has(currentRoom.hostId)) currentRoom.hostId = id;
        broadcastRoom(currentRoom, { t: 'startGame' }, id);
        console.log(`[StartGame] [${currentRoom.code}] Partida iniciada por ${playerName} (${id})`);
        break;
      }

      case 'p': {
        playerState.x      = msg.x      ?? playerState.x;
        playerState.z      = msg.z      ?? playerState.z;
        playerState.y      = msg.y      ?? playerState.y;
        playerState.yaw    = msg.yaw    ?? playerState.yaw;
        playerState.pitch  = msg.pitch  ?? playerState.pitch;
        playerState.hp     = msg.hp     ?? playerState.hp;
        playerState.downed = !!msg.downed;
        playerState.anim   = msg.anim   ?? playerState.anim;
        playerState.wpn    = msg.wpn    ?? playerState.wpn;
        break;
      }

      // Caída de compañero (Downed state)
      case 'downed': {
        if (!currentRoom) return;
        playerState.downed = true;
        playerState.hp = 0;
        broadcastRoom(currentRoom, { t: 'downed', id, name: playerName }, id);
        console.log(`[Downed] [${currentRoom.code}] ${playerName} ha caído en combate`);
        break;
      }

      // Revivir a compañero
      case 'revive': {
        if (!currentRoom) return;
        const targetId = msg.targetId;
        const targetPlayer = currentRoom.players.get(targetId);
        if (targetPlayer) {
          targetPlayer.downed = false;
          targetPlayer.hp = 40;
          broadcastRoom(currentRoom, { t: 'revive', targetId, reviverId: id, reviverName: playerName });
          console.log(`[Revive] [${currentRoom.code}] ${playerName} revivió a ${targetPlayer.name}`);
        }
        break;
      }

      // Marcador táctico (Ping 3D en el mapa)
      case 'ping': {
        if (!currentRoom) return;
        broadcastRoom(currentRoom, { t: 'ping', id, name: playerName, x: msg.x, z: msg.z, pingType: msg.pingType || 'alert' }, id);
        break;
      }

      case 'zkill': {
        if (!currentRoom) return;
        const zid = msg.zid;
        if (zid === undefined) break;
        if (!currentRoom.killedZombies.has(zid)) {
          currentRoom.killedZombies.add(zid);
          broadcastRoom(currentRoom, { t: 'zkill', zid, killerId: id });
        }
        break;
      }

      case 'zspawn': {
        if (!currentRoom) return;
        const { zid, type, x, z } = msg;
        broadcastRoom(currentRoom, { t: 'zspawn', zid, type, x, z }, id);
        break;
      }

      case 'zpos': {
        if (!currentRoom || id !== currentRoom.hostId) return;
        if (msg.list) broadcastRoom(currentRoom, { t: 'zpos', list: msg.list }, id);
        break;
      }

      case 'wave': {
        if (!currentRoom) return;
        const n = Number(msg.n);
        currentRoom.killedZombies.clear();
        broadcastRoom(currentRoom, { t: 'wave', n });
        console.log(`[Wave] [${currentRoom.code}] Oleada ${n} iniciada`);
        break;
      }

      case 'waveEnd': {
        if (!currentRoom) return;
        broadcastRoom(currentRoom, { t: 'waveEnd', wave: msg.wave, bonus: msg.bonus });
        break;
      }

      case 'shoot': {
        if (!currentRoom) return;
        broadcastRoom(currentRoom, { t: 'shoot', id, zid: msg.zid, dmg: msg.dmg }, id);
        break;
      }

      case 'shootAnim': {
        if (!currentRoom) return;
        broadcastRoom(currentRoom, { t: 'shootAnim', id, wpn: msg.wpn, yaw: msg.yaw, pitch: msg.pitch }, id);
        break;
      }

      case 'chat': {
        if (!currentRoom) return;
        const chatMsg = String(msg.msg || '').slice(0, 256);
        broadcastRoom(currentRoom, { t: 'chat', id, name: playerName, msg: chatMsg });
        break;
      }

      default:
        break;
    }
  });

  const handleDisconnect = () => {
    socketToRoom.delete(ws);
    if (!currentRoom || !currentRoom.players.has(id)) return;
    currentRoom.players.delete(id);
    console.log(`[-] [${currentRoom.code}] Desconectado: ${playerName} (${id}) | Restantes: ${currentRoom.players.size}`);
    broadcastRoom(currentRoom, { t: 'leave', id });

    if (id === currentRoom.hostId) {
      currentRoom.hostId = null;
      promoteNextHost(currentRoom);
    }
  };

  ws.on('close', handleDisconnect);
  ws.on('error', (err) => {
    console.error(`[Error] ${playerName} (${id}): ${err.message}`);
    handleDisconnect();
  });
});

server.listen(PORT, () => {
  console.log(`Servidor multijugador Zona Cero activo en puerto ${PORT}`);
});
