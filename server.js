// ============================================================
//  ZONA CERO 3D — Servidor Multijugador en la Nube (Render)
//  Tecnología: Node.js + http + ws (WebSocket)
//  Multi-Salas (Códigos 4 caracteres) + Salas Públicas/Privadas
//  Modos (Campaña Nivel 10 / Infinita) + 4 Dificultades
//  Victoria Sincronizada (Votación Reiniciar / Seguir) + Host Migration
//  Revivir + Pings 3D + Suministros + Scoreboard MVP
// ============================================================

'use strict';

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8765;
const MAX_PLAYERS_PER_ROOM = 4; // Límite cooperativo: 4 jugadores
const TICK_MS = 50;            // broadcast de world-state cada 50ms (20fps)

// Estructura de Salas: Map<roomCode, Room>
// Room = {
//   code: string,
//   name: string,
//   isPrivate: boolean,
//   mode: 'campaign' | 'survival',
//   difficulty: 'easy' | 'normal' | 'hard' | 'extreme',
//   hostId: string,
//   players: Map<id, playerState>,
//   killedZombies: Set,
//   sharedDrops: Map,
//   stats: Map,
//   victoryVotes: Map<id, 'restart' | 'continue'>,
//   createdAt: number
// }
const rooms = new Map();
const socketToRoom = new Map(); // ws -> { roomId, playerId }

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

function getOrCreateRoom(code, meta = {}) {
  const c = String(code || 'GLOBAL').trim().toUpperCase();
  if (!rooms.has(c)) {
    rooms.set(c, {
      code: c,
      name: (meta.name && String(meta.name).trim().slice(0, 20)) || `Sala ${c}`,
      isPrivate: meta.isPrivate !== undefined ? !!meta.isPrivate : true,
      mode: meta.mode === 'survival' ? 'survival' : 'campaign',
      difficulty: ['easy', 'normal', 'hard', 'extreme'].includes(meta.difficulty) ? meta.difficulty : 'normal',
      hostId: null,
      players: new Map(),
      killedZombies: new Set(),
      sharedDrops: new Map(), // dropId -> { type, x, z, from }
      stats: new Map(),       // playerId -> { kills, headshots, revives, score, name }
      victoryVotes: new Map(),// playerId -> 'restart' | 'continue'
      createdAt: Date.now()
    });
    console.log(`[Sala Creada] Código: ${c} | Nombre: ${rooms.get(c).name} | Modo: ${rooms.get(c).mode} | Dif: ${rooms.get(c).difficulty} | Privada: ${rooms.get(c).isPrivate}`);
  }
  return rooms.get(c);
}

function broadcastRoom(room, obj, excludeId = null) {
  if (!room) return;
  const msg = JSON.stringify(obj);
  for (const [id, p] of room.players) {
    if (id !== excludeId && p.ws && p.ws.readyState === 1) {
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
    console.log(`[Host Migration] [${room.code}] Nuevo host asignado: ${hPlayer ? hPlayer.name : ''} (${first})`);
    broadcastRoom(room, { t: 'host', id: first, hostName: hPlayer ? hPlayer.name : 'Host' });
  } else {
    room.hostId = null;
    if (room.code !== 'GLOBAL') {
      rooms.delete(room.code);
      console.log(`[Sala Destruida] Código: ${room.code} (quedó vacía)`);
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
  for (const [, room] of rooms) {
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
      // ── Consulta de salas públicas (Punto 9) ──
      case 'listRooms': {
        const publicRooms = [];
        for (const [code, r] of rooms) {
          if (!r.isPrivate && code !== 'GLOBAL') {
            publicRooms.push({
              code: r.code,
              name: r.name,
              mode: r.mode,
              difficulty: r.difficulty,
              playersCount: r.players.size,
              maxPlayers: MAX_PLAYERS_PER_ROOM,
              isFull: r.players.size >= MAX_PLAYERS_PER_ROOM
            });
          }
        }
        send(ws, { t: 'roomList', rooms: publicRooms });
        break;
      }

      // ── Unirse o crear sala (Puntos 2, 5, 6, 8, 9) ──
      case 'join': {
        const name = String(msg.name || playerName).trim().slice(0, 18) || 'Soldado';
        const isCreating = !!msg.create;
        let roomCode = String(msg.room || msg.code || '').trim().toUpperCase();

        if (isCreating) {
          roomCode = generateRoomCode();
          const customName = msg.roomName ? String(msg.roomName).trim().slice(0, 20) : `Sala de ${name}`;
          currentRoom = getOrCreateRoom(roomCode, {
            name: customName || `Sala de ${name}`,
            isPrivate: msg.isPrivate !== undefined ? !!msg.isPrivate : true,
            mode: msg.mode || 'campaign',
            difficulty: msg.difficulty || 'normal'
          });
        } else {
          // Unirse con código o sala existente
          if (!roomCode) roomCode = 'GLOBAL';

          if (roomCode.length < 4 && roomCode !== 'GLOBAL') {
            send(ws, { t: 'error', msg: 'El código de sala debe tener al menos 4 caracteres.' });
            return;
          }

          if (!rooms.has(roomCode)) {
            send(ws, { t: 'error', msg: `Sala "${roomCode}" no encontrada. Verifica el código o crea una nueva.` });
            return;
          }

          currentRoom = rooms.get(roomCode);
        }

        if (currentRoom.players.size >= MAX_PLAYERS_PER_ROOM) {
          send(ws, { t: 'error', msg: `La sala ${roomCode} está llena (${MAX_PLAYERS_PER_ROOM}/${MAX_PLAYERS_PER_ROOM} jugadores)` });
          return;
        }

        playerState.name = name;
        playerState.hasJoined = true;
        playerName = name;
        currentRoom.players.set(id, playerState);
        currentRoom.stats.set(id, { kills: 0, headshots: 0, revives: 0, score: 0, name });
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
          t:          'welcome',
          id,
          room:       currentRoom.code,
          roomName:   currentRoom.name,
          isPrivate:  currentRoom.isPrivate,
          mode:       currentRoom.mode,
          difficulty: currentRoom.difficulty,
          isHost:     id === currentRoom.hostId,
          players:    existingPlayers,
        });

        broadcastRoom(currentRoom, {
          t: 'join',
          id,
          name,
          room: currentRoom.code
        }, id);

        console.log(`[Join] [${currentRoom.code}] ${name} (${id}) | Host: ${id === currentRoom.hostId} | Total: ${currentRoom.players.size}/${MAX_PLAYERS_PER_ROOM}`);
        break;
      }

      case 'startGame': {
        if (!currentRoom) return;
        if (!currentRoom.hostId || !currentRoom.players.has(currentRoom.hostId)) currentRoom.hostId = id;
        broadcastRoom(currentRoom, {
          t: 'startGame',
          mode: currentRoom.mode,
          difficulty: currentRoom.difficulty
        }, id);
        console.log(`[StartGame] [${currentRoom.code}] Partida iniciada por ${playerName} (${id}) [Modo: ${currentRoom.mode}, Dif: ${currentRoom.difficulty}]`);
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

          const st = currentRoom.stats.get(id);
          if (st) st.revives = (st.revives || 0) + 1;

          broadcastRoom(currentRoom, { t: 'revive', targetId, reviverId: id, reviverName: playerName });
          console.log(`[Revive] [${currentRoom.code}] ${playerName} revivió a ${targetPlayer.name}`);
        }
        break;
      }

      // ── PUNTO 1: Victoria sincronizada en Oleada 10 ──
      case 'victory': {
        if (!currentRoom) return;
        currentRoom.victoryVotes.clear();
        broadcastRoom(currentRoom, {
          t: 'victory',
          wave: msg.wave || 10,
          room: currentRoom.code
        });
        console.log(`[Victoria] [${currentRoom.code}] ¡Victoria compartida en Oleada ${msg.wave || 10}!`);
        break;
      }

      // ── PUNTO 1: Votación de fin de partida (Reiniciar vs Seguir jugando) ──
      case 'victoryVote': {
        if (!currentRoom) return;
        const vote = msg.vote; // 'restart' | 'continue'
        currentRoom.victoryVotes.set(id, vote);

        const totalPlayers = currentRoom.players.size;
        const votesList = [];
        let restartCount = 0;
        let continueCount = 0;

        for (const [pid, v] of currentRoom.victoryVotes) {
          const pl = currentRoom.players.get(pid);
          votesList.push({ id: pid, name: pl ? pl.name : 'Soldado', vote: v });
          if (v === 'restart') restartCount++;
          if (v === 'continue') continueCount++;
        }

        console.log(`[VictoryVote] [${currentRoom.code}] ${playerName} votó "${vote}" (${votesList.length}/${totalPlayers})`);

        // Si todos votaron por la misma opción:
        if (restartCount === totalPlayers) {
          console.log(`[VictoryAction] [${currentRoom.code}] Todos acordaron REINICIAR.`);
          currentRoom.victoryVotes.clear();
          currentRoom.killedZombies.clear();
          currentRoom.sharedDrops.clear();
          broadcastRoom(currentRoom, { t: 'victoryAction', action: 'restart' });
        } else if (continueCount === totalPlayers) {
          console.log(`[VictoryAction] [${currentRoom.code}] Todos acordaron SEGUIR JUGANDO.`);
          currentRoom.victoryVotes.clear();
          broadcastRoom(currentRoom, { t: 'victoryAction', action: 'continue' });
        } else {
          // Notificar estado de votos para feedback visual ("Esperando confirmación...")
          broadcastRoom(currentRoom, {
            t: 'victoryVoteStatus',
            votes: votesList,
            total: totalPlayers
          });
        }
        break;
      }

      // Ping Táctico 3D en el mapa
      case 'ping': {
        if (!currentRoom) return;
        broadcastRoom(currentRoom, {
          t: 'ping',
          id,
          name: playerName,
          x: msg.x,
          z: msg.z,
          pingType: msg.pingType || 'alerta'
        });
        console.log(`[Ping] [${currentRoom.code}] ${playerName} marcó (${msg.pingType || 'alerta'}) en (${msg.x?.toFixed(1)}, ${msg.z?.toFixed(1)})`);
        break;
      }

      // Soltar suministro compartido en el suelo (munición o botiquín)
      case 'sharedDrop': {
        if (!currentRoom) return;
        const dropId = uid();
        const dropData = {
          dropId,
          type: msg.dropType || 'ammo',
          x: msg.x,
          z: msg.z,
          fromName: playerName,
          fromId: id
        };
        currentRoom.sharedDrops.set(dropId, dropData);
        broadcastRoom(currentRoom, { t: 'sharedDrop', ...dropData });
        console.log(`[Drop] [${currentRoom.code}] ${playerName} soltó suministro: ${msg.dropType}`);
        break;
      }

      // Recoger suministro compartido
      case 'pickupSharedDrop': {
        if (!currentRoom) return;
        const dropId = msg.dropId;
        if (currentRoom.sharedDrops.has(dropId)) {
          const drop = currentRoom.sharedDrops.get(dropId);
          currentRoom.sharedDrops.delete(dropId);
          broadcastRoom(currentRoom, {
            t: 'pickupSharedDrop',
            dropId,
            pickerId: id,
            pickerName: playerName,
            dropType: drop.type
          });
          console.log(`[Pickup] [${currentRoom.code}] ${playerName} recogió ${drop.type}`);
        }
        break;
      }

      case 'zkill': {
        if (!currentRoom) return;
        const zid = msg.zid;
        if (zid === undefined) break;
        if (!currentRoom.killedZombies.has(zid)) {
          currentRoom.killedZombies.add(zid);

          const st = currentRoom.stats.get(id);
          if (st) {
            st.kills = (st.kills || 0) + 1;
            if (msg.headshot) st.headshots = (st.headshots || 0) + 1;
            st.score = (st.score || 0) + 50;
          }

          broadcastRoom(currentRoom, { t: 'zkill', zid, killerId: id, headshot: !!msg.headshot });
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

      // Sincronización y presentación de Estadísticas de fin de partida (Scoreboard / MVP)
      case 'scoreboard': {
        if (!currentRoom) return;
        const statsList = [];
        for (const [pid, st] of currentRoom.stats) {
          statsList.push({ id: pid, ...st });
        }
        broadcastRoom(currentRoom, {
          t: 'scoreboard',
          stats: statsList,
          isVictory: !!msg.isVictory,
          wave: msg.wave || 1
        });
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

      // Salida voluntaria de la sala
      case 'leave': {
        handleDisconnect();
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
    currentRoom.victoryVotes.delete(id);
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
  console.log(`Servidor multijugador Zona Cero v2.0 activo en puerto ${PORT}`);
});
