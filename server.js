'use strict';

const http = require('http');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const DISCONNECT_GRACE_MS = 90_000;
const MAX_PAYLOAD = 16 * 1024;
const ROOM_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const VALID_CHARACTERS = new Set([
  'dragon','rogue','wolf','priest','sniper','shadow','augment','dragonette','plague','dice',
  'guardian','paladin','berserker','voidknight','samurai','shaman','bard','valkyrie','archangel','monk'
]);

const rooms = new Map();
const clients = new Set();
let connectionCounter = 0;

function json(res, status, body) {
  const payload = Buffer.from(JSON.stringify(body));
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': payload.length,
    'Cache-Control': 'no-store'
  });
  res.end(payload);
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    return json(res, 200, {
      ok: true,
      service: 'Chibi Arena Online',
      protocol: 61,
      rooms: rooms.size,
      clients: clients.size,
      uptime: Math.round(process.uptime())
    });
  }
  json(res, 404, { ok: false, error: 'Not found' });
});

function wsAccept(key) {
  return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
}

function makeFrame(opcode, payload = Buffer.alloc(0)) {
  if (!Buffer.isBuffer(payload)) payload = Buffer.from(payload);
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len <= 0xffff) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

function rawSend(ws, opcode, payload) {
  if (!ws || ws.closed || !ws.socket || ws.socket.destroyed) return false;
  try { ws.socket.write(makeFrame(opcode, payload)); return true; }
  catch { return false; }
}

function send(ws, data) {
  return rawSend(ws, 0x1, Buffer.from(JSON.stringify(data), 'utf8'));
}

function closeWs(ws, code = 1000, reason = '') {
  if (!ws || ws.closed) return;
  const reasonBuf = Buffer.from(String(reason).slice(0, 120), 'utf8');
  const payload = Buffer.alloc(2 + reasonBuf.length);
  payload.writeUInt16BE(code, 0);
  reasonBuf.copy(payload, 2);
  rawSend(ws, 0x8, payload);
  ws.closed = true;
  setTimeout(() => { try { ws.socket.destroy(); } catch {} }, 20).unref();
}

function cleanName(value) {
  const name = String(value || 'Joueur').replace(/[<>]/g, '').trim().slice(0, 20);
  return name || 'Joueur';
}
function cleanCode(value) { return String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 5); }
function cleanTeam(value) {
  if (!Array.isArray(value)) return [];
  const team = value.filter(x => typeof x === 'string' && VALID_CHARACTERS.has(x)).slice(0, 2);
  return [...new Set(team)];
}
function makeCode() {
  for (let attempt = 0; attempt < 1000; attempt++) {
    let code = '';
    const bytes = crypto.randomBytes(5);
    for (let i = 0; i < 5; i++) code += ROOM_ALPHABET[bytes[i] % ROOM_ALPHABET.length];
    if (!rooms.has(code)) return code;
  }
  throw new Error('Impossible de générer un code de salle');
}
function makeToken() { return crypto.randomBytes(24).toString('base64url'); }

function publicState(room) {
  return {
    type: 'room_state', roomCode: room.code, sequence: room.sequence, hostSlot: room.hostSlot,
    players: [...room.players.values()].sort((a,b)=>a.slot-b.slot).map(p => ({
      slot:p.slot, name:p.name, team:p.team, connected:!!p.connected
    }))
  };
}
function broadcastState(room) {
  room.sequence++;
  const state = publicState(room);
  for (const p of room.players.values()) send(p.ws, state);
  return state;
}
function detachSocket(ws) { ws.roomCode=null; ws.slot=null; ws.resumeToken=null; }

function leaveCurrentRoom(ws, explicit=false) {
  if (!ws.roomCode || ws.slot == null) return;
  const room = rooms.get(ws.roomCode);
  if (!room) return detachSocket(ws);
  const p = room.players.get(ws.slot);
  if (!p || p.token !== ws.resumeToken) return detachSocket(ws);
  if (!explicit && p.ws && p.ws !== ws) return detachSocket(ws);

  if (explicit) {
    if (p.cleanupTimer) clearTimeout(p.cleanupTimer);
    room.players.delete(p.slot);
    detachSocket(ws);
    if (!room.players.size) { rooms.delete(room.code); return; }
    if (room.hostSlot === p.slot) room.hostSlot = [...room.players.keys()].sort()[0];
    broadcastState(room);
    return;
  }

  p.connected=false; p.ws=null; p.disconnectedAt=Date.now();
  if (p.cleanupTimer) clearTimeout(p.cleanupTimer);
  p.cleanupTimer=setTimeout(()=>{
    const current=room.players.get(p.slot);
    if (!current || current.connected || current.token!==p.token) return;
    room.players.delete(p.slot);
    if (!room.players.size) { rooms.delete(room.code); return; }
    if (room.hostSlot===p.slot) room.hostSlot=[...room.players.keys()].sort()[0];
    broadcastState(room);
  },DISCONNECT_GRACE_MS);
  broadcastState(room);
  detachSocket(ws);
}

function attachPlayer(ws,room,player){
  if(player.ws && player.ws!==ws && !player.ws.closed){send(player.ws,{type:'error',code:'SESSION_REPLACED',message:'Cette session a été reprise dans un autre onglet.'});closeWs(player.ws,4001,'Session replaced')}
  if(player.cleanupTimer){clearTimeout(player.cleanupTimer);player.cleanupTimer=null}
  player.ws=ws;player.connected=true;player.disconnectedAt=null;ws.roomCode=room.code;ws.slot=player.slot;ws.resumeToken=player.token;
}
function createRoom(ws,m){
  leaveCurrentRoom(ws,true);const code=makeCode();const room={code,createdAt:Date.now(),sequence:0,hostSlot:1,players:new Map()};
  const player={slot:1,token:makeToken(),clientId:String(m.clientId||ws.clientId||'').slice(0,80),name:cleanName(m.name||ws.playerName),team:cleanTeam(m.team),ws:null,connected:true,disconnectedAt:null,cleanupTimer:null};
  room.players.set(1,player);rooms.set(code,room);attachPlayer(ws,room,player);const state=broadcastState(room);send(ws,{type:'room_created',roomCode:code,slot:1,resumeToken:player.token,state});
}
function joinRoom(ws,m){
  const code=cleanCode(m.roomCode),room=rooms.get(code);if(!room)return send(ws,{type:'error',code:'ROOM_NOT_FOUND',message:'Salle introuvable ou expirée.'});if(room.players.size>=2)return send(ws,{type:'error',code:'ROOM_FULL',message:'Cette salle contient déjà deux joueurs.'});
  leaveCurrentRoom(ws,true);const slot=room.players.has(1)?2:1;const player={slot,token:makeToken(),clientId:String(m.clientId||ws.clientId||'').slice(0,80),name:cleanName(m.name||ws.playerName),team:cleanTeam(m.team),ws:null,connected:true,disconnectedAt:null,cleanupTimer:null};
  room.players.set(slot,player);attachPlayer(ws,room,player);const state=broadcastState(room);send(ws,{type:'room_joined',roomCode:code,slot,resumeToken:player.token,state});
}
function resumeRoom(ws,m){
  const code=cleanCode(m.roomCode),token=String(m.token||''),room=rooms.get(code);if(!room)return send(ws,{type:'error',code:'ROOM_EXPIRED',message:'La salle n’existe plus.'});const player=[...room.players.values()].find(p=>p.token===token);if(!player)return send(ws,{type:'error',code:'RESUME_DENIED',message:'Jeton de reconnexion invalide.'});
  leaveCurrentRoom(ws,true);player.name=cleanName(m.name||player.name);const team=cleanTeam(m.team);if(team.length===2)player.team=team;attachPlayer(ws,room,player);const state=broadcastState(room);send(ws,{type:'resumed',roomCode:code,slot:player.slot,resumeToken:player.token,state});
}
function updateTeam(ws,m){
  if(!ws.roomCode)return send(ws,{type:'error',code:'NOT_IN_ROOM',message:'Tu n’es dans aucune salle.'});const room=rooms.get(ws.roomCode),p=room&&room.players.get(ws.slot);if(!p||p.token!==ws.resumeToken)return;const team=cleanTeam(m.team);if(team.length!==2)return send(ws,{type:'error',code:'INVALID_TEAM',message:'Une équipe doit contenir exactement deux combattants différents.'});p.team=team;broadcastState(room);
}
function syncTest(ws,m){
  const room=ws.roomCode&&rooms.get(ws.roomCode);if(!room)return send(ws,{type:'error',code:'NOT_IN_ROOM',message:'Tu n’es dans aucune salle.'});room.sequence++;send(ws,{type:'sync_test_result',roomCode:room.code,sequence:room.sequence,clientTime:Number(m.clientTime||Date.now()),serverTime:Date.now()});
}

function handleMessage(ws, text) {
  let m; try { m=JSON.parse(text); } catch { return send(ws,{type:'error',code:'BAD_JSON',message:'Message JSON invalide.'}); }
  if(!m || typeof m.type!=='string') return;
  switch(m.type){
    case 'hello':ws.clientId=String(m.clientId||'').slice(0,80);ws.playerName=cleanName(m.name);send(ws,{type:'hello_ack',serverTime:Date.now(),protocol:61});break;
    case 'create_room':createRoom(ws,m);break;
    case 'join_room':joinRoom(ws,m);break;
    case 'resume':resumeRoom(ws,m);break;
    case 'team_update':updateTeam(ws,m);break;
    case 'sync_test':syncTest(ws,m);break;
    case 'ping':send(ws,{type:'pong',clientTime:Number(m.clientTime||Date.now()),serverTime:Date.now()});break;
    case 'leave_room':leaveCurrentRoom(ws,true);break;
    default:send(ws,{type:'error',code:'UNKNOWN_TYPE',message:'Type de message inconnu.'});
  }
}

function parseFrames(ws, chunk) {
  ws.buffer = Buffer.concat([ws.buffer, chunk]);
  while (ws.buffer.length >= 2) {
    const b0=ws.buffer[0], b1=ws.buffer[1], fin=!!(b0&0x80), opcode=b0&0x0f, masked=!!(b1&0x80);
    let len=b1&0x7f, offset=2;
    if (!fin && opcode !== 0x0) { closeWs(ws,1003,'Fragmented frames unsupported'); return; }
    if (len===126) { if(ws.buffer.length<4)return; len=ws.buffer.readUInt16BE(2); offset=4; }
    else if(len===127){ if(ws.buffer.length<10)return; const big=ws.buffer.readBigUInt64BE(2); if(big>BigInt(MAX_PAYLOAD)){closeWs(ws,1009,'Payload too large');return} len=Number(big); offset=10; }
    if(len>MAX_PAYLOAD){closeWs(ws,1009,'Payload too large');return}
    if(!masked){closeWs(ws,1002,'Client frames must be masked');return}
    if(ws.buffer.length<offset+4+len)return;
    const mask=ws.buffer.subarray(offset,offset+4);offset+=4;
    const payload=Buffer.from(ws.buffer.subarray(offset,offset+len));
    for(let i=0;i<payload.length;i++)payload[i]^=mask[i%4];
    ws.buffer=ws.buffer.subarray(offset+len);
    ws.lastSeen=Date.now();
    if(opcode===0x1)handleMessage(ws,payload.toString('utf8'));
    else if(opcode===0x8){closeWs(ws,1000,'Bye');return}
    else if(opcode===0x9)rawSend(ws,0xA,payload);
    else if(opcode===0xA)ws.lastPong=Date.now();
    else if(opcode!==0x0){closeWs(ws,1003,'Unsupported opcode');return}
  }
}

server.on('upgrade',(req,socket,head)=>{
  const key=req.headers['sec-websocket-key'];
  const upgrade=String(req.headers.upgrade||'').toLowerCase();
  if(upgrade!=='websocket'||!key){socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');socket.destroy();return}
  const accept=wsAccept(key);
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  socket.setNoDelay(true);socket.setKeepAlive(true,15000);
  const ws={id:++connectionCounter,socket,buffer:Buffer.alloc(0),closed:false,lastSeen:Date.now(),lastPong:Date.now(),clientId:null,playerName:'Joueur',roomCode:null,slot:null,resumeToken:null};
  clients.add(ws);
  socket.on('data',chunk=>parseFrames(ws,chunk));
  socket.on('error',()=>{});
  socket.on('close',()=>{if(!ws.closed)ws.closed=true;clients.delete(ws);leaveCurrentRoom(ws,false)});
  if(head&&head.length)parseFrames(ws,head);
});

const heartbeat=setInterval(()=>{
  const now=Date.now();
  for(const ws of clients){
    if(ws.closed)continue;
    if(now-ws.lastSeen>45_000 && now-ws.lastPong>45_000){closeWs(ws,1001,'Heartbeat timeout');continue}
    rawSend(ws,0x9,Buffer.from('hb'));
  }
},15_000);heartbeat.unref();

server.listen(PORT,HOST,()=>{
  console.log(`Chibi Arena V61: http://localhost:${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
});

function shutdown(){clearInterval(heartbeat);for(const room of rooms.values())for(const p of room.players.values())if(p.cleanupTimer)clearTimeout(p.cleanupTimer);for(const ws of clients)closeWs(ws,1001,'Server shutdown');server.close(()=>process.exit(0));setTimeout(()=>process.exit(0),1200).unref()}
process.on('SIGINT',shutdown);process.on('SIGTERM',shutdown);
