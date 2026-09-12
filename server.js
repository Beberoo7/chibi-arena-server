'use strict';

const http = require('http');
const crypto = require('crypto');
const { createBattleEngine } = require('./battle_engine');

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const DISCONNECT_GRACE_MS = 90_000;
const PLAN_MS = Number(process.env.PLAN_MS || 30_000);
const MAX_PAYLOAD = 128 * 1024;
const ROOM_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const ARENAS = new Set(['coliseum','forest','necropole','citadelle','dragon','arcane']);
const VALID_CHARACTERS = new Set([
  'dragon','rogue','wolf','priest','sniper','shadow','augment','dragonette','plague','dice',
  'guardian','paladin','berserker','voidknight','samurai','shaman','bard','valkyrie','archangel','monk'
]);

const rooms = new Map();
const bgRooms = new Map();
const clients = new Set();
let connectionCounter = 0;

function json(res, status, body) {
  const payload = Buffer.from(JSON.stringify(body));
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': payload.length,
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*'
  });
  res.end(payload);
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health')) {
    let matches = 0;
    for (const room of rooms.values()) if (room.match && room.match.phase !== 'end') matches++;
    return json(res, 200, {
      ok: true,
      service: 'Chibi Arena Online',
      protocol: 69,
      authoritativeCombat: true,
      rooms: rooms.size,
      battlegroundRooms: bgRooms.size,
      matches,
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
    header = Buffer.alloc(2); header[0] = 0x80 | opcode; header[1] = len;
  } else if (len <= 0xffff) {
    header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}
function rawSend(ws, opcode, payload) {
  if (!ws || ws.closed || !ws.socket || ws.socket.destroyed) return false;
  try { ws.socket.write(makeFrame(opcode, payload)); return true; } catch { return false; }
}
function send(ws, data) { return rawSend(ws, 0x1, Buffer.from(JSON.stringify(data), 'utf8')); }
function closeWs(ws, code = 1000, reason = '') {
  if (!ws || ws.closed) return;
  const reasonBuf = Buffer.from(String(reason).slice(0, 120), 'utf8');
  const payload = Buffer.alloc(2 + reasonBuf.length); payload.writeUInt16BE(code, 0); reasonBuf.copy(payload, 2);
  rawSend(ws, 0x8, payload); ws.closed = true;
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
    let code = ''; const bytes = crypto.randomBytes(5);
    for (let i = 0; i < 5; i++) code += ROOM_ALPHABET[bytes[i] % ROOM_ALPHABET.length];
    if (!rooms.has(code) && !bgRooms.has(code)) return code;
  }
  throw new Error('Impossible de générer un code de salle');
}
function makeToken() { return crypto.randomBytes(24).toString('base64url'); }
function makeMatchId() { return crypto.randomBytes(12).toString('base64url'); }

function stripFighter(f) {
  if (!f) return f;
  const { skill1, skill2, skill3, passive, role, color, name, ...dynamic } = f;
  return dynamic;
}
function wireState(state) {
  if (!state) return null;
  return {
    round: state.round,
    phase: state.phase,
    ended: !!state.ended,
    winnerSlot: state.winnerSlot == null ? null : state.winnerSlot,
    roundFreezeSourceKey: state.roundFreezeSourceKey || null,
    teams: {
      1: (state.teams && state.teams[1] || []).map(stripFighter),
      2: (state.teams && state.teams[2] || []).map(stripFighter)
    },
    stats: state.stats || {}
  };
}
function wireEvents(events) {
  return (events || []).map(e => ({
    kind: e.kind,
    plan: e.plan || null,
    logs: e.logs || [],
    state: wireState(e.state)
  }));
}
function roomMatchSummary(room) {
  const m = room.match;
  return m ? {
    id: m.id, arena: m.arena, phase: m.phase, round: m.round,
    deadline: m.deadline || 0, ended: m.phase === 'end', sequence: m.sequence
  } : null;
}
function publicState(room) {
  return {
    type: 'room_state', roomCode: room.code, sequence: room.sequence, hostSlot: room.hostSlot,
    match: roomMatchSummary(room),
    players: [...room.players.values()].sort((a,b)=>a.slot-b.slot).map(p => ({
      slot:p.slot, name:p.name, team:p.team, connected:!!p.connected
    }))
  };
}
function broadcast(room, data) { for (const p of room.players.values()) send(p.ws, data); }
function broadcastState(room) {
  room.sequence++;
  const state = publicState(room); broadcast(room, state); return state;
}
function detachSocket(ws) { ws.roomCode=null; ws.slot=null; ws.resumeToken=null; }
function clearMatchTimers(match) {
  if (!match) return;
  if (match.planTimer) clearTimeout(match.planTimer);
  if (match.phaseTimer) clearTimeout(match.phaseTimer);
  match.planTimer = null; match.phaseTimer = null;
}
function destroyRoom(room) { clearMatchTimers(room.match); rooms.delete(room.code); }

function winnerFromForfeit(room, loserSlot, reason='Abandon') {
  const m=room.match; if(!m || m.phase==='end') return;
  clearMatchTimers(m); m.phase='end'; m.deadline=0; m.sequence++;
  const winnerSlot=loserSlot===1?2:1;
  broadcast(room,{type:'match_end',matchId:m.id,sequence:m.sequence,winnerSlot,forfeit:true,reason,state:wireState(m.engine.getState())});
  broadcastState(room);
}

function leaveCurrentRoom(ws, explicit=false) {
  if (!ws.roomCode || ws.slot == null) return;
  const room = rooms.get(ws.roomCode); if (!room) return detachSocket(ws);
  const p = room.players.get(ws.slot); if (!p || p.token !== ws.resumeToken) return detachSocket(ws);
  if (!explicit && p.ws && p.ws !== ws) return detachSocket(ws);

  if (explicit) {
    if (room.match && room.match.phase !== 'end') winnerFromForfeit(room, p.slot, 'Un joueur a quitté la salle.');
    if (p.cleanupTimer) clearTimeout(p.cleanupTimer);
    room.players.delete(p.slot); detachSocket(ws);
    if (!room.players.size) { destroyRoom(room); return; }
    if (room.hostSlot === p.slot) room.hostSlot = [...room.players.keys()].sort()[0];
    broadcastState(room); return;
  }

  p.connected=false; p.ws=null; p.disconnectedAt=Date.now();
  if (p.cleanupTimer) clearTimeout(p.cleanupTimer);
  p.cleanupTimer=setTimeout(()=>{
    const current=room.players.get(p.slot);
    if (!current || current.connected || current.token!==p.token) return;
    if (room.match && room.match.phase !== 'end') winnerFromForfeit(room,p.slot,'Reconnexion expirée.');
    room.players.delete(p.slot);
    if (!room.players.size) { destroyRoom(room); return; }
    if (room.hostSlot===p.slot) room.hostSlot=[...room.players.keys()].sort()[0];
    broadcastState(room);
  },DISCONNECT_GRACE_MS);
  broadcastState(room); detachSocket(ws);
}

function attachPlayer(ws,room,player){
  if(player.ws && player.ws!==ws && !player.ws.closed){send(player.ws,{type:'error',code:'SESSION_REPLACED',message:'Cette session a été reprise dans un autre onglet.'});closeWs(player.ws,4001,'Session replaced')}
  if(player.cleanupTimer){clearTimeout(player.cleanupTimer);player.cleanupTimer=null}
  player.ws=ws;player.connected=true;player.disconnectedAt=null;ws.roomCode=room.code;ws.slot=player.slot;ws.resumeToken=player.token;
}
function createRoom(ws,m){
  leaveCurrentRoom(ws,true);const code=makeCode();const room={code,createdAt:Date.now(),sequence:0,hostSlot:1,players:new Map(),match:null};
  const player={slot:1,token:makeToken(),clientId:String(m.clientId||ws.clientId||'').slice(0,80),name:cleanName(m.name||ws.playerName),team:cleanTeam(m.team),ws:null,connected:true,disconnectedAt:null,cleanupTimer:null};
  room.players.set(1,player);rooms.set(code,room);attachPlayer(ws,room,player);const state=broadcastState(room);send(ws,{type:'room_created',roomCode:code,slot:1,resumeToken:player.token,state});
}
function joinRoom(ws,m){
  const code=cleanCode(m.roomCode),room=rooms.get(code);if(!room)return send(ws,{type:'error',code:'ROOM_NOT_FOUND',message:'Salle introuvable ou expirée.'});if(room.players.size>=2)return send(ws,{type:'error',code:'ROOM_FULL',message:'Cette salle contient déjà deux joueurs.'});
  if(room.match && room.match.phase!=='end')return send(ws,{type:'error',code:'MATCH_STARTED',message:'Le combat a déjà commencé.'});
  leaveCurrentRoom(ws,true);const slot=room.players.has(1)?2:1;const player={slot,token:makeToken(),clientId:String(m.clientId||ws.clientId||'').slice(0,80),name:cleanName(m.name||ws.playerName),team:cleanTeam(m.team),ws:null,connected:true,disconnectedAt:null,cleanupTimer:null};
  room.players.set(slot,player);attachPlayer(ws,room,player);const state=broadcastState(room);send(ws,{type:'room_joined',roomCode:code,slot,resumeToken:player.token,state});
}
function sendMatchSync(ws,room){
  const m=room.match;if(!m)return;
  send(ws,{type:'match_sync',matchId:m.id,arena:m.arena,phase:m.phase,round:m.round,deadline:m.deadline||0,sequence:m.sequence,readySlots:[1,2].filter(x=>!!m.plans[x]),state:wireState(m.engine.getState())});
}
function resumeRoom(ws,m){
  const code=cleanCode(m.roomCode),token=String(m.token||''),room=rooms.get(code);if(!room)return send(ws,{type:'error',code:'ROOM_EXPIRED',message:'La salle n’existe plus.'});const player=[...room.players.values()].find(p=>p.token===token);if(!player)return send(ws,{type:'error',code:'RESUME_DENIED',message:'Jeton de reconnexion invalide.'});
  leaveCurrentRoom(ws,true);player.name=cleanName(m.name||player.name);const team=cleanTeam(m.team);if(team.length===2 && (!room.match || room.match.phase==='end'))player.team=team;attachPlayer(ws,room,player);const state=broadcastState(room);send(ws,{type:'resumed',roomCode:code,slot:player.slot,resumeToken:player.token,state});sendMatchSync(ws,room);
}
function updateTeam(ws,m){
  if(!ws.roomCode)return send(ws,{type:'error',code:'NOT_IN_ROOM',message:'Tu n’es dans aucune salle.'});const room=rooms.get(ws.roomCode),p=room&&room.players.get(ws.slot);if(!p||p.token!==ws.resumeToken)return;
  if(room.match && room.match.phase!=='end')return send(ws,{type:'error',code:'MATCH_ACTIVE',message:'Impossible de changer d’équipe pendant un combat.'});
  const team=cleanTeam(m.team);if(team.length!==2)return send(ws,{type:'error',code:'INVALID_TEAM',message:'Une équipe doit contenir exactement deux combattants différents.'});p.team=team;broadcastState(room);
}
function syncTest(ws,m){
  const room=ws.roomCode&&rooms.get(ws.roomCode);if(!room)return send(ws,{type:'error',code:'NOT_IN_ROOM',message:'Tu n’es dans aucune salle.'});room.sequence++;send(ws,{type:'sync_test_result',roomCode:room.code,sequence:room.sequence,clientTime:Number(m.clientTime||Date.now()),serverTime:Date.now()});
}

function beginPlanning(room, first=false) {
  const m=room.match;if(!m || m.phase==='end')return;
  clearMatchTimers(m);m.phase='planning';m.round=m.engine.getRound();m.deadline=Date.now()+PLAN_MS;m.plans={1:null,2:null};m.sequence++;
  broadcast(room,{type:first?'match_started':'round_begin',matchId:m.id,arena:m.arena,round:m.round,deadline:m.deadline,sequence:m.sequence,state:wireState(m.engine.getState())});
  broadcastState(room);
  m.planTimer=setTimeout(()=>resolveMatchRound(room,true),PLAN_MS+120);m.planTimer.unref?.();
}
function startMatch(ws,m){
  const room=ws.roomCode&&rooms.get(ws.roomCode);if(!room)return send(ws,{type:'error',code:'NOT_IN_ROOM',message:'Tu n’es dans aucune salle.'});
  if(ws.slot!==room.hostSlot)return send(ws,{type:'error',code:'HOST_ONLY',message:'Seul le créateur de la salle choisit l’arène et lance le combat.'});
  const arena=String(m.arena||'');if(!ARENAS.has(arena))return send(ws,{type:'error',code:'BAD_ARENA',message:'Arène invalide.'});
  const p1=room.players.get(1),p2=room.players.get(2);if(!p1||!p2||!p1.connected||!p2.connected)return send(ws,{type:'error',code:'PLAYERS_MISSING',message:'Les deux joueurs doivent être connectés.'});
  if(p1.team.length!==2||p2.team.length!==2)return send(ws,{type:'error',code:'TEAMS_NOT_READY',message:'Les deux équipes doivent contenir deux combattants.'});
  if(room.match)clearMatchTimers(room.match);
  let engine;try{engine=createBattleEngine(p1.team,p2.team)}catch(e){console.error('engine init',e);return send(ws,{type:'error',code:'ENGINE_INIT',message:'Impossible de démarrer le moteur de combat.'})}
  room.match={id:makeMatchId(),arena,engine,phase:'planning',round:1,deadline:0,plans:{1:null,2:null},sequence:0,planTimer:null,phaseTimer:null};
  beginPlanning(room,true);
}
function submitPlans(ws,m){
  const room=ws.roomCode&&rooms.get(ws.roomCode),match=room&&room.match;if(!match)return send(ws,{type:'error',code:'NO_MATCH',message:'Aucun combat en cours.'});
  if(match.phase!=='planning')return send(ws,{type:'error',code:'NOT_PLANNING',message:'La manche n’est plus en phase de planification.'});
  if(String(m.matchId||'')!==match.id||Number(m.round)!==match.round)return send(ws,{type:'error',code:'STALE_ROUND',message:'Plan reçu pour une ancienne manche.'});
  if(match.plans[ws.slot])return send(ws,{type:'plan_ack',matchId:match.id,round:match.round,alreadyLocked:true});
  let validation;try{validation=match.engine.validatePlans(ws.slot,m.plans)}catch(e){console.error('validate',e);return send(ws,{type:'error',code:'PLAN_ENGINE',message:'Erreur de validation du plan.'})}
  if(!validation.ok)return send(ws,{type:'error',code:'INVALID_PLAN',message:validation.error||'Plan invalide.'});
  match.plans[ws.slot]=validation.plans;match.sequence++;
  send(ws,{type:'plan_ack',matchId:match.id,round:match.round,sequence:match.sequence});
  broadcast(room,{type:'round_status',matchId:match.id,round:match.round,deadline:match.deadline,sequence:match.sequence,readySlots:[1,2].filter(x=>!!match.plans[x])});
  if(match.plans[1]&&match.plans[2])resolveMatchRound(room,false);
}
function resolveMatchRound(room, timeoutTriggered=false){
  const m=room.match;if(!m||m.phase!=='planning')return;
  clearMatchTimers(m);m.phase='resolution';m.deadline=0;m.sequence++;
  let result;try{result=m.engine.resolveRound(m.plans[1],m.plans[2])}catch(e){console.error('resolve engine',e);broadcast(room,{type:'error',code:'ENGINE_RESOLVE',message:'Le serveur a interrompu cette manche pour éviter une désynchronisation.'});m.phase='end';return}
  const events=wireEvents(result.events),finalState=wireState(result.finalState);
  const actionCount=events.filter(e=>e.kind==='action'||e.kind==='skip').length;
  const resolutionMs=Math.max(1200,actionCount*720+(events.some(e=>e.kind==='end_round')?650:300));
  broadcast(room,{type:'round_resolution',matchId:m.id,round:m.round,sequence:m.sequence,timeoutTriggered,events,finalState,resolutionMs,ended:result.ended,winnerSlot:result.winnerSlot});
  if(result.ended){
    m.phase='end';m.round=result.finalState.round;m.sequence++;
    m.phaseTimer=setTimeout(()=>{broadcast(room,{type:'match_end',matchId:m.id,sequence:m.sequence,winnerSlot:result.winnerSlot,forfeit:false,state:finalState});broadcastState(room)},resolutionMs);m.phaseTimer.unref?.();
  }else{
    m.round=result.finalState.round;
    m.phaseTimer=setTimeout(()=>beginPlanning(room,false),resolutionMs);m.phaseTimer.unref?.();
  }
}
function forfeitMatch(ws){
  const room=ws.roomCode&&rooms.get(ws.roomCode);if(!room||!room.match||room.match.phase==='end')return;winnerFromForfeit(room,ws.slot,'Abandon de la partie.');
}


/* ========================= V69 — BATTLEGROUND ONLINE 2 À 8 ========================= */
const BG_MAX_TEAMS = 8;
const BG_MIN_HUMANS = 2;
const BG_TEAM_HP = 20;
const BG_BOT_NAMES = ['RAVEN','NOVA','KIRA','ONYX','SOL','MIRA','VEX'];
const BG_TANKS = new Set(['guardian','paladin','valkyrie']);
const BG_SUPPORTS = new Set(['priest','augment','dragonette','bard']);
const BG_CHARACTERS = [...VALID_CHARACTERS];

function bgRoleGroup(id){
  if(BG_TANKS.has(id)) return 'TANK';
  if(BG_SUPPORTS.has(id)) return 'SUPPORT';
  return 'DPS';
}
function bgValidPair(a,b){
  if(!a||!b||a===b) return false;
  const ra=bgRoleGroup(a), rb=bgRoleGroup(b);
  if(ra==='TANK' && rb==='TANK') return false;
  if(ra==='SUPPORT' && rb==='SUPPORT') return false;
  return true;
}
function bgPairKey(team){ return [...team].sort().join('|'); }
function bgShuffle(arr){
  const a=[...arr];
  for(let i=a.length-1;i>0;i--){ const j=crypto.randomInt(i+1); [a[i],a[j]]=[a[j],a[i]]; }
  return a;
}
function bgAllValidPairs(){
  const out=[];
  for(let i=0;i<BG_CHARACTERS.length;i++) for(let j=i+1;j<BG_CHARACTERS.length;j++){
    if(bgValidPair(BG_CHARACTERS[i],BG_CHARACTERS[j])) out.push([BG_CHARACTERS[i],BG_CHARACTERS[j]]);
  }
  return out;
}
function bgUniqueTeams(count, forbidden=new Set()){
  const pool=bgShuffle(bgAllValidPairs()).filter(p=>!forbidden.has(bgPairKey(p)));
  if(pool.length<count) throw new Error('Pas assez de paires Battleground valides');
  return pool.slice(0,count);
}
function bgDamage(round,survivors){
  const tier = round<=3?0:round<=6?1:round<=9?2:3;
  return survivors>=2 ? 3+tier : 2+tier;
}
function bgTeamById(match,id){ return match && match.teams.find(t=>t.id===id) || null; }
function bgHumanEntity(match,slot){ return match && match.teams.find(t=>t.human && t.slot===slot) || null; }
function bgAlive(match){ return match.teams.filter(t=>t.alive); }
function bgRanked(match){
  return [...match.teams].sort((a,b)=>
    Number(b.alive)-Number(a.alive) ||
    b.hp-a.hp ||
    b.wins-a.wins ||
    b.winStreak-a.winStreak ||
    a.losses-b.losses ||
    a.seed-b.seed
  );
}
function bgPairStatKey(a,b){ return [a,b].sort().join('~'); }
function bgPairCount(match,a,b){ return match.pairCounts.get(bgPairStatKey(a,b)) || 0; }
function bgPairLast(match,a,b){ return match.pairLast.get(bgPairStatKey(a,b)) || 0; }
function bgRecordPair(match,a,b){
  const k=bgPairStatKey(a,b);
  match.pairCounts.set(k,(match.pairCounts.get(k)||0)+1);
  match.pairLast.set(k,match.round);
}
function bgWireTeam(t){
  return {
    id:t.id, slot:t.slot==null?null:t.slot, name:t.name, team:[...t.team],
    hp:t.hp, alive:!!t.alive, human:!!t.human, connected:t.human?!!t.connected:true,
    wins:t.wins||0, losses:t.losses||0, winStreak:t.winStreak||0, bestStreak:t.bestStreak||0,
    seed:t.seed, eliminatedRound:t.eliminatedRound||null
  };
}
function bgPublicState(room,viewerSlot=null){
  const m=room.match;
  return {
    roomCode:room.code,
    mySlot:viewerSlot,
    hostSlot:room.hostSlot,
    started:!!(m && m.phase!=='end'),
    phase:m?m.phase:'lobby',
    round:m?m.round:0,
    matchId:m?m.id:null,
    players:[...room.players.values()].sort((a,b)=>a.slot-b.slot).map(p=>({
      slot:p.slot,name:p.name,connected:!!p.connected,team:m?(bgHumanEntity(m,p.slot)?.team||[]):[]
    })),
    teams:m?m.teams.map(bgWireTeam):[],
    alive:m?bgAlive(m).length:0
  };
}
function bgSendState(room){
  room.sequence++;
  for(const p of room.players.values()){
    if(p.connected && p.ws) send(p.ws,{type:'bg_room_state',roomCode:room.code,sequence:room.sequence,state:bgPublicState(room,p.slot)});
  }
}
function bgSendToRoom(room,data){
  for(const p of room.players.values()) if(p.connected && p.ws) send(p.ws,data);
}
function bgClearTimer(obj,key){
  if(obj && obj[key]){ clearTimeout(obj[key]); obj[key]=null; }
}
function bgClearMatchTimers(match){
  if(!match) return;
  ['assignmentTimer','revealTimer','redistributionTimer','redistributionAckTimer'].forEach(k=>bgClearTimer(match,k));
  if(match.duels) for(const d of match.duels.values()) ['planTimer','phaseTimer'].forEach(k=>bgClearTimer(d,k));
}
function bgDestroyRoom(room){
  bgClearMatchTimers(room.match);
  bgRooms.delete(room.code);
}
function bgDetachSocket(ws){
  ws.bgRoomCode=null; ws.bgSlot=null; ws.bgResumeToken=null;
}
function bgAttachPlayer(ws,room,player){
  if(player.ws && player.ws!==ws && !player.ws.closed){
    send(player.ws,{type:'bg_error',code:'SESSION_REPLACED',message:'Cette session Battleground a été reprise dans un autre onglet.'});
    closeWs(player.ws,4001,'BG session replaced');
  }
  if(player.cleanupTimer){ clearTimeout(player.cleanupTimer); player.cleanupTimer=null; }
  player.ws=ws; player.connected=true; player.disconnectedAt=null;
  ws.bgRoomCode=room.code; ws.bgSlot=player.slot; ws.bgResumeToken=player.token;
  const ent=bgHumanEntity(room.match,player.slot); if(ent){ ent.connected=true; ent.name=player.name; }
}
function bgNextFreeSlot(room){
  for(let i=1;i<=BG_MAX_TEAMS;i++) if(!room.players.has(i)) return i;
  return null;
}
function bgCreateRoom(ws,m){
  bgLeaveCurrentRoom(ws,true);
  const code=makeCode();
  const room={code,createdAt:Date.now(),sequence:0,hostSlot:1,players:new Map(),match:null};
  const player={slot:1,token:makeToken(),clientId:String(m.clientId||ws.clientId||'').slice(0,80),name:cleanName(m.name||ws.playerName),ws:null,connected:true,disconnectedAt:null,cleanupTimer:null};
  room.players.set(1,player); bgRooms.set(code,room); bgAttachPlayer(ws,room,player);
  send(ws,{type:'bg_room_created',roomCode:code,slot:1,resumeToken:player.token,state:bgPublicState(room,1)});
  bgSendState(room);
}
function bgJoinRoom(ws,m){
  const code=cleanCode(m.roomCode), room=bgRooms.get(code);
  if(!room) return send(ws,{type:'bg_error',code:'ROOM_NOT_FOUND',message:'Salle Battleground introuvable ou expirée.'});
  if(room.match && room.match.phase!=='end') return send(ws,{type:'bg_error',code:'MATCH_STARTED',message:'Le Battleground a déjà commencé.'});
  if(room.players.size>=BG_MAX_TEAMS) return send(ws,{type:'bg_error',code:'ROOM_FULL',message:'Cette salle contient déjà 8 joueurs.'});
  const slot=bgNextFreeSlot(room); if(slot==null) return send(ws,{type:'bg_error',code:'ROOM_FULL',message:'Aucun emplacement libre.'});
  bgLeaveCurrentRoom(ws,true);
  const player={slot,token:makeToken(),clientId:String(m.clientId||ws.clientId||'').slice(0,80),name:cleanName(m.name||ws.playerName),ws:null,connected:true,disconnectedAt:null,cleanupTimer:null};
  room.players.set(slot,player); bgAttachPlayer(ws,room,player);
  send(ws,{type:'bg_room_joined',roomCode:code,slot,resumeToken:player.token,state:bgPublicState(room,slot)});
  bgSendState(room);
}
function bgResumeRoom(ws,m){
  const code=cleanCode(m.roomCode), token=String(m.token||''), room=bgRooms.get(code);
  if(!room) return send(ws,{type:'bg_error',code:'ROOM_EXPIRED',message:'La salle Battleground n’existe plus.'});
  const player=[...room.players.values()].find(p=>p.token===token);
  if(!player) return send(ws,{type:'bg_error',code:'RESUME_DENIED',message:'Jeton de reconnexion Battleground invalide.'});
  bgLeaveCurrentRoom(ws,true);
  player.name=cleanName(m.name||player.name);
  bgAttachPlayer(ws,room,player);
  const state=bgPublicState(room,player.slot);
  send(ws,{type:'bg_room_resumed',roomCode:code,slot:player.slot,resumeToken:player.token,state});
  bgSendState(room);
  bgSendMatchSync(ws,room,player.slot);
}
function bgConvertDisconnectedToBot(room,player){
  const ent=bgHumanEntity(room.match,player.slot);
  if(ent && ent.alive){
    ent.human=false; ent.connected=true; ent.slot=null; ent.name=player.name+' • BOT';
  }
  room.players.delete(player.slot);
  if(room.hostSlot===player.slot){
    const slots=[...room.players.keys()].sort((a,b)=>a-b);
    room.hostSlot=slots[0]||null;
  }
  if(!room.players.size && (!room.match || room.match.phase==='end')) bgDestroyRoom(room);
  else bgSendState(room);
}
function bgForfeitEntity(room,entity,reason='Abandon'){
  const m=room.match;
  if(!m || !entity || !entity.alive) return;
  const before=entity.hp;
  entity.hp=0; entity.alive=false; entity.losses++; entity.winStreak=0; entity.eliminatedRound=m.round;
  const d=[...m.duels.values()].find(x=>!x.finished&&(x.aId===entity.id||(!x.ghost&&x.bId===entity.id)));
  if(d){
    bgClearTimer(d,'planTimer'); bgClearTimer(d,'phaseTimer');
    const a=bgTeamById(m,d.aId), b=d.ghost?null:bgTeamById(m,d.bId);
    let winner=null, loser=entity;
    if(d.ghost){
      d.finished=true;
      d.result={ghost:true,aId:a.id,ghostSourceId:d.ghostSourceId,ghostTeam:[...d.ghostTeam],ghostName:d.ghostName||'SPECTRE',
        aBefore:before,aAfter:0,ghostWon:true,ghostLost:false,draw:false,winnerId:null,loserId:a.id,survivors:2,damage:before,reason};
    }else{
      winner=(a.id===entity.id)?b:a;
      if(winner){winner.wins++;winner.winStreak++;winner.bestStreak=Math.max(winner.bestStreak,winner.winStreak);}
      d.finished=true;
      d.result={ghost:false,aId:a.id,bId:b.id,aBefore:d.aBefore,bBefore:d.bBefore,aAfter:a.hp,bAfter:b.hp,
        winnerId:winner?winner.id:null,loserId:entity.id,survivors:2,damage:before,draw:false,reason};
    }
    m.results.push(d.result);
    bgNotifyDuelWaiting(room,d);
    bgMaybeRoundComplete(room);
  }
  bgSendState(room);
}
function bgLeaveCurrentRoom(ws,explicit=false){
  if(!ws.bgRoomCode || ws.bgSlot==null) return;
  const room=bgRooms.get(ws.bgRoomCode); if(!room) return bgDetachSocket(ws);
  const p=room.players.get(ws.bgSlot); if(!p || p.token!==ws.bgResumeToken) return bgDetachSocket(ws);
  if(!explicit && p.ws && p.ws!==ws) return bgDetachSocket(ws);
  if(explicit){
    if(p.cleanupTimer) clearTimeout(p.cleanupTimer);
    const ent=bgHumanEntity(room.match,p.slot);
    if(room.match && room.match.phase!=='end' && ent && ent.alive) bgForfeitEntity(room,ent,'Le joueur a quitté le Battleground.');
    room.players.delete(p.slot); bgDetachSocket(ws);
    if(!room.players.size){ if(!room.match || room.match.phase==='end') bgDestroyRoom(room); return; }
    if(room.hostSlot===p.slot) room.hostSlot=[...room.players.keys()].sort((a,b)=>a-b)[0];
    bgSendState(room); return;
  }
  p.connected=false; p.ws=null; p.disconnectedAt=Date.now();
  const ent=bgHumanEntity(room.match,p.slot); if(ent) ent.connected=false;
  if(p.cleanupTimer) clearTimeout(p.cleanupTimer);
  p.cleanupTimer=setTimeout(()=>{
    const cur=room.players.get(p.slot);
    if(!cur || cur.connected || cur.token!==p.token) return;
    bgConvertDisconnectedToBot(room,cur);
  },DISCONNECT_GRACE_MS);
  bgSendState(room); bgDetachSocket(ws);
}
function bgHumanSlots(room,onlyAlive=false){
  const out=[];
  for(const p of room.players.values()){
    if(!p.connected) continue;
    const ent=bgHumanEntity(room.match,p.slot);
    if(onlyAlive && (!ent||!ent.alive)) continue;
    out.push(p.slot);
  }
  return out;
}
function bgSmartBotPlans(engine,slot){
  const st=engine.getState(), side=slot===1?'player':'enemy', foe=slot===1?'enemy':'player';
  const mine=st.teams[slot]||[], theirs=st.teams[slot===1?2:1]||[], out=[];
  const livingMine=mine.filter(f=>f.alive), livingFoes=theirs.filter(f=>f.alive);
  for(const f of livingMine){
    const avail=['skill3','skill2','skill1'].filter(k=>f[k] && (f['cd'+k.slice(-1)]||0)<=0);
    let key='skill1';
    if(avail.includes('skill3') && Math.random()<0.48) key='skill3';
    else if(avail.includes('skill2') && Math.random()<0.58) key='skill2';
    else if(avail.includes('skill1')) key='skill1';
    else key=avail[0]||'skill1';
    const kind=f[key]?.target||'enemy';
    let target=null;
    if(kind==='self') target={side,index:f.index};
    else if(kind==='ally'){
      const ally=[...livingMine].sort((a,b)=>(a.hp/a.maxHp)-(b.hp/b.maxHp))[0]||f;
      target={side,index:ally.index};
    }else{
      const enemy=[...livingFoes].sort((a,b)=>(a.hp/a.maxHp)-(b.hp/b.maxHp))[0];
      if(enemy) target={side:foe,index:enemy.index};
    }
    out.push({side,actor:{side,index:f.index},skill:key,target});
  }
  return out;
}
function bgRunBotDuel(engine,maxRounds=60){
  let result=null;
  for(let i=0;i<maxRounds;i++){
    result=engine.resolveRound(bgSmartBotPlans(engine,1),bgSmartBotPlans(engine,2));
    if(result.ended) break;
  }
  return result;
}
function bgChooseGhostFighter(match,pool){
  return [...pool].sort((a,b)=>(match.ghostCounts.get(a.id)||0)-(match.ghostCounts.get(b.id)||0) || a.losses-b.losses || a.seed-b.seed)[0];
}
function bgBestOpponent(match,a,cands){
  return [...cands].sort((x,y)=>
    bgPairCount(match,a.id,x.id)-bgPairCount(match,a.id,y.id) ||
    bgPairLast(match,a.id,x.id)-bgPairLast(match,a.id,y.id) ||
    x.losses-y.losses ||
    x.seed-y.seed
  )[0];
}
function bgBuildPairings(match){
  let pool=bgShuffle(bgAlive(match));
  const pairings=[];
  if(pool.length%2===1){
    const fighter=bgChooseGhostFighter(match,pool);
    pool=pool.filter(t=>t.id!==fighter.id);
    const dead=[...match.teams].filter(t=>!t.alive&&t.id!==fighter.id).sort((a,b)=>(b.eliminatedRound||0)-(a.eliminatedRound||0)||b.seed-a.seed);
    const source=dead[0]||null;
    if(source){
      match.ghostCounts.set(fighter.id,(match.ghostCounts.get(fighter.id)||0)+1);
      pairings.push({id:'g'+match.round+'-'+fighter.id,aId:fighter.id,ghost:true,ghostSourceId:source.id,ghostTeam:[...source.team],ghostName:'SPECTRE '+source.name});
    }
  }
  while(pool.length>=2){
    const a=pool.shift();
    const b=bgBestOpponent(match,a,pool);
    pool=pool.filter(t=>t.id!==b.id);
    bgRecordPair(match,a.id,b.id);
    pairings.push({id:'r'+match.round+'-'+pairings.length,aId:a.id,bId:b.id,ghost:false});
  }
  return pairings;
}
function bgEntityHumanPlayer(room,entity){
  return entity && entity.human && entity.slot!=null ? room.players.get(entity.slot) : null;
}
function bgDuelHumans(room,duel){
  const m=room.match, arr=[];
  const a=bgTeamById(m,duel.aId), b=duel.ghost?null:bgTeamById(m,duel.bId);
  const pa=bgEntityHumanPlayer(room,a); if(pa) arr.push({player:pa,duelSlot:1,entity:a});
  const pb=bgEntityHumanPlayer(room,b); if(pb) arr.push({player:pb,duelSlot:2,entity:b});
  return arr;
}
function bgSendDuel(room,duel,data){
  for(const h of bgDuelHumans(room,duel)) if(h.player.connected&&h.player.ws) send(h.player.ws,{...data,duelSlot:h.duelSlot});
}
function bgDuelOpponent(match,duel,duelSlot){
  if(duelSlot===1){
    if(duel.ghost) return {id:duel.ghostSourceId,name:duel.ghostName,team:[...duel.ghostTeam],hp:20,ghost:true};
    const b=bgTeamById(match,duel.bId); return b?bgWireTeam(b):null;
  }
  const a=bgTeamById(match,duel.aId); return a?bgWireTeam(a):null;
}
function bgBeginDuelPlanning(room,duel,first=false){
  const m=room.match; if(!m||duel.finished) return;
  bgClearTimer(duel,'planTimer'); bgClearTimer(duel,'phaseTimer');
  duel.phase='planning'; duel.combatRound=duel.engine.getRound(); duel.plans={1:null,2:null}; duel.deadline=Date.now()+PLAN_MS; duel.sequence++;
  const state=wireState(duel.engine.getState());
  for(const h of bgDuelHumans(room,duel)){
    if(!h.player.connected||!h.player.ws) continue;
    send(h.player.ws,{
      type:first?'bg_duel_started':'bg_duel_round_begin',roomCode:room.code,matchId:m.id,duelId:duel.id,
      bgRound:m.round,combatRound:duel.combatRound,deadline:duel.deadline,sequence:duel.sequence,duelSlot:h.duelSlot,
      opponent:bgDuelOpponent(m,duel,h.duelSlot),state
    });
  }
  const humans=bgDuelHumans(room,duel).filter(h=>h.player.connected);
  if(!humans.length){ bgResolveDuelRound(room,duel,true); return; }
  duel.planTimer=setTimeout(()=>bgResolveDuelRound(room,duel,true),PLAN_MS+120); duel.planTimer.unref?.();
}
function bgCreateDuel(room,pairing){
  const m=room.match, a=bgTeamById(m,pairing.aId), b=pairing.ghost?null:bgTeamById(m,pairing.bId);
  const teamB=pairing.ghost?pairing.ghostTeam:b.team;
  const engine=createBattleEngine(a.team,teamB);
  const duel={
    ...pairing,engine,phase:'planning',combatRound:1,deadline:0,plans:{1:null,2:null},sequence:0,
    finished:false,result:null,planTimer:null,phaseTimer:null,aBefore:a.hp,bBefore:b?b.hp:20
  };
  m.duels.set(duel.id,duel);
  return duel;
}
function bgStartDuel(room,pairing){
  let duel;
  try{ duel=bgCreateDuel(room,pairing); }
  catch(e){ console.error('BG engine init',e); return; }
  const humans=bgDuelHumans(room,duel).filter(h=>h.player.connected);
  if(!humans.length){
    let result;
    try{ result=bgRunBotDuel(duel.engine); }catch(e){ console.error('BG bot duel',e); }
    if(!result || !result.ended){
      result=duel.engine.resolveRound(duel.engine.fallbackPlans(1),duel.engine.fallbackPlans(2));
    }
    bgFinishDuel(room,duel,result);
    return;
  }
  bgBeginDuelPlanning(room,duel,true);
}
function bgSubmitPlans(ws,m){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode), match=room&&room.match;
  if(!match || match.phase!=='duels') return send(ws,{type:'bg_error',code:'NO_BG_DUEL',message:'Aucun duel Battleground actif.'});
  const entity=bgHumanEntity(match,ws.bgSlot);
  if(!entity || !entity.alive) return send(ws,{type:'bg_error',code:'BG_ELIMINATED',message:'Ton équipe est éliminée.'});
  const duel=[...match.duels.values()].find(d=>!d.finished&&(d.aId===entity.id||(!d.ghost&&d.bId===entity.id)));
  if(!duel || String(m.duelId||'')!==duel.id) return send(ws,{type:'bg_error',code:'BAD_DUEL',message:'Ce duel n’est plus actif.'});
  if(duel.phase!=='planning') return send(ws,{type:'bg_error',code:'NOT_PLANNING',message:'Le duel n’est plus en planification.'});
  const duelSlot=duel.aId===entity.id?1:2;
  if(Number(m.combatRound)!==duel.combatRound) return send(ws,{type:'bg_error',code:'STALE_DUEL_ROUND',message:'Plan reçu pour une ancienne manche du duel.'});
  if(duel.plans[duelSlot]) return send(ws,{type:'bg_plan_ack',duelId:duel.id,combatRound:duel.combatRound,alreadyLocked:true});
  let validation;
  try{ validation=duel.engine.validatePlans(duelSlot,m.plans); }
  catch(e){ console.error('BG validate',e); return send(ws,{type:'bg_error',code:'PLAN_ENGINE',message:'Erreur de validation du plan Battleground.'}); }
  if(!validation.ok) return send(ws,{type:'bg_error',code:'INVALID_PLAN',message:validation.error||'Plan invalide.'});
  duel.plans[duelSlot]=validation.plans; duel.sequence++;
  send(ws,{type:'bg_plan_ack',duelId:duel.id,combatRound:duel.combatRound,sequence:duel.sequence});
  const ready=[1,2].filter(s=>!!duel.plans[s]);
  bgSendDuel(room,duel,{type:'bg_duel_status',duelId:duel.id,bgRound:match.round,combatRound:duel.combatRound,deadline:duel.deadline,sequence:duel.sequence,readySides:ready});
  const humans=bgDuelHumans(room,duel).filter(h=>h.player.connected).map(h=>h.duelSlot);
  if(humans.every(s=>!!duel.plans[s])) bgResolveDuelRound(room,duel,false);
}
function bgResolveDuelRound(room,duel,timeoutTriggered=false){
  if(!duel || duel.finished || duel.phase!=='planning') return;
  bgClearTimer(duel,'planTimer'); duel.phase='resolution'; duel.deadline=0; duel.sequence++;
  let p1=duel.plans[1], p2=duel.plans[2];
  const m=room.match, a=bgTeamById(m,duel.aId), b=duel.ghost?null:bgTeamById(m,duel.bId);
  const pa=bgEntityHumanPlayer(room,a), pb=bgEntityHumanPlayer(room,b);
  if(!p1) p1=(pa&&pa.connected)?duel.engine.fallbackPlans(1):bgSmartBotPlans(duel.engine,1);
  if(!p2) p2=(pb&&pb.connected)?duel.engine.fallbackPlans(2):bgSmartBotPlans(duel.engine,2);
  let result;
  try{ result=duel.engine.resolveRound(p1,p2); }
  catch(e){ console.error('BG resolve',e); duel.finished=true; return bgMaybeRoundComplete(room); }
  const events=wireEvents(result.events), finalState=wireState(result.finalState);
  const actionCount=events.filter(e=>e.kind==='action'||e.kind==='skip').length;
  const resolutionMs=Math.max(1000,actionCount*560+(events.some(e=>e.kind==='end_round')?500:260));
  bgSendDuel(room,duel,{
    type:'bg_duel_resolution',duelId:duel.id,bgRound:m.round,combatRound:duel.combatRound,sequence:duel.sequence,
    timeoutTriggered,events,finalState,resolutionMs,ended:result.ended,winnerSlot:result.winnerSlot
  });
  if(result.ended){
    duel.phase='end';
    duel.phaseTimer=setTimeout(()=>bgFinishDuel(room,duel,result),resolutionMs); duel.phaseTimer.unref?.();
  }else{
    duel.combatRound=result.finalState.round;
    duel.phaseTimer=setTimeout(()=>bgBeginDuelPlanning(room,duel,false),resolutionMs); duel.phaseTimer.unref?.();
  }
}
function bgApplyDuelResult(room,duel,result){
  const m=room.match, a=bgTeamById(m,duel.aId), b=duel.ghost?null:bgTeamById(m,duel.bId);
  const state=result.finalState||duel.engine.getState(), w=Number(result.winnerSlot);
  if(duel.ghost){
    const live=a; const before=live.hp;
    if(w===2){
      const survivors=(state.teams[2]||[]).filter(f=>f.alive).length||1;
      const damage=bgDamage(m.round,survivors); live.hp=Math.max(0,live.hp-damage);
      live.losses++; live.winStreak=0;
      if(live.hp<=0){live.alive=false;live.eliminatedRound=m.round;}
      return {ghost:true,aId:live.id,ghostSourceId:duel.ghostSourceId,ghostTeam:[...duel.ghostTeam],ghostName:duel.ghostName,
        aBefore:before,aAfter:live.hp,ghostWon:true,ghostLost:false,draw:false,winnerId:null,loserId:live.id,survivors,damage};
    }
    if(w===1){
      live.wins++;live.winStreak++;live.bestStreak=Math.max(live.bestStreak,live.winStreak);
      return {ghost:true,aId:live.id,ghostSourceId:duel.ghostSourceId,ghostTeam:[...duel.ghostTeam],ghostName:duel.ghostName,
        aBefore:before,aAfter:live.hp,ghostWon:false,ghostLost:true,draw:false,winnerId:live.id,loserId:null,survivors:(state.teams[1]||[]).filter(f=>f.alive).length||1,damage:0};
    }
    live.winStreak=0;
    return {ghost:true,aId:live.id,ghostSourceId:duel.ghostSourceId,ghostTeam:[...duel.ghostTeam],ghostName:duel.ghostName,
      aBefore:before,aAfter:live.hp,ghostWon:false,ghostLost:false,draw:true,winnerId:null,loserId:null,survivors:0,damage:0};
  }
  const beforeA=a.hp,beforeB=b.hp;
  if(w===1 || w===2){
    const winner=w===1?a:b, loser=w===1?b:a, ws=w, survivors=(state.teams[ws]||[]).filter(f=>f.alive).length||1;
    const damage=bgDamage(m.round,survivors); loser.hp=Math.max(0,loser.hp-damage);
    winner.wins++; winner.winStreak++; winner.bestStreak=Math.max(winner.bestStreak,winner.winStreak);
    loser.losses++; loser.winStreak=0;
    if(loser.hp<=0){loser.alive=false;loser.eliminatedRound=m.round;}
    return {ghost:false,aId:a.id,bId:b.id,aBefore:beforeA,bBefore:beforeB,aAfter:a.hp,bAfter:b.hp,
      winnerId:winner.id,loserId:loser.id,survivors,damage,draw:false};
  }
  a.winStreak=0;b.winStreak=0;
  return {ghost:false,aId:a.id,bId:b.id,aBefore:beforeA,bBefore:beforeB,aAfter:a.hp,bAfter:b.hp,
    winnerId:null,loserId:null,survivors:0,damage:0,draw:true};
}
function bgFinishDuel(room,duel,result){
  if(!duel || duel.finished) return;
  bgClearTimer(duel,'planTimer'); bgClearTimer(duel,'phaseTimer');
  duel.finished=true; duel.phase='end';
  const applied=bgApplyDuelResult(room,duel,result);
  duel.result=applied; room.match.results.push(applied);
  bgNotifyDuelWaiting(room,duel);
  bgSendState(room);
  bgMaybeRoundComplete(room);
}
function bgNotifyDuelWaiting(room,duel){
  const m=room.match;
  const finished=[...m.duels.values()].filter(d=>d.finished).length, total=m.duels.size;
  bgSendDuel(room,duel,{type:'bg_round_waiting',round:m.round,finished,total});
}
function bgMaybeRoundComplete(room){
  const m=room.match; if(!m || m.phase!=='duels') return;
  if([...m.duels.values()].some(d=>!d.finished)) return;
  m.phase='round_reveal'; m.roundAcks=new Set();
  const results=[...m.results];
  for(const p of room.players.values()){
    if(p.connected&&p.ws) send(p.ws,{type:'bg_round_reveal',roomCode:room.code,matchId:m.id,round:m.round,results,state:bgPublicState(room,p.slot)});
  }
  bgSendState(room);
  bgClearTimer(m,'revealTimer');
  m.revealTimer=setTimeout(()=>bgAfterReveal(room),25_000); m.revealTimer.unref?.();
}
function bgRoundAck(ws){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode), m=room&&room.match;
  if(!m || m.phase!=='round_reveal') return;
  m.roundAcks.add(ws.bgSlot);
  const needed=bgHumanSlots(room,false);
  if(needed.every(s=>m.roundAcks.has(s))) bgAfterReveal(room);
}
function bgAfterReveal(room){
  const m=room.match;if(!m||m.phase!=='round_reveal')return;
  bgClearTimer(m,'revealTimer');
  if(bgAlive(m).length<=1) return bgEndMatch(room);
  if(m.round===6) return bgBeginRedistribution(room);
  m.round++; bgBeginRound(room);
}
function bgBeginRound(room){
  const m=room.match;if(!m||m.phase==='end')return;
  m.phase='duels';m.results=[];m.duels=new Map();m.pairings=bgBuildPairings(m);
  bgSendState(room);
  for(const pairing of m.pairings) bgStartDuel(room,pairing);
  bgMaybeRoundComplete(room);
}
function bgBotWantsReroll(t){
  const total=t.wins+t.losses;if(total<=0)return Math.random()<.5;
  const rate=t.wins/total;
  let chance=rate>=.72?.08:rate>=.55?.25:rate>=.38?.55:.82;
  if(t.winStreak>=3)chance*=.25;
  return Math.random()<chance;
}
function bgBeginRedistribution(room){
  const m=room.match; m.phase='redistribution';
  const choices=new Map(),botChoices=new Map();
  for(const t of bgAlive(m)) if(!t.human) botChoices.set(t.id,bgBotWantsReroll(t));
  m.redistribution={choices,botChoices,deadline:Date.now()+20_000,timer:null,acks:new Set(),ackTimer:null,result:null};
  for(const p of room.players.values()){
    const ent=bgHumanEntity(m,p.slot);
    if(p.connected&&p.ws&&ent&&ent.alive) send(p.ws,{type:'bg_redistribution_offer',round:7,team:[...ent.team],hp:ent.hp,wins:ent.wins,deadline:m.redistribution.deadline});
  }
  bgSendState(room);
  m.redistribution.timer=setTimeout(()=>bgResolveRedistribution(room),20_000);m.redistribution.timer.unref?.();
}
function bgRedistributionChoice(ws,msg){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode),m=room&&room.match,r=m&&m.redistribution;
  if(!m||m.phase!=='redistribution'||!r)return;
  const ent=bgHumanEntity(m,ws.bgSlot); if(!ent||!ent.alive)return;
  if(!r.choices.has(ent.id)) r.choices.set(ent.id,!!msg.change);
  const livingHumans=bgAlive(m).filter(t=>t.human&&t.slot!=null&&room.players.get(t.slot)?.connected);
  if(livingHumans.every(t=>r.choices.has(t.id))) bgResolveRedistribution(room);
}
function bgResolveRedistribution(room){
  const m=room.match,r=m&&m.redistribution;if(!m||m.phase!=='redistribution'||!r||r.result)return;
  if(r.timer){clearTimeout(r.timer);r.timer=null;}
  const living=bgAlive(m), decisions=new Map();
  for(const t of living){
    if(t.human) decisions.set(t.id,r.choices.has(t.id)?r.choices.get(t.id):false);
    else decisions.set(t.id,!!r.botChoices.get(t.id));
  }
  const keepKeys=new Set(living.filter(t=>!decisions.get(t.id)).map(t=>bgPairKey(t.team)));
  let available=bgShuffle(bgAllValidPairs()).filter(p=>!keepKeys.has(bgPairKey(p)));
  const used=new Set(keepKeys), changed=new Set();
  for(const t of living.filter(x=>decisions.get(x.id))){
    const oldKey=bgPairKey(t.team);
    let idx=available.findIndex(p=>!used.has(bgPairKey(p))&&bgPairKey(p)!==oldKey);
    if(idx<0)idx=available.findIndex(p=>!used.has(bgPairKey(p)));
    if(idx<0)continue;
    const nt=available.splice(idx,1)[0]; t.team=[...nt]; used.add(bgPairKey(nt)); changed.add(t.id);
  }
  r.result={decisions,changed};
  const summary=living.map(t=>({id:t.id,slot:t.slot,name:t.name,changed:changed.has(t.id),team:[...t.team]}));
  for(const p of room.players.values()){
    if(!p.connected||!p.ws)continue;
    const ent=bgHumanEntity(m,p.slot);
    send(p.ws,{type:'bg_redistribution_result',changed:!!(ent&&changed.has(ent.id)),myTeam:ent?[...ent.team]:null,teams:summary,state:bgPublicState(room,p.slot)});
  }
  bgSendState(room);
  r.ackTimer=setTimeout(()=>bgFinishRedistribution(room),15_000);r.ackTimer.unref?.();
}
function bgRedistributionAck(ws){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode),m=room&&room.match,r=m&&m.redistribution;
  if(!m||m.phase!=='redistribution'||!r||!r.result)return;
  r.acks.add(ws.bgSlot);
  const needed=bgHumanSlots(room,false);
  if(needed.every(s=>r.acks.has(s))) bgFinishRedistribution(room);
}
function bgFinishRedistribution(room){
  const m=room.match,r=m&&m.redistribution;if(!m||m.phase!=='redistribution'||!r)return;
  if(r.ackTimer){clearTimeout(r.ackTimer);r.ackTimer=null;}
  m.redistribution=null;m.round=7;bgBeginRound(room);
}
function bgStartMatch(ws){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode);
  if(!room)return send(ws,{type:'bg_error',code:'NOT_IN_BG_ROOM',message:'Tu n’es dans aucune salle Battleground.'});
  if(ws.bgSlot!==room.hostSlot)return send(ws,{type:'bg_error',code:'HOST_ONLY',message:'Seul l’hôte peut lancer le Battleground.'});
  const humans=[...room.players.values()].filter(p=>p.connected);
  if(humans.length<BG_MIN_HUMANS)return send(ws,{type:'bg_error',code:'NEED_PLAYERS',message:'Il faut au moins 2 joueurs humains connectés.'});
  if(room.match)bgClearMatchTimers(room.match);
  const pairs=bgUniqueTeams(BG_MAX_TEAMS);
  const teams=[];let seed=0;
  const sorted=[...room.players.values()].filter(p=>p.connected).sort((a,b)=>a.slot-b.slot);
  for(const p of sorted){
    teams.push({id:'h'+p.slot,slot:p.slot,name:p.name,team:[...pairs[seed]],hp:BG_TEAM_HP,alive:true,human:true,connected:true,wins:0,losses:0,winStreak:0,bestStreak:0,seed,eliminatedRound:null});seed++;
  }
  let botIndex=0;
  while(teams.length<BG_MAX_TEAMS){
    teams.push({id:'b'+botIndex,slot:null,name:BG_BOT_NAMES[botIndex%BG_BOT_NAMES.length],team:[...pairs[seed]],hp:BG_TEAM_HP,alive:true,human:false,connected:true,wins:0,losses:0,winStreak:0,bestStreak:0,seed,eliminatedRound:null});
    botIndex++;seed++;
  }
  room.match={id:makeMatchId(),phase:'assignment',round:1,teams,pairCounts:new Map(),pairLast:new Map(),ghostCounts:new Map(),duels:new Map(),pairings:[],results:[],assignmentAcks:new Set(),assignmentTimer:null,revealTimer:null,redistribution:null};
  for(const p of room.players.values()){
    if(!p.connected||!p.ws)continue;
    const ent=bgHumanEntity(room.match,p.slot);
    send(p.ws,{type:'bg_assignment',roomCode:room.code,matchId:room.match.id,team:ent?[...ent.team]:[],state:bgPublicState(room,p.slot)});
  }
  bgSendState(room);
  room.match.assignmentTimer=setTimeout(()=>bgBeginAfterAssignments(room),15_000);room.match.assignmentTimer.unref?.();
}
function bgAssignmentAck(ws){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode),m=room&&room.match;
  if(!m||m.phase!=='assignment')return;
  m.assignmentAcks.add(ws.bgSlot);
  const needed=bgHumanSlots(room,false);
  if(needed.every(s=>m.assignmentAcks.has(s))) bgBeginAfterAssignments(room);
}
function bgBeginAfterAssignments(room){
  const m=room.match;if(!m||m.phase!=='assignment')return;
  bgClearTimer(m,'assignmentTimer');m.round=1;bgBeginRound(room);
}
function bgEndMatch(room){
  const m=room.match;if(!m||m.phase==='end')return;
  bgClearMatchTimers(m);m.phase='end';
  const ranking=bgRanked(m),winner=ranking[0]||null;
  const placeById=new Map(ranking.map((t,i)=>[t.id,i+1]));
  for(const p of room.players.values()){
    if(!p.connected||!p.ws)continue;
    const ent=bgHumanEntity(m,p.slot);
    send(p.ws,{type:'bg_match_end',roomCode:room.code,matchId:m.id,winnerId:winner?.id||null,winnerName:winner?.name||null,place:ent?(placeById.get(ent.id)||8):8,state:bgPublicState(room,p.slot)});
  }
  bgSendState(room);
  if(!room.players.size) bgDestroyRoom(room);
}
function bgSendMatchSync(ws,room,slot){
  const m=room.match;if(!m||m.phase==='end')return;
  const ent=bgHumanEntity(m,slot);
  if(!ent)return;
  if(m.phase==='assignment'){
    send(ws,{type:'bg_assignment',roomCode:room.code,matchId:m.id,team:[...ent.team],state:bgPublicState(room,slot)});return;
  }
  if(m.phase==='redistribution'){
    const r=m.redistribution;
    if(ent.alive && r && !r.result) send(ws,{type:'bg_redistribution_offer',round:7,team:[...ent.team],hp:ent.hp,wins:ent.wins,deadline:r.deadline});
    else if(r&&r.result){
      const summary=bgAlive(m).map(t=>({id:t.id,slot:t.slot,name:t.name,changed:r.result.changed.has(t.id),team:[...t.team]}));
      send(ws,{type:'bg_redistribution_result',changed:r.result.changed.has(ent.id),myTeam:[...ent.team],teams:summary,state:bgPublicState(room,slot)});
    }
    return;
  }
  const duel=[...m.duels.values()].find(d=>!d.finished&&(d.aId===ent.id||(!d.ghost&&d.bId===ent.id)));
  if(duel){
    const ds=duel.aId===ent.id?1:2;
    send(ws,{type:'bg_duel_sync',roomCode:room.code,matchId:m.id,duelId:duel.id,bgRound:m.round,combatRound:duel.combatRound,phase:duel.phase,deadline:duel.deadline,duelSlot:ds,opponent:bgDuelOpponent(m,duel,ds),state:wireState(duel.engine.getState())});
  }else if(m.phase==='round_reveal'){
    send(ws,{type:'bg_round_reveal',roomCode:room.code,matchId:m.id,round:m.round,results:[...m.results],state:bgPublicState(room,slot)});
  }else{
    send(ws,{type:'bg_match_state',state:bgPublicState(room,slot)});
  }
}
function bgForfeitMatch(ws){
  const room=ws.bgRoomCode&&bgRooms.get(ws.bgRoomCode),m=room&&room.match;if(!m||m.phase==='end')return;
  const ent=bgHumanEntity(m,ws.bgSlot);if(ent&&ent.alive)bgForfeitEntity(room,ent,'Abandon du Battleground.');
}

function handleMessage(ws, text) {
  let m; try { m=JSON.parse(text); } catch { return send(ws,{type:'error',code:'BAD_JSON',message:'Message JSON invalide.'}); }
  if(!m || typeof m.type!=='string') return;
  switch(m.type){
    case 'hello':ws.clientId=String(m.clientId||'').slice(0,80);ws.playerName=cleanName(m.name);send(ws,{type:'hello_ack',serverTime:Date.now(),protocol:69,authoritativeCombat:true,battlegroundMulti:true});break;
    case 'create_room':createRoom(ws,m);break;
    case 'join_room':joinRoom(ws,m);break;
    case 'resume':resumeRoom(ws,m);break;
    case 'team_update':updateTeam(ws,m);break;
    case 'sync_test':syncTest(ws,m);break;
    case 'start_match':startMatch(ws,m);break;
    case 'submit_plans':submitPlans(ws,m);break;
    case 'forfeit_match':forfeitMatch(ws);break;
    case 'bg_create_room':bgCreateRoom(ws,m);break;
    case 'bg_join_room':bgJoinRoom(ws,m);break;
    case 'bg_resume':bgResumeRoom(ws,m);break;
    case 'bg_start':bgStartMatch(ws,m);break;
    case 'bg_assignment_ack':bgAssignmentAck(ws);break;
    case 'bg_submit_plans':bgSubmitPlans(ws,m);break;
    case 'bg_round_ack':bgRoundAck(ws);break;
    case 'bg_redistribution_choice':bgRedistributionChoice(ws,m);break;
    case 'bg_redistribution_ack':bgRedistributionAck(ws);break;
    case 'bg_forfeit_match':bgForfeitMatch(ws);break;
    case 'bg_leave_room':bgLeaveCurrentRoom(ws,true);break;
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
    const payload=Buffer.from(ws.buffer.subarray(offset,offset+len));for(let i=0;i<payload.length;i++)payload[i]^=mask[i%4];ws.buffer=ws.buffer.subarray(offset+len);ws.lastSeen=Date.now();
    if(opcode===0x1)handleMessage(ws,payload.toString('utf8'));else if(opcode===0x8){closeWs(ws,1000,'Bye');return}else if(opcode===0x9)rawSend(ws,0xA,payload);else if(opcode===0xA)ws.lastPong=Date.now();else if(opcode!==0x0){closeWs(ws,1003,'Unsupported opcode');return}
  }
}

server.on('upgrade',(req,socket,head)=>{
  const key=req.headers['sec-websocket-key'];const upgrade=String(req.headers.upgrade||'').toLowerCase();if(upgrade!=='websocket'||!key){socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');socket.destroy();return}
  const accept=wsAccept(key);socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');socket.setNoDelay(true);socket.setKeepAlive(true,15000);
  const ws={id:++connectionCounter,socket,buffer:Buffer.alloc(0),closed:false,lastSeen:Date.now(),lastPong:Date.now(),clientId:null,playerName:'Joueur',roomCode:null,slot:null,resumeToken:null,bgRoomCode:null,bgSlot:null,bgResumeToken:null};clients.add(ws);
  socket.on('data',chunk=>parseFrames(ws,chunk));socket.on('error',()=>{});socket.on('close',()=>{if(!ws.closed)ws.closed=true;clients.delete(ws);leaveCurrentRoom(ws,false);bgLeaveCurrentRoom(ws,false)});if(head&&head.length)parseFrames(ws,head);
});

const heartbeat=setInterval(()=>{const now=Date.now();for(const ws of clients){if(ws.closed)continue;if(now-ws.lastSeen>45_000&&now-ws.lastPong>45_000){closeWs(ws,1001,'Heartbeat timeout');continue}rawSend(ws,0x9,Buffer.from('hb'))}},15_000);heartbeat.unref();
server.listen(PORT,HOST,()=>{console.log(`Chibi Fighter V69: http://localhost:${PORT}`);console.log(`WebSocket: ws://localhost:${PORT}`);console.log('Arena autoritaire + Battleground 2-8: ACTIFS')});
function shutdown(){clearInterval(heartbeat);for(const room of rooms.values()){clearMatchTimers(room.match);for(const p of room.players.values())if(p.cleanupTimer)clearTimeout(p.cleanupTimer)}for(const room of bgRooms.values()){bgClearMatchTimers(room.match);for(const p of room.players.values())if(p.cleanupTimer)clearTimeout(p.cleanupTimer)}for(const ws of clients)closeWs(ws,1001,'Server shutdown');server.close(()=>process.exit(0));setTimeout(()=>process.exit(0),1200).unref()}
process.on('SIGINT',shutdown);process.on('SIGTERM',shutdown);
