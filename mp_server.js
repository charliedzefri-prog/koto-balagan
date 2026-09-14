// Кото-Балаган — multiplayer server (Node + ws)
// Серирует: игру, общий чат (WS + HTTP API), профили, друзей, живой PvP (релей)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ROOT = __dirname;
const STATE_FILE = path.join(ROOT, 'mp_state.json');

// ---------- state ----------
let state = { players: {}, chat: [] };
try {
  const old = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  if (old && typeof old.players === 'object') state.players = old.players;
  if (old && Array.isArray(old.chat)) state.chat = old.chat;
} catch (e) {}
// миграция истории из старого python-сервера
try {
  if (!state.chat.length) {
    const cl = JSON.parse(fs.readFileSync(path.join(ROOT, 'chatlog.json'), 'utf8'));
    if (Array.isArray(cl)) state.chat = cl.slice(-60).map(m => ({ name: m.name, avatar: '', text: m.text, ts: m.ts }));
  }
} catch (e) {}
let saveT = null;
function saveState() {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    try { fs.writeFileSync(STATE_FILE, JSON.stringify({ players: state.players, chat: state.chat.slice(-100) })); } catch (e) {}
    ghSave();
  }, 400);
}

// ---------- persistent state: GitHub repo (survives redeploys, ephemeral disk) ----------
const GH = {
  token: process.env.STATE_GH_TOKEN || '',
  repo: process.env.STATE_GH_REPO || '', // owner/name
  file: process.env.STATE_GH_FILE || 'state.json',
  branch: process.env.STATE_GH_BRANCH || '', // branch to store state (isolated from deploys)
  sha: null
};
const ghUrl = () => 'https://api.github.com/repos/' + GH.repo + '/contents/' + GH.file + (GH.branch ? '?ref=' + GH.branch : '');
function ghHeaders() {
  return { Authorization: 'Bearer ' + GH.token, Accept: 'application/vnd.github+json', 'Content-Type': 'application/json' };
}
async function ghRefreshSha() {
  try {
    const g = await fetch(ghUrl(), { headers: ghHeaders() });
    if (g.ok) GH.sha = (await g.json()).sha;
    else if (g.status === 404) GH.sha = null;
  } catch (e) {}
}
async function ghPush() {
  if (!GH.token || !GH.repo) return;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const content = Buffer.from(JSON.stringify({ players: state.players, chat: state.chat.slice(-100) })).toString('base64');
      const body = { message: 'state ' + new Date().toISOString(), content };
      if (GH.sha) body.sha = GH.sha;
      if (GH.branch) body.branch = GH.branch;
      const r = await fetch(ghUrl(), { method: 'PUT', headers: ghHeaders(), body: JSON.stringify(body) });
      if (r.ok) { const d = await r.json(); GH.sha = d.content && d.content.sha; return; }
      if (r.status === 409 && attempt === 0) { await ghRefreshSha(); continue; }
      console.log('gh push http ' + r.status);
      return;
    } catch (e) { console.log('gh push error ' + e.message); return; }
  }
}
let ghT = null;
function ghSave() {
  if (!GH.token || !GH.repo) return;
  clearTimeout(ghT);
  ghT = setTimeout(ghPush, 15000);
}
async function ghLoad() {
  if (!GH.token || !GH.repo) return;
  try {
    const r = await fetch(ghUrl(), { headers: ghHeaders() });
    if (r.status === 404) { console.log('gh state: none yet, starting fresh'); return; }
    if (!r.ok) { console.log('gh state load http ' + r.status); return; }
    const d = await r.json();
    GH.sha = d.sha;
    const old = JSON.parse(Buffer.from(d.content, 'base64').toString('utf8'));
    if (old && typeof old.players === 'object') state.players = old.players;
    if (old && Array.isArray(old.chat)) state.chat = old.chat.slice(-200);
    Object.values(state.players).forEach(p => { p.online = false; });
    console.log('gh state loaded: ' + Object.keys(state.players).length + ' players, ' + state.chat.length + ' msgs');
  } catch (e) { console.log('gh state load error ' + e.message); }
}

// ---------- http ----------
const srv = http.createServer((req, res) => {
  const u = (req.url || '/').split('?')[0];
  if (u === '/api/chat' && req.method === 'GET') {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ msgs: state.chat.slice(-60) }));
    return;
  }
  if (u === '/api/chat' && req.method === 'POST') {
    let b = '';
    req.on('data', c => (b += c));
    req.on('end', () => {
      try {
        const d = JSON.parse(b);
        const name = String(d.name || 'Кот').slice(0, 16);
        const text = String(d.text || '').slice(0, 120);
        if (text) {
          const m = { name, avatar: '', text, ts: Math.floor(Date.now() / 1000) };
          state.chat.push(m);
          if (state.chat.length > 200) state.chat.shift();
          saveState();
          broadcast({ t: 'chat', m });
        }
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: true }));
      } catch (e) { res.end('{}'); }
    });
    return;
  }
  if (u === '/api/health') { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ ok: true, online: Object.values(state.players).filter(p => p.online).length })); return; }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  fs.readFile(path.join(ROOT, 'index.html'), (err, data) => {
    if (err) { res.statusCode = 404; res.end('not found'); } else res.end(data);
  });
});

// ---------- websocket ----------
const wss = new WebSocketServer({ server: srv, path: '/ws' });
const rooms = {};   // room -> {a:ws, b:ws, hostPid}
const queue = [];   // ws в поиске

function to(ws, msg) { try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); } catch (e) {} }
function broadcast(msg, except) { wss.clients.forEach(ws => { if (ws.player && ws !== except) to(ws, msg); }); }
function findWs(pid) { for (const c of wss.clients) if (c.player && c.player.pid === pid) return c; return null; }
function pbrief(p) { return { pid: p.pid, name: p.name, avatar: p.avatar, rating: p.rating || 100 }; }

function friendsBrief(p) {
  return (p.friends || []).map(fid => {
    const fp = state.players[fid];
    return fp ? pbrief(fp) : { pid: fid, name: '?', avatar: '🐱', rating: 0 };
  });
}
function fListBoth(a, b) {
  if (a.player) to(a, { t: 'f_list', friends: friendsBrief(a.player) });
  if (b.player) to(b, { t: 'f_list', friends: friendsBrief(b.player) });
}
function tryMatch() {
  if (queue.length < 2) return;
  const a = queue.shift(), b = queue.shift();
  if (!a.player || !b.player) {
    if (a.player) queue.push(a);
    if (b.player) queue.push(b);
    return;
  }
  const room = 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 5);
  rooms[room] = { a, b, aPid: a.player.pid, bPid: b.player.pid, hostPid: a.player.pid };
  to(a, { t: 'pvp_found', room, host: a.player.pid, opp: pbrief(b.player) });
  to(b, { t: 'pvp_found', room, host: a.player.pid, opp: pbrief(a.player) });
}

wss.on('connection', ws => {
  ws.player = null;
  const close = () => {
    if (!ws.player) return;
    if (ws.voiceRoom) {
      const rm = rooms[ws.voiceRoom];
      if (rm) { const other = rm.a === ws ? rm.b : rm.a; if (other) to(other, { t: 'vo_state', m: 1 }); }
      ws.voiceRoom = null;
    }
    const p = ws.player;
    p.online = false;
    saveState();
    broadcast({ t: 'presence', pid: p.pid, on: false });
    for (const r of Object.keys(rooms)) {
      const rm = rooms[r];
      if (rm.a === ws || rm.b === ws) {
        const isA = rm.a === ws;
        rm[isA ? 'a' : 'b'] = null;
        const other = isA ? rm.b : rm.a;
        if (other) to(other, { t: 'pvp_cancel', room: r });
        setTimeout(() => { const c = rooms[r]; if (c && !c.a && !c.b) delete rooms[r]; }, 120000);
      }
    }
    const qi = queue.indexOf(ws);
    if (qi >= 0) queue.splice(qi, 1);
    const m = { name: 'СЕРВЕР', avatar: '', text: p.name + ' вышел', ts: Math.floor(Date.now() / 1000) };
    state.chat.push(m);
    if (state.chat.length > 200) state.chat.shift();
    broadcast({ t: 'chat', m });
    saveState();
  };
  ws.on('close', close);
  ws.on('error', close);

  ws.on('message', (raw, isBinary) => {
    if (isBinary) {
      // voice: raw PCM16 16kHz mono -> other player of the same room, no logging
      try {
        if (raw.length > 8192) return;
        const rm = rooms[ws.voiceRoom];
        if (rm) {
          const other = rm.a === ws ? rm.b : rm.a;
          if (other && other.readyState === 1) other.send(raw);
        }
      } catch (e) {}
      return;
    }
    let d;
    try { d = JSON.parse(raw); } catch (e) { return; }
    switch (d.t) {
      case 'join': {
        const pid = String(d.pid || 'u' + crypto.randomBytes(6).toString('hex')).slice(0, 48);
        const name = String(d.name || 'Кот').slice(0, 16);
        const avatar = String(d.avatar || '🐱').slice(0, 8);
        let p = state.players[pid];
        const first = !p;
        p = p || { pid, name, avatar, rating: 100, wins: 0, losses: 0, friends: [], inbox: [], online: false, createdAt: Date.now() };
        p.name = name;
        p.avatar = avatar;
        if (Number.isFinite(+d.rating)) p.rating = +d.rating;
        if (Number.isFinite(+d.wins)) p.wins = +d.wins;
        if (Number.isFinite(+d.losses)) p.losses = +d.losses;
        p.online = true;
        state.players[pid] = p;
        ws.player = p;
        for (const r of Object.keys(rooms)) {
          const rm = rooms[r];
          const slot = (rm.aPid === p.pid && !rm.a && rm.b) ? 'a' : (rm.bPid === p.pid && !rm.b && rm.a) ? 'b' : null;
          if (!slot) continue;
          rm[slot] = ws;
          const otherPid = slot === 'a' ? rm.bPid : rm.aPid;
          const otherP = state.players[otherPid] || { pid: otherPid, name: '?', avatar: '🐱', rating: 0 };
          const other = slot === 'a' ? rm.b : rm.a;
          to(ws, { t: 'pvp_found', room: r, host: rm.hostPid, opp: pbrief(otherP) });
          if (other) to(other, { t: 'pvp_found', room: r, host: rm.hostPid, opp: pbrief(p) });
        }
        const others = Object.values(state.players).filter(x => x !== p).map(pbrief);
        to(ws, { t: 'joined', pid, players: others, friends: friendsBrief(p), requests: friendsBrief(p), chat: state.chat.slice(-40) });
        if (first) {
          const m = { name: 'СЕРВЕР', avatar: '', text: name + ' зашёл', ts: Math.floor(Date.now() / 1000) };
          state.chat.push(m);
          if (state.chat.length > 200) state.chat.shift();
          saveState();
          broadcast({ t: 'chat', m }, ws);
        }
        broadcast({ t: 'presence', pid, on: true, name, avatar, rating: p.rating }, ws);
        saveState();
        break;
      }
      case 'chat': {
        if (!ws.player) break;
        const text = String(d.text || '').slice(0, 120);
        if (!text) break;
        const m = { name: ws.player.name, avatar: ws.player.avatar, text, ts: Math.floor(Date.now() / 1000) };
        state.chat.push(m);
        if (state.chat.length > 200) state.chat.shift();
        saveState();
        broadcast({ t: 'chat', m });
        break;
      }
      case 'f_add': {
        if (!ws.player) break;
        const tgt = String(d.target || '').trim();
        if (!tgt) break;
        let tp = state.players[tgt], tid = tp && tp.pid;
        if (!tp) {
          const list = Object.values(state.players).filter(x => String(x.name).toLowerCase() === tgt.toLowerCase());
          if (list.length) { tp = list[0]; tid = tp.pid; }
        }
        if (!tp) { to(ws, { t: 'f_result', ok: false, msg: 'игрок не найден' }); break; }
        if (tid === ws.player.pid) { to(ws, { t: 'f_result', ok: false, msg: 'это ты сам 😄' }); break; }
        if ((ws.player.friends || []).includes(tid)) { to(ws, { t: 'f_result', ok: false, msg: 'уже в друзьях' }); break; }
        if ((tp.friends || []).includes(ws.player.pid)) {
          ws.player.friends.push(tid);
          tp.friends.push(ws.player.pid);
          ws.player.inbox = (ws.player.inbox || []).filter(x => x !== tid);
          fListBoth(ws, tp);
          to(ws, { t: 'f_result', ok: true, name: tp.name });
          saveState();
          break;
        }
        tp.inbox = tp.inbox || [];
        if (!tp.inbox.includes(ws.player.pid)) tp.inbox.push(ws.player.pid);
        const tw = findWs(tid);
        if (tw) to(tw, { t: 'f_req', from: pbrief(ws.player) });
        to(ws, { t: 'f_result', ok: true, name: tp.name });
        saveState();
        break;
      }
      case 'f_accept': {
        if (!ws.player) break;
        const fid = String(d.pid || '');
        const fp = state.players[fid];
        if (!fp) break;
        ws.player.friends = ws.player.friends || [];
        fp.friends = fp.friends || [];
        if (!ws.player.friends.includes(fid)) ws.player.friends.push(fid);
        if (!fp.friends.includes(ws.player.pid)) fp.friends.push(ws.player.pid);
        ws.player.inbox = (ws.player.inbox || []).filter(x => x !== fid);
        fListBoth(ws, findWs(fid) || { player: fp, _fake: true });
        to(ws, { t: 'f_result', ok: true, name: fp.name });
        saveState();
        break;
      }
      case 'f_decline': {
        if (!ws.player) break;
        ws.player.inbox = (ws.player.inbox || []).filter(x => x !== String(d.pid || ''));
        saveState();
        break;
      }
      case 'pvp_search': {
        if (!ws.player) break;
        if (!queue.includes(ws)) queue.push(ws);
        tryMatch();
        break;
      }
      case 'pvp_cancel': {
        const qi = queue.indexOf(ws);
        if (qi >= 0) queue.splice(qi, 1);
        for (const r of Object.keys(rooms)) {
          const rm = rooms[r];
          if (rm.a === ws) { to(rm.b, { t: 'pvp_cancel', room: r }); delete rooms[r]; }
          else if (rm.b === ws) { to(rm.a, { t: 'pvp_cancel', room: r }); delete rooms[r]; }
        }
        break;
      }
      case 'pvp_invite': {
        if (!ws.player) break;
        const tgt = String(d.target || '');
        const tp = state.players[tgt];
        const room = String(d.room || 'r' + Date.now().toString(36));
        if (!tp || !tp.online) { to(ws, { t: 'invite_result', ok: false, msg: 'соперник не в сети' }); break; }
        rooms[room] = { a: ws, b: null, aPid: ws.player.pid, bPid: null, hostPid: ws.player.pid, invite: tgt };
        const tw = findWs(tgt);
        if (tw) to(tw, { t: 'pvp_invited', room, from: pbrief(ws.player) });
        to(ws, { t: 'invite_result', ok: true });
        setTimeout(() => { const rm = rooms[room]; if (rm && rm.b === null) delete rooms[room]; }, 60000);
        break;
      }
      case 'pvp_accept': {
        if (!ws.player) break;
        const room = String(d.room || '');
        const rm = rooms[room];
        if (!rm || rm.b !== null) break;
        rm.b = ws;
        rm.bPid = ws.player.pid;
        delete rm.invite;
        const hostP = state.players[rm.hostPid];
        to(rm.a, { t: 'pvp_found', room, host: rm.hostPid, opp: pbrief(ws.player) });
        to(rm.b, { t: 'pvp_found', room, host: rm.hostPid, opp: hostP ? pbrief(hostP) : { pid: rm.hostPid, name: '?', avatar: '🐱', rating: 100 } });
        break;
      }
      case 'pvp_reject': {
        const room = String(d.room || '');
        const rm = rooms[room];
        if (rm) { to(rm.a, { t: 'pvp_cancel', room }); delete rooms[room]; }
        break;
      }
      case 'pvp_squad':
      case 'pvp_snap':
      case 'pvp_deploy':
      case 'pvp_bomb':
      case 'pvp_upg':
      case 'pvp_end': {
        const rm = rooms[String(d.room || '')];
        if (!rm) break;
        const other = rm.a === ws ? rm.b : rm.a;
        if (other && other.player) to(other, d);
        break;
      }
      case 'vo_open': {
        const r = String(d.room || '');
        const rm = rooms[r];
        if (rm && (rm.a === ws || rm.b === ws)) {
          ws.voiceRoom = r;
          const other = rm.a === ws ? rm.b : rm.a;
          if (other) to(other, { t: 'vo_state', m: 0 });
        }
        break;
      }
      case 'vo_close': {
        const r = ws.voiceRoom;
        ws.voiceRoom = null;
        if (r) {
          const rm = rooms[r];
          if (rm) { const other = rm.a === ws ? rm.b : rm.a; if (other) to(other, { t: 'vo_state', m: 1 }); }
        }
        break;
      }
      case 'vo_mute': {
        const rm = rooms[ws.voiceRoom];
        if (rm) { const other = rm.a === ws ? rm.b : rm.a; if (other) to(other, { t: 'vo_state', m: d.m ? 1 : 0 }); }
        break;
      }
    }
  });
});

// ---------- ambient bot chat ----------
const BOT_NAMES = ['Кот_Вася', 'МяуКру', 'Барсик228', 'Ночной_Кот', 'Котлета', 'ДонКот'];
const BOT_LINES = [
  'кто в арену? ДонКот ждёт',
  'выпал MR на 17-й крутке — пити — легенда',
  'отличный кот!',
  'кто на живой бой? жми «НАЙТИ СОПЕРНИКА»!',
  'чат работает! 🎉',
  'ивент с Бездной — жёстко',
  'Логово до 10 — база несокрушима',
  'найди соперника — рейтинг +25',
  'привет всем котикам 🐱'
];
setInterval(() => {
  const m = {
    name: BOT_NAMES[(Math.random() * BOT_NAMES.length) | 0],
    avatar: '🐱',
    text: BOT_LINES[(Math.random() * BOT_LINES.length) | 0],
    ts: Math.floor(Date.now() / 1000)
  };
  state.chat.push(m);
  if (state.chat.length > 200) state.chat.shift();
  saveState();
  broadcast({ t: 'chat', m });
}, 45000);
setInterval(() => {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify({ players: state.players, chat: state.chat.slice(-100) })); } catch (e) {}
}, 30000);

ghLoad();
process.on('SIGTERM', () => {
  try {
    clearTimeout(ghT);
    const content = Buffer.from(JSON.stringify({ players: state.players, chat: state.chat.slice(-100) })).toString('base64');
    const body = { message: 'state (shutdown) ' + new Date().toISOString(), content };
    if (GH.sha) body.sha = GH.sha;
    if (GH.branch) body.branch = GH.branch;
    const t = setTimeout(() => process.exit(0), 3000);
    if (GH.token && GH.repo) {
      fetch(ghUrl(), { method: 'PUT', headers: ghHeaders(), body: JSON.stringify(body) }).catch(() => {}).finally(() => { clearTimeout(t); process.exit(0); });
    } else process.exit(0);
  } catch (e) { process.exit(0); }
});
srv.listen(PORT, '0.0.0.0', () => console.log('Кото-Балаган MP server on :' + PORT));
