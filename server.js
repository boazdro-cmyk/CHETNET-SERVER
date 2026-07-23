// Chatnet Relay Server
// A small persistent WebSocket chat backend: users register with just a
// display name (no phone number), can add contacts, create groups, and
// chat in DMs, groups, or one open community/global chat.
//
// Deploy on Render as a Web Service:
//   Build command: npm install
//   Start command: npm start
// The app connects over WebSocket at wss://<your-service>.onrender.com

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');
const webpush = require('web-push');

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'data.json');
const MAX_FILE_BYTES = 3 * 1024 * 1024; // ~3MB raw file size cap (keeps data.json manageable on Render's free disk)

// ---------------- Persistence ----------------
function loadDB() {
  try {
    return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch (e) {
    return { users: {}, contacts: {}, groups: {}, messages: { global: [], dm: {}, group: {} }, readState: { dm: {}, group: {} } };
  }
}
let db = loadDB();
if (!db.readState) db.readState = { dm: {}, group: {} };
if (!db.pushSubs) db.pushSubs = {}; // lowercased name -> array of PushSubscription objects
let vapidWasGenerated = false;
if (!db.vapid) {
  db.vapid = webpush.generateVAPIDKeys(); // { publicKey, privateKey } — generated once, then persisted
  vapidWasGenerated = true;
}
webpush.setVapidDetails('mailto:admin@example.com', db.vapid.publicKey, db.vapid.privateKey);
if (vapidWasGenerated) { try { fs.writeFileSync(DB_FILE, JSON.stringify(db)); } catch (e) {} }

let saveTimer = null;
function saveDB() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(DB_FILE, JSON.stringify(db), () => {});
  }, 150); // small debounce so bursts of messages don't hammer disk I/O
}

function pairKey(a, b) {
  return [a, b].map(s => s.toLowerCase()).sort().join('__');
}
function capMessages(arr) {
  return arr.length > 500 ? arr.slice(-500) : arr;
}

// ---------------- Connected sockets ----------------
const socketsByName = new Map(); // lowercased name -> Set<ws>

function sendTo(name, obj) {
  const set = socketsByName.get(name.toLowerCase());
  if (!set) return;
  const json = JSON.stringify(obj);
  for (const sock of set) {
    if (sock.readyState === WebSocket.OPEN) sock.send(json);
  }
}
function broadcastAll(obj, exceptSock) {
  const json = JSON.stringify(obj);
  for (const set of socketsByName.values()) {
    for (const sock of set) {
      if (sock !== exceptSock && sock.readyState === WebSocket.OPEN) sock.send(json);
    }
  }
}

function conversationsFor(name) {
  const lower = name.toLowerCase();
  const partners = new Set();
  for (const key of Object.keys(db.messages.dm)) {
    const [a, b] = key.split('__');
    if (a === lower || b === lower) {
      // recover original-cased partner name from users directory if possible
      const other = a === lower ? b : a;
      const canonical = Object.keys(db.users).find(u => u.toLowerCase() === other) || other;
      partners.add(canonical);
    }
  }
  return [...partners];
}

function isOnline(name) {
  return socketsByName.has(name.toLowerCase());
}

function sendPushTo(name, title, body) {
  const key = name.toLowerCase();
  const subs = db.pushSubs[key];
  if (!subs || subs.length === 0) return;
  const payload = JSON.stringify({ title, body });
  subs.forEach(sub => {
    webpush.sendNotification(sub, payload).catch(err => {
      // Expired/invalid subscriptions (410/404) get cleaned up so we stop retrying them.
      if (err && (err.statusCode === 410 || err.statusCode === 404)) {
        db.pushSubs[key] = (db.pushSubs[key] || []).filter(s => s.endpoint !== sub.endpoint);
        saveDB();
      }
    });
  });
}

function groupsFor(name) {
  return Object.values(db.groups).filter(g => g.members.some(m => m.toLowerCase() === name.toLowerCase()));
}

// ---------------- HTTP (health check + push public key) ----------------
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  if (req.url === '/vapid-public-key') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end(db.vapid.publicKey);
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Chatnet relay is running.');
});

const wss = new WebSocket.Server({ server, maxPayload: 6 * 1024 * 1024 });

wss.on('connection', (sock) => {
  sock.name = null;

  sock.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    // Must say hello before anything else
    if (msg.type === 'hello') {
      const name = String(msg.name || '').trim().slice(0, 30);
      if (!name) { sendJson(sock, { type: 'error', code: 'bad-name', reason: 'Name required.' }); return; }

      const existingKey = Object.keys(db.users).find(u => u.toLowerCase() === name.toLowerCase());
      const providedToken = typeof msg.token === 'string' ? msg.token : null;

      let userRecord;
      if (existingKey) {
        userRecord = db.users[existingKey];
        // Grandfather in accounts created before tokens existed: first reconnect claims it.
        if (userRecord.token && userRecord.token !== providedToken) {
          sendJson(sock, { type: 'error', code: 'name-taken', reason: 'השם הזה כבר תפוס. תבחרו שם אחר.' });
          return;
        }
        if (!userRecord.token) { userRecord.token = crypto.randomBytes(8).toString('hex'); saveDB(); }
      } else {
        userRecord = { joinedAt: Date.now(), avatar: null, token: crypto.randomBytes(8).toString('hex') };
        db.users[name] = userRecord;
        saveDB();
        broadcastAll({ type: 'usersUpdate', users: Object.keys(db.users) }, sock);
      }

      sock.name = existingKey || name;
      if (!db.contacts[name.toLowerCase()]) db.contacts[name.toLowerCase()] = [];

      const wasOnline = socketsByName.has(name.toLowerCase());
      let set = socketsByName.get(name.toLowerCase());
      if (!set) { set = new Set(); socketsByName.set(name.toLowerCase(), set); }
      set.add(sock);
      if (!wasOnline) broadcastAll({ type: 'presence', name: sock.name, online: true }, sock);

      const avatars = {};
      for (const [uname, udata] of Object.entries(db.users)) avatars[uname.toLowerCase()] = udata.avatar || null;

      sendJson(sock, {
        type: 'welcome',
        name: sock.name,
        token: userRecord.token,
        users: Object.keys(db.users),
        avatars,
        online: [...socketsByName.keys()],
        contacts: db.contacts[name.toLowerCase()],
        groups: groupsFor(sock.name),
        conversations: conversationsFor(sock.name)
      });
      return;
    }

    if (!sock.name) { sendJson(sock, { type: 'error', reason: 'Say hello first.' }); return; }
    const me = sock.name;

    switch (msg.type) {
      case 'addContact': {
        const name = String(msg.name || '').trim().slice(0, 30);
        if (!name) return;
        const list = db.contacts[me.toLowerCase()] || (db.contacts[me.toLowerCase()] = []);
        if (!list.some(c => c.toLowerCase() === name.toLowerCase())) {
          list.push(name);
          saveDB();
        }
        sendJson(sock, { type: 'contacts', contacts: list });
        break;
      }
      case 'removeContact': {
        const name = String(msg.name || '').trim();
        let list = db.contacts[me.toLowerCase()] || [];
        list = list.filter(c => c.toLowerCase() !== name.toLowerCase());
        db.contacts[me.toLowerCase()] = list;
        saveDB();
        sendJson(sock, { type: 'contacts', contacts: list });
        break;
      }
      case 'setAvatar': {
        const avatar = String(msg.avatar || '').trim().slice(0, 8) || null;
        if (!db.users[me]) db.users[me] = { joinedAt: Date.now(), avatar: null };
        db.users[me].avatar = avatar;
        saveDB();
        broadcastAll({ type: 'avatarUpdate', name: me, avatar });
        break;
      }
      case 'createGroup': {
        const name = String(msg.name || 'Group').trim().slice(0, 40);
        const members = Array.from(new Set([me, ...(Array.isArray(msg.members) ? msg.members : [])]));
        const group = {
          id: 'g_' + Date.now() + '_' + crypto.randomBytes(3).toString('hex'),
          name, members, createdBy: me, createdAt: Date.now()
        };
        db.groups[group.id] = group;
        saveDB();
        members.forEach(m => sendTo(m, { type: 'groupCreated', group }));
        break;
      }
      case 'history': {
        const scope = msg.scope;
        let list = [];
        let readState = {};
        if (scope === 'global') { list = db.messages.global; }
        else if (scope === 'dm') {
          list = db.messages.dm[pairKey(me, String(msg.id || ''))] || [];
          readState = db.readState.dm[pairKey(me, String(msg.id || ''))] || {};
        } else if (scope === 'group') {
          list = db.messages.group[msg.id] || [];
          readState = db.readState.group[msg.id] || {};
        }
        sendJson(sock, { type: 'history', scope, id: msg.id || null, messages: capMessages(list).slice(-300), readState });
        break;
      }
      case 'send': {
        const scope = msg.scope;
        const text = String(msg.text || '').trim().slice(0, 4000);
        let file = null;
        if (msg.file && typeof msg.file.data === 'string') {
          const approxBytes = msg.file.data.length * 0.75; // base64 -> raw estimate
          if (approxBytes > MAX_FILE_BYTES) {
            sendJson(sock, { type: 'error', reason: 'הקובץ גדול מדי (מקסימום 3MB).' });
            return;
          }
          file = {
            name: String(msg.file.name || 'file').slice(0, 120),
            mime: String(msg.file.mime || 'application/octet-stream').slice(0, 100),
            size: msg.file.size || null,
            data: msg.file.data
          };
        }
        let poll = null;
        if (msg.poll && msg.poll.question && Array.isArray(msg.poll.options) && msg.poll.options.length >= 2) {
          poll = {
            question: String(msg.poll.question).trim().slice(0, 200),
            options: msg.poll.options.slice(0, 10).map(o => ({ text: String(o).trim().slice(0, 80), votes: [] }))
          };
        }
        if (!text && !file && !poll) return;
        const message = { id: crypto.randomBytes(6).toString('hex'), from: me, text, ts: Date.now(), file, poll };

        if (scope === 'global') {
          db.messages.global = capMessages([...db.messages.global, message]);
          saveDB();
          broadcastAll({ type: 'message', scope: 'global', message });
        } else if (scope === 'dm') {
          const to = String(msg.to || '').trim();
          if (!to) return;
          const key = pairKey(me, to);
          db.messages.dm[key] = capMessages([...(db.messages.dm[key] || []), message]);
          saveDB();
          if (to.toLowerCase() === me.toLowerCase()) {
            sendTo(to, { type: 'message', scope: 'dm', id: me, message }); // notes-to-self: one copy to all my devices
          } else {
            sendJson(sock, { type: 'message', scope: 'dm', id: to, message });
            sendTo(to, { type: 'message', scope: 'dm', id: me, message });
            if (!isOnline(to)) sendPushTo(to, me, message.text || (message.file ? '📎 קובץ' : message.poll ? '📊 סקר' : ''));
          }
        } else if (scope === 'group') {
          const groupId = msg.to;
          const group = db.groups[groupId];
          if (!group) return;
          db.messages.group[groupId] = capMessages([...(db.messages.group[groupId] || []), message]);
          saveDB();
          group.members.forEach(m => {
            sendTo(m, { type: 'message', scope: 'group', id: groupId, message });
            if (m.toLowerCase() !== me.toLowerCase() && !isOnline(m)) {
              sendPushTo(m, group.name + ' · ' + me, message.text || (message.file ? '📎 קובץ' : message.poll ? '📊 סקר' : ''));
            }
          });
        }
        break;
      }
      case 'vote': {
        const scope = msg.scope;
        const messageId = msg.messageId;
        const optionIndex = Number(msg.optionIndex);
        let list, key;
        if (scope === 'dm') { key = pairKey(me, String(msg.id || '')); list = db.messages.dm[key]; }
        else if (scope === 'group') { key = msg.id; list = db.messages.group[key]; }
        else { key = 'global'; list = db.messages.global; }
        if (!list) return;
        const message = list.find(m => m.id === messageId);
        if (!message || !message.poll || !message.poll.options[optionIndex]) return;
        message.poll.options.forEach(o => { o.votes = o.votes.filter(v => v.toLowerCase() !== me.toLowerCase()); });
        message.poll.options[optionIndex].votes.push(me);
        saveDB();
        const update = { type: 'pollUpdate', scope, id: msg.id || null, messageId, poll: message.poll };
        if (scope === 'global') broadcastAll(update);
        else if (scope === 'dm') { sendTo(msg.id, update); sendJson(sock, update); }
        else if (scope === 'group') { const group = db.groups[key]; if (group) group.members.forEach(m => sendTo(m, update)); }
        break;
      }
      case 'markRead': {
        const scope = msg.scope;
        const uptoTs = Number(msg.uptoTs) || Date.now();
        if (scope === 'dm') {
          const key = pairKey(me, String(msg.id || ''));
          if (!db.readState.dm[key]) db.readState.dm[key] = {};
          db.readState.dm[key][me.toLowerCase()] = uptoTs;
          saveDB();
          if (msg.id && msg.id.toLowerCase() !== me.toLowerCase()) {
            sendTo(msg.id, { type: 'readUpdate', scope: 'dm', id: me, reader: me, uptoTs });
          }
        } else if (scope === 'group') {
          const groupId = msg.id;
          const group = db.groups[groupId];
          if (!group) return;
          if (!db.readState.group[groupId]) db.readState.group[groupId] = {};
          db.readState.group[groupId][me.toLowerCase()] = uptoTs;
          saveDB();
          group.members.forEach(m => {
            if (m.toLowerCase() !== me.toLowerCase()) sendTo(m, { type: 'readUpdate', scope: 'group', id: groupId, reader: me, uptoTs });
          });
        }
        break;
      }
      case 'typing': {
        const scope = msg.scope;
        if (scope === 'global') {
          broadcastAll({ type: 'typing', scope: 'global', from: me }, sock);
        } else if (scope === 'dm') {
          const to = String(msg.to || '').trim();
          if (!to) return;
          sendTo(to, { type: 'typing', scope: 'dm', id: me, from: me });
        } else if (scope === 'group') {
          const group = db.groups[msg.to];
          if (!group) return;
          group.members.forEach(m => {
            if (m.toLowerCase() !== me.toLowerCase()) sendTo(m, { type: 'typing', scope: 'group', id: msg.to, from: me });
          });
        }
        break;
      }
      case 'push-subscribe': {
        const sub = msg.subscription;
        if (!sub || !sub.endpoint) return;
        const key = me.toLowerCase();
        const list = db.pushSubs[key] || (db.pushSubs[key] = []);
        if (!list.some(s => s.endpoint === sub.endpoint)) {
          list.push(sub);
          saveDB();
        }
        break;
      }
      case 'push-unsubscribe': {
        const endpoint = msg.endpoint;
        const key = me.toLowerCase();
        if (db.pushSubs[key]) {
          db.pushSubs[key] = db.pushSubs[key].filter(s => s.endpoint !== endpoint);
          saveDB();
        }
        break;
      }
      case 'ping': sendJson(sock, { type: 'pong' }); break;
      default: {
        // Generic passthrough for WebRTC call signaling: call-invite, call-accept,
        // call-decline, call-roster, call-signal, call-end. Server just relays to
        // msg.to (a single username), always stamping the authenticated sender name.
        if (typeof msg.type === 'string' && msg.type.indexOf('call-') === 0) {
          const to = String(msg.to || '').trim();
          if (!to) return;
          sendTo(to, Object.assign({}, msg, { from: me }));
        }
        break;
      }
    }
  });

  sock.on('close', () => {
    if (sock.name) {
      const set = socketsByName.get(sock.name.toLowerCase());
      if (set) {
        set.delete(sock);
        if (set.size === 0) {
          socketsByName.delete(sock.name.toLowerCase());
          broadcastAll({ type: 'presence', name: sock.name, online: false });
        }
      }
    }
  });
});

function sendJson(sock, obj) {
  if (sock.readyState === WebSocket.OPEN) sock.send(JSON.stringify(obj));
}

server.listen(PORT, () => console.log('Chatnet relay listening on port ' + PORT));
