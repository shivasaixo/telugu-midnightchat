const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] }, maxHttpBufferSize: 1e7, transports: ['websocket', 'polling'] });
const PORT = process.env.PORT || 3000;
const OWNER_EMAIL = 'ssai531210@gmail.com';
const OWNER_USERNAME = 'virajuxo';
const OWNER_PASSWORD = 'vivox200';
const RANKS = ['owner', 'co-owner', 'leader', 'asst-leader', 'vip', 'member', 'guest'];
const MAX_MESSAGE_LENGTH = 500;
const MAX_AVATAR_LENGTH = 1e7;
const RATE_WINDOW_MS = 10000;
const MAX_MESSAGES_PER_WINDOW = 6;
const VALID_COLORS = new Set(['#8be9fd', '#50fa7b', '#ffb86c', '#ff79c6', '#bd93f9', '#f8f8f2', '#f1fa8c']);
const VALID_FONTS = new Set(['sans', 'cursive', 'mono', 'cyber', 'gothic', 'neon']);
const VALID_MOODS = new Set(['🌙 Late Night Vibe', '🎧 Listening to Music', '☕ Chill', '💬 Open to DM']);
const users = new Map();
const accounts = new Map();
let roomLocked = false;

process.on('uncaughtException', (error) => console.error('Uncaught server exception:', error));
process.on('unhandledRejection', (reason) => console.error('Unhandled server rejection:', reason));
server.on('error', (error) => console.error('HTTP server error:', error));

app.use(express.static(path.join(__dirname, 'public')));
accounts.set(OWNER_EMAIL, { username: OWNER_USERNAME, passwordHash: hash(OWNER_PASSWORD), rank: 'owner' });

function hash(value) { return crypto.createHash('sha256').update(String(value)).digest('hex'); }
function clean(value, maxLength) { return String(value || '').replace(/[\u0000-\u001F\u007F]/g, '').trim().slice(0, maxLength); }
function escapeHtml(value, maxLength) { return clean(value, maxLength).replace(/[&<>'"]/g, (character) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' })[character]); }
function sanitizeAvatar(value) { const avatar = String(value || ''); return /^data:image\/(png|jpe?g|gif|webp);base64,[a-z0-9+/=]+$/i.test(avatar) && avatar.length <= MAX_AVATAR_LENGTH ? avatar : ''; }
function safeColor(value) { return VALID_COLORS.has(value) ? value : '#8be9fd'; }
function safeFont(value) { return VALID_FONTS.has(value) ? value : 'sans'; }
function safeMood(value) { return VALID_MOODS.has(value) ? value : '🌙 Late Night Vibe'; }
function levelFor(xp) { return Math.max(1, Math.floor(Math.sqrt(xp / 25)) + 1); }
function rankLabel(rank) { return rank === 'asst-leader' ? 'ASST LEADER' : rank === 'co-owner' ? 'CO-OWNER' : rank.toUpperCase(); }
function dayKey(date = new Date()) { return date.toISOString().slice(0, 10); }
function publicUser(user) { return { id: user.id, username: user.username, email: user.email, avatar: user.avatar, rank: user.rank, rankLabel: rankLabel(user.rank), xp: user.xp, level: user.level, likes: user.likes, color: user.color, fontStyle: user.fontStyle, mood: user.mood, aboutMe: user.aboutMe, allowDMs: user.allowDMs, streak: user.streak, online: Boolean(user.socketId) }; }
function userFromSocket(socket) { return users.get(socket.data.userId); }
function isOwner(user) { return user && user.rank === 'owner' && user.email === OWNER_EMAIL && user.username === OWNER_USERNAME; }
function isAdmin(user) { return isOwner(user); }
function findOnline(id) { const user = users.get(id); return user && user.socketId ? io.sockets.sockets.get(user.socketId) : null; }
function emitSystem(message, target = io) { target.emit('system message', { message: escapeHtml(message, 240), timestamp: new Date().toISOString() }); }
function safeHandler(socket, handler) {
  return (...args) => {
    const acknowledgement = typeof args[args.length - 1] === 'function' ? args.pop() : null;
    try {
      const result = handler(...args, acknowledgement);
      if (result && typeof result.then === 'function') result.catch((error) => handleSocketError(socket, error, acknowledgement));
    } catch (error) {
      handleSocketError(socket, error, acknowledgement);
    }
  };
}
function handleSocketError(socket, error, acknowledgement) {
  console.error('Socket event failed:', error);
  const response = { ok: false, error: 'Unable to process that request.' };
  if (acknowledgement) acknowledgement(response);
  socket.emit('chat error', response.error);
}

function presence() {
  const online = [...users.values()].filter((user) => user.socketId);
  io.emit('presence', { count: online.length, users: online.map(publicUser), roomLocked });
}

function emitFriends(user) {
  const friends = [...user.friends].map((id) => users.get(id)).filter(Boolean).map(publicUser);
  const requests = [...user.friendRequests].map((id) => users.get(id)).filter(Boolean).map(publicUser);
  const socket = findOnline(user.id);
  if (socket) socket.emit('friends:update', { friends, requests });
}

function authenticate(auth = {}) {
  const mode = auth.mode === 'account' ? 'account' : 'guest';
  const email = clean(auth.email, 160).toLowerCase();
  const username = clean(auth.username, 32) || 'Guest';
  if (mode === 'account') {
    if (!email || !auth.password) return { error: 'Email and password are required.' };
    const account = accounts.get(email);
    if (email === OWNER_EMAIL && username.toLowerCase() === OWNER_USERNAME && auth.password === OWNER_PASSWORD) return createOrRestore(email, username, auth, 'owner');
    if (!account) {
      accounts.set(email, { username: escapeHtml(username, 32) || 'Member', passwordHash: hash(auth.password), rank: 'member' });
      return createOrRestore(email, username, auth, 'member');
    }
    if (account.passwordHash !== hash(auth.password)) return { error: 'Invalid account credentials.' };
    return createOrRestore(email, account.username, auth, account.rank);
  }
  return createOrRestore(`guest:${crypto.randomUUID()}`, username, auth, 'guest');
}

function createOrRestore(id, username, auth, rank) {
  const existing = users.get(id);
  const user = existing || { id, email: id.startsWith('guest:') ? '' : id, username, avatar: '', rank, xp: 0, level: 1, likes: 0, streak: 0, lastLoginDay: '', color: '#8be9fd', fontStyle: 'sans', mood: '🌙 Late Night Vibe', aboutMe: '', allowDMs: true, friends: new Set(), friendRequests: new Set(), mutedUntil: 0, banned: false };
  user.username = rank === 'owner' ? OWNER_USERNAME : escapeHtml(username, 32) || user.username;
  user.avatar = sanitizeAvatar(auth.avatar) || user.avatar;
  user.rank = rank;
  user.level = levelFor(user.xp);
  const today = dayKey();
  if (user.lastLoginDay !== today) {
    const previous = new Date(`${user.lastLoginDay}T00:00:00Z`);
    const daysSinceLastLogin = user.lastLoginDay ? Math.round((Date.now() - previous.getTime()) / 86400000) : 0;
    user.streak = daysSinceLastLogin === 1 ? user.streak + 1 : 1;
    user.lastLoginDay = today;
  }
  users.set(user.id, user);
  return user;
}

function sendSession(socket, user) { socket.emit('session', { ...publicUser(user), isOwner: isOwner(user), canModerate: isAdmin(user), roomLocked }); }
function addXp(user, amount) { const oldLevel = user.level; user.xp += amount; user.level = levelFor(user.xp); const socket = findOnline(user.id); if (user.level > oldLevel) { emitSystem(`${user.username} reached Level ${user.level}!`); if (socket) socket.emit('level up', { ...publicUser(user), previousLevel: oldLevel }); } if (socket) socket.emit('profile:update', publicUser(user)); }
function canMessage(socket) { const user = userFromSocket(socket); if (!user || user.banned) return false; if (user.mutedUntil > Date.now()) { socket.emit('chat error', 'You are muted for a short time.'); return false; } if (roomLocked && !isAdmin(user)) { socket.emit('chat error', 'The global room is locked by the owner.'); return false; } const now = Date.now(); socket.data.messageTimes = (socket.data.messageTimes || []).filter((time) => now - time < RATE_WINDOW_MS); if (socket.data.messageTimes.length >= MAX_MESSAGES_PER_WINDOW) { socket.emit('chat error', 'Please slow down for a moment.'); return false; } socket.data.messageTimes.push(now); return true; }

io.on('connection', (socket) => {
  const result = authenticate(socket.handshake.auth);
  if (result.error) { socket.emit('auth:error', result.error); socket.disconnect(true); return; }
  const user = result;
  if (user.banned) { socket.emit('auth:error', 'This account is banned.'); socket.disconnect(true); return; }
  user.socketId = socket.id;
  user.joinedAt = user.joinedAt || Date.now();
  socket.data.userId = user.id;
  socket.data.messageTimes = [];
  sendSession(socket, user);
  emitSystem(`${user.username} joined the room.`);
  presence();

  socket.on('chat message', safeHandler(socket, (payload = {}, acknowledgement) => {
    if (!canMessage(socket)) { if (acknowledgement) acknowledgement({ ok: false, error: 'Message was not accepted.' }); return; }
    const message = escapeHtml(payload.text, MAX_MESSAGE_LENGTH);
    const image = sanitizeAvatar(payload.image);
    if (!message && !image) { if (acknowledgement) acknowledgement({ ok: false, error: 'Message is empty.' }); return; }
    addXp(user, 5);
    io.emit('chat message', { ...publicUser(user), message, image, timestamp: new Date().toISOString(), mine: false });
    if (acknowledgement) acknowledgement({ ok: true });
  }));

  socket.on('profile:update', safeHandler(socket, (payload = {}) => { user.username = user.rank === 'owner' ? OWNER_USERNAME : escapeHtml(payload.username, 32) || user.username; user.avatar = sanitizeAvatar(payload.avatar) || user.avatar; user.color = safeColor(payload.color); user.fontStyle = safeFont(payload.fontStyle); user.mood = safeMood(payload.mood); user.aboutMe = escapeHtml(payload.aboutMe, 240); user.allowDMs = payload.allowDMs !== false; sendSession(socket, user); presence(); }));
  socket.on('typing', safeHandler(socket, (payload = {}) => { socket.broadcast.emit('typing', { username: user.username, isTyping: Boolean(payload.isTyping) }); }));
  socket.on('xp:tick', safeHandler(socket, () => { if (Date.now() - (user.lastXpAt || 0) > 50000) { user.lastXpAt = Date.now(); addXp(user, 1); } }));
  socket.on('like:user', safeHandler(socket, (id) => { const target = users.get(id); if (target && target.id !== user.id) { target.likes += 1; io.emit('likes:update', { id: target.id, likes: target.likes }); } }));
  socket.on('friend:request', safeHandler(socket, (id) => { const target = users.get(id); if (!target || target.id === user.id) return; target.friendRequests.add(user.id); emitFriends(target); socket.emit('chat error', `Friend request sent to ${target.username}.`); }));
  socket.on('friend:respond', safeHandler(socket, ({ id, accept } = {}) => { const target = users.get(id); if (!target || !user.friendRequests.has(id)) return; user.friendRequests.delete(id); if (accept) { user.friends.add(id); target.friends.add(user.id); emitFriends(target); } emitFriends(user); presence(); }));
  socket.on('dm:send', safeHandler(socket, ({ id, text, image } = {}, acknowledgement) => { const target = users.get(id); if (!target || (!user.friends.has(id) && !isAdmin(user)) || (!target.allowDMs && !user.friends.has(id) && !isAdmin(user))) { if (acknowledgement) acknowledgement({ ok: false, error: 'This user is not accepting private messages.' }); return; } if (!canMessage(socket)) { if (acknowledgement) acknowledgement({ ok: false, error: 'Message was not accepted.' }); return; } const payload = { from: publicUser(user), text: escapeHtml(text, MAX_MESSAGE_LENGTH), image: sanitizeAvatar(image), timestamp: new Date().toISOString() }; const targetSocket = findOnline(id); socket.emit('dm:message', payload); if (targetSocket) targetSocket.emit('dm:message', payload); if (acknowledgement) acknowledgement({ ok: true }); }));
  socket.on('admin:action', safeHandler(socket, ({ action, id, rank, duration } = {}) => { if (!isAdmin(user)) return; const target = users.get(id); if (action === 'lock') { roomLocked = Boolean(duration); presence(); return; } if (action === 'wipe') { io.emit('chat:wiped'); emitSystem('The owner wiped the global chat.'); return; } if (!target || target.id === user.id) return; if (action === 'rank' && RANKS.includes(rank)) target.rank = rank; if (action === 'mute') target.mutedUntil = Date.now() + 60000; if (action === 'ban') { target.banned = true; const targetSocket = findOnline(id); if (targetSocket) { targetSocket.emit('auth:error', 'You were banned by the owner.'); targetSocket.disconnect(true); } } if (action === 'kick') { const targetSocket = findOnline(id); if (targetSocket) targetSocket.disconnect(true); } sendSession(socket, user); presence(); }));
  socket.on('disconnect', safeHandler(socket, () => { user.socketId = null; emitSystem(`${user.username} left the room.`); presence(); }));
});

setInterval(() => { for (const user of users.values()) if (user.socketId && Date.now() - (user.lastXpAt || 0) > 50000) { user.lastXpAt = Date.now(); addXp(user, 1); } }, 60000);

function getLocalIp() {
  const interfaces = os.networkInterfaces();
  for (const entries of Object.values(interfaces)) for (const entry of entries || []) if (entry.family === 'IPv4' && !entry.internal) return entry.address;
  return '127.0.0.1';
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Telugu MidnightChat listening on http://localhost:${PORT}`);
  console.log(`LAN access: http://${getLocalIp()}:${PORT}`);
});
