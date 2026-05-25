require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const { checkExpiredEvents, packEventToZip } = require('./events_storage');
const QRCode = require('qrcode');
const admin = require('firebase-admin');

// ─── Konfigurasjon ────────────────────────────────────────────────────────────
const SIGNAL_DOMAIN = process.env.SIGNAL_DOMAIN || 'signal.ay.no';
const ACTIVATION_BASE_URL = process.env.ACTIVATION_BASE_URL || 'https://ay.no/aktiver';
const PORT = process.env.PORT || 3000;

// ─── Firebase init ────────────────────────────────────────────────────────────
// Railway: legg inn FIREBASE_SERVICE_ACCOUNT som miljøvariabel (JSON som string)
let firebaseInitialized = false;
try {
  const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
    ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
    : require('./serviceAccountKey.json');

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: `https://${process.env.FIREBASE_PROJECT_ID || 'chess-arena-1e641'}-default-rtdb.firebaseio.com`
  });
  firebaseInitialized = true;
  console.log('[Firebase] Initialisert OK');
} catch (err) {
  console.warn('[Firebase] Ikke tilgjengelig — kjører uten Firebase:', err.message);
}

const db = firebaseInitialized ? admin.database() : null;

// ─── Express + HTTP server ────────────────────────────────────────────────────
const app = express();

// Railway bruker proxy — trust proxy for korrekt IP og https-deteksjon
app.set('trust proxy', 1);
app.use(cors({
  origin: [
    'https://ay.no',
    'https://signal.ay.no',
    /\.railway\.app$/,
    'http://localhost:*'
  ],
  credentials: true
}));
app.use(express.json());

const server = http.createServer(app);

// ─── WebSocket server ─────────────────────────────────────────────────────────
// Railway: bruk path '/ws' slik at /health og andre ruter ikke kolliderer
const wss = new WebSocket.Server({
  server,
  path: '/ws',
  // Railway har 15 min timeout — ping holder tilkoblingen oppe
  clientTracking: true
});

// Lagrer tilkoblede klienter: deviceId → ws
const clients = new Map();

wss.on('connection', (ws, req) => {
  let deviceId = null;

  // Hold tilkoblingen oppe mot Railways 15-min timeout
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'ping':
        ws.send(JSON.stringify({ type: 'pong' }));
        break;

      // Enhet registrerer seg (TV, mobil, desktop)
      case 'register': {
        deviceId = msg.deviceId;
        const deviceType = msg.deviceType || 'ukjent';
        clients.set(deviceId, ws);
        ws.send(JSON.stringify({ type: 'registered', deviceId }));
        console.log(`[WS] Registrert: ${deviceId} (${deviceType})`);

        // Varsle alle mobilapper om at en TV har koblet til igjen
        if (deviceType === 'tv') {
          for (const [id, client] of clients.entries()) {
            if (id !== deviceId && client.readyState === 1) {
              client.send(JSON.stringify({
                type: 'device_connected',
                deviceId,
                deviceType,
              }));
            }
          }
        }
        break;
      }

      // WebRTC signalering — videresend til mottaker
      case 'signal': {
        const target = clients.get(msg.targetId);
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(JSON.stringify({
            type: 'signal',
            fromId: deviceId,
            signal: msg.signal
          }));
        }
        break;
      }

      // TV ber om ny aktiveringskode
      case 'request_code': {
        const code = generateCode();
        const activationUrl = `${ACTIVATION_BASE_URL}?kode=${code}`;

        if (db) {
          const ref = db.ref(`activation_codes/${code}`);
          await ref.set({
            created: Date.now(),
            expires: Date.now() + 5 * 60 * 1000,
            deviceId: msg.deviceId,
            deviceType: msg.deviceType || 'tv',
            userId: null,
            activated: false
          });

          // Lytt på Firebase — si fra til TV når aktivert
          ref.on('value', (snap) => {
            const data = snap.val();
            if (data && data.activated && data.userId) {
              const tvWs = clients.get(msg.deviceId);
              if (tvWs && tvWs.readyState === WebSocket.OPEN) {
                tvWs.send(JSON.stringify({
                  type: 'activated',
                  userId: data.userId,
                  userEmail: data.userEmail
                }));
              }
              ref.off();
            }
          });
        }

        ws.send(JSON.stringify({
          type: 'activation_code',
          code,
          activationUrl,
          expiresIn: 300
        }));
        break;
      }

      // Desktop-server melder seg på som tilgjengelig
      case 'server_online': {
        if (db) {
          await db.ref(`servers/${msg.userId}`).set({
            deviceId,
            online: true,
            lastSeen: Date.now(),
            mediaCount: msg.mediaCount || 0,
            version: msg.version || '1.0.0'
          });
        }
        ws.send(JSON.stringify({ type: 'server_registered' }));
        break;
      }

      // TV ber om å koble til brukerens hjemmeserver
      case 'connect_to_server': {
        if (!db) {
          ws.send(JSON.stringify({ type: 'error', message: 'Firebase ikke tilgjengelig' }));
          return;
        }
        const serverSnap = await db.ref(`servers/${msg.userId}`).get();
        const serverData = serverSnap.val();
        if (!serverData || !serverData.online) {
          ws.send(JSON.stringify({ type: 'error', message: 'Hjemmeserveren er offline' }));
          return;
        }
        const desktopWs = clients.get(serverData.deviceId);
        if (desktopWs && desktopWs.readyState === WebSocket.OPEN) {
          desktopWs.send(JSON.stringify({
            type: 'incoming_connection',
            fromDeviceId: deviceId,
            fromType: 'tv',
            userId: msg.userId
          }));
        }
        ws.send(JSON.stringify({
          type: 'connecting',
          serverDeviceId: serverData.deviceId
        }));
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', async () => {
    if (deviceId) {
      clients.delete(deviceId);
      console.log(`[WS] Frakoblet: ${deviceId}`);
    }
  });

  ws.on('error', (err) => {
    console.error(`[WS] Feil for ${deviceId}:`, err.message);
  });
});

// ─── Keepalive ping mot Railways 15-min timeout ───────────────────────────────
const pingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000); // Hvert 30. sekund

wss.on('close', () => clearInterval(pingInterval));

// ─── REST API ─────────────────────────────────────────────────────────────────

// Aktiver enhet fra nettleser (mobil/PC skanner QR)
app.post('/api/activate', async (req, res) => {
  const { code, idToken } = req.body;
  if (!code || !idToken) return res.status(400).json({ error: 'Mangler kode eller token' });
  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  try {
    const decoded = await admin.auth().verifyIdToken(idToken);
    const userId = decoded.uid;
    const userEmail = decoded.email;

    const ref = db.ref(`activation_codes/${code}`);
    const snap = await ref.get();
    const data = snap.val();

    if (!data) return res.status(404).json({ error: 'Kode ikke funnet' });
    if (data.activated) return res.status(400).json({ error: 'Kode allerede brukt' });
    if (Date.now() > data.expires) return res.status(400).json({ error: 'Kode utløpt — be om ny' });

    await ref.update({ activated: true, userId, userEmail });
    res.json({ success: true, message: 'TV aktivert!' });
  } catch (err) {
    console.error('[Activate]', err);
    res.status(401).json({ error: 'Ugyldig token' });
  }
});

// Medieliste fra brukerens hjemmeserver (metadata)
app.get('/api/media/:userId', async (req, res) => {
  const { userId } = req.params;
  const idToken = req.headers.authorization?.replace('Bearer ', '');
  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });
  try {
    const decoded = await admin.auth().verifyIdToken(idToken);
    if (decoded.uid !== userId) return res.status(403).json({ error: 'Ikke autorisert' });
    const snap = await db.ref(`media_index/${userId}`).get();
    res.json(snap.val() || []);
  } catch {
    res.status(401).json({ error: 'Ugyldig token' });
  }
});

// Helsesjekk — Railway bruker denne
app.get('/health', (_, res) => {
  res.json({
    status: 'ok',
    clients: clients.size,
    uptime: Math.floor(process.uptime()),
    domain: SIGNAL_DOMAIN
  });
});

// Info-rute
app.get('/', (_, res) => {
  res.json({
    name: 'AyMedia Signalserver',
    version: '1.0.0',
    domain: SIGNAL_DOMAIN,
    ws: `wss://${SIGNAL_DOMAIN}/ws`,
    activation: ACTIVATION_BASE_URL
  });
});

// ─── Hjelpefunksjoner ─────────────────────────────────────────────────────────
function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

// ─── Start server ─────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`AyMedia signalserver kjører på port ${PORT}`);
  console.log(`WebSocket: wss://${SIGNAL_DOMAIN}/ws`);
  console.log(`REST API:  https://${SIGNAL_DOMAIN}/api`);
  console.log(`Klienter: ${clients.size}`);
});

// ─── SMS-hjelpefunksjon ───────────────────────────────────────────────────────
async function sendSmsToNumber(phone, message) {
  const smsUrl = process.env.SMS_GATEWAY_URL || 'https://sms.ay.no';
  const smsApiKey = process.env.SMS_GATEWAY_API_KEY;
  const headers = { 'Content-Type': 'application/json' };
  if (smsApiKey) headers['Authorization'] = 'Bearer ' + smsApiKey;
  try {
    await fetch(`${smsUrl}/api/3rdparty/v1/message`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ message, phoneNumbers: [phone] }),
    });
    console.log(`[SMS] Sendt til ${phone}`);
  } catch (err) {
    console.error(`[SMS] Feil:`, err.message);
  }
}

// ─── Events scheduler — sjekk hvert minutt ───────────────────────────────────
setInterval(() => checkExpiredEvents(db, sendSmsToNumber), 60 * 1000);

// Kjør ved oppstart
setTimeout(() => checkExpiredEvents(db, sendSmsToNumber), 5000);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[Server] Avslutter...');
  clearInterval(pingInterval);
  server.close(() => process.exit(0));
});

// ─── SMS-basert autentisering ─────────────────────────────────────────────────

// Lagrer SMS-koder midlertidig: telefonnummer → {kode, expires, userId}
const smsCodes = new Map();

// Send SMS-verifiseringskode
app.post('/api/auth/send-code', async (req, res) => {
  const { phone } = req.body;
  if (!phone) return res.status(400).json({ error: 'Telefonnummer mangler' });

  // Normaliser telefonnummer
  let normalizedPhone = phone.replace(/\s/g, '');
  if (normalizedPhone.startsWith('0')) normalizedPhone = '+47' + normalizedPhone.slice(1);
  if (!normalizedPhone.startsWith('+')) normalizedPhone = '+47' + normalizedPhone;

  // Generer 6-tegns kode
  const code = Math.floor(100000 + Math.random() * 900000).toString();
  const expires = Date.now() + 10 * 60 * 1000; // 10 minutter

  smsCodes.set(normalizedPhone, { code, expires });

  // Send SMS via sms.ay.no
  try {
    const smsUrl = process.env.SMS_GATEWAY_URL || 'https://sms.ay.no';
    const smsUser = process.env.SMS_GATEWAY_BRUKERNAVN;
    const smsPass = process.env.SMS_GATEWAY_PASSORD;

    const smsHeaders = { 'Content-Type': 'application/json' };
    const smsApiKey = process.env.SMS_GATEWAY_API_KEY;
    if (smsApiKey) {
      // sms.ay.no støtter Bearer token
      smsHeaders['Authorization'] = 'Bearer ' + smsApiKey;
    } else if (smsUser && smsPass) {
      smsHeaders['Authorization'] = 'Basic ' + Buffer.from(`${smsUser}:${smsPass}`).toString('base64');
    }

    const smsRes = await fetch(`${smsUrl}/api/3rdparty/v1/message`, {
      method: 'POST',
      headers: smsHeaders,
      body: JSON.stringify({
        message: `Din AyMedia-kode er: ${code}\nKoden er gyldig i 10 minutter.`,
        phoneNumbers: [normalizedPhone],
      }),
    });

    if (!smsRes.ok) {
      const err = await smsRes.text();
      console.error('[SMS] Feil ved sending:', err);
      return res.status(500).json({ error: 'Kunne ikke sende SMS — prøv igjen' });
    }

    console.log(`[SMS] Kode sendt til ${normalizedPhone}`);
    res.json({ success: true, message: 'SMS sendt!' });

  } catch (err) {
    console.error('[SMS] Unntak:', err.message);
    res.status(500).json({ error: 'SMS-tjeneste utilgjengelig' });
  }
});

// Verifiser SMS-kode
app.post('/api/auth/verify-code', async (req, res) => {
  const { phone, code } = req.body;
  if (!phone || !code) return res.status(400).json({ error: 'Mangler telefon eller kode' });

  let normalizedPhone = phone.replace(/\s/g, '');
  if (normalizedPhone.startsWith('0')) normalizedPhone = '+47' + normalizedPhone.slice(1);
  if (!normalizedPhone.startsWith('+')) normalizedPhone = '+47' + normalizedPhone;

  const stored = smsCodes.get(normalizedPhone);

  if (!stored) return res.status(400).json({ error: 'Ingen kode funnet — be om ny' });
  if (Date.now() > stored.expires) {
    smsCodes.delete(normalizedPhone);
    return res.status(400).json({ error: 'Koden er utløpt — be om ny' });
  }
  if (stored.code !== code.trim()) {
    return res.status(400).json({ error: 'Feil kode — prøv igjen' });
  }

  // Kode riktig — slett og returner token
  smsCodes.delete(normalizedPhone);

  // Lag eller hent bruker i Firebase
  let userId = null;
  if (db) {
    try {
      // Bruk telefonnummer som bruker-ID (normalisert)
      const safePhone = normalizedPhone.replace(/\+/g, '').replace(/[^0-9]/g, '');
      userId = `phone_${safePhone}`;
      await db.ref(`users/${userId}`).update({
        phone: normalizedPhone,
        lastLogin: Date.now(),
      });
    } catch (err) {
      console.error('[Auth] Firebase feil:', err.message);
    }
  }

  // Enkel token — i produksjon bør dette være JWT
  const token = Buffer.from(JSON.stringify({
    userId: userId || `phone_${normalizedPhone}`,
    phone: normalizedPhone,
    expires: Date.now() + 30 * 24 * 60 * 60 * 1000, // 30 dager
  })).toString('base64');

  res.json({ success: true, token, userId: userId || `phone_${normalizedPhone}` });
});

// Aktiver TV med telefon-token
app.post('/api/activate-phone', async (req, res) => {
  const { code, token } = req.body;
  if (!code || !token) return res.status(400).json({ error: 'Mangler kode eller token' });

  let userData;
  try {
    userData = JSON.parse(Buffer.from(token, 'base64').toString());
    if (Date.now() > userData.expires) return res.status(401).json({ error: 'Token utløpt' });
  } catch {
    return res.status(401).json({ error: 'Ugyldig token' });
  }

  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  const ref = db.ref(`activation_codes/${code}`);
  const snap = await ref.get();
  const data = snap.val();

  if (!data) return res.status(404).json({ error: 'Kode ikke funnet' });
  if (data.activated) return res.status(400).json({ error: 'Kode allerede brukt' });
  if (Date.now() > data.expires) return res.status(400).json({ error: 'Kode utløpt — be om ny' });

  await ref.update({
    activated: true,
    userId: userData.userId,
    userPhone: userData.phone,
  });

  res.json({ success: true, message: 'TV aktivert!' });
});

// ─── EVENTS API ───────────────────────────────────────────────────────────────

const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { checkExpiredEvents, packEventToZip } = require('./events_storage');

// Generer unik arrangementskode
function generateEventCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 8; i++) {
    if (i === 4) code += '-';
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

// Opprett arrangement
app.post('/api/events/create', async (req, res) => {
  const { token, name, date, theme, hostName } = req.body;
  if (!token || !name) return res.status(400).json({ error: 'Mangler token eller navn' });

  let userData;
  try {
    userData = JSON.parse(Buffer.from(token, 'base64').toString());
    if (Date.now() > userData.expires) return res.status(401).json({ error: 'Token utløpt' });
  } catch {
    return res.status(401).json({ error: 'Ugyldig token' });
  }

  const eventCode = generateEventCode();
  const eventId = uuidv4();
  const { ownerPhone, ownerPhone2 } = req.body;

  // 5 dager fra nå
  const expiresAt = Date.now() + 5 * 24 * 60 * 60 * 1000;

  if (db) {
    await db.ref(`events/${eventCode}`).set({
      id: eventId,
      code: eventCode,
      name,
      date: date || null,
      theme: theme || 'default',
      hostName: hostName || '',
      ownerId: userData.userId,
      ownerPhone: ownerPhone || userData.phone || null,
      ownerPhone2: ownerPhone2 || null,
      created: Date.now(),
      expiresAt,
      active: true,
      uploadCount: 0,
      zipStatus: null,
      settings: {
        requireApproval: true,
        maxUploads: 1000,
        allowVideo: true,
        allowMessages: true,
      }
    });
  }

  res.json({ success: true, eventCode, eventId });
});

// Hent arrangement
app.get('/api/events/:code', async (req, res) => {
  const { code } = req.params;
  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  const snap = await db.ref(`events/${code}`).get();
  const event = snap.val();
  if (!event) return res.status(404).json({ error: 'Arrangement ikke funnet' });
  if (!event.active) return res.status(410).json({ error: 'Arrangement er avsluttet' });

  // Returner offentlig info (ikke ownerId)
  res.json({
    code: event.code,
    name: event.name,
    date: event.date,
    theme: event.theme,
    hostName: event.hostName,
    settings: event.settings,
    uploadCount: event.uploadCount || 0,
  });
});

// Last opp bilde/video metadata fra gjest
app.post('/api/events/:code/upload', async (req, res) => {
  const { code } = req.params;
  const { guestName, message, fileType, fileName, fileSize, fileData } = req.body;

  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  const snap = await db.ref(`events/${code}`).get();
  const event = snap.val();
  if (!event || !event.active) return res.status(404).json({ error: 'Arrangement ikke funnet' });

  const uploadId = uuidv4();
  const status = event.settings?.requireApproval ? 'pending' : 'approved';

  await db.ref(`events/${code}/uploads/${uploadId}`).set({
    id: uploadId,
    guestName: guestName || 'Gjest',
    message: message || '',
    fileType: fileType || 'image',
    fileName: fileName || '',
    fileSize: fileSize || 0,
    fileData: fileData || null, // Base64 for små bilder
    status,
    timestamp: Date.now(),
  });

  // Oppdater teller
  await db.ref(`events/${code}/uploadCount`).transaction(count => (count || 0) + 1);

  // Varsle storskjerm via WebSocket om godkjent innhold
  if (status === 'approved') {
    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        client.send(JSON.stringify({
          type: 'event_new_upload',
          eventCode: code,
          uploadId,
          guestName: guestName || 'Gjest',
          message: message || '',
          fileType,
        }));
      }
    });
  }

  res.json({ success: true, uploadId, status });
});

// Hent godkjente opplastinger (for storskjerm)
app.get('/api/events/:code/uploads', async (req, res) => {
  const { code } = req.params;
  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  const snap = await db.ref(`events/${code}/uploads`).get();
  const uploads = snap.val() || {};
  const approved = Object.values(uploads)
    .filter((u) => u.status === 'approved')
    .sort((a, b) => a.timestamp - b.timestamp);

  res.json(approved);
});

// Moderer opplasting (godkjenn/avvis)
app.post('/api/events/:code/uploads/:uploadId/moderate', async (req, res) => {
  const { code, uploadId } = req.params;
  const { token, action } = req.body; // action: 'approve' | 'reject'

  if (!token) return res.status(401).json({ error: 'Ikke autorisert' });
  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  let userData;
  try {
    userData = JSON.parse(Buffer.from(token, 'base64').toString());
  } catch {
    return res.status(401).json({ error: 'Ugyldig token' });
  }

  // Sjekk at bruker eier arrangementet
  const eventSnap = await db.ref(`events/${code}`).get();
  const event = eventSnap.val();
  if (!event || event.ownerId !== userData.userId) {
    return res.status(403).json({ error: 'Ikke autorisert' });
  }

  const status = action === 'approve' ? 'approved' : 'rejected';
  await db.ref(`events/${code}/uploads/${uploadId}/status`).set(status);

  // Varsle storskjerm om nytt godkjent innhold
  if (status === 'approved') {
    const uploadSnap = await db.ref(`events/${code}/uploads/${uploadId}`).get();
    const upload = uploadSnap.val();
    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        client.send(JSON.stringify({
          type: 'event_new_upload',
          eventCode: code,
          uploadId,
          guestName: upload?.guestName || 'Gjest',
          message: upload?.message || '',
          fileType: upload?.fileType,
        }));
      }
    });
  }

  res.json({ success: true, status });
});

// Hent alle arrangementer for en bruker
app.get('/api/events/user/list', async (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token || !db) return res.status(401).json({ error: 'Ikke autorisert' });

  let userData;
  try {
    userData = JSON.parse(Buffer.from(token, 'base64').toString());
  } catch {
    return res.status(401).json({ error: 'Ugyldig token' });
  }

  const snap = await db.ref('events').orderByChild('ownerId').equalTo(userData.userId).get();
  const events = snap.val() || {};
  res.json(Object.values(events).sort((a, b) => b.created - a.created));
});

// ─── Nedlasting av event ZIP ──────────────────────────────────────────────────
app.get('/api/events/:code/download-info', async (req, res) => {
  const { code } = req.params;
  if (!db) return res.status(503).json({ error: 'Firebase ikke tilgjengelig' });

  const snap = await db.ref(`events/${code}`).get();
  const event = snap.val();
  if (!event) return res.status(404).json({ error: 'Arrangement ikke funnet' });

  if (event.zipStatus === 'ready') {
    const daysLeft = event.zipDeleteAt
      ? Math.ceil((event.zipDeleteAt - Date.now()) / (1000 * 60 * 60 * 24))
      : 10;

    res.json({
      ready: true,
      eventName: event.name,
      uploadCount: event.zipUploadCount || 0,
      daysLeft: Math.max(0, daysLeft),
      downloadUrl: `https://ay.no/events/${code}/download`,
    });
  } else if (event.zipStatus === 'processing') {
    res.json({ ready: false, status: 'processing', message: 'Pakker filer...' });
  } else if (event.zipStatus === 'deleted') {
    res.json({ ready: false, status: 'deleted', message: 'Filer er slettet' });
  } else if (!event.active) {
    res.json({ ready: false, status: 'pending', message: 'Pakker filer snart...' });
  } else {
    const expiresAt = event.expiresAt || (event.created + 5 * 24 * 60 * 60 * 1000);
    const daysLeft = Math.ceil((expiresAt - Date.now()) / (1000 * 60 * 60 * 24));
    res.json({
      ready: false,
      status: 'active',
      message: `Arrangementet er aktivt i ${Math.max(0, daysLeft)} dager til`,
      daysLeft: Math.max(0, daysLeft),
    });
  }
});

// Manuell lukking av arrangement (brudeparet kan lukke tidlig)
app.post('/api/events/:code/close', async (req, res) => {
  const { code } = req.params;
  const { token } = req.body;
  if (!token || !db) return res.status(401).json({ error: 'Ikke autorisert' });

  let userData;
  try {
    userData = JSON.parse(Buffer.from(token, 'base64').toString());
  } catch {
    return res.status(401).json({ error: 'Ugyldig token' });
  }

  const snap = await db.ref(`events/${code}`).get();
  const event = snap.val();
  if (!event || event.ownerId !== userData.userId) {
    return res.status(403).json({ error: 'Ikke autorisert' });
  }

  await db.ref(`events/${code}`).update({
    active: false,
    closedAt: Date.now(),
    zipStatus: 'pending',
  });

  // Start pakking
  const { packEventToZip } = require('./events_storage');
  packEventToZip(code, db, async (phone, msg) => {
    // SMS-sending
    const smsUrl = process.env.SMS_GATEWAY_URL || 'https://sms.ay.no';
    const smsApiKey = process.env.SMS_GATEWAY_API_KEY;
    const headers = { 'Content-Type': 'application/json' };
    if (smsApiKey) headers['Authorization'] = 'Bearer ' + smsApiKey;
    try {
      await fetch(`${smsUrl}/api/3rdparty/v1/message`, {
        method: 'POST', headers,
        body: JSON.stringify({ message: msg, phoneNumbers: [phone] }),
      });
    } catch (_) {}
  });

  res.json({ success: true, message: 'Arrangement lukket — pakker filer og sender SMS' });
});
