require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
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
        clients.set(deviceId, ws);
        ws.send(JSON.stringify({ type: 'registered', deviceId }));
        console.log(`[WS] Registrert: ${deviceId} (${msg.deviceType || 'ukjent'})`);
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
