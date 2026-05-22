# AyMedia Signalserver — Railway Deploy

## Steg 1: Deploy til Railway

```bash
cd server

# Installer Railway CLI om du ikke har det
npm install -g @railway/cli

# Logg inn
railway login

# Koble til eksisterende prosjekt (der smsgateway kjører)
railway link

# Lag ny service i prosjektet
railway service create --name aymedia-signal

# Deploy
railway up
```

Etter deploy får du en URL som:
`aymedia-signal-production.up.railway.app`

---

## Steg 2: CNAME hos domeneregistrar

Logg inn hos din registrar (Domeneshop, One.com, o.l.) og legg til:

```
Type:    CNAME
Navn:    signal
Verdi:   aymedia-signal-production.up.railway.app
TTL:     3600
```

Vent 5-30 min på DNS-propagering.

---

## Steg 3: Custom domain i Railway

1. Gå til Railway dashboard → din service
2. Settings → Networking → Custom Domain
3. Legg til: `signal.ay.no`
4. Railway verifiserer CNAME automatisk
5. SSL-sertifikat settes opp automatisk (Let's Encrypt)

---

## Steg 4: Miljøvariabler i Railway

Gå til Railway dashboard → din service → Variables og legg til:

```
SIGNAL_DOMAIN          = signal.ay.no
ACTIVATION_BASE_URL    = https://ay.no/aktiver
FIREBASE_PROJECT_ID    = chess-arena-1e641
FIREBASE_SERVICE_ACCOUNT = { ... }   ← hele JSON fra Firebase Console
```

### Hente Firebase serviceAccountKey:
1. Firebase Console → chess-arena-1e641
2. Prosjektinnstillinger (tannhjul) → Tjenestekontoer
3. "Generer ny privatnøkkel" → last ned JSON
4. Åpne filen → kopier HELE innholdet
5. Lim inn som verdi for FIREBASE_SERVICE_ACCOUNT i Railway
   (Railway håndterer multi-line verdier fint)

---

## Steg 5: Verifiser

```bash
# Sjekk at serveren kjører
curl https://signal.ay.no/health

# Forventet respons:
# {"status":"ok","clients":0,"uptime":42,"domain":"signal.ay.no"}

# Test WebSocket (installer wscat)
npm install -g wscat
wscat -c wss://signal.ay.no/ws
```

---

## Flutter-appene

URL er allerede satt til `wss://signal.ay.no/ws` som default.

Vil du bruke en annen URL (f.eks. under testing):
```bash
# Android TV
flutter run --dart-define=SIGNAL_URL=wss://signal.ay.no/ws

# Desktop
flutter run -d windows --dart-define=SIGNAL_URL=wss://signal.ay.no/ws
```

---

## Kostnader på Railway

Railway fakturerer per bruk. Signalserveren er lett:
- CPU: minimal (bare WebSocket signalering)
- RAM: ~50-100 MB
- Estimert kostnad: $1-3 USD/mnd ved normal bruk
- Godt innenfor Railway Hobby-plan ($5/mnd inkl. $5 credits)
