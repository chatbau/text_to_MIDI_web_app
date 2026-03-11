# Text to MIDI Web App

This app wraps your existing `text_to_midi_live.py` mapping logic with a small web UI.

## Run

```bash
cd /Users/jabeau/Desktop/python/text_to_MIDI/web_app
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14 -m pip install -r requirements.txt
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3.14 -m uvicorn app:app --reload --port 8000
```

Then open: `http://127.0.0.1:8000`

## Edit

- Backend: `app.py`
- Frontend: `static/index.html`
- Mapping logic: `../text_to_midi_live.py`

Everything is plain files, so you can always edit directly.

## Paid Access Setup (Stripe + Patreon)

This app now supports:
- Stripe subscription access (`Pay with Stripe`)
- Patreon member access (`Connect Patreon`)

Set env vars before running:

```bash
export APP_BASE_URL="http://127.0.0.1:8000"
export APP_AUTH_SECRET="replace-with-a-long-random-secret"

# Stripe
export STRIPE_SECRET_KEY="sk_live_or_test_..."
export STRIPE_PRICE_ID="price_..."
export STRIPE_WEBHOOK_SECRET="whsec_..."

# Patreon
export PATREON_CLIENT_ID="..."
export PATREON_CLIENT_SECRET="..."
export PATREON_REDIRECT_URI="http://127.0.0.1:8000/api/billing/patreon/callback"
# Optional: comma-separated Patreon tier IDs that should grant access.
export PATREON_REQUIRED_TIERS=""
```

Notes:
- Local users are stored in `access.db` (SQLite) in this folder.
- Paid-only API routes: `/api/send-live`, `/api/update-live-settings`, `/api/save-midi`.
- Patreon access is synced on login and via `POST /api/billing/patreon/sync`.
