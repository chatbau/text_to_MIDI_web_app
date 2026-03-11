from __future__ import annotations

import importlib.util
import random
import re
import threading
import time
import hashlib
import hmac
import base64
import os
import secrets
import sqlite3
from collections import deque
import urllib.error
import urllib.parse
import urllib.request
import json
import tempfile
import html
from pathlib import Path
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Form, HTTPException, Query, Request, Response, Header
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

try:
    import stripe
except Exception:
    stripe = None

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
MAPPER_PATH = BASE_DIR / "text_to_midi_live.py"
if not MAPPER_PATH.exists():
    MAPPER_PATH = ROOT_DIR / "text_to_midi_live.py"


def load_mapper():
    spec = importlib.util.spec_from_file_location("text_to_midi_live", str(MAPPER_PATH))
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load text_to_midi_live.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mapper = load_mapper()
app = FastAPI(title="Text to MIDI")
SEND_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
NOTE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
SEND_ACTIVE = False
NOTE_SEQ = 0
NOTE_FEED: list[dict] = []
QUOTE_LOCK = threading.Lock()
QUOTE_POOL: list[dict] = []
RECENT_QUOTES = deque(maxlen=60)
RNG_LOCK = threading.Lock()
LIVE_STATE = {
    "text": "",
    "tempo_bpm": 60,
    "key": "C",
    "mode": "major",
    "port_name": "",
    "bend_amount": 0,
    "loop_enabled": False,
    "loop_gap_ms": 0,
    "octave_shift": 0,
    "degree_shift": 0,
    "voicing_mode": "closed",
}
MIN_LOOP_GAP_MS = 90

KEY_OPTIONS = ["C", "C#", "Db", "D", "D#", "Eb", "E", "F", "F#", "Gb", "G", "G#", "Ab", "A", "A#", "Bb", "B"]
MODE_OPTIONS = [
    "major",
    "minor",
    "chromatic",
    "major7",
    "minor7",
    "major_pentatonic",
    "minor_pentatonic",
    "dorian",
    "phrygian",
    "lydian",
    "mixolydian",
    "locrian",
]
ADVENTURE_TIME_TITLES = [
    "Adventure Time",
    "Finn the Human",
    "Jake the Dog",
    "Princess Bubblegum",
    "Marceline",
    "Ice King",
    "BMO",
    "Lumpy Space Princess",
    "Earl of Lemongrab",
    "Flame Princess",
    "Fionna and Cake",
]
BOJACK_HORSEMAN_TITLES = [
    "BoJack Horseman",
    "BoJack Horseman (character)",
    "Diane Nguyen",
    "Princess Carolyn",
    "Todd Chavez",
    "Mr. Peanutbutter",
    "Sarah Lynn",
]
ADVENTURE_TIME_FALLBACK_QUOTES = [
    "Sucking at something is the first step to being sorta good at something.",
    "People get built different. We do not need to figure it out, we just need to respect it.",
    "To live life, you need problems. If you get everything you want right away, what is the point?",
    "Everything small is just a smaller version of something big.",
    "Sometimes life is scary and dark. That is why we must find the light.",
    "Homies help homies. Always.",
    "Dude, all this stuff I made up is totally from my imagination.",
    "I should not have drunk that much tea.",
    "Bad biscuits make the baker broke, bro.",
    "Today is an amazing day for an adventure.",
    "I have approximate knowledge of many things.",
    "You are my best friend in the world.",
    "I just wanted to be useful for once.",
    "We can do this together, no sweat.",
    "A little weirdness is healthy for the soul.",
    "I came here to party and tell the truth.",
    "This is what confidence looks like.",
    "Even heroes get scared and keep going.",
    "I know this looks bad, but trust the process.",
    "A true king protects everyone, even strangers.",
    "No one is born chill. You practice.",
    "I like the way your brain solves chaos.",
    "You can be soft and still be strong.",
    "This is a mess, but it is our mess.",
    "You cannot speedrun growing up.",
    "Sometimes the map is wrong and the heart is right.",
    "Breathe in, scream later.",
    "The universe is huge, and we are still here.",
    "A promise is a kind of magic.",
    "If it is funny and kind, it is probably right.",
    "I am not running away. I am repositioning.",
    "You do not need a crown to act noble.",
    "Courage is doing the thing while shaky.",
    "That was reckless. Also, kind of amazing.",
    "Do not talk to me before my pancake hour.",
    "I trust you with my weird side.",
    "If the song is honest, sing it loud.",
    "Being nice is not the same as being weak.",
    "I do not want perfect. I want real.",
    "Sometimes sorry is a beginning, not an ending.",
    "We can be brave one minute at a time.",
    "No destiny beats teamwork.",
    "I am choosing hope on purpose.",
    "A joke can carry a lot of pain.",
    "The sword is cool, but kindness is cooler.",
    "You can change and still be yourself.",
    "I did a bad thing. I can do a better thing next.",
    "Even candy kingdoms need hard conversations.",
    "The weird route is still a route.",
    "Good friends call you in, not just out.",
    "Today we fight the chaos with snacks.",
    "I am not a side character in my own life.",
    "Big feelings are not a crime.",
    "Take the long way if it keeps you kind.",
    "I cannot fix everything, but I can show up.",
    "If it sounds impossible, we are early.",
]
BOJACK_HORSEMAN_FALLBACK_QUOTES = [
    "When you look at someone through rose-colored glasses, all the red flags just look like flags.",
    "It gets easier. Every day it gets a little easier. But you gotta do it every day.",
    "In this terrifying world, all we have are the connections that we make.",
    "Sometimes life's a bitch and then you keep living.",
    "The key to being happy is to keep yourself busy with unimportant nonsense.",
    "Closure is a made-up thing by Steven Spielberg to sell movie tickets.",
    "I am responsible for my own happiness, and that is terrifying.",
    "You cannot keep doing this. You cannot keep doing bad things and feel fine.",
    "I need you to tell me that I am a good person.",
    "There is no other side. This is it.",
    "You turn yourself around. That is what it is all about.",
    "Every day I wake up and I keep trying.",
    "I keep waiting to feel normal, and normal never arrives.",
    "I confuse being needed with being loved.",
    "You are not beyond help, but you are out of excuses.",
    "I am tired of mistaking drama for depth.",
    "If you want to be better, be better now.",
    "A grand gesture is not the same as accountability.",
    "You cannot edit your life in post.",
    "Fame does not fix loneliness.",
    "I keep making the same mistake in new outfits.",
    "You apologize beautifully and repeat perfectly.",
    "Silence can be kinder than another lie.",
    "I do not need a perfect ending. I need an honest one.",
    "This is what recovery looks like on a random Tuesday.",
    "You cannot punish yourself into being good.",
    "Being cynical is not the same as being smart.",
    "The joke is getting old, and so am I.",
    "I keep trying to outrun myself and keep losing.",
    "Sometimes the healthiest move is boring.",
    "I thought I wanted attention. I wanted care.",
    "A public apology is still private work.",
    "You are not the hero of every room.",
    "I did not need a comeback. I needed boundaries.",
    "I can be charming and still be wrong.",
    "The version of me in my head is not evidence.",
    "I hurt people when I am scared. That is still on me.",
    "There is no clean slate, only next choices.",
    "I keep calling chaos destiny.",
    "The hardest part is showing up when no one claps.",
    "I cannot outsource my conscience.",
    "Sometimes progress is just not making it worse.",
    "I wanted forgiveness before change.",
    "You cannot guilt people into staying.",
    "Nostalgia is not a plan.",
    "I miss the past because it cannot reject me.",
    "Being busy is not the same as being okay.",
    "I learned to perform feelings before I learned to feel them.",
    "You can be loved and still feel alone.",
    "I am trying to choose honesty over control.",
    "You are allowed to leave people who keep hurting you.",
    "My pain is real, but it is not a permission slip.",
    "I keep asking for understanding instead of doing the work.",
    "You do not get credit for a promise you never keep.",
    "This is not rock bottom. It is another warning.",
    "I can make a better choice before the disaster.",
]
WORD_POOL = [
    "motion", "quiet", "signal", "rhythm", "grain", "amber", "echo", "circuit", "wild", "steady", "floating",
    "paper", "memory", "transient", "hollow", "crystal", "breathing", "heavy", "light", "hidden", "magnetic",
    "velvet", "kinetic", "drifting", "pulse", "shadow", "luminous", "fragile", "friction", "silver", "warm",
    "future", "timber", "mosaic", "radar", "planet", "window", "resonant", "speckled", "ghost", "signal",
    "fragment", "weather", "engine", "fabric", "field", "forest", "digital", "analog", "paper", "nocturnal",
]
MONTH_NAME_RE = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december|"
    r"jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b",
    flags=re.IGNORECASE,
)
QUOTE_CACHE_PATH = BASE_DIR / "cartoon_quotes_cache.json"
AUTH_DB_PATH = BASE_DIR / "access.db"
AUTH_COOKIE_NAME = "text_to_midi_session"
SESSION_TTL_DAYS = 30
AUTH_SECRET = os.getenv("APP_AUTH_SECRET", "change-me-in-production")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000").rstrip("/")

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_BILLING_MODE = os.getenv("STRIPE_BILLING_MODE", "one_time").strip().lower()

PATREON_CLIENT_ID = os.getenv("PATREON_CLIENT_ID", "").strip()
PATREON_CLIENT_SECRET = os.getenv("PATREON_CLIENT_SECRET", "").strip()
PATREON_REDIRECT_URI = os.getenv("PATREON_REDIRECT_URI", f"{APP_BASE_URL}/api/billing/patreon/callback").strip()
PATREON_REQUIRED_TIERS = {
    t.strip() for t in os.getenv("PATREON_REQUIRED_TIERS", "").split(",") if t.strip()
}

if stripe and STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(AUTH_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db():
    with _db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              entitlement_source TEXT NOT NULL DEFAULT 'none',
              paid_until TEXT NULL,
              stripe_customer_id TEXT NULL,
              stripe_subscription_id TEXT NULL,
              patreon_user_id TEXT NULL,
              patreon_access_token TEXT NULL,
              patreon_refresh_token TEXT NULL,
              patreon_expires_at TEXT NULL,
              last_patreon_sync TEXT NULL,
              created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_stripe_customer ON users(stripe_customer_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_patreon_user ON users(patreon_user_id)")
        conn.commit()


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1)
    return f"scrypt${base64.urlsafe_b64encode(salt).decode()}${base64.urlsafe_b64encode(digest).decode()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, salt_b64, digest_b64 = stored.split("$", 2)
        if scheme != "scrypt":
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode())
        expected = base64.urlsafe_b64decode(digest_b64.encode())
        got = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1)
        return hmac.compare_digest(got, expected)
    except Exception:
        return False


def make_session_token(user_id: int, expires_at: datetime) -> str:
    payload = f"{int(user_id)}:{int(expires_at.timestamp())}"
    sig = hmac.new(AUTH_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{payload}:{sig}".encode("utf-8")).decode("utf-8")


def parse_session_token(token: str) -> tuple[int, int] | None:
    try:
        raw = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        user_id_s, exp_s, sig = raw.split(":", 2)
        payload = f"{user_id_s}:{exp_s}"
        expected = hmac.new(AUTH_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        user_id = int(user_id_s)
        exp = int(exp_s)
        if exp < int(time.time()):
            return None
        return user_id, exp
    except Exception:
        return None


def get_user_by_id(user_id: int) -> sqlite3.Row | None:
    with _db() as conn:
        return conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()


def get_user_by_email(email: str) -> sqlite3.Row | None:
    with _db() as conn:
        return conn.execute("SELECT * FROM users WHERE lower(email) = lower(?)", (email.strip(),)).fetchone()


def set_auth_cookie(response: Response, user_id: int):
    exp = utc_now() + timedelta(days=SESSION_TTL_DAYS)
    token = make_session_token(user_id, exp)
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=SESSION_TTL_DAYS * 24 * 3600,
    )


def clear_auth_cookie(response: Response):
    response.delete_cookie(AUTH_COOKIE_NAME)


def user_has_paid_access(user: sqlite3.Row | None) -> bool:
    if not user:
        return False
    source = str(user["entitlement_source"] or "none")
    if source in {"stripe", "patreon", "manual"}:
        paid_until = user["paid_until"]
        if not paid_until:
            return True
        try:
            dt = datetime.fromisoformat(str(paid_until))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt > utc_now()
        except Exception:
            return False
    return False


def require_paid_user(request: Request) -> sqlite3.Row:
    token = request.cookies.get(AUTH_COOKIE_NAME, "")
    parsed = parse_session_token(token) if token else None
    if not parsed:
        raise HTTPException(status_code=401, detail="Login required.")
    user_id, _ = parsed
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Session invalid.")
    if not user_has_paid_access(user):
        raise HTTPException(status_code=402, detail="Paid access required.")
    return user


def get_current_user(request: Request) -> sqlite3.Row | None:
    token = request.cookies.get(AUTH_COOKIE_NAME, "")
    parsed = parse_session_token(token) if token else None
    if not parsed:
        return None
    user, _ = (get_user_by_id(parsed[0]), parsed[1])
    return user


def public_user_payload(user: sqlite3.Row | None) -> dict:
    if not user:
        return {"logged_in": False, "paid_access": False}
    return {
        "logged_in": True,
        "email": user["email"],
        "paid_access": user_has_paid_access(user),
        "entitlement_source": user["entitlement_source"] or "none",
    }


def available_output_ports():
    try:
        return mapper.mido.get_output_names()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read MIDI ports: {exc}")


init_auth_db()


def random_text():
    style = random.choice(["two_words", "sentence", "sentence", "paragraph"])
    if style == "two_words":
        return " ".join(random.sample(WORD_POOL, 2))
    if style == "sentence":
        n = random.randint(6, 14)
        words = [random.choice(WORD_POOL) for _ in range(n)]
        words[0] = words[0].capitalize()
        return " ".join(words) + random.choice([".", ".", "?", "!"])

    sentence_count = random.randint(2, 4)
    chunks = []
    for _ in range(sentence_count):
        n = random.randint(7, 16)
        words = [random.choice(WORD_POOL) for _ in range(n)]
        words[0] = words[0].capitalize()
        chunks.append(" ".join(words) + random.choice([".", ".", "?", "!"]))
    return " ".join(chunks)


def wikipedia_random_extract() -> tuple[str, str]:
    params = {
        "action": "query",
        "format": "json",
        "generator": "random",
        "grnnamespace": 0,
        "grnlimit": 1,
        "prop": "extracts",
        "explaintext": "1",
        "exintro": "0",
        "exchars": "12000",
        "utf8": "1",
    }
    url = "https://en.wikipedia.org/w/api.php?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "text_to_MIDI-webapp/1.0 (local tool; contact: local-user)"},
    )
    with urllib.request.urlopen(req, timeout=7.0) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))

    pages = data.get("query", {}).get("pages", {})
    if not pages:
        raise RuntimeError("Wikipedia returned no random page.")
    page = next(iter(pages.values()))
    title = (page.get("title") or "").strip()
    extract = clean_text_block(page.get("extract") or "")
    if not extract:
        raise RuntimeError("Random Wikipedia page had empty extract.")
    return title, extract


def split_into_sentences(text: str) -> list[str]:
    text = clean_text_block(text)
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [clean_text_block(part) for part in parts if clean_text_block(part)]


def is_clean_wiki_sentence(sentence: str) -> bool:
    s = sentence.strip()
    if len(s) < 30 or len(s) > 260:
        return False
    if re.search(r"https?://|www\.", s, flags=re.IGNORECASE):
        return False
    if re.search(r"\b\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?\b", s):
        return False
    if re.search(r"\b(1[0-9]{3}|20[0-9]{2})\b", s):
        return False
    if MONTH_NAME_RE.search(s) and re.search(r"\b\d{1,2}\b", s):
        return False
    if re.search(r"\b(ISBN|ISSN|DOI|Coordinates|latitude|longitude)\b", s, flags=re.IGNORECASE):
        return False
    return True


def random_wikipedia_sentences(min_sentences: int = 1, max_sentences: int = 6) -> tuple[str, str]:
    target_count = random.randint(min_sentences, max_sentences)
    for _ in range(12):
        title, extract = wikipedia_random_extract()
        sentences = [s for s in split_into_sentences(extract) if is_clean_wiki_sentence(s)]
        if not sentences:
            continue
        take = min(target_count, len(sentences))
        start_max = len(sentences) - take
        start = random.randint(0, start_max) if start_max > 0 else 0
        selected = sentences[start : start + take]
        return " ".join(selected), title
    raise RuntimeError("Could not find clean random Wikipedia sentences.")


def wikipedia_extract_for_title(title: str) -> str:
    params = {
        "action": "query",
        "format": "json",
        "titles": title,
        "prop": "extracts",
        "explaintext": "1",
        "exintro": "0",
        "exchars": "5000",
        "utf8": "1",
    }
    url = "https://en.wikipedia.org/w/api.php?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "text_to_MIDI-webapp/1.0 (local tool; contact: local-user)",
        },
    )

    with urllib.request.urlopen(req, timeout=6.0) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))

    pages = data.get("query", {}).get("pages", {})
    if not pages:
        raise RuntimeError("Wikipedia returned no pages.")

    # titles query usually returns one page; pick the first with non-empty extract.
    extracts = []
    for page in pages.values():
        extract = (page.get("extract") or "").strip()
        if extract:
            extracts.append(extract)
    if not extracts:
        raise RuntimeError("Wikipedia extract was empty.")

    return random.choice(extracts)


def clean_text_block(text: str) -> str:
    # Normalize whitespace and remove citation-like brackets.
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def wikiquote_html_for_title(title: str) -> str:
    params = {
        "action": "parse",
        "format": "json",
        "page": title,
        "prop": "text",
        "formatversion": "2",
        "redirects": "1",
    }
    url = "https://en.wikiquote.org/w/api.php?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "text_to_MIDI-webapp/1.0 (local tool; contact: local-user)",
        },
    )
    with urllib.request.urlopen(req, timeout=7.0) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    parsed = data.get("parse", {})
    html_text = parsed.get("text", "")
    if not html_text:
        raise RuntimeError("No quote HTML found for page.")
    return html_text


def extract_quotes_from_wikiquote_html(raw_html: str) -> list[str]:
    # Wikiquote pages typically render quotes as list items.
    items = re.findall(r"<li[^>]*>(.*?)</li>", raw_html, flags=re.IGNORECASE | re.DOTALL)
    quotes: list[str] = []
    for item in items:
        # Remove nested lists that are often commentary/attribution details.
        item = re.sub(r"<ul[^>]*>.*?</ul>", " ", item, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", item)
        text = html.unescape(text)
        text = re.sub(r"\[[^\]]+\]", "", text)
        text = clean_text_block(text)
        text = text.strip(" -–—:;\"'“”‘’")
        if len(text) < 28 or len(text) > 320:
            continue
        if text.lower().startswith(("see also", "external links", "references")):
            continue
        # Keep lines that look like spoken quotations.
        if re.search(r"[.!?]$", text) or "," in text:
            quotes.append(text)
    # Deduplicate while preserving order.
    seen = set()
    out = []
    for q in quotes:
        if q not in seen:
            seen.add(q)
            out.append(q)
    return out


def _fallback_quote_entries() -> list[dict]:
    return [
        {"text": q, "source_title": "Adventure Time", "source": "local-adventure-time-fallback"}
        for q in ADVENTURE_TIME_FALLBACK_QUOTES
    ] + [
        {"text": q, "source_title": "BoJack Horseman", "source": "local-bojack-horseman-fallback"}
        for q in BOJACK_HORSEMAN_FALLBACK_QUOTES
    ]


def _normalize_quote_entry(text: str, source_title: str, source: str) -> dict | None:
    t = clean_text_block(text).strip(" -–—:;\"'“”‘’")
    if len(t) < 18 or len(t) > 320:
        return None
    return {"text": t, "source_title": source_title, "source": source}


def _dedupe_quote_entries(entries: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for e in entries:
        key = (e.get("text", "").lower(), e.get("source_title", ""))
        if not key[0] or key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _fetch_quotes_from_titles() -> list[dict]:
    pools = [
        ("wikiquote-adventure-time", ADVENTURE_TIME_TITLES),
        ("wikiquote-bojack-horseman", BOJACK_HORSEMAN_TITLES),
    ]
    entries: list[dict] = []
    for source_name, titles in pools:
        for title in titles:
            try:
                html_text = wikiquote_html_for_title(title)
                quotes = extract_quotes_from_wikiquote_html(html_text)
                for q in quotes:
                    item = _normalize_quote_entry(q, title, source_name)
                    if item:
                        entries.append(item)
            except Exception:
                continue
    return _dedupe_quote_entries(entries)


def _load_cached_quote_pool() -> list[dict]:
    try:
        if not QUOTE_CACHE_PATH.exists():
            return []
        raw = json.loads(QUOTE_CACHE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return []
        entries = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            item = _normalize_quote_entry(
                str(row.get("text", "")),
                str(row.get("source_title", "Unknown")),
                str(row.get("source", "local-cache")),
            )
            if item:
                entries.append(item)
        return _dedupe_quote_entries(entries)
    except Exception:
        return []


def _save_cached_quote_pool(entries: list[dict]):
    try:
        QUOTE_CACHE_PATH.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def get_quote_pool() -> list[dict]:
    global QUOTE_POOL
    with QUOTE_LOCK:
        if QUOTE_POOL:
            return QUOTE_POOL

        cached = _load_cached_quote_pool()
        if len(cached) >= 100:
            QUOTE_POOL = cached
            return QUOTE_POOL

        fetched = _fetch_quotes_from_titles()
        merged = _dedupe_quote_entries(cached + fetched + _fallback_quote_entries())
        if len(merged) < 100:
            # Ensure non-empty and deterministic behavior even with no network.
            merged = _dedupe_quote_entries(merged + _fallback_quote_entries())
        if merged:
            _save_cached_quote_pool(merged)
            QUOTE_POOL = merged
        else:
            QUOTE_POOL = _fallback_quote_entries()
        return QUOTE_POOL


def random_cartoon_quote() -> tuple[str, str, str]:
    pool = get_quote_pool()
    if not pool:
        fallback = _fallback_quote_entries()
        item = random.choice(fallback)
        return item["text"], item["source_title"], item["source"]

    recent = set(RECENT_QUOTES)
    candidates = [q for q in pool if q["text"] not in recent]
    if not candidates:
        RECENT_QUOTES.clear()
        candidates = pool[:]

    item = random.choice(candidates)
    RECENT_QUOTES.append(item["text"])
    return item["text"], item["source_title"], item["source"]


def stable_variation_seed(run_seed: int, text: str) -> int:
    payload = f"{int(run_seed)}|{text}".encode("utf-8", errors="ignore")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def build_events(text: str, key: str, mode: str, variation_seed: int | None = None):
    key_pc = mapper.parse_key_root(key)
    if key_pc is None:
        raise HTTPException(status_code=400, detail="Invalid key.")

    mode_name = mapper.parse_mode(mode) or mode
    if mode_name not in mapper.MODE_INTERVALS:
        raise HTTPException(status_code=400, detail="Invalid mode.")

    if variation_seed is None:
        events = mapper.text_to_events(
            text,
            key_root_pc=key_pc,
            mode_intervals=mapper.MODE_INTERVALS[mode_name],
        )
    else:
        # Keep a fixed "humanized" variation for this text inside one send session.
        with RNG_LOCK:
            prev_state = random.getstate()
            random.seed(int(variation_seed))
            try:
                events = mapper.text_to_events(
                    text,
                    key_root_pc=key_pc,
                    mode_intervals=mapper.MODE_INTERVALS[mode_name],
                )
            finally:
                random.setstate(prev_state)
    note_events = [e for e in events if e.get("chord")]
    if not note_events:
        raise HTTPException(status_code=400, detail="No MIDI notes generated for this input.")
    return events, note_events


def wait_with_stop(stop_event: threading.Event, seconds: float):
    end = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < end:
        if stop_event.is_set():
            return False
        time.sleep(0.02)
    return True


def panic_midi_port(port_name: str):
    try:
        with mapper.mido.open_output(port_name) as outport:
            for ch in range(16):
                outport.send(mapper.Message("control_change", channel=ch, control=123, value=0))  # all notes off
                outport.send(mapper.Message("control_change", channel=ch, control=120, value=0))  # all sound off
                outport.send(mapper.Message("pitchwheel", channel=ch, pitch=0))
            # Extra safety for synths ignoring CC 123/120.
            for ch in range(16):
                for note in range(128):
                    outport.send(mapper.Message("note_off", channel=ch, note=note, velocity=0))
    except Exception:
        # Panic should never raise for API callers.
        pass


def panic_midi_outputs():
    ports = []
    try:
        ports = mapper.mido.get_output_names()
    except Exception:
        ports = []

    with STATE_LOCK:
        selected = (LIVE_STATE.get("port_name") or "").strip()
    if selected and selected not in ports:
        ports.insert(0, selected)

    for port in ports:
        panic_midi_port(port)


def clear_note_feed():
    global NOTE_SEQ
    with NOTE_LOCK:
        NOTE_SEQ = 0
        NOTE_FEED.clear()


def push_note_event(
    note: int,
    velocity: int,
    source_token: str = "",
    source_units=None,
    source_syllables: int = 0,
    note_index: int | None = None,
    chord_size: int | None = None,
):
    global NOTE_SEQ
    name = mapper.midi_note_to_name(int(note))
    token = str(source_token or "")
    units = list(source_units or [])
    syllables = int(source_syllables or 0)
    idx = int(note_index or 0)
    single_unit = ""
    if units:
        single_unit = str(units[idx % len(units)])
    with NOTE_LOCK:
        NOTE_SEQ += 1
        NOTE_FEED.append(
            {
                "id": NOTE_SEQ,
                "note": int(note),
                "name": name,
                "velocity": int(velocity),
                "source_token": token,
                "source_units": units,
                "source_unit": single_unit,
                "source_syllables": syllables,
                "note_index": idx,
                "chord_size": int(chord_size or 0),
                "ts": round(time.time(), 3),
            }
        )
        if len(NOTE_FEED) > 600:
            del NOTE_FEED[:200]


def patreon_api_request(url: str, access_token: str, method: str = "GET", data: dict | None = None) -> dict:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "text_to_MIDI-webapp/1.0",
    }
    body = None
    if data is not None:
        body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=8.0) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def patreon_token_exchange(code: str) -> dict:
    if not (PATREON_CLIENT_ID and PATREON_CLIENT_SECRET):
        raise HTTPException(status_code=500, detail="Patreon OAuth is not configured.")
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": PATREON_CLIENT_ID,
        "client_secret": PATREON_CLIENT_SECRET,
        "redirect_uri": PATREON_REDIRECT_URI,
    }
    req = urllib.request.Request(
        "https://www.patreon.com/api/oauth2/token",
        data=urllib.parse.urlencode(data).encode("utf-8"),
        headers={"User-Agent": "text_to_MIDI-webapp/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=8.0) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def patreon_refresh_token(refresh_token: str) -> dict:
    data = {
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "client_id": PATREON_CLIENT_ID,
        "client_secret": PATREON_CLIENT_SECRET,
    }
    req = urllib.request.Request(
        "https://www.patreon.com/api/oauth2/token",
        data=urllib.parse.urlencode(data).encode("utf-8"),
        headers={"User-Agent": "text_to_MIDI-webapp/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=8.0) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def sync_patreon_membership(user_id: int) -> bool:
    with _db() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return False
        access_token = row["patreon_access_token"] or ""
        refresh_token = row["patreon_refresh_token"] or ""
        expires_at = row["patreon_expires_at"]
        if not access_token:
            return False

        try:
            if expires_at:
                exp_dt = datetime.fromisoformat(str(expires_at))
                if exp_dt.tzinfo is None:
                    exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                if exp_dt <= utc_now() + timedelta(seconds=30) and refresh_token:
                    tok = patreon_refresh_token(refresh_token)
                    access_token = tok.get("access_token", access_token)
                    refresh_token = tok.get("refresh_token", refresh_token)
                    ttl = int(tok.get("expires_in", 3600))
                    exp_new = utc_now() + timedelta(seconds=ttl)
                    conn.execute(
                        "UPDATE users SET patreon_access_token = ?, patreon_refresh_token = ?, patreon_expires_at = ? WHERE id = ?",
                        (access_token, refresh_token, exp_new.isoformat(), user_id),
                    )
                    conn.commit()

            identity = patreon_api_request(
                "https://www.patreon.com/api/oauth2/v2/identity?include=memberships,memberships.currently_entitled_tiers&fields%5Bmember%5D=patron_status,last_charge_status",
                access_token,
            )
            included = identity.get("included", []) or []
            member_rows = [x for x in included if x.get("type") == "member"]
            tier_rows = {str(x.get("id")): x for x in included if x.get("type") == "tier"}

            paid = False
            for member in member_rows:
                attrs = member.get("attributes", {}) or {}
                patron_status = str(attrs.get("patron_status") or "")
                charge_status = str(attrs.get("last_charge_status") or "")
                rel = member.get("relationships", {}) or {}
                tiers_data = ((rel.get("currently_entitled_tiers") or {}).get("data") or [])
                tier_ids = {str(t.get("id")) for t in tiers_data if t.get("id")}
                if PATREON_REQUIRED_TIERS and tier_ids.isdisjoint(PATREON_REQUIRED_TIERS):
                    continue
                if patron_status.lower() in {"active_patron", "former_patron"} and charge_status.lower() in {"paid", "free"}:
                    paid = True
                    break
                if patron_status.lower() == "active_patron":
                    paid = True
                    break

            ent_source = "patreon" if paid else "none"
            conn.execute(
                "UPDATE users SET entitlement_source = ?, paid_until = NULL, last_patreon_sync = ? WHERE id = ?",
                (ent_source, utc_now().isoformat(), user_id),
            )
            conn.commit()
            return paid
        except Exception:
            return user_has_paid_access(row)


@app.post("/api/auth/register")
def auth_register(response: Response, email: str = Form(...), password: str = Form(...)):
    email = email.strip().lower()
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        raise HTTPException(status_code=400, detail="Invalid email.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    if get_user_by_email(email):
        raise HTTPException(status_code=409, detail="Email already exists.")
    with _db() as conn:
        cur = conn.execute(
            "INSERT INTO users(email, password_hash, created_at) VALUES (?, ?, ?)",
            (email, hash_password(password), utc_now().isoformat()),
        )
        user_id = int(cur.lastrowid)
        conn.commit()
    set_auth_cookie(response, user_id)
    user = get_user_by_id(user_id)
    return {"ok": True, "user": public_user_payload(user)}


@app.post("/api/auth/login")
def auth_login(response: Response, email: str = Form(...), password: str = Form(...)):
    user = get_user_by_email(email.strip().lower())
    if not user or not verify_password(password, str(user["password_hash"])):
        raise HTTPException(status_code=401, detail="Invalid email/password.")
    if user["patreon_user_id"]:
        sync_patreon_membership(int(user["id"]))
        user = get_user_by_id(int(user["id"])) or user
    set_auth_cookie(response, int(user["id"]))
    return {"ok": True, "user": public_user_payload(user)}


@app.post("/api/auth/logout")
def auth_logout(response: Response):
    clear_auth_cookie(response)
    return {"ok": True}


@app.get("/api/auth/me")
def auth_me(request: Request):
    user = get_current_user(request)
    if user and user["patreon_user_id"]:
        sync_patreon_membership(int(user["id"]))
        user = get_user_by_id(int(user["id"])) or user
    return {"user": public_user_payload(user)}


@app.post("/api/billing/stripe/checkout")
def stripe_checkout(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Login required.")
    if not stripe or not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        raise HTTPException(status_code=500, detail="Stripe is not configured.")

    customer_id = user["stripe_customer_id"]
    if not customer_id:
        customer = stripe.Customer.create(email=user["email"], metadata={"user_id": str(user["id"])})
        customer_id = customer["id"]
        with _db() as conn:
            conn.execute("UPDATE users SET stripe_customer_id = ? WHERE id = ?", (customer_id, int(user["id"])))
            conn.commit()

    checkout_mode = "subscription" if STRIPE_BILLING_MODE == "subscription" else "payment"
    session = stripe.checkout.Session.create(
        mode=checkout_mode,
        customer=customer_id,
        line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
        success_url=f"{APP_BASE_URL}/?billing=success",
        cancel_url=f"{APP_BASE_URL}/?billing=cancel",
        client_reference_id=str(user["id"]),
        allow_promotion_codes=True,
    )
    return {"checkout_url": session.get("url", "")}


@app.post("/api/billing/stripe/portal")
def stripe_portal(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Login required.")
    if not stripe or not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe is not configured.")
    if STRIPE_BILLING_MODE != "subscription":
        raise HTTPException(status_code=400, detail="Billing portal is only available for subscription mode.")
    customer_id = (user["stripe_customer_id"] or "").strip()
    if not customer_id:
        raise HTTPException(status_code=400, detail="No Stripe customer for this account.")
    sess = stripe.billing_portal.Session.create(customer=customer_id, return_url=f"{APP_BASE_URL}/")
    return {"portal_url": sess.get("url", "")}


@app.post("/api/billing/stripe/webhook")
async def stripe_webhook(request: Request, stripe_signature: str = Header(default="", alias="Stripe-Signature")):
    if not stripe or not STRIPE_SECRET_KEY:
        return JSONResponse({"ok": True, "ignored": True})
    payload = await request.body()
    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload=payload, sig_header=stripe_signature, secret=STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload.decode("utf-8", errors="replace"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Stripe webhook.")

    etype = event.get("type", "")
    data = (event.get("data") or {}).get("object") or {}
    customer_id = str(data.get("customer") or "")
    sub_id = str(data.get("id") or "")

    if customer_id:
        with _db() as conn:
            user = conn.execute("SELECT * FROM users WHERE stripe_customer_id = ?", (customer_id,)).fetchone()
            if user:
                if etype == "checkout.session.completed":
                    payment_status = str(data.get("payment_status") or "").lower()
                    if payment_status in {"paid", "no_payment_required"}:
                        conn.execute(
                            "UPDATE users SET entitlement_source = 'stripe', paid_until = NULL WHERE id = ?",
                            (int(user["id"]),),
                        )
                elif STRIPE_BILLING_MODE == "subscription":
                    if etype in {"customer.subscription.created", "customer.subscription.updated"}:
                        status = str(data.get("status") or "").lower()
                        if status in {"active", "trialing"}:
                            conn.execute(
                                "UPDATE users SET entitlement_source = 'stripe', stripe_subscription_id = ?, paid_until = NULL WHERE id = ?",
                                (sub_id or user["stripe_subscription_id"], int(user["id"])),
                            )
                    elif etype in {"customer.subscription.deleted", "customer.subscription.paused"}:
                        conn.execute(
                            "UPDATE users SET entitlement_source = 'none', stripe_subscription_id = NULL WHERE id = ?",
                            (int(user["id"]),),
                        )
                conn.commit()
    return {"ok": True}


@app.get("/api/billing/patreon/connect")
def patreon_connect(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Login required.")
    if not PATREON_CLIENT_ID:
        raise HTTPException(status_code=500, detail="Patreon is not configured.")
    state = secrets.token_urlsafe(20)
    auth_url = (
        "https://www.patreon.com/oauth2/authorize"
        f"?response_type=code&client_id={urllib.parse.quote(PATREON_CLIENT_ID)}"
        f"&redirect_uri={urllib.parse.quote(PATREON_REDIRECT_URI)}"
        "&scope=identity%20identity.memberships"
        f"&state={urllib.parse.quote(str(user['id']) + ':' + state)}"
    )
    return {"auth_url": auth_url}


@app.get("/api/billing/patreon/callback")
def patreon_callback(code: str = Query(default=""), state: str = Query(default="")):
    if not code or ":" not in state:
        return RedirectResponse(url=f"{APP_BASE_URL}/?patreon=error")
    user_id_str, _nonce = state.split(":", 1)
    try:
        user_id = int(user_id_str)
    except Exception:
        return RedirectResponse(url=f"{APP_BASE_URL}/?patreon=error")

    try:
        token_data = patreon_token_exchange(code)
        access_token = str(token_data.get("access_token") or "")
        refresh_token = str(token_data.get("refresh_token") or "")
        expires_in = int(token_data.get("expires_in") or 3600)
        expires_at = utc_now() + timedelta(seconds=expires_in)

        ident = patreon_api_request(
            "https://www.patreon.com/api/oauth2/v2/identity?fields%5Buser%5D=email",
            access_token,
        )
        pdata = ident.get("data", {}) or {}
        patreon_user_id = str(pdata.get("id") or "")
        with _db() as conn:
            conn.execute(
                """
                UPDATE users
                SET patreon_user_id = ?, patreon_access_token = ?, patreon_refresh_token = ?, patreon_expires_at = ?
                WHERE id = ?
                """,
                (patreon_user_id, access_token, refresh_token, expires_at.isoformat(), user_id),
            )
            conn.commit()
        sync_patreon_membership(user_id)
        return RedirectResponse(url=f"{APP_BASE_URL}/?patreon=connected")
    except Exception:
        return RedirectResponse(url=f"{APP_BASE_URL}/?patreon=error")


@app.post("/api/billing/patreon/sync")
def patreon_sync(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Login required.")
    ok = sync_patreon_membership(int(user["id"]))
    fresh = get_user_by_id(int(user["id"]))
    return {"ok": ok, "user": public_user_payload(fresh)}


@app.get("/api/ports")
def get_ports():
    return {"ports": available_output_ports()}


@app.get("/api/live-notes")
def get_live_notes(after: int = Query(default=0, ge=0)):
    with NOTE_LOCK:
        events = [e for e in NOTE_FEED if int(e["id"]) > int(after)]
        latest = NOTE_SEQ
    with SEND_LOCK:
        active = SEND_ACTIVE
    return {"events": events, "latest_id": latest, "active": active}


@app.get("/api/randomize")
def randomize_payload():
    txt, source_title, source = random_cartoon_quote()

    ports = available_output_ports()
    return {
        "tempo_bpm": int(round(random.triangular(20, 200, 55))),
        "key": random.choice(KEY_OPTIONS),
        "mode": random.choice(MODE_OPTIONS),
        "text": txt,
        "text_source": source,
        "text_style": "sentences",
        "source_title": source_title,
        "port_name": random.choice(ports) if ports else "",
    }


@app.get("/api/randomize-text")
def randomize_text_only():
    txt, source_title, source = random_cartoon_quote()
    return {
        "text": txt,
        "text_source": source,
        "text_style": "quotes",
        "source_title": source_title,
    }


def normalize_voicing_mode(value: str | None) -> str:
    mode = str(value or "closed").strip().lower()
    return "open" if mode == "open" else "closed"


@app.post("/api/send-live")
def send_live(
    request: Request,
    text: str = Form(...),
    tempo_bpm: int = Form(60),
    key: str = Form("C"),
    mode: str = Form("major"),
    port_name: str = Form(""),
    bend_amount: int = Form(0),
    loop_enabled: bool = Form(False),
    loop_gap_ms: int = Form(0),
    octave_shift: int = Form(0),
    degree_shift: int = Form(0),
    voicing_mode: str = Form("closed"),
):
    require_paid_user(request)
    global SEND_ACTIVE
    text = text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required.")

    tempo_bpm = max(20, min(200, int(tempo_bpm)))
    bend_amount = max(0, min(100, int(bend_amount)))
    loop_gap_ms = max(0, min(5000, int(loop_gap_ms)))
    if loop_enabled:
        loop_gap_ms = max(MIN_LOOP_GAP_MS, loop_gap_ms)
    octave_shift = max(-3, min(3, int(octave_shift)))
    degree_shift = max(-14, min(14, int(degree_shift)))
    voicing_mode = normalize_voicing_mode(voicing_mode)
    with SEND_LOCK:
        if SEND_ACTIVE:
            raise HTTPException(status_code=409, detail="A MIDI send is already in progress.")
        SEND_ACTIVE = True
        STOP_EVENT.clear()
        with STATE_LOCK:
            LIVE_STATE["text"] = text
            LIVE_STATE["tempo_bpm"] = tempo_bpm
            LIVE_STATE["key"] = key
            LIVE_STATE["mode"] = mode
            LIVE_STATE["port_name"] = port_name.strip()
            LIVE_STATE["bend_amount"] = bend_amount
            LIVE_STATE["loop_enabled"] = bool(loop_enabled)
            LIVE_STATE["loop_gap_ms"] = loop_gap_ms
            LIVE_STATE["octave_shift"] = octave_shift
            LIVE_STATE["degree_shift"] = degree_shift
            LIVE_STATE["voicing_mode"] = voicing_mode

    run_seed = random.getrandbits(64)

    try:
        completed = True
        loop_count = 0
        last_note_names = []
        last_port = ""
        with STATE_LOCK:
            state = dict(LIVE_STATE)

        if not bool(state["loop_enabled"]):
            current_text = state["text"].strip()
            if not current_text:
                raise HTTPException(status_code=400, detail="Text is required.")

            base_seed = stable_variation_seed(run_seed, current_text)
            base_events, note_events = build_events(
                current_text,
                key=state["key"],
                mode=state["mode"],
                variation_seed=base_seed,
            )
            ports = available_output_ports()
            if not ports:
                raise HTTPException(status_code=400, detail="No MIDI output ports found.")
            chosen_port = state["port_name"] if state["port_name"] else ports[0]
            if chosen_port not in ports:
                chosen_port = ports[0]

            def state_provider():
                with STATE_LOCK:
                    live = dict(LIVE_STATE)
                key_pc_live = mapper.parse_key_root(live.get("key", "C"))
                mode_name_live = mapper.parse_mode(live.get("mode", "major")) or live.get("mode", "major")
                mode_intervals_live = mapper.MODE_INTERVALS.get(mode_name_live, mapper.MODE_INTERVALS["major"])
                return {
                    "tempo_bpm": int(live.get("tempo_bpm", 60)),
                    "key_root_pc": key_pc_live if key_pc_live is not None else mapper.NOTE_NAME_TO_PC["C"],
                    "mode_intervals": mode_intervals_live,
                    "octave_shift": int(live.get("octave_shift", 0)),
                    "degree_shift": int(live.get("degree_shift", 0)),
                    "bend_amount": int(live.get("bend_amount", 0)),
                    "voicing_mode": normalize_voicing_mode(live.get("voicing_mode", "closed")),
                }

            loop_count = 1
            completed = mapper.send_live_reactive(
                chosen_port,
                base_events,
                state_provider=state_provider,
                stop_requested=STOP_EVENT.is_set,
                note_callback=push_note_event,
            )
            # Build a small preview of last-sent notes with latest settings.
            latest_state = state_provider()
            preview_events = mapper.transform_events_pitch(
                base_events,
                key_root_pc=latest_state["key_root_pc"],
                mode_intervals=latest_state["mode_intervals"],
                octave_shift=latest_state["octave_shift"],
                degree_shift=latest_state["degree_shift"],
            )
            preview_events = mapper.apply_voicing_to_events(
                preview_events,
                voicing_mode=latest_state["voicing_mode"],
            )
            preview_notes = [e for e in preview_events if e.get("chord")]
            last_note_names = [[mapper.midi_note_to_name(n) for n in e["chord"]] for e in preview_notes]
            last_port = chosen_port

            with STATE_LOCK:
                state = dict(LIVE_STATE)
        else:
            while True:
                with STATE_LOCK:
                    state = dict(LIVE_STATE)

                # Resolve latest settings each loop pass so UI updates can take effect.
                current_text = state["text"].strip()
                if not current_text:
                    raise HTTPException(status_code=400, detail="Text is required.")

                loop_seed = stable_variation_seed(run_seed, current_text)
                events, note_events = build_events(
                    current_text,
                    key=state["key"],
                    mode=state["mode"],
                    variation_seed=loop_seed,
                )
                key_pc = mapper.parse_key_root(state["key"])
                mode_name = mapper.parse_mode(state["mode"]) or state["mode"]
                mode_intervals = mapper.MODE_INTERVALS.get(mode_name, mapper.MODE_INTERVALS["major"])
                events = mapper.transform_events_pitch(
                    events,
                    key_root_pc=key_pc if key_pc is not None else mapper.NOTE_NAME_TO_PC["C"],
                    mode_intervals=mode_intervals,
                    octave_shift=int(state.get("octave_shift", 0)),
                    degree_shift=int(state.get("degree_shift", 0)),
                )
                events = mapper.apply_voicing_to_events(
                    events,
                    voicing_mode=normalize_voicing_mode(state.get("voicing_mode", "closed")),
                )
                ports = available_output_ports()
                if not ports:
                    raise HTTPException(status_code=400, detail="No MIDI output ports found.")
                chosen_port = state["port_name"] if state["port_name"] else ports[0]
                if chosen_port not in ports:
                    chosen_port = ports[0]

                bent_events = mapper.add_pitch_bend_to_events(events, bend_amount=int(state["bend_amount"]))
                timed_events = mapper.apply_tempo_to_events(bent_events, int(state["tempo_bpm"]))

                loop_count += 1
                completed = mapper.send_live(
                    chosen_port,
                    timed_events,
                    stop_requested=STOP_EVENT.is_set,
                    note_callback=push_note_event,
                )
                last_note_names = [[mapper.midi_note_to_name(n) for n in e["chord"]] for e in events if e.get("chord")]
                last_port = chosen_port
                if not completed:
                    break
                if not bool(state["loop_enabled"]):
                    break
                if not wait_with_stop(STOP_EVENT, int(state["loop_gap_ms"]) / 1000.0):
                    completed = False
                    break
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to send MIDI live: {exc}")
    finally:
        with SEND_LOCK:
            SEND_ACTIVE = False
            STOP_EVENT.clear()

    return JSONResponse(
        {
            "status": "MIDI sent" if completed else "MIDI stopped",
            "port": last_port,
            "notes": last_note_names,
            "looped": bool(state["loop_enabled"]),
            "loops_completed": loop_count,
        }
    )


@app.post("/api/stop-live")
def stop_live():
    with SEND_LOCK:
        active = SEND_ACTIVE
    STOP_EVENT.set()
    panic_midi_outputs()
    if active:
        return {"status": "Stopping current MIDI send and clearing MIDI output..."}
    return {"status": "No active MIDI send. Sent panic reset to MIDI outputs."}


@app.post("/api/update-live-settings")
def update_live_settings(
    request: Request,
    text: str | None = Form(default=None),
    tempo_bpm: int | None = Form(default=None),
    key: str | None = Form(default=None),
    mode: str | None = Form(default=None),
    port_name: str | None = Form(default=None),
    bend_amount: int | None = Form(default=None),
    loop_enabled: bool | None = Form(default=None),
    loop_gap_ms: int | None = Form(default=None),
    octave_shift: int | None = Form(default=None),
    degree_shift: int | None = Form(default=None),
    voicing_mode: str | None = Form(default=None),
):
    require_paid_user(request)
    stop_now = False
    with STATE_LOCK:
        prev_loop = bool(LIVE_STATE.get("loop_enabled", False))
        if text is not None:
            LIVE_STATE["text"] = text
        if tempo_bpm is not None:
            LIVE_STATE["tempo_bpm"] = max(20, min(200, int(tempo_bpm)))
        if key is not None:
            LIVE_STATE["key"] = key
        if mode is not None:
            LIVE_STATE["mode"] = mode
        if port_name is not None:
            LIVE_STATE["port_name"] = port_name.strip()
        if bend_amount is not None:
            LIVE_STATE["bend_amount"] = max(0, min(100, int(bend_amount)))
        if loop_enabled is not None:
            LIVE_STATE["loop_enabled"] = bool(loop_enabled)
            if prev_loop and not bool(loop_enabled):
                stop_now = True
        if loop_gap_ms is not None:
            next_gap = max(0, min(5000, int(loop_gap_ms)))
            loop_on = bool(loop_enabled) if loop_enabled is not None else bool(LIVE_STATE["loop_enabled"])
            if loop_on:
                next_gap = max(MIN_LOOP_GAP_MS, next_gap)
            LIVE_STATE["loop_gap_ms"] = next_gap
        if octave_shift is not None:
            LIVE_STATE["octave_shift"] = max(-3, min(3, int(octave_shift)))
        if degree_shift is not None:
            LIVE_STATE["degree_shift"] = max(-14, min(14, int(degree_shift)))
        if voicing_mode is not None:
            LIVE_STATE["voicing_mode"] = normalize_voicing_mode(voicing_mode)
        state = dict(LIVE_STATE)

    if stop_now:
        STOP_EVENT.set()

    with SEND_LOCK:
        active = SEND_ACTIVE
    return {"status": "updated", "active": active, "state": state}


@app.post("/api/save-midi")
def save_midi_file(
    request: Request,
    text: str = Form(...),
    tempo_bpm: int = Form(60),
    key: str = Form("C"),
    mode: str = Form("major"),
    bend_amount: int = Form(0),
    octave_shift: int = Form(0),
    degree_shift: int = Form(0),
    voicing_mode: str = Form("closed"),
):
    require_paid_user(request)
    text = text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required.")
    tempo_bpm = max(20, min(200, int(tempo_bpm)))
    events, _ = build_events(text, key=key, mode=mode)
    key_pc = mapper.parse_key_root(key)
    mode_name = mapper.parse_mode(mode) or mode
    mode_intervals = mapper.MODE_INTERVALS.get(mode_name, mapper.MODE_INTERVALS["major"])
    octave_shift = max(-3, min(3, int(octave_shift)))
    degree_shift = max(-14, min(14, int(degree_shift)))
    events = mapper.transform_events_pitch(
        events,
        key_root_pc=key_pc if key_pc is not None else mapper.NOTE_NAME_TO_PC["C"],
        mode_intervals=mode_intervals,
        octave_shift=octave_shift,
        degree_shift=degree_shift,
    )
    events = mapper.apply_voicing_to_events(events, voicing_mode=normalize_voicing_mode(voicing_mode))
    bend_amount = max(0, min(100, int(bend_amount)))
    events = mapper.add_pitch_bend_to_events(events, bend_amount=bend_amount)

    out_name = mapper.build_output_filename(text)
    tmp_path = Path(tempfile.gettempdir()) / out_name
    mapper.save_midi(str(tmp_path), events, source_text=text, tempo_bpm=tempo_bpm)
    return FileResponse(path=str(tmp_path), media_type="audio/midi", filename=out_name)


app.mount("/", StaticFiles(directory=str(BASE_DIR / "static"), html=True), name="static")
