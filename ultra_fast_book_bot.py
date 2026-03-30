""" Ultra-fast Telegram book bot.

Goal:

User writes a book name.

If full or partial name matches, send the book immediately.

If nothing matches, reply: "❌ নেই"

No Gemini.

No admin panel.

No broadcast.

No reply routing.


Best free architecture:

Local SQLite database for books.

In-memory index loaded once at startup.

Webhook mode when WEBHOOK_URL is provided.

Polling fallback when WEBHOOK_URL is not provided.


Data sources:

1. books.db (primary, local)


2. books.csv (optional bootstrap file; columns: title,file_id)


3. Google Sheet import once at startup if IMPORT_FROM_SHEET=1 and creds.json exists.



Environment variables:

BOT_TOKEN: Telegram bot token (required)

DB_PATH: SQLite path (default: books.db)

BOOKS_CSV: CSV bootstrap file (default: books.csv)

IMPORT_FROM_SHEET: 1/0 (default: 0)

SHEET_NAME: Google spreadsheet name (default: MyBotDB)

SHEET_WORKSHEET: worksheet name (default: Sheet1)

TITLE_COL: 1-based title column index in worksheet (default: 1)

FILE_ID_COL: 1-based file_id column index in worksheet (default: 2)

WEBHOOK_URL: full public HTTPS URL for webhook mode (optional)

WEBHOOK_SECRET: secret token for webhook mode (optional but recommended)

URL_PATH: webhook path segment (default: bot token)

LISTEN: listen host for webhook mode (default: 0.0.0.0)

PORT: port for webhook mode (default: 8080)

CONCURRENT_UPDATES: concurrent update workers (default: 32)

OWNER_ID: optional Telegram user id (currently unused; kept for future maintenance)


Expected CSV format: title,file_id সুবহে সাদিক,ABCDEF12345:... """

from future import annotations

import asyncio import csv import logging import os import re import sqlite3 import unicodedata from dataclasses import dataclass from pathlib import Path from threading import RLock, Thread from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import gspread from oauth2client.service_account import ServiceAccountCredentials from telegram import Update from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

-------------------- CONFIG --------------------

TOKEN = os.environ.get("BOT_TOKEN", "").strip() if not TOKEN: raise RuntimeError("BOT_TOKEN is required")

DB_PATH = Path(os.environ.get("DB_PATH", "books.db")) BOOKS_CSV = Path(os.environ.get("BOOKS_CSV", "books.csv")) IMPORT_FROM_SHEET = os.environ.get("IMPORT_FROM_SHEET", "0").strip() == "1" SHEET_NAME = os.environ.get("SHEET_NAME", "MyBotDB").strip() SHEET_WORKSHEET = os.environ.get("SHEET_WORKSHEET", "Sheet1").strip() TITLE_COL = int(os.environ.get("TITLE_COL", "1")) FILE_ID_COL = int(os.environ.get("FILE_ID_COL", "2")) OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)

WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip() WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip() URL_PATH = os.environ.get("URL_PATH", TOKEN).strip() LISTEN = os.environ.get("LISTEN", "0.0.0.0").strip() PORT = int(os.environ.get("PORT", "8080")) CONCURRENT_UPDATES = int(os.environ.get("CONCURRENT_UPDATES", "32"))

MAX_SEND_PER_QUERY = int(os.environ.get("MAX_SEND_PER_QUERY", "10"))

logging.basicConfig( level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", ) logger = logging.getLogger("book-bot")

-------------------- NORMALIZATION --------------------

_BN_TO_LATIN = { "অ": "a", "আ": "a", "ই": "i", "ঈ": "i", "উ": "u", "ঊ": "u", "ঋ": "ri", "এ": "e", "ঐ": "oi", "ও": "o", "ঔ": "ou", "ক": "k", "খ": "kh", "গ": "g", "ঘ": "gh", "ঙ": "ng", "চ": "ch", "ছ": "chh", "জ": "j", "ঝ": "jh", "ঞ": "ny", "ট": "t", "ঠ": "th", "ড": "d", "ঢ": "dh", "ণ": "n", "ত": "t", "থ": "th", "দ": "d", "ধ": "dh", "ন": "n", "প": "p", "ফ": "f", "ব": "b", "ভ": "bh", "ম": "m", "য": "y", "র": "r", "ল": "l", "শ": "sh", "ষ": "sh", "স": "s", "হ": "h", "ড়": "r", "ঢ়": "rh", "য়": "y", "ৎ": "t", "ং": "ng", "ঃ": "h", "ঁ": "n", "া": "a", "ি": "i", "ী": "i", "ু": "u", "ূ": "u", "ৃ": "ri", "ে": "e", "ৈ": "oi", "ো": "o", "ৌ": "ou", "্": "", }

_NON_ALNUM_RE = re.compile(r"[^\w\u0980-\u09FF]+", re.UNICODE) _SPACE_RE = re.compile(r"\s+") _URL_RE = re.compile(r"(https?://\S+|www.\S+|t.me/\S+|telegram.me/\S+)", re.I)

def normalize(text: str) -> str: if not text: return "" text = unicodedata.normalize("NFKC", str(text)).casefold().strip() text = _URL_RE.sub("", text) text = _NON_ALNUM_RE.sub("", text) return text

def romanize_bangla(text: str) -> str: """Very light transliteration used only for search keys.""" if not text: return "" text = unicodedata.normalize("NFKC", str(text)) out: List[str] = [] for ch in text: out.append(_BN_TO_LATIN.get(ch, ch)) joined = "".join(out).casefold() joined = _URL_RE.sub("", joined) joined = re.sub(r"[^a-z0-9]+", "", joined) return joined

def search_key(text: str) -> str: """Combined key: Bengali-safe normalization + romanized fallback.""" n = normalize(text) r = romanize_bangla(text) return f"{n}|{r}" if r else n

def clean_text(text: str) -> str: text = str(text or "").strip() text = text.replace("_", " ").replace("-", " ") text = _URL_RE.sub("", text) text = _SPACE_RE.sub(" ", text).strip() return text

def looks_like_empty_query(text: str) -> bool: return len(normalize(text)) == 0 and len(romanize_bangla(text)) == 0

-------------------- DATA MODEL --------------------

@dataclass(frozen=True) class BookRecord: title: str file_id: str norm: str roman: str key: str

-------------------- STORE --------------------

class BookStore: def init(self, db_path: Path): self.db_path = db_path self._lock = RLock() self._conn = sqlite3.connect(self.db_path, check_same_thread=False) self._conn.execute("PRAGMA journal_mode=WAL;") self._conn.execute("PRAGMA synchronous=NORMAL;") self._conn.execute("PRAGMA temp_store=MEMORY;") self._conn.execute( """ CREATE TABLE IF NOT EXISTS books ( id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, title_norm TEXT NOT NULL, title_roman TEXT NOT NULL, file_id TEXT NOT NULL, UNIQUE(title_norm, file_id) ) """ ) self._conn.execute("CREATE INDEX IF NOT EXISTS idx_books_title_norm ON books(title_norm)") self._conn.execute("CREATE INDEX IF NOT EXISTS idx_books_title_roman ON books(title_roman)") self._conn.commit()

self._books: List[BookRecord] = []
    self._by_norm: Dict[str, List[BookRecord]] = {}
    self._by_roman: Dict[str, List[BookRecord]] = {}
    self._loaded = False

def count(self) -> int:
    cur = self._conn.execute("SELECT COUNT(*) FROM books")
    return int(cur.fetchone()[0])

def exists(self) -> bool:
    return self.count() > 0

def insert_many(self, rows: Iterable[Tuple[str, str]]) -> int:
    rows_list = []
    for title, file_id in rows:
        title = clean_text(title)
        file_id = clean_text(file_id)
        if not title or not file_id:
            continue
        rows_list.append((title, normalize(title), romanize_bangla(title), file_id))

    if not rows_list:
        return 0

    with self._lock:
        before = self.count()
        self._conn.executemany(
            "INSERT OR IGNORE INTO books(title, title_norm, title_roman, file_id) VALUES (?, ?, ?, ?)",
            rows_list,
        )
        self._conn.commit()
        after = self.count()
        self._loaded = False
        return after - before

def load_cache(self, force: bool = False) -> None:
    with self._lock:
        if self._loaded and not force:
            return
        cur = self._conn.execute("SELECT title, file_id, title_norm, title_roman FROM books")
        books: List[BookRecord] = []
        by_norm: Dict[str, List[BookRecord]] = {}
        by_roman: Dict[str, List[BookRecord]] = {}

        for title, file_id, title_norm, title_roman in cur.fetchall():
            rec = BookRecord(
                title=title,
                file_id=file_id,
                norm=title_norm,
                roman=title_roman,
                key=f"{title_norm}|{title_roman}",
            )
            books.append(rec)
            by_norm.setdefault(title_norm, []).append(rec)
            if title_roman:
                by_roman.setdefault(title_roman, []).append(rec)

        self._books = books
        self._by_norm = by_norm
        self._by_roman = by_roman
        self._loaded = True
        logger.info("Loaded %s books into memory", len(self._books))

def bootstrap(self) -> None:
    """Load existing data, optionally import from CSV or Google Sheet if DB is empty."""
    if self.exists():
        self.load_cache(force=True)
        return

    imported = 0
    if BOOKS_CSV.exists():
        imported += self.import_from_csv(BOOKS_CSV)
        logger.info("Imported %s books from CSV", imported)

    if imported == 0 and IMPORT_FROM_SHEET:
        imported += self.import_from_google_sheet()
        logger.info("Imported %s books from Google Sheet", imported)

    self.load_cache(force=True)

def import_from_csv(self, csv_path: Path) -> int:
    if not csv_path.exists():
        return 0

    rows: List[Tuple[str, str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            if i == 0 and len(row) >= 2 and row[0].lower().strip() in {"title", "book", "book title", "name"}:
                continue
            if len(row) < 2:
                continue
            rows.append((row[0], row[1]))
    return self.insert_many(rows)

def import_from_google_sheet(self) -> int:
    creds_path = Path("creds.json")
    if not creds_path.exists():
        logger.warning("creds.json not found; skipping Google Sheet import")
        return 0

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(str(creds_path), scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        ws = spreadsheet.worksheet(SHEET_WORKSHEET)
        values = ws.get_all_values()

        rows: List[Tuple[str, str]] = []
        for row in values[1:]:
            if len(row) < max(TITLE_COL, FILE_ID_COL):
                continue
            title = row[TITLE_COL - 1].strip()
            file_id = row[FILE_ID_COL - 1].strip()
            if title and file_id:
                rows.append((title, file_id))

        return self.insert_many(rows)
    except Exception as e:
        logger.exception("Google Sheet import failed: %s", e)
        return 0

def search(self, query: str, limit: int = MAX_SEND_PER_QUERY) -> List[BookRecord]:
    query = clean_text(query)
    if looks_like_empty_query(query):
        return []

    q_norm = normalize(query)
    q_roman = romanize_bangla(query)

    with self._lock:
        if not self._loaded:
            self.load_cache(force=True)

        # 1) Exact normalized and romanized lookups.
        exact: List[BookRecord] = []
        seen = set()
        for rec in self._by_norm.get(q_norm, []):
            if rec.key not in seen:
                seen.add(rec.key)
                exact.append(rec)
        for rec in self._by_roman.get(q_roman, []):
            if rec.key not in seen:
                seen.add(rec.key)
                exact.append(rec)
        if exact:
            return exact[:limit]

        # 2) Partial substring search over in-memory index.
        matches: List[BookRecord] = []
        for rec in self._books:
            if q_norm and (q_norm in rec.norm or rec.norm in q_norm):
                if rec.key not in seen:
                    seen.add(rec.key)
                    matches.append(rec)
                    if len(matches) >= limit:
                        break
                continue
            if q_roman and rec.roman:
                if q_roman in rec.roman or rec.roman in q_roman:
                    if rec.key not in seen:
                        seen.add(rec.key)
                        matches.append(rec)
                        if len(matches) >= limit:
                            break

        return matches[:limit]

def close(self) -> None:
    with self._lock:
        try:
            self._conn.close()
        except Exception:
            pass

STORE = BookStore(DB_PATH)

-------------------- TELEGRAM HANDLERS --------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE): await update.message.reply_text( "আসসালামু আলাইকুম। বইয়ের নাম লিখুন।\n" "মিল পেলে বই চলে যাবে, না পেলে বলবে নেই।" )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE): await update.message.reply_text( "ব্যবহার:\n" "শুধু বইয়ের নাম লিখুন।\n" "উদাহরণ: সুবহে সাদিক\n" "মিল পেলে বই যাবে, না পেলে নেই বলবে।" )

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE): message = update.message if not message or not message.text: return

query = message.text.strip()
if not query or query.startswith("/"):
    return

await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

try:
    results = await asyncio.to_thread(STORE.search, query, MAX_SEND_PER_QUERY)
    if not results:
        await message.reply_text("❌ নেই")
        return

    # Send matched books immediately, up to the configured cap.
    for rec in results:
        try:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=rec.file_id,
                caption=f"📘 {rec.title}",
            )
        except Exception as e:
            logger.warning("Failed to send %s: %s", rec.title, e)

except Exception as e:
    logger.exception("Search handler error: %s", e)
    await message.reply_text("❌ নেই")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE): logger.exception("Unhandled error: %s", context.error)

-------------------- APP BOOTSTRAP --------------------
async def post_init(application: Application):
    # Load DB / import data before the bot starts serving updates.
    await asyncio.to_thread(STORE.bootstrap)


def build_application() -> Application:
    app = (
        Application.builder()
        .token(TOKEN)
        .concurrent_updates(CONCURRENT_UPDATES)
        .connection_pool_size(32)
        .pool_timeout(5.0)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    app.add_error_handler(error_handler)
    return app


def main() -> None:
    application = build_application()

    # If WEBHOOK_URL is set, prefer webhook mode for better concurrency on always-on hosting.
    if WEBHOOK_URL:
        logger.info("Starting in webhook mode")
        application.run_webhook(
            listen=LISTEN,
            port=PORT,
            url_path=URL_PATH,
            webhook_url=WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET or None,
            drop_pending_updates=True,
        )
    else:
        logger.info("Starting in polling mode")
        application.run_polling(drop_pending_updates=True)


