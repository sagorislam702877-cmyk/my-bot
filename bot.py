import asyncio
import logging
import os
import re
import sqlite3
from threading import RLock

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

# ================= CONFIG =================

TOKEN = "YOUR_BOT_TOKEN"
SHEET_NAME = "MyBotDB"

# ================= LOGGING =================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================= DATABASE =================

class BookDB:
    def __init__(self):
        self.conn = sqlite3.connect("books.db", check_same_thread=False)
        self.lock = RLock()

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            norm TEXT,
            file_id TEXT
        )
        """)

        self.conn.commit()

        self.cache = []

    def normalize(self, text):
        text = str(text).lower().strip()
        text = re.sub(r"[\s\W_]+", "", text)
        return text

    def insert(self, title, file_id):
        norm = self.normalize(title)
        with self.lock:
            self.conn.execute(
                "INSERT INTO books (title, norm, file_id) VALUES (?, ?, ?)",
                (title, norm, file_id)
            )
            self.conn.commit()

    def load_cache(self):
        with self.lock:
            rows = self.conn.execute("SELECT title, norm, file_id FROM books").fetchall()
            self.cache = rows
            logger.info(f"Loaded {len(rows)} books into RAM")

    def search(self, query):
        q = self.normalize(query)
        result = []

        for title, norm, file_id in self.cache:
            if q in norm:
                result.append((title, file_id))

        return result[:10]


DB = BookDB()

# ================= GOOGLE SHEET =================

def load_from_sheet():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open(SHEET_NAME).sheet1
        data = sheet.get_all_values()

        count = 0
        for row in data[1:]:
            if len(row) < 2:
                continue
            title = row[0].strip()
            file_id = row[1].strip()
            if title and file_id:
                DB.insert(title, file_id)
                count += 1

        logger.info(f"Imported {count} books from sheet")

    except Exception as e:
        logger.error(f"Sheet error: {e}")

# ================= BOT =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "আসসালামু আলাইকুম। বইয়ের নাম লিখুন।"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    results = DB.search(text)

    if not results:
        await update.message.reply_text("❌ নেই")
        return

    for title, file_id in results:
        try:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=file_id,
                caption=f"📘 {title}"
            )
        except Exception as e:
            logger.error(e)

# ================= MAIN =================

async def main():
    print("Loading data...")

    load_from_sheet()
    DB.load_cache()

    app = (
        Application.builder()
        .token(TOKEN)
        .concurrent_updates(50)   # 🔥 high concurrency
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot Running...")

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    await app.idle()

if __name__ == "__main__":
    asyncio.run(main())
