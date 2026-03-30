from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
from pathlib import Path
from threading import RLock
from typing import List, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# ================= CONFIG =================

TOKEN = "YOUR_BOT_TOKEN"
SHEET_NAME = "MyBotDB"

DB_PATH = Path("books.db")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ================= NORMALIZE =================

def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"[\s\W_]+", "", text)
    return text

# ================= DATABASE =================

class BookStore:
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = RLock()

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS books(
                id INTEGER PRIMARY KEY,
                title TEXT,
                norm TEXT,
                file_id TEXT
            )
        """)

        self.conn.commit()

        self.books = []

    def load_from_sheet(self):
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
            client = gspread.authorize(creds)

            sheet = client.open(SHEET_NAME).sheet1
            data = sheet.get_all_values()[1:]

            rows = []
            for row in data:
                if len(row) >= 2:
                    title = row[0].strip()
                    file_id = row[1].strip()
                    if title and file_id:
                        rows.append((title, normalize(title), file_id))

            with self.lock:
                self.conn.executemany(
                    "INSERT INTO books(title,norm,file_id) VALUES (?,?,?)",
                    rows
                )
                self.conn.commit()

            logger.info(f"Imported {len(rows)} books")

        except Exception as e:
            logger.error(f"Sheet error: {e}")

    def load_cache(self):
        with self.lock:
            cur = self.conn.execute("SELECT title,norm,file_id FROM books")
            self.books = cur.fetchall()

    def search(self, query: str):
        q = normalize(query)
        result = []

        for title, norm, file_id in self.books:
            if q in norm:
                result.append((title, file_id))
                if len(result) >= 5:
                    break

        return result


STORE = BookStore(DB_PATH)

# ================= TELEGRAM =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "আসসালামু আলাইকুম। বইয়ের নাম লিখুন।"
    )

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    results = await asyncio.to_thread(STORE.search, text)

    if results:
        for name, file_id in results:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=file_id,
                caption=f"📘 {name}"
            )
    else:
        await update.message.reply_text("❌ নেই")

# ================= MAIN =================

def main():
    STORE.load_from_sheet()
    STORE.load_cache()

    app = Application.builder().token(TOKEN).concurrent_updates(50).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT, handle))

    print("Bot Running...")
    app.run_polling()

if __name__ == "__main__":
    main()
