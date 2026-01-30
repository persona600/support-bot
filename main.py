import os
import sqlite3
from datetime import datetime
import aiohttp

from aiogram import Bot, Dispatcher, executor, types

# ===== Telegram =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID_RAW = os.getenv("GROUP_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN. Добавь его в Variables на Railway.")
if not GROUP_ID_RAW:
    raise RuntimeError("Не задан GROUP_ID. Добавь его в Variables на Railway.")

GROUP_ID = int(GROUP_ID_RAW)

# ===== LPTracker (optional) =====
LP_LOGIN = os.getenv("LP_LOGIN", "").strip()
LP_PASSWORD = os.getenv("LP_PASSWORD", "").strip()
LP_PROJECT_ID_RAW = os.getenv("LP_PROJECT_ID", "").strip()
LP_SERVICE = os.getenv("LP_SERVICE", "TelegramSupportBot").strip()

LP_BASE = "https://direct.lptracker.ru"
LP_PROJECT_ID = int(LP_PROJECT_ID_RAW) if LP_PROJECT_ID_RAW.isdigit() else None

# ===== DB =====
DB_PATH = "links.sqlite"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS links (
            group_message_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_links (
            user_id INTEGER PRIMARY KEY,
            lead_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS threads (
            user_id INTEGER PRIMARY KEY,
            thread_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def save_link(group_message_id: int, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO links(group_message_id, user_id, created_at) VALUES (?, ?, ?)",
        (group_message_id, user_id, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


def get_user_id_by_group_message_id(group_message_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM links WHERE group_message_id = ?", (group_message_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def save_crm_link(user_id: int, le_
