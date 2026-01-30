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

# ===== LPTracker =====
LP_LOGIN = os.getenv("LP_LOGIN", "").strip()
LP_PASSWORD = os.getenv("LP_PASSWORD", "").strip()
LP_PROJECT_ID_RAW = os.getenv("LP_PROJECT_ID", "").strip()
LP_SERVICE = os.getenv("LP_SERVICE", "TelegramSupportBot").strip()

LP_BASE = "https://direct.lptracker.ru"
LP_PROJECT_ID = int(LP_PROJECT_ID_RAW) if LP_PROJECT_ID_RAW else None

# ===== DB =====
DB_PATH = "links.sqlite"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # reply mapping: message_id in group -> client user_id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS links (
            group_message_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # CRM mapping: telegram user_id -> lead_id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_links (
            user_id INTEGER PRIMARY KEY,
            lead_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # Topics mapping: telegram user_id -> message_thread_id (topic id)
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


def save_crm_link(user_id: int, lead_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO crm_links(user_id, lead_id, created_at) VALUES (?, ?, ?)",
        (user_id, lead_id, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


def get_lead_id_by_user_id(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT lead_id FROM crm_links WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def save_thread(user_id: int, thread_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO threads(user_id, thread_id, created_at) VALUES (?, ?, ?)",
        (user_id, thread_id, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


def get_thread(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT thread_id FROM threads WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# ===== LPTracker token cache =====
_lp_token = None


async def lpt_login(session: aiohttp.ClientSession) -> str:
    global _lp_token
    if not (LP_LOGIN and LP_PASSWORD and LP_PROJECT_ID):
        return ""

    payload = {"login": LP_LOGIN, "password": LP_PASSWORD, "service": LP_SERVICE, "version": "1.0"}
    async with session.post(f"{LP_BASE}/login", json=payload) as resp:
        data = await resp.json(content_type=None)
        if data.get("status") != "success":
            raise RuntimeError(f"LPTracker login error: {data}")
        _lp_token = data["result"]["token"]
        return _lp_token


async def lpt_request(session: aiohttp.ClientSession, method: str, path: str, json_body=None):
    global _lp_token
    if not (LP_LOGIN and LP_PASSWORD and LP_PROJECT_ID):
        return None

    if not _lp_token:
        await lpt_login(session)

    headers = {"token": _lp_token, "Content-Type": "application/json"}

    async with session.request(method, f"{LP_BASE}{path}", json=json_body, headers=headers) as resp:
        data = await resp.json(content_type=None)

        # token expired -> relogin once
        if data.get("status") == "error":
            errors = data.get("errors") or []
            if any(e.get("code") == 401 for e in errors):
                await lpt_login(session)
                headers["token"] = _lp_token
                async with session.request(method, f"{LP_BASE}{path}", json=json_body, headers=headers) as resp2:
                    return await resp2.json(content_type=None)

        return data


async def lpt_create_lead(session: aiohttp.ClientSession, tg_user: types.User) -> int:
    name = (tg_user.full_name or "Telegram client").strip()
    lead_name = f"Telegram: {name}".strip()

    body = {
        "contact": {"project_id": LP_PROJECT_ID, "name": lead_name},
        "name": lead_name
    }

    data = await lpt_request(session, "POST", "/lead", json_body=body)
    if not data or data.get("status") != "success":
        raise RuntimeError(f"LPTracker create lead error: {data}")

    return int(data["result"]["id"])


async def lpt_add_comment(session: aiohttp.ClientSession, lead_id: int, text: str):
    data = await lpt_request(session, "POST", f"/lead/{lead_id}/comment", json_body={"text": text})
    if not data or data.get("status") != "success":
        raise RuntimeError(f"LPTracker add comment error: {data}")


# ===== Telegram Topics (Forum) helper =====
async def tg_create_forum_topic(chat_id: int, name: str) -> int:
    """
    Creates a forum topic via raw Telegram HTTP API and returns message_thread_id.
    Works even if aiogram doesn't have a wrapper.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
    payload = {"chat_id": chat_id, "name": name[:128]}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json(content_type=None)

    if not data.get("ok"):
        raise RuntimeError(f"Telegram createForumTopic error: {data}")

    return int(data["result"]["message_thread_id"])


async def ensure_topic_for_user(user: types.User) -> int:
    """
    Returns existing topic id for this client or creates a new one.
    Topic title = ONLY first name (e.g. 'Сергей')
    """
    thread_id = get_thread(user.id)
    if thread_id:
        return thread_id

    title = (user.first_name or user.full_name or "Клиент").strip()
    thread_id = await tg_create_forum_topic(GROUP_ID, title)
    save_thread(user.id, thread_id)
    return thread_id


# ===== Bot =====
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)


def client_header(user: types.User) -> str:
    # можно оставить как есть — это сообщение увидят только операторы
    username = f"@{user.username}" if user.username else "нет"
    return (
        f"👤 <b>Клиент</b>: {user.full_name}\n"
        f"🔗 <b>Username</b>: {username}\n"
        f"🆔 <b>ID</b>: <code>{user.id}</code>\n"
        f"✍️ <i>Отвечайте на это сообщение реплаем — бот отправит ответ клиенту.</i>"
    )


@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.reply(f"chat_id = <code>{message.chat.id}</code>")


# 1) Client -> Topic + LPTracker
@dp.message_handler(content_types=types.ContentTypes.ANY, chat_type=types.ChatType.PRIVATE)
async def from_client_to_group(message: types.Message):
    # создаём/получаем топик под клиента
    try:
        thread_id = await ensure_topic_for_user(message.from_user)
    except Exception as e:
        # если топики не включены в группе — сообщим оператору в General
        await bot.send_message(chat_id=GROUP_ID, text=f"⚠️ Не удалось создать топик. Проверь, что в группе включены Темы.\n<code>{e}</code>")
        thread_id = None

    header = client_header(message.from_user)

    # --- Telegram -> Group topic
    if message.text:
        sent = await bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=thread_id,
            text=f"{header}\n\n💬 <b>Сообщение клиента:</b>\n{message.text}"
        )
        save_link(sent.message_id, message.from_user.id)
    else:
        copied = await message.copy_to(chat_id=GROUP_ID, message_thread_id=thread_id)
        save_link(copied.message_id, message.from_user.id)

        await bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=thread_id,
            text=header + "\n\n📎 <b>Клиент прислал вложение/медиа.</b>\n"
                        "↩️ <b>Ответьте реплаем НА СКОПИРОВАННОЕ вложение</b>, и бот отправит ответ клиенту."
        )

    # --- Telegram -> LPTracker
    try:
        async with aiohttp.ClientSession() as session:
            lead_id = get_lead_id_by_user_id(message.
