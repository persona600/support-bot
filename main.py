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

# ===== LPTracker contact field cache =====
_lp_telegram_field_id = None  # int | 0 | None


def lpt_enabled() -> bool:
    return bool(LP_LOGIN and LP_PASSWORD and LP_PROJECT_ID)


async def lpt_login(session: aiohttp.ClientSession) -> str:
    global _lp_token
    payload = {"login": LP_LOGIN, "password": LP_PASSWORD, "service": LP_SERVICE, "version": "1.0"}
    async with session.post(f"{LP_BASE}/login", json=payload) as resp:
        data = await resp.json(content_type=None)
    if data.get("status") != "success":
        raise RuntimeError(f"LPTracker login error: {data}")
    _lp_token = data["result"]["token"]
    return _lp_token


async def lpt_request(session: aiohttp.ClientSession, method: str, path: str, json_body=None):
    global _lp_token
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
                data = await resp2.json(content_type=None)

    return data


async def lpt_get_contact_field_id_by_name(session: aiohttp.ClientSession, field_name: str) -> int | None:
    """
    Находит ID кастомного поля контакта по названию (например "Telegram").
    Кешируем значение, чтобы не дергать API на каждое сообщение.
    """
    global _lp_telegram_field_id

    # уже искали: _lp_telegram_field_id = int (нашли) или 0 (не нашли)
    if _lp_telegram_field_id is not None:
        return _lp_telegram_field_id if _lp_telegram_field_id != 0 else None

    data = await lpt_request(session, "GET", f"/project/{LP_PROJECT_ID}/fields", json_body=None)
    if not data or data.get("status") != "success":
        _lp_telegram_field_id = 0
        return None

    fields = data.get("result") or []
    target = field_name.strip().lower()

    for f in fields:
        name = str(f.get("name", "")).strip().lower()
        if name == target:
            _lp_telegram_field_id = int(f["id"])
            return _lp_telegram_field_id

    _lp_telegram_field_id = 0
    return None


async def lpt_create_lead(session: aiohttp.ClientSession, tg_user: types.User) -> int:
    """
    ВАЖНОЕ ИСПРАВЛЕНИЕ:
    LPTracker требует contact.details (email/phone). Поэтому кладем details внутрь contact.
    Также пишем username в кастомное поле контакта "Telegram" (если такое поле есть в проекте).
    """
    lead_name = f"Telegram: {(tg_user.full_name or 'Клиент').strip()}"

    # обязательное: contact.details
    details_list = [
        {"type": "email", "data": f"tg{tg_user.id}@telegram.invalid"}
    ]

    # кастомное поле контакта "Telegram" (как у тебя в карточке)
    contact_fields = {}
    if tg_user.username:
        telegram_field_id = await lpt_get_contact_field_id_by_name(session, "Telegram")
        if telegram_field_id:
            contact_fields[str(telegram_field_id)] = tg_user.username

    body = {
        "contact": {
            "project_id": LP_PROJECT_ID,
            "name": (tg_user.full_name or "Клиент").strip(),
            "details": details_list
        },
        "name": lead_name
    }

    if contact_fields:
        body["contact"]["fields"] = contact_fields

    data = await lpt_request(session, "POST", "/lead", json_body=body)
    if data.get("status") != "success":
        raise RuntimeError(f"LPTracker create lead error: {data}")

    return int(data["result"]["id"])


async def lpt_add_comment(session: aiohttp.ClientSession, lead_id: int, text: str):
    data = await lpt_request(session, "POST", f"/lead/{lead_id}/comment", json_body={"text": text})
    if data.get("status") != "success":
        raise RuntimeError(f"LPTracker add comment error: {data}")


# ===== Telegram Topics helper =====
async def tg_create_forum_topic(chat_id: int, name: str) -> int:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
    payload = {"chat_id": chat_id, "name": name[:128]}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json(content_type=None)

    if not data.get("ok"):
        raise RuntimeError(f"Telegram createForumTopic error: {data}")

    return int(data["result"]["message_thread_id"])


async def ensure_topic_for_user(user: types.User) -> int:
    thread_id = get_thread(user.id)
    if thread_id:
        return thread_id

    # ВАЖНО: только имя
    title = (user.first_name or user.full_name or "Клиент").strip()
    thread_id = await tg_create_forum_topic(GROUP_ID, title)
    save_thread(user.id, thread_id)
    return thread_id


# ===== Bot =====
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)


def client_header(user: types.User) -> str:
    username = f"@{user.username}" if user.username else "нет"
    return (
        f"👤 <b>Клиент</b>: {user.full_name}\n"
        f"🔗 <b>Username</b>: {username}\n"
        f"🆔 <b>ID</b>: <code>{user.id}</code>\n"
        f"✍️ <i>Отвечайте на сообщение цитатой</i>"
    )


@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.reply(f"chat_id = <code>{message.chat.id}</code>")


@dp.message_handler(content_types=types.ContentTypes.ANY, chat_type=types.ChatType.PRIVATE)
async def from_client_to_group(message: types.Message):
    # topic for this client
    thread_id = None
    try:
        thread_id = await ensure_topic_for_user(message.from_user)
    except Exception as e:
        await bot.send_message(
            chat_id=GROUP_ID,
            text=f"⚠️ Не удалось создать топик. Проверь, что в группе включены Темы.\n<code>{e}</code>"
        )

    header = client_header(message.from_user)

    # send to topic
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

    # LPTracker (optional)
    if lpt_enabled():
        try:
            async with aiohttp.ClientSession() as session:
                lead_id = get_lead_id_by_user_id(message.from_user.id)
                if not lead_id:
                    lead_id = await lpt_create_lead(session, message.from_user)
                    save_crm_link(message.from_user.id, lead_id)

                if message.text:
                    username = f"@{message.from_user.username}" if message.from_user.username else "нет"
                    comment = (
                        f"Telegram сообщение от клиента:\n"
                        f"Имя: {message.from_user.full_name}\n"
                        f"Username: {username}\n"
                        f"Telegram ID: {message.from_user.id}\n\n"
                        f"{message.text}"
                    )
                    await lpt_add_comment(session, lead_id, comment)
                else:
                    await lpt_add_comment(session, lead_id, "Telegram: клиент прислал вложение/медиа (файл).")
        except Exception as e:
            await bot.send_message(
                chat_id=GROUP_ID,
                message_thread_id=thread_id,
                text=f"⚠️ <b>LPTracker:</b> не удалось записать сообщение в CRM.\n<code>{e}</code>"
            )


@dp.message_handler(content_types=types.ContentTypes.ANY)
async def from_group_to_client(message: types.Message):
    # работаем только в нашей группе
    if message.chat.id != GROUP_ID:
        return
    # игнорируем сообщения от ботов (в т.ч. от нашего бота),
    # иначе бот будет ругаться сам на себя
    if message.from_user and message.from_user.is_bot:
        return

    # не реагируем на команды типа /id
    if message.text and message.text.strip().startswith("/"):
        return

    # если менеджер написал БЕЗ reply — показываем предупреждение
    if not message.reply_to_message:
        warning_text = "❗ Сообщение клиенту не отправлено. Отвечать клиенту нужно через цитату. Отправьте свой ответ повторно через цитирование сообщения клиента"
        await message.reply(warning_text)
        return

    # если ответили не на сообщение клиента
    replied_id = message.reply_to_message.message_id
    user_id = get_user_id_by_group_message_id(replied_id)
    if not user_id:
        await message.reply("❗ Сообщение клиенту не отправлено. Отвечать клиенту нужно через цитату. Отправьте свой ответ повторно через цитирование сообщения клиента")
        return

    # отправляем клиенту
    if message.text:
        await bot.send_message(chat_id=user_id, text=message.text)
    else:
        await message.copy_to(chat_id=user_id)


if __name__ == "__main__":
    init_db()
    executor.start_polling(dp, skip_updates=True)
