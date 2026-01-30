import os
import sqlite3
from datetime import datetime

from aiogram import Bot, Dispatcher, executor, types

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID_RAW = os.getenv("GROUP_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN. Добавь его в Variables на Railway.")
if not GROUP_ID_RAW:
    raise RuntimeError("Не задан GROUP_ID. Добавь его в Variables на Railway.")

GROUP_ID = int(GROUP_ID_RAW)

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


bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)


def client_header(user: types.User) -> str:
    name = (user.full_name or "").strip()
    username = f"@{user.username}" if user.username else "нет"
    return (
        f"👤 <b>Клиент</b>: {name}\n"
        f"🔗 <b>Username</b>: {username}\n"
        f"🆔 <b>ID</b>: <code>{user.id}</code>\n"
        f"✍️ <i>Ответьте на это сообщение реплаем — бот отправит ответ клиенту.</i>"
    )


@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.reply(f"chat_id = <code>{message.chat.id}</code>")


# 1) Клиент пишет боту в личку -> в группу
@dp.message_handler(content_types=types.ContentTypes.ANY, chat_type=types.ChatType.PRIVATE)
async def from_client_to_group(message: types.Message):
    header = client_header(message.from_user)

    if message.text:
        sent = await bot.send_message(
            chat_id=GROUP_ID,
            text=f"{header}\n\n💬 <b>Сообщение:</b>\n{message.text}"
        )
        save_link(sent.message_id, message.from_user.id)
        return

    # Медиа/файлы: копируем в группу
    copied = await message.copy_to(chat_id=GROUP_ID)
    save_link(copied.message_id, message.from_user.id)

    await bot.send_message(
        chat_id=GROUP_ID,
        text=header + "\n\n📎 <b>Клиент прислал вложение/медиа.</b>\n"
                    "↩️ <b>Ответьте реплаем НА СКОПИРОВАННОЕ вложение</b>, и бот отправит ответ клиенту."
    )


# 2) В группе: оператор отвечает реплаем -> клиенту
@dp.message_handler(content_types=types.ContentTypes.ANY)
async def from_group_to_client(message: types.Message):
    if message.chat.id != GROUP_ID:
        return

    if not message.reply_to_message:
        return

    replied_id = message.reply_to_message.message_id
    user_id = get_user_id_by_group_message_id(replied_id)
    if not user_id:
        return

    if message.text:
        await bot.send_message(chat_id=user_id, text=message.text)
        return

    await message.copy_to(chat_id=user_id)


if __name__ == "__main__":
    init_db()
    executor.start_polling(dp, skip_updates=True)

