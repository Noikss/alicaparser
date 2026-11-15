"""
Telegram бот — парсер комментаторов каналов
Оптимизирован по памяти: ленивые импорты, автоочистка, gc после парсинга

Запуск: python tg_bot.py
"""

import asyncio
import gc
import os
import io
import re
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, Document, FSInputFile, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.base import StorageKey
from aiogram.filters import CommandStart, Command

import qrcode

from telethon import TelegramClient
from telethon.tl.types import User
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import ChatInviteAlready, PeerChannel, Chat
from telethon.errors import (
    FloodWaitError, ChannelPrivateError,
    ChatAdminRequiredError, SessionPasswordNeededError,
    AuthKeyUnregisteredError
)

# ==========================================
# НАСТРОЙКИ
# ==========================================

BOT_TOKEN = "8730555122:AAE-VtpEK0HrJceNTZGpBFhmrMSzCDAqVKs"

API_ID = 37443553
API_HASH = "a9a89f77413936f88b395a27ff956102"

POSTS_LIMIT = 0  # 0 = без лимита, парсим весь канал от первого до последнего поста

SESSIONS_DIR = "sessions"
OUTPUT_DIR = "outputs"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Через сколько минут неактивности отключать клиент (0 = не отключать)
# Во время парсинга клиент помечается как busy и не отключается независимо от этого значения
IDLE_DISCONNECT_MINUTES = 60

# Тянуть ли "О себе" (bio) — это ОТДЕЛЬНЫЙ запрос к Telegram на каждого
# уникального пользователя. Сильно замедляет парсинг и повышает риск флуд-вейтов.
# False = колонка "О себе" будет пустой, но парсинг заметно быстрее.
FETCH_BIO = True

# Быстрый режим: читать группу обсуждений одним потоком (по 100 комментов за запрос),
# вместо отдельного запроса на каждый пост. Обычно в разы быстрее и реже ловит флуд,
# т.к. использует GetHistory вместо более лимитированного GetReplies.
# False = старый способ (по постам), на случай если с каким-то каналом будут проблемы.
FAST_MODE = True

# Часовой пояс, в котором нужно показывать время сообщений в таблице
# (Telethon отдаёт даты в UTC, тут переводим в нужный пояс)
LOCAL_TZ = ZoneInfo("Europe/Moscow")

# ==========================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилище клиентов: user_id -> {"client": ..., "last_used": datetime}
clients: dict = {}

# Хранилище активных QR-логинов: user_id -> telethon QRLogin объект
qr_logins: dict = {}


class ParseState(StatesGroup):
    waiting_qr_scan = State()
    waiting_2fa = State()
    waiting_channels_file = State()
    parsing = State()


# ==========================================
# УПРАВЛЕНИЕ КЛИЕНТАМИ
# ==========================================

async def get_client(user_id: int) -> TelegramClient | None:
    """Получить клиент и обновить время последнего использования."""
    if user_id in clients:
        clients[user_id]["last_used"] = datetime.now()
        return clients[user_id]["client"]
    return None


async def set_client(user_id: int, client: TelegramClient):
    """Сохранить клиент."""
    clients[user_id] = {
        "client": client,
        "last_used": datetime.now(),
        "busy": False
    }


def touch_client(user_id: int):
    """Обновить время последнего использования (без получения клиента)."""
    if user_id in clients:
        clients[user_id]["last_used"] = datetime.now()


def set_busy(user_id: int, busy: bool):
    """Пометить клиент как занятый парсингом — такой клиент не отключается по таймауту."""
    if user_id in clients:
        clients[user_id]["busy"] = busy
        clients[user_id]["last_used"] = datetime.now()


async def drop_client(user_id: int):
    """Отключить и удалить клиент из памяти."""
    if user_id not in clients:
        return
    try:
        await clients[user_id]["client"].disconnect()
    except Exception:
        pass
    del clients[user_id]
    gc.collect()


def make_qr_png(url: str) -> bytes:
    """Генерирует PNG-картинку QR-кода из ссылки tg://login?token=..."""
    img = qrcode.make(url)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


async def send_login_qr(message: Message, user_id: int, client: TelegramClient):
    """Создаёт QR-логин, шлёт картинку и запускает фоновое ожидание сканирования."""
    qr_login = await client.qr_login()
    qr_logins[user_id] = qr_login

    qr_bytes = make_qr_png(qr_login.url)
    await message.answer_photo(
        BufferedInputFile(qr_bytes, filename="qr.png"),
        caption=(
            "📱 Отсканируй QR-код в приложении Telegram:\n\n"
            "Настройки → Устройства → Подключить устройство\n\n"
            "⏳ QR-код действует ~60 секунд, если не успеешь — пришлю новый автоматически."
        )
    )

    asyncio.create_task(wait_for_qr_login(message, user_id, client, qr_login))


async def wait_for_qr_login(message: Message, user_id: int, client: TelegramClient, qr_login):
    """Ждёт, пока пользователь отсканирует QR, обновляет код при истечении."""
    key = StorageKey(bot_id=bot.id, chat_id=message.chat.id, user_id=user_id)
    state = FSMContext(storage=dp.storage, key=key)

    try:
        await qr_login.wait(timeout=60)
    except SessionPasswordNeededError:
        qr_logins.pop(user_id, None)
        await set_client(user_id, client)  # клиент нужен для ввода пароля 2FA
        await state.set_state(ParseState.waiting_2fa)
        await message.answer("🔐 Аккаунт защищён 2FA. Введи пароль:")
        return
    except asyncio.TimeoutError:
        # Код истёк — если пользователь всё ещё в состоянии ожидания, обновляем QR
        current_state = await state.get_state()
        if current_state != ParseState.waiting_qr_scan.state:
            return
        try:
            await qr_login.recreate()
            qr_bytes = make_qr_png(qr_login.url)
            await message.answer_photo(
                BufferedInputFile(qr_bytes, filename="qr.png"),
                caption="🔄 QR-код обновлён, отсканируй новый:"
            )
            asyncio.create_task(wait_for_qr_login(message, user_id, client, qr_login))
        except Exception as e:
            await message.answer(f"❌ Не удалось обновить QR-код: {e}\n\nПопробуй /start заново.")
            await drop_client(user_id)
            qr_logins.pop(user_id, None)
            await state.clear()
        return
    except Exception as e:
        await message.answer(f"❌ Ошибка авторизации: {e}\n\nПопробуй /start заново.")
        await drop_client(user_id)
        qr_logins.pop(user_id, None)
        await state.clear()
        return

    # Успешный вход
    qr_logins.pop(user_id, None)
    await set_client(user_id, client)
    await state.set_state(ParseState.waiting_channels_file)
    await message.answer(
        "✅ Авторизация успешна!\n\n"
        "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
        "<code>channel1\nchannel2</code>",
        parse_mode="HTML"
    )


async def cleanup_idle_clients():
    """Фоновая задача: отключать клиентов которые давно не используются."""
    while True:
        await asyncio.sleep(300)  # проверяем каждые 5 минут
        if IDLE_DISCONNECT_MINUTES <= 0:
            continue
        cutoff = datetime.now() - timedelta(minutes=IDLE_DISCONNECT_MINUTES)
        idle_users = [
            uid for uid, data in clients.items()
            if data["last_used"] < cutoff and not data.get("busy")
        ]
        for uid in idle_users:
            logger.info(f"Отключаем idle клиент user_id={uid}")
            await drop_client(uid)


# ==========================================
# ХЕЛПЕРЫ
# ==========================================

DC_MAP = {1: "US", 2: "EU", 3: "US", 4: "EU", 5: "AS"}


def get_status(user):
    if not hasattr(user, "status") or not user.status:
        return "n/a"
    t = type(user.status).__name__
    if t == "UserStatusOnline":       return "Онлайн"
    if t == "UserStatusRecently":     return "Был(а) недавно"
    if t == "UserStatusLastWeek":     return "На этой неделе"
    if t == "UserStatusLastMonth":    return "В этом месяце"
    if t == "UserStatusOffline":
        wo = user.status.was_online
        return wo.astimezone(LOCAL_TZ).strftime("%d/%m/%Y %H:%M:%S") if wo else "n/a"
    return "n/a"


def get_username(user) -> str:
    """У Telegram теперь бывает несколько юзернеймов: основной лежит в .username,
    но у части аккаунтов он пустой, а активные — в .usernames."""
    if getattr(user, "username", None):
        return user.username
    for u in (getattr(user, "usernames", None) or []):
        if getattr(u, "active", False):
            return u.username
    return ""


def get_user_info(user):
    bio = getattr(user, "about", None) or "n/a"
    photo = getattr(user, "photo", None)
    dc_id = ""
    if photo and hasattr(photo, "dc_id"):
        dc_id = DC_MAP.get(photo.dc_id, str(photo.dc_id))
    return bio, dc_id


# Кеш био: user_id -> строка. Живёт в пределах одного запуска бота,
# чтобы не дёргать Telegram повторно по одному и тому же человеку.
bio_cache: dict = {}


async def fetch_bio(client, user) -> str:
    """Догружает 'О себе' отдельным запросом GetFullUserRequest.
    В обычном объекте пользователя из сообщения этого поля нет."""
    if not FETCH_BIO:
        return ""

    uid = user.id
    if uid in bio_cache:
        return bio_cache[uid]

    try:
        full = await client(GetFullUserRequest(user))
        about = getattr(full.full_user, "about", None) or ""
    except FloodWaitError as e:
        # Ждём и пробуем один раз ещё
        await asyncio.sleep(e.seconds)
        try:
            full = await client(GetFullUserRequest(user))
            about = getattr(full.full_user, "about", None) or ""
        except Exception:
            about = ""
    except Exception:
        about = ""

    bio_cache[uid] = about
    return about


ILLEGAL_XLSX_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def clean_cell(value):
    """Чистит значение для Excel: убирает управляющие символы (openpyxl на них падает)
    и обрезает слишком длинный текст (лимит ячейки Excel — 32767 символов)."""
    if not isinstance(value, str):
        return value
    value = ILLEGAL_XLSX_RE.sub("", value)
    if len(value) > 32000:
        value = value[:32000] + "…[обрезано]"
    return value


def make_row(user, channel_username, post_id, comment, bio=None):
    _, dc_id = get_user_info(user)
    if bio is None:
        bio = ""
    return {
        "ID канала":                      channel_username,
        "ID":                             user.id,
        "Имя":                            clean_cell(user.first_name or ""),
        "Фамилия":                        clean_cell(user.last_name or ""),
        "Имя пользователя":               get_username(user),
        "Есть фото":                      "TRUE" if user.photo else "FALSE",
        "Телефон":                        getattr(user, "phone", "") or "",
        "Есть премиум":                   "TRUE" if getattr(user, "premium", False) else "FALSE",
        "Статус":                         get_status(user),
        "Язык":                           getattr(user, "lang_code", "") or "",
        "О себе":                         clean_cell(bio),
        "Пол":                            "",
        "Удален":                         "TRUE" if getattr(user, "deleted", False) else "FALSE",
        "Бот":                            "TRUE" if getattr(user, "bot", False) else "FALSE",
        "Фейк":                           "TRUE" if getattr(user, "fake", False) else "FALSE",
        "Скам":                           "TRUE" if getattr(user, "scam", False) else "FALSE",
        "Требуется премиум для контакта": "FALSE",
        "DC ID":                          dc_id,
        "Истории недоступны":             "TRUE",
        "Макс ID истории":                "",
        "ID поста":                       post_id,
        "Дата сообщения":                 comment.date.astimezone(LOCAL_TZ).strftime("%d/%m/%Y %H:%M:%S") if comment.date else "",
        "Текст сообщения":                clean_cell(comment.text or ""),
        "ID сообщения":                   comment.id,
        "Ссылка на комментарий":          (
            f"https://t.me/{channel_username}/{post_id}?comment={comment.id}"
            if post_id else f"https://t.me/{channel_username}/{comment.id}"
        ),
    }


async def get_discussion_group(client, channel):
    """Возвращает привязанную группу обсуждений или None, если её нет."""
    try:
        full = await client(GetFullChannelRequest(channel))
        linked_id = getattr(full.full_chat, "linked_chat_id", None)
        if not linked_id:
            return None
        for chat in full.chats:
            if chat.id == linked_id:
                return chat
        return await client.get_entity(linked_id)
    except Exception:
        return None


async def parse_via_group(client, channel_username, channel, group, status_msg,
                          people, seen_msg):
    """Быстрый способ: читаем группу обсуждений подряд, по 100 сообщений за запрос.
    Идём от старых к новым — так копия поста всегда встречается раньше комментариев к ней,
    и мы знаем, к какому посту канала относится каждый комментарий."""
    post_map = {}        # id сообщения в группе -> id поста в канале
    offset_id = 0
    processed = 0
    interrupted = False

    for attempt in range(5):
        try:
            async for msg in client.iter_messages(
                group, reverse=True, offset_id=offset_id
            ):
                offset_id = msg.id

                # Копия поста канала — запоминаем связь и идём дальше
                fwd = getattr(msg, "fwd_from", None)
                if fwd is not None and getattr(fwd, "channel_post", None):
                    post_map[msg.id] = fwd.channel_post
                    continue

                if getattr(msg, "action", None):   # служебные сообщения
                    continue

                try:
                    if not msg.sender_id:
                        continue
                    user = msg.sender
                    if not isinstance(user, User):
                        continue
                    if getattr(user, "bot", False):
                        continue

                    if msg.id in seen_msg:
                        continue
                    seen_msg.add(msg.id)

                    # К какому посту канала относится комментарий
                    rt = getattr(msg, "reply_to", None)
                    top = None
                    if rt is not None:
                        top = getattr(rt, "reply_to_top_id", None) or \
                              getattr(rt, "reply_to_msg_id", None)
                    post_id = post_map.get(top, "")

                    uid = user.id
                    cdate = msg.date
                    rec = people.get(uid)

                    if rec is None:
                        bio = await fetch_bio(client, user)
                        people[uid] = {
                            "row": make_row(user, channel_username, post_id, msg, bio),
                            "count": 1,
                            "first": cdate,
                            "last": cdate,
                        }
                    else:
                        rec["count"] += 1
                        if cdate and rec["first"] and cdate < rec["first"]:
                            rec["first"] = cdate
                        if cdate and rec["last"] and cdate > rec["last"]:
                            rec["last"] = cdate
                            rec["row"] = make_row(
                                user, channel_username, post_id, msg,
                                bio_cache.get(uid, "")
                            )

                    processed += 1
                    if processed % 200 == 0:
                        try:
                            await status_msg.edit_text(
                                f"⚡ @{channel_username} — комментариев: {processed}, "
                                f"уникальных людей: {len(people)}"
                            )
                        except Exception:
                            pass

                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception:
                    continue

            break   # группа дочитана до конца

        except FloodWaitError as e:
            logger.info(
                f"FloodWait {e.seconds}s в обсуждении @{channel_username}, "
                f"продолжу с {offset_id} (попытка {attempt+1})"
            )
            try:
                await status_msg.edit_text(
                    f"⏸ @{channel_username} — пауза {e.seconds} сек, жду и продолжаю…"
                )
            except Exception:
                pass
            await asyncio.sleep(e.seconds + 2)
            continue
        except ConnectionError:
            try:
                await client.connect()
            except Exception:
                pass
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logger.exception(f"Обсуждение @{channel_username} прервано")
            interrupted = True
            break
    else:
        interrupted = True

    return interrupted


async def resolve_channel(client, ref: str):
    """Возвращает (entity, name).
    name — то, что кладём в таблицу и ссылки: юзернейм, либо 'c/<id>' для закрытых каналов.
    Поддерживает: @username, инвайт-ссылку (если аккаунт уже вступил в канал),
    ссылку вида t.me/c/<id>."""

    # --- инвайт-ссылка: t.me/+hash ---
    if ref.startswith("+"):
        invite_hash = ref[1:]
        res = await client(CheckChatInviteRequest(invite_hash))
        if isinstance(res, ChatInviteAlready):
            chat = res.chat
            name = getattr(chat, "username", None) or f"c/{chat.id}"
            return chat, name
        # ChatInvite / ChatInvitePeek — аккаунт в канале не состоит
        title = getattr(res, "title", "") or ref
        raise PermissionError(
            f"аккаунт не вступил в закрытый канал «{title}» — вступи в него "
            f"с этого аккаунта и отправь файл ещё раз"
        )

    # --- t.me/c/<id> ---
    if ref.startswith("c/"):
        chan_id = int(ref.split("/")[1])
        try:
            entity = await client.get_entity(PeerChannel(chan_id))
        except ValueError:
            # сущности нет в кеше сессии — подгружаем список диалогов и пробуем снова
            await client.get_dialogs()
            try:
                entity = await client.get_entity(PeerChannel(chan_id))
            except ValueError:
                raise PermissionError(
                    "аккаунт не состоит в этом закрытом канале (или ссылка неверная)"
                )
        return entity, getattr(entity, "username", None) or f"c/{entity.id}"

    # --- обычный публичный юзернейм ---
    entity = await client.get_entity(ref)
    return entity, ref


async def parse_channel(client, channel_username, status_msg: Message):
    # Копим по одному человеку, а не по одному комментарию:
    # user_id -> {"row": строка, "count": сколько комментов, "first"/"last": даты}
    people = {}
    seen_msg = set()   # id уже учтённых комментариев (защита от повторов при ретраях)
    rows = []          # заполняется в самом конце, из people

    try:
        channel, channel_username = await resolve_channel(client, channel_username)
    except PermissionError as e:
        await status_msg.answer(f"❌ {channel_username}: {e}")
        return rows
    except ChannelPrivateError:
        await status_msg.answer(f"❌ Канал @{channel_username} закрытый или нет доступа")
        return rows
    except ChatAdminRequiredError:
        await status_msg.answer(f"❌ @{channel_username} — нет прав доступа")
        return rows
    except AuthKeyUnregisteredError:
        await status_msg.answer("❌ Сессия устарела. Напиши /start и авторизуйся заново.")
        return rows
    except Exception as e:
        await status_msg.answer(f"❌ Ошибка канала @{channel_username}: {e}")
        return rows

    # ---- Быстрый путь: читаем группу обсуждений целиком ----
    # Если по ссылке — сам чат/группа (а не канал с комментариями), читаем его сообщения напрямую
    is_group = bool(getattr(channel, "megagroup", False)) or isinstance(channel, Chat)

    if is_group or FAST_MODE:
        group = channel if is_group else await get_discussion_group(client, channel)
        if group is not None:
            interrupted = await parse_via_group(
                client, channel_username, channel, group, status_msg, people, seen_msg
            )
            if interrupted and not people and not is_group:
                # группа обсуждений недоступна (например, закрытая и аккаунт в ней не состоит) —
                # пробуем медленный способ через сам канал
                logger.info(
                    f"@{channel_username}: группа обсуждений не читается, "
                    f"иду медленным способом по постам"
                )
            else:
                if interrupted:
                    logger.warning(
                        f"@{channel_username}: обсуждение пройдено не полностью, "
                        f"людей {len(people)}"
                    )
                for rec in people.values():
                    row = rec["row"]
                    row["Кол-во комментариев"] = rec["count"]
                    row["Дата первого комментария"] = (
                        rec["first"].astimezone(LOCAL_TZ).strftime("%d/%m/%Y %H:%M:%S")
                        if rec["first"] else ""
                    )
                    rows.append(row)
                people.clear()
                seen_msg.clear()
                return rows
        else:
            logger.info(
                f"@{channel_username}: группы обсуждений нет, "
                f"иду медленным способом по постам"
            )

    # ---- Медленный путь: запрос на каждый пост ----
    processed = 0
    offset_id = 0        # с какого поста продолжать (iter_messages идёт от новых к старым)
    interrupted = False

    for outer_attempt in range(5):
        try:
            async for post in client.iter_messages(
                channel, limit=POSTS_LIMIT or None, offset_id=offset_id
            ):
                offset_id = post.id  # запоминаем позицию, чтобы продолжить после сбоя

                if not post.replies or post.replies.replies == 0:
                    continue

                post_id = post.id

                # Итерацию комментариев повторяем при флуде/обрыве, а не бросаем пост:
                # уже собранные комментарии отсеются через seen, потерь не будет
                for attempt in range(3):
                    try:
                        async for comment in client.iter_messages(channel, reply_to=post_id):
                            try:
                                if not comment.sender_id:
                                    continue
                                user = comment.sender

                                # sender может быть каналом (анонимный админ / коммент от лица канала),
                                # у Channel нет first_name — раньше это роняло обработку всего поста
                                if not isinstance(user, User):
                                    continue
                                if getattr(user, "bot", False):
                                    continue

                                # Один комментарий учитываем ровно один раз.
                                # Важно: при ретрае после флуда пост перечитывается
                                # с начала — без этой проверки счётчик задвоится.
                                if comment.id in seen_msg:
                                    continue
                                seen_msg.add(comment.id)

                                uid = user.id
                                cdate = comment.date
                                rec = people.get(uid)

                                if rec is None:
                                    bio = await fetch_bio(client, user)
                                    people[uid] = {
                                        "row": make_row(user, channel_username, post_id, comment, bio),
                                        "count": 1,
                                        "first": cdate,
                                        "last": cdate,
                                    }
                                else:
                                    rec["count"] += 1
                                    if cdate and rec["first"] and cdate < rec["first"]:
                                        rec["first"] = cdate
                                    # строку храним по самому свежему комментарию
                                    if cdate and rec["last"] and cdate > rec["last"]:
                                        rec["last"] = cdate
                                        rec["row"] = make_row(
                                            user, channel_username, post_id, comment,
                                            bio_cache.get(uid, "")
                                        )

                            except FloodWaitError as e:
                                await asyncio.sleep(e.seconds)
                            except Exception:
                                # пропускаем только этот комментарий, а не весь пост
                                continue

                        break  # порция комментариев дочитана до конца

                    except FloodWaitError as e:
                        logger.info(f"FloodWait {e.seconds}s на комментариях поста {post_id}, попытка {attempt+1}")
                        await asyncio.sleep(e.seconds + 2)
                        continue
                    except ConnectionError:
                        try:
                            await client.connect()
                        except Exception:
                            pass
                        await asyncio.sleep(5)
                        continue
                    except Exception:
                        break

                processed += 1
                if processed % 50 == 0:
                    try:
                        await status_msg.edit_text(
                            f"📡 @{channel_username} — обработано постов с комментариями: {processed}, "
                            f"уникальных людей: {len(people)}"
                        )
                    except Exception:
                        pass

            break  # канал дочитан до конца без сбоев

        except FloodWaitError as e:
            # Раньше здесь молча терялся весь остаток канала. Теперь ждём и продолжаем
            # с того поста, на котором остановились.
            logger.info(
                f"FloodWait {e.seconds}s на постах @{channel_username}, "
                f"продолжу с поста {offset_id} (попытка {outer_attempt+1})"
            )
            try:
                await status_msg.edit_text(
                    f"⏸ @{channel_username} — Telegram просит паузу {e.seconds} сек, жду и продолжаю…"
                )
            except Exception:
                pass
            await asyncio.sleep(e.seconds + 2)
            continue

        except ConnectionError:
            try:
                await client.connect()
            except Exception:
                pass
            await asyncio.sleep(5)
            continue

        except Exception as e:
            logger.exception(f"Прерван обход постов @{channel_username}")
            interrupted = True
            try:
                await status_msg.answer(
                    f"⚠️ @{channel_username}: обход прерван ({e}). "
                    f"Сохраняю собранное: {len(people)} человек."
                )
            except Exception:
                pass
            break

    else:
        interrupted = True

    if interrupted:
        logger.warning(f"@{channel_username}: канал пройден не полностью, людей {len(people)}")

    # Разворачиваем в плоский список: одна строка = один человек
    for rec in people.values():
        row = rec["row"]
        row["Кол-во комментариев"] = rec["count"]
        row["Дата первого комментария"] = (
            rec["first"].astimezone(LOCAL_TZ).strftime("%d/%m/%Y %H:%M:%S")
            if rec["first"] else ""
        )
        rows.append(row)

    people.clear()
    seen_msg.clear()

    return rows


def save_excel(rows: list, user_id: int, channel: str = None) -> str:
    """Сохраняет данные в Excel и возвращает путь к файлу.
    pandas и openpyxl импортируются здесь — не при старте бота."""
    import pandas as pd  # ленивый импорт — экономит ~40MB в idle

    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if channel:
        safe = re.sub(r'[^A-Za-z0-9_\-]', '_', channel)[:50]
        name = f"{safe}_{stamp}.xlsx"
    else:
        name = f"комментаторы_{user_id}_{stamp}.xlsx"

    output_file = os.path.join(OUTPUT_DIR, name)
    df = pd.DataFrame(rows)

    # Самые активные — сверху
    if "Кол-во комментариев" in df.columns:
        df = df.sort_values("Кол-во комментариев", ascending=False)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Комментаторы")
        ws = writer.sheets["Комментаторы"]
        for col in ws.columns:
            max_len = max((len(str(c.value)) for c in col if c.value), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)
        ws.freeze_panes = "A2"

    return output_file


# ==========================================
# ХЭНДЛЕРЫ
# ==========================================

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    session_file = f"{SESSIONS_DIR}/session_{user_id}"

    # Сбрасываем старый клиент
    await drop_client(user_id)
    qr_logins.pop(user_id, None)
    await state.clear()

    # Проверяем сохранённую сессию
    if os.path.exists(f"{session_file}.session"):
        try:
            client = TelegramClient(session_file, API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                await set_client(user_id, client)
                await state.set_state(ParseState.waiting_channels_file)
                await message.answer(
                    "👋 С возвращением!\n\n"
                    "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
                    "<code>channel1\nchannel2</code>\n\n"
                    "Команды:\n"
                    "/clear — очистить сессию и память\n"
                    "/status — состояние бота",
                    parse_mode="HTML"
                )
                return
            else:
                await client.disconnect()
                os.remove(f"{session_file}.session")
        except Exception:
            if os.path.exists(f"{session_file}.session"):
                os.remove(f"{session_file}.session")

    await message.answer(
        "👋 Привет! Я парсер комментаторов Telegram каналов.\n\n"
        "🔑 Для входа отсканируй QR-код, который сейчас пришлю..."
    )

    client = TelegramClient(session_file, API_ID, API_HASH)
    await client.connect()

    try:
        await state.set_state(ParseState.waiting_qr_scan)
        await send_login_qr(message, user_id, client)
    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ Ошибка создания QR-кода: {e}\n\nПопробуй /start ещё раз.")
        await state.clear()


@dp.message(Command("clear"))
async def cmd_clear(message: Message, state: FSMContext):
    user_id = message.from_user.id
    was_connected = user_id in clients

    await drop_client(user_id)
    qr_logins.pop(user_id, None)
    bio_cache.clear()
    await state.clear()

    # Удаляем сессию с диска
    session_file = f"{SESSIONS_DIR}/session_{user_id}.session"
    if os.path.exists(session_file):
        os.remove(session_file)

    # Удаляем временные файлы пользователя
    for f in os.listdir(OUTPUT_DIR):
        if str(user_id) in f:
            try:
                os.remove(os.path.join(OUTPUT_DIR, f))
            except Exception:
                pass

    gc.collect()

    msg = "🧹 Готово!\n\n"
    msg += "✅ Telethon-клиент отключён\n" if was_connected else ""
    msg += "✅ Сессия удалена с диска\n"
    msg += "✅ Временные файлы удалены\n"
    msg += "✅ Память освобождена\n\n"
    msg += "Для нового парсинга — /start"
    await message.answer(msg)


@dp.message(Command("status"))
async def cmd_status(message: Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()

    connected = user_id in clients
    last_used = ""
    if connected:
        lu = clients[user_id]["last_used"]
        last_used = f"\n🕐 Последняя активность: {lu.strftime('%H:%M:%S')}"

    total_clients = len(clients)

    await message.answer(
        f"📊 <b>Статус бота</b>\n\n"
        f"{'🟢' if connected else '🔴'} Telethon: {'подключён' if connected else 'не подключён'}{last_used}\n"
        f"📋 Состояние: {current_state or 'нет'}\n"
        f"👥 Активных сессий: {total_clients}",
        parse_mode="HTML"
    )


@dp.message(ParseState.waiting_qr_scan)
async def handle_waiting_qr(message: Message, state: FSMContext):
    await message.answer(
        "⏳ Жду, пока ты отсканируешь QR-код в приложении Telegram "
        "(Настройки → Устройства → Подключить устройство).\n\n"
        "Если код истёк — новый пришлю автоматически. Если нужно начать заново — /start"
    )


@dp.message(ParseState.waiting_2fa)
async def handle_2fa(message: Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    client = await get_client(user_id)

    if not client:
        await message.answer("❌ Сессия потеряна. Начни заново /start")
        await state.clear()
        return

    try:
        await client.sign_in(password=password)
        await state.set_state(ParseState.waiting_channels_file)
        await message.answer(
            "✅ Авторизация успешна!\n\n"
            "📁 Отправь .txt файл со списком каналов (по одному на строку):\n\n"
            "<code>channel1\nchannel2</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {e}\n\nНачни заново /start")
        await state.clear()


def normalize_channel(line: str) -> str:
    """Приводит строку к юзернейму канала: убирает ссылку, @, слэши и хвосты.
    Инвайт-ссылки (t.me/+hash, t.me/joinchat/hash) возвращает как '+hash',
    ссылки на закрытые каналы (t.me/c/123456/78) — как 'c/123456'."""
    s = line.strip()
    for prefix in ("https://", "http://"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    for prefix in ("t.me/", "telegram.me/", "www.t.me/", "telegram.dog/"):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    s = s.split("?")[0].strip()

    # t.me/joinchat/HASH -> +HASH
    if s.startswith("joinchat/"):
        return "+" + s.split("/")[1].strip()
    # t.me/+HASH
    if s.startswith("+"):
        return "+" + s[1:].split("/")[0].strip()
    # t.me/c/123456/78 -> c/123456
    if s.startswith("c/"):
        parts = s.split("/")
        if len(parts) >= 2 and parts[1].isdigit():
            return f"c/{parts[1]}"

    s = s.lstrip("@").strip("/")
    # отрезаем возможные хвосты вида channel/123
    s = s.split("/")[0]
    return s.strip()


@dp.message(ParseState.waiting_channels_file, F.document)
async def handle_channels_file(message: Message, state: FSMContext):
    document: Document = message.document
    user_id = message.from_user.id

    if not document.file_name.endswith(".txt"):
        await message.answer("❌ Нужен файл формата .txt")
        return

    # Скачиваем файл
    file_path = f"{OUTPUT_DIR}/channels_{user_id}.txt"
    await bot.download(document, destination=file_path)

    with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        channels = []
        for line in f:
            c = normalize_channel(line)
            if c and c not in channels:
                channels.append(c)
    os.remove(file_path)  # сразу удаляем временный файл

    if not channels:
        await message.answer("❌ Файл пустой или неверный формат")
        return

    await message.answer(
        f"✅ Найдено каналов: {len(channels)}\n"
        f"📋 {chr(10).join(['@' + c for c in channels])}\n\n"
        f"⏳ Начинаю парсинг..."
    )

    await state.set_state(ParseState.parsing)

    # Получаем или создаём клиент
    client = await get_client(user_id)
    if not client:
        session_file = f"{SESSIONS_DIR}/session_{user_id}"
        client = TelegramClient(session_file, API_ID, API_HASH)
        await client.connect()
        await set_client(user_id, client)

    # ========== ПАРСИНГ ==========
    set_busy(user_id, True)  # защита от idle-отключения во время долгого парсинга

    total_rows = 0
    ok_channels = []
    failed_channels = []

    try:
        for i, channel in enumerate(channels, 1):
            status_msg = await message.answer(f"📡 [{i}/{len(channels)}] Парсим @{channel}...")

            try:
                # На случай если соединение отвалилось на предыдущем канале
                if not client.is_connected():
                    await client.connect()

                rows = await parse_channel(client, channel, status_msg)

            except Exception as e:
                logger.exception(f"Ошибка при парсинге @{channel}")
                failed_channels.append(channel)
                await status_msg.edit_text(f"❌ [{i}/{len(channels)}] @{channel} — ошибка: {e}")
                # пробуем восстановить соединение и идём к следующему каналу
                try:
                    if not client.is_connected():
                        await client.connect()
                except Exception:
                    pass
                continue

            touch_client(user_id)

            if not rows:
                await status_msg.edit_text(
                    f"⚠️ [{i}/{len(channels)}] @{channel} — комментаторов не найдено"
                )
                continue

            # Сразу сохраняем и отправляем файл по этому каналу
            try:
                output_file = save_excel(rows, user_id, channel)
                await message.answer_document(
                    FSInputFile(output_file),
                    caption=(
                        f"✅ [{i}/{len(channels)}] @{channel} — "
                        f"{len(rows)} уникальных человек"
                    )
                )
                os.remove(output_file)
                ok_channels.append(channel)
                total_rows += len(rows)
                await status_msg.edit_text(
                    f"✅ [{i}/{len(channels)}] @{channel} — {len(rows)} уникальных человек"
                )
            except Exception as e:
                logger.exception(f"Ошибка при сохранении файла для @{channel}")
                failed_channels.append(channel)
                await message.answer(f"❌ @{channel}: не удалось сохранить файл — {e}")

            # Освобождаем память после каждого канала
            rows.clear()
            del rows
            gc.collect()

    finally:
        set_busy(user_id, False)

    # ========== ИТОГ ==========
    summary = (
        f"🏁 Парсинг завершён!\n\n"
        f"👥 Всего уникальных людей: {total_rows}\n"
        f"✅ Успешно каналов: {len(ok_channels)} из {len(channels)}"
    )
    if failed_channels:
        summary += "\n❌ С ошибкой: " + ", ".join("@" + c for c in failed_channels)

    await message.answer(summary)

    gc.collect()

    await state.set_state(ParseState.waiting_channels_file)
    await message.answer(
        "📁 Можешь отправить новый .txt файл для следующего парсинга.\n\n"
        "💡 Чтобы освободить память — /clear"
    )


@dp.message(ParseState.parsing)
async def handle_during_parsing(message: Message):
    await message.answer(
        "⏳ Идёт парсинг, дождись окончания.\n"
        "Файлы приходят по мере готовности каналов.\n\n"
        "/status — состояние бота"
    )


@dp.message(ParseState.waiting_channels_file)
async def handle_wrong_input(message: Message):
    await message.answer(
        "📁 Отправь .txt файл со списком каналов.\n\n"
        "Команды:\n"
        "/clear — очистить память и сессию\n"
        "/status — состояние бота"
    )


# ==========================================
# ЗАПУСК
# ==========================================

async def main():
    # Запускаем фоновую очистку idle-клиентов
    asyncio.create_task(cleanup_idle_clients())
    print("🤖 Бот запущен!")
    print(f"⏱ Idle-таймаут клиентов: {IDLE_DISCONNECT_MINUTES} мин.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
