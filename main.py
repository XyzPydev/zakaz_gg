# -------------- BASIC IMPORTS --------------
import asyncio
import hashlib
import json
import math
import os
import threading
from pathlib import Path
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
import uuid
import tempfile
import shutil
from functools import wraps
from zoneinfo import ZoneInfo

import re
from typing import Optional, Tuple, List, Dict, Any
from aiogram.fsm.context import FSMContext
from aiogram import Bot, Dispatcher, F, types, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest as BadRequest,
    TelegramUnauthorizedError as Unauthorized,
    TelegramNotFound as ChatNotFound,
    TelegramRetryAfter as RetryAfter, TelegramBadRequest,
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile, ReplyKeyboardMarkup, \
    ReplyKeyboardRemove
from aiogram.types import InputMediaPhoto, SuccessfulPayment
from aiogram.types import LabeledPrice, PreCheckoutQuery, ContentType
from aiogram.types import Message, CallbackQuery, KeyboardButton, KeyboardButtonRequestChat, ChatAdministratorRights
from aiogram.utils.keyboard import InlineKeyboardBuilder

getcontext().prec = 50

# -------------- FILES IMPORTS -------------- #

GOLD_MULTIPLIERS = [2, 4, 8, 16, 32, 64,
                    128, 256, 512, 1024, 2048, 4096
                    ]
TOWER_MULTIPLIERS = [1.19, 1.48, 1.86, 2.32, 2.9, 3.62, 4.53, 5.66, 7.08]

API_TOKEN = "8257726098:AAHWUAtUqvzUas_UbqxkdUdOnlEGXROEVD0"

SPECIAL_ADMINS = [5143424934, 8493326566]

DB_PATH = "data.db"
ADMINS = json.load(open("admins_data.json", encoding="utf-8"))["admins"]
BANNED = json.load(open("banned.json", encoding="utf-8"))["banned"]

STATUSES = ["🫶 Игрок", "🔧 Читер", "🤣 Невезунчик", "⚜️ Уважительный", "💊 Что с ним?", "🏆 Победитель", "🎰 Лудоман",
            "💎 Миллионер", "🏅 Ветеран", "🥇 Топ 1", "💎 Топ Донатер", "🤙 Админ", "💣 Владелец", "💸 Гивавейщик", "🥇 Элита",
            "📢 Известность", "🔮 как...", "🪬 Фантом", "🔑 Легенда", "💎 Багач", "🎭 Пранкер", "♾️ ХХХ", "♠️ Масть", "⚡️ Зеп"]
BUYABLE_STATUSES = ["🪬 Фантом", "🔑 Легенда", "💎 Багач", "🎭 Пранкер", "♾️ ХХХ", "♠️ Масть", "⚡️ Зеп"]
BUYABLE_STATUSES_PRICES = ["m50000", "m100000", "m250000", "l500000", "m1000000", "s100", "m5000000"]
DEFAULT_UNIQUE_STATUSES = ["🍂 lsqnz", "💎 Заместитель", "👻 Ferzister"]

QS_PATH = Path("qs.json")

BACK = "⬅️ Назад"

# -------------- BOT AND DISPATCHER -------------- #

bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# -------------- FLOOD CONTROL -------------- #

_last_action_time = {}


def flood_protect(min_delay: float = 0.5):
    """
    Декоратор для callback-ів.
    Якщо користувач клацає частіше, ніж раз на min_delay секунд —
    йому пишеться "Подожди немного...".
    Якщо всередині handler-а виникає помилка редагування повідомлення
    через спам/ліміти/неможливість редагувати — просто відповідає "Зачекай".
    """

    def decorator(handler):
        @wraps(handler)
        async def wrapper(callback, *args, **kwargs):
            user_id = str(callback.from_user.id)
            now = time.time()
            last = _last_action_time.get(user_id, 0)

            if now - last < min_delay:
                texts = [
                    "⏳ Подожди немного...", "⏳ Да куда ты спешиш?", "⏳ Ща сек, обработка...",
                    "⏳ Ну погоди же ты...", "⏳ Не спеши...", "⏳ Загружаюсь...",
                    "⏳ Да погоди йомайо...", "⏳ Хочешь ошибку чтоли?", "⏳ Помедленней..."
                ]
                return await callback.answer(f"{random.choice(texts)}", show_alert=False)

            _last_action_time[user_id] = now

            try:
                return await handler(callback, *args, **kwargs)
            except Exception as e:
                # Переводимо текст помилки в нижній регістр для пошуку ключових слів
                msg = str(e).lower()

                # Ключові слова/фрази, які вказують на помилку редагування через спам/ліміти/неможливість редагувати
                spam_or_edit_errors = (
                    "spam", "slow_down", "too many requests", "retry after",
                    "message to edit not found", "message is not modified",
                    "can't edit message", "can't delete message", "cant edit",
                    "edit_message", "edit_text", "message can't be edited", "editmessagetext",
                )

                if any(k in msg for k in spam_or_edit_errors):
                    # Ввічлива кратка відповідь замість помилки
                    try:
                        await callback.answer("⏳ Подожди немного...", show_alert=False)
                    except Exception as inner:
                        pass
                    return None
                raise

        return wrapper

    return decorator


# -------------- DB -------------- #

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


async def handle_error(username, error, error_user_id, error_code):
    await bot.send_message("-1002554074011", f"Ошибка у @{username}\n\nError: {error}")
    await bot.send_message(error_user_id,
                           f"🛑 Упс, произошла ошибка! Мы уже решаем данную проблему!\n\nКод ошибки: {error_code}")


async def load_data(key) -> Any:
    key = str(key)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
        cursor.execute("SELECT value FROM json_data WHERE key = ?", (str(key),))
        row = cursor.fetchone()
        return json.loads(row["value"]) if row else None


async def save_data(key, data: Any):
    key = str(key)
    json_str = json.dumps(data, ensure_ascii=False)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
        cursor.execute("""
            INSERT INTO json_data (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, json_str))
        conn.commit()


def load_check(code: str) -> dict:
    """Завантажує чек за кодом, повертає словник або {} якщо не знайдено"""
    code = str(code)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checks (
                code TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                per_user REAL NOT NULL,
                remaining INTEGER NOT NULL,
                claimed TEXT DEFAULT '[]'
            )
        """)
        cursor.execute("SELECT * FROM checks WHERE code = ?", (code,))
        row = cursor.fetchone()
        if not row:
            return {}
        data = dict(row)
        # перетворюємо JSON список claimed на Python list
        try:
            data["claimed"] = json.loads(data.get("claimed", "[]"))
        except json.JSONDecodeError:
            data["claimed"] = []
        return data


def save_check(code: str, data: dict):
    """Зберігає або оновлює чек у таблиці checks"""
    code = str(code)
    claimed_json = json.dumps(data.get("claimed", []), ensure_ascii=False)
    creator_id = data["creator_id"]
    per_user = data["per_user"]
    remaining = data["remaining"]

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO checks (code, creator_id, per_user, remaining, claimed)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                creator_id = excluded.creator_id,
                per_user = excluded.per_user,
                remaining = excluded.remaining,
                claimed = excluded.claimed
        """, (code, creator_id, per_user, remaining, claimed_json))
        conn.commit()


async def create_user_data(user_id):
    data = {
        "coins": 0,
        "GGs": 0,
        "lost_coins": 0,
        "won_coins": 0,
        "status": 0
    }
    await anbskjfa(user_id)
    await save_data(str(user_id), data)


async def anbskjfa(uid):
    chat = await bot.get_chat(int(uid))
    username = chat.username
    await send_log(f"Игрок @{username} впервые зашел в МегаДроп! Поздравляем!")


async def gsname(name, id=None):
    try:
        new_name = ""
        if len(name) < 13:
            if not id:
                return name
            data = await load_data(str(id))
            clan_name = data.get("clan", None)
            if clan_name:
                so = f"[🛡 {clan_name}] " + name
                return so
            else:
                return name
        else:
            new_name = name[:12] + "..."

        if not id:
            return new_name
        data = await load_data(str(id))
        clan_name = data.get("clan", None)

        if clan_name:
            so = f"[🛡 {clan_name}] " + new_name
            return so
        else:
            return new_name
    except:
        if len(name) < 13:
            return name
        else:
            return name[:12] + "..."


def gline():
    line = "• " * 13
    return html.code(line)


def format_balance(balance):
    balance = float(balance)
    if balance == 0:
        return "0"
    exponent = int(math.log10(abs(balance)))
    group = exponent // 3
    scaled_balance = balance / (10 ** (group * 3))
    formatted_balance = f"{scaled_balance:.2f}"
    suffix = "к" * group
    return formatted_balance.rstrip('0').rstrip('.') + suffix


def parse_bet_input(arg: str) -> int:
    if arg is None:
        return -1

    s = str(arg).strip().lower()
    s = s.replace(" ", "").replace("_", "")
    m = re.fullmatch(r'([0-9]+(?:[.,][0-9]+)?)([kк]*)', s)
    if not m:
        return -1

    num_str, k_suffix = m.groups()
    num_str = num_str.replace(',', '.')
    try:
        num = Decimal(num_str)
    except Exception:
        return -1

    multiplier = Decimal(1000) ** len(k_suffix)
    result = num * multiplier

    try:
        return int(result)
    except Exception:
        return -1


LOG_PATH = Path("log.json")
DEFAULT_LOG = {"events": []}


def ensure_log_file(path: Path = LOG_PATH, default: dict = DEFAULT_LOG):
    """
    Переконується, що файл існує. Якщо ні — створює з дефолтною структурою.
    Повертає шлях (Path).
    """
    path = Path(path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        # створюємо новий файл з дефолтною структурою
        with path.open("w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
    return path


def load_log(path: Path = LOG_PATH) -> dict:
    """
    Безпечне завантаження log.json.
    Якщо файл відсутній — створює з дефолтом і повертає дефолт.
    Якщо JSON пошкоджений — робить бекап (log.json.bak_<timestamp>) і створює новий чистий файл.
    Повертає структуру (dict).
    """
    path = ensure_log_file(Path(path))
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            # якщо приходить не той тип — виправимо на дефолт
            if not isinstance(data, dict):
                return DEFAULT_LOG.copy()
            return data
    except json.JSONDecodeError:
        # JSON пошкоджений — робимо бекап і створюємо чистий файл
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        bak_name = path.with_name(f"{path.name}.bak_{ts}")
        try:
            shutil.copy2(path, bak_name)
        except Exception:
            # якщо копіювання не вдалось — намагайся перейменувати
            try:
                path.replace(bak_name)
            except Exception:
                pass
        # створюємо новий чистий файл
        with path.open("w", encoding="utf-8") as f:
            json.dump(DEFAULT_LOG, f, ensure_ascii=False, indent=2)
        return DEFAULT_LOG.copy()
    except FileNotFoundError:
        # нехай ensure_log_file створить його
        ensure_log_file(path)
        return DEFAULT_LOG.copy()


def save_log(data: dict, path: Path = LOG_PATH):
    """
    Безпечне збереження log.json через тимчасовий файл і os.replace (atomic).
    data має бути серіалізованим в JSON (dict).
    """
    path = Path(path)
    # записуємо у тимчасовий файл в тій же теці
    dirpath = path.parent or Path(".")
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(dirpath))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
            json.dump(data, tmpf, ensure_ascii=False, indent=2)
            tmpf.flush()
            os.fsync(tmpf.fileno())
        # атомарно замінюємо
        os.replace(tmp, str(path))
    except Exception:
        # якщо щось пішло не так — видаляємо тимчасовий файл
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise


def append_log(entry: dict | str, path: Path = LOG_PATH, add_timestamp: bool = False) -> dict:
    """
    Додає в log.json *текстовий* елемент у log['events'].
    - Якщо entry не рядок, буде зроблено json.dumps(entry).
    - Якщо add_timestamp=True, перед текстом додається мітка часу у форматі "%d.%m.%Y %H:%M".
    Повертає повний лог (dict).
    """
    log = load_log(path)

    # нормалізація структури: переконаємось, що є ключ 'events' як список
    if "events" not in log or not isinstance(log["events"], list):
        log["events"] = []

    # Приводимо entry до рядка
    if isinstance(entry, str):
        text = entry
    else:
        # серіалізуємо словник/інші типи у компактний JSON-рядок (читається як текст)
        try:
            text = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            text = str(entry)

    if add_timestamp:
        ts = datetime.now().strftime("%d.%m.%Y %H:%M")
        text = f"{ts} {text}"

    # Додаємо просто рядок у список events
    log["events"].append(text)

    # Зберігаємо і повертаємо
    save_log(log, path)
    return log


async def send_log(log):
    dt = datetime.now(ZoneInfo("Europe/Kyiv"))
    s = dt.strftime("%d.%m.%Y %H:%M")
    append_log(f"Дата: {s} | {log}")


def ckb(user_id):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=f"cancel_i:{user_id}")]])
    return kb


@dp.callback_query(F.data.startswith("cancel_i:"))
async def handle_normal_cancel(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    data = query.data
    query_id = data.split(":")[1]

    if int(user_id) != int(query_id):
        return query.answer("Это не твоя кнопка!")

    await query.message.edit_text("Действие отменено")
    await state.clear()


def get_status(status_id) -> str:
    status_id = int(status_id)
    if str(status_id)[:3] == "999":
        return get_unique_statuses()[int(str(status_id)[3:])]
    else:
        return STATUSES[status_id]


def gadmins():
    return json.load(open("admins_data.json", encoding="utf-8"))["admins"]


# -------------- HANDLERS -------------- #

@dp.message(lambda message: message.from_user.id in json.load(open("banned.json", encoding="utf-8"))["banned"])
async def handle_banned(message: Message):
    info = json.load(open("banned.json", encoding="utf-8"))["banned"]
    if message.chat.type != "private":
        return

    text = (
        f"{await gsname(message.from_user.first_name, message.from_user.id)}, вы забанены!"
        f"\n"
        f"Цена разбана: 50 ⭐️"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Купить разбан", url="https://t.me/sollamon")]
    ])
    await message.reply(text, reply_markup=kb)


# -------------- EVENT -------------- #

@dp.message(F.text.startswith("/event"))
async def handle_event(message: Message):
    user_id = message.from_user.id
    name = message.from_user.first_name

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🎃 Завод Тыкв", callback_data=f"event_halloween:{user_id}")]])

    await message.answer(
        f"🎟️ {await gsname(name, user_id)}, добро пожаловать в меню ивента!\n{gline()}\n\n🎃 {html.bold("Текущее событие")}: <i>Хеллоуин</i>\n<b>❓ Описание:</b> {html.italic("Стройте и улучшайте свой завод тыкв! Получайте призы за продажи тыкв!")}",
        reply_markup=kb)


@dp.callback_query(F.data.startswith("event_halloween:"))
async def handle_event_halloween(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    if int(user_id) != int(callback.data.split(":")[1]):
        return await callback.answer("Это не твоя кнопка!")

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if not data.get("event_halloween_factory"):
        await callback.message.edit_text("😅 У вас еще нету завода тыкв!", reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔥 Построить завод", callback_data=f"create_pumpkin_factory:{user_id}")]]))

    factory = data["event_halloween_factory"]
    level = factory["level"]
    pumpkins = factory["pumpkins"]
    last_claim = factory["last_claim"]

    now = datetime.now(timezone.utc)

    last_claim_iso = factory.get("last_claim")
    parsed_time = None

    if last_claim_iso:
        try:
            parsed_time = datetime.fromisoformat(last_claim_iso)
            if parsed_time.tzinfo is None:
                parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        except Exception:
            # якщо формат некоректний — скидати в now (щоб не видати помилкові тику)
            parsed_time = now
    else:
        # якщо last_claim відсутній — спробуємо використати created_at (якщо ви зберігаєте)
        created_iso = factory.get("created_at")
        if created_iso:
            try:
                parsed_time = datetime.fromisoformat(created_iso)
                if parsed_time.tzinfo is None:
                    parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            except Exception:
                parsed_time = now
        else:
            # немає ні last_claim, ні created_at — завод щойно створено або дані старі
            # збережемо last_claim = now і дамо 0 pending
            factory["last_claim"] = now.isoformat()
            await save_data(user_id, data)
            pumpkins_pending = 0

    # якщо parsed_time ініціалізовано — порахувати накопичення
    if 'pumpkins_pending' not in locals():
        if parsed_time:
            seconds = (now - parsed_time).total_seconds()
            if seconds < 0:
                seconds = 0

            # обмеження на максимальне накопичення (наприклад 7 днів) — опціонально
            # max_seconds = 3600 * 24 * 7
            # if seconds > max_seconds:
            #     seconds = max_seconds

            pumpkins_per_hour = level * 100  # ваша формула
            pumpkins_pending = int(seconds * (pumpkins_per_hour / 3600.0))

            # якщо хочете — оновлюємо last_claim лише після реального забору,
            # тому тут залишаємо last_claim незмінним (ми оновимо його нижче після додавання)
        else:
            pumpkins_pending = 0

    cost = level * 100000
    creating_pumpkins_per_hour = level * 100
    text = f"🔥 Завод тыкв\n{gline()}\n🌟 Уровень: {level}\n🎃 Тыкв: {pumpkins}\n🎃 На заводе: {pumpkins_pending}\n💰 Стоимость улучшения: {format_balance(cost)} mDrops / {creating_pumpkins_per_hour} тыкв в час"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Собрать тыквы", callback_data=f"claim_pumpkins:{user_id}"),
         InlineKeyboardButton(text=f"🔥 Улучшить завод [{format_balance(cost)} mDrops]",
                              callback_data=f"upgrade_pumpkin_factory:{user_id}")],
        [InlineKeyboardButton(text="🛍 Магазин", callback_data=f"pumpkins_shop:{user_id}")]])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("create_pumpkin_factory:"))
async def handle_create_pumpkin_factory(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if int(user_id) != int(callback.data.split(":")[1]):
        return await callback.answer("Это не твоя кнопка!")

    if data.get("event_halloween_factory"):
        return await callback.answer("У вас уже есть завод тыкв!")

    data["event_halloween_factory"] = {
        "level": 1,
        "pumpkins": 0,
        "last_claim": None,
    }
    await save_data(user_id, data)
    await callback.message.edit_text("🔥 Вы успешно построили завод тыкв!", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Перейти в завод", callback_data=f"pumpkin_factory_menu:{user_id}")]]))


@dp.callback_query(F.data.startswith("pumpkin_factory_menu:"))
async def handle_pumpkin_factory_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    if int(user_id) != int(callback.data.split(":")[1]):
        return await callback.answer("Это не твоя кнопка!")

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if not data.get("event_halloween_factory"):
        return await callback.answer("У вас нет завода тыкв!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Построить завод", callback_data=f"create_pumpkin_factory:{user_id}")]]))

    factory = data["event_halloween_factory"]
    level = factory["level"]
    pumpkins = factory["pumpkins"]
    last_claim = factory["last_claim"]

    now = datetime.now(timezone.utc)

    last_claim_iso = factory.get("last_claim")
    parsed_time = None

    if last_claim_iso:
        try:
            parsed_time = datetime.fromisoformat(last_claim_iso)
            if parsed_time.tzinfo is None:
                parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        except Exception:
            # якщо формат некоректний — скидати в now (щоб не видати помилкові тику)
            parsed_time = now
    else:
        # якщо last_claim відсутній — спробуємо використати created_at (якщо ви зберігаєте)
        created_iso = factory.get("created_at")
        if created_iso:
            try:
                parsed_time = datetime.fromisoformat(created_iso)
                if parsed_time.tzinfo is None:
                    parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            except Exception:
                parsed_time = now
        else:
            # немає ні last_claim, ні created_at — завод щойно створено або дані старі
            # збережемо last_claim = now і дамо 0 pending
            factory["last_claim"] = now.isoformat()
            await save_data(user_id, data)
            pumpkins_pending = 0

    # якщо parsed_time ініціалізовано — порахувати накопичення
    if 'pumpkins_pending' not in locals():
        if parsed_time:
            seconds = (now - parsed_time).total_seconds()
            if seconds < 0:
                seconds = 0

            # обмеження на максимальне накопичення (наприклад 7 днів) — опціонально
            # max_seconds = 3600 * 24 * 7
            # if seconds > max_seconds:
            #     seconds = max_seconds

            pumpkins_per_hour = level * 100  # ваша формула
            pumpkins_pending = int(seconds * (pumpkins_per_hour / 3600.0))

            # якщо хочете — оновлюємо last_claim лише після реального забору,
            # тому тут залишаємо last_claim незмінним (ми оновимо його нижче після додавання)
        else:
            pumpkins_pending = 0

    cost = level * 100000
    creating_pumpkins_per_hour = level * 100
    text = f"🔥 Завод тыкв\n{gline()}\n🌟 Уровень: {level}\n🎃 Тыкв: {pumpkins}\n🎃 На заводе: {pumpkins_pending}\n💰 Стоимость улучшения: {format_balance(cost)} mDrops / {creating_pumpkins_per_hour} тыкв в час"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Собрать тыквы", callback_data=f"claim_pumpkins:{user_id}"),
         InlineKeyboardButton(text=f"🔥 Улучшить завод [{format_balance(cost)} mDrops]",
                              callback_data=f"upgrade_pumpkin_factory:{user_id}")],
        [InlineKeyboardButton(text="🛍 Магазин", callback_data=f"pumpkins_shop:{user_id}")]])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("upgrade_pumpkin_factory:"))
async def handle_upgrade_pumpkin_factory(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    if int(user_id) != int(callback.data.split(":")[1]):
        return await callback.answer("Это не твоя кнопка!")

    data = await load_data(user_id)

    if not data.get("event_halloween_factory"):
        return await callback.answer("У вас нет завода тыкв!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Построить завод", callback_data=f"create_pumpkin_factory:{user_id}")]]))

    factory = data["event_halloween_factory"]
    level = factory["level"]
    pumpkins = factory["pumpkins"]

    if level >= 10:
        return await callback.answer("У вас максимальный уровень завода тыкв!")

    cost = level * 100000

    if data.get("coins", 0) < cost:
        return await callback.answer("У вас недостаточно монет для улучшения завода тыкв!")

    data["coins"] -= cost
    factory["level"] += 1

    await save_data(user_id, data)
    await callback.message.edit_text(f"🔥 Вы успешно улучшили завод тыкв на {level} уровень!",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔥 Перейти в завод",
                                                               callback_data=f"pumpkin_factory_menu:{user_id}")]]))


@dp.callback_query(F.data.startswith("claim_pumpkins:"))
async def handle_claim_pumpkins(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    if int(user_id) != int(callback.data.split(":")[1]):
        return await callback.answer("Это не твоя кнопка!")

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if not data.get("event_halloween_factory"):
        return await callback.answer("У вас нет завода тыкв!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Построить завод", callback_data=f"create_pumpkin_factory:{user_id}")]]))

    factory = data["event_halloween_factory"]
    level = factory["level"]
    pumpkins = factory["pumpkins"]
    last_claim = factory["last_claim"]

    now = datetime.now(timezone.utc)

    last_claim_iso = factory.get("last_claim")
    parsed_time = None

    if last_claim_iso:
        try:
            parsed_time = datetime.fromisoformat(last_claim_iso)
            if parsed_time.tzinfo is None:
                parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        except Exception:
            # якщо формат некоректний — скидати в now (щоб не видати помилкові тику)
            parsed_time = now
    else:
        # якщо last_claim відсутній — спробуємо використати created_at (якщо ви зберігаєте)
        created_iso = factory.get("created_at")
        if created_iso:
            try:
                parsed_time = datetime.fromisoformat(created_iso)
                if parsed_time.tzinfo is None:
                    parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            except Exception:
                parsed_time = now
        else:
            # немає ні last_claim, ні created_at — завод щойно створено або дані старі
            # збережемо last_claim = now і дамо 0 pending
            factory["last_claim"] = now.isoformat()
            await save_data(user_id, data)
            pumpkins_pending = 0

    # якщо parsed_time ініціалізовано — порахувати накопичення
    if 'pumpkins_pending' not in locals():
        if parsed_time:
            seconds = (now - parsed_time).total_seconds()
            if seconds < 0:
                seconds = 0

            # обмеження на максимальне накопичення (наприклад 7 днів) — опціонально
            # max_seconds = 3600 * 24 * 7
            # if seconds > max_seconds:
            #     seconds = max_seconds

            pumpkins_per_hour = level * 100  # ваша формула
            pumpkins_pending = int(seconds * (pumpkins_per_hour / 3600.0))

            # якщо хочете — оновлюємо last_claim лише після реального забору,
            # тому тут залишаємо last_claim незмінним (ми оновимо його нижче після додавання)
        else:
            pumpkins_pending = 0

    if pumpkins_pending <= 0:
        return await callback.answer("У вас нет тыкв для сбора!")

    factory["pumpkins"] += pumpkins_pending
    factory["pumpkins_pending"] = 0
    factory["last_claim"] = datetime.now().isoformat()
    await save_data(user_id, data)

    await callback.message.edit_text(f"🔥 Вы успешно собрали {pumpkins_pending} тыкв!",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔥 Перейти в завод",
                                                               callback_data=f"pumpkin_factory_menu:{user_id}")]]))


UUH = "😒"


@dp.callback_query(F.data.startswith("pumpkins_shop"))
async def pumpkins_shop(cb: CallbackQuery):
    user_id = cb.from_user.id
    name = cb.from_user.first_name

    if int(cb.data.split(":")[1]) != int(user_id):
        return await cb.answer("Это не твоя кнопка!")

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 mDrops", callback_data=f"buy_with_pumpkins:mdrops:{user_id}"),
         InlineKeyboardButton(text="🔄 GGs", callback_data=f"buy_with_pumpkins:ggs:{user_id}")],
        [InlineKeyboardButton(text="1️⃣", callback_data=f"buy_with_pumpkins:1:{user_id}"),
         InlineKeyboardButton(text="2️⃣", callback_data=f"buy_with_pumpkins:2:{user_id}"),
         InlineKeyboardButton(text="3️⃣", callback_data=f"buy_with_pumpkins:3:{user_id}")],
        [InlineKeyboardButton(text="4️⃣", callback_data=f"buy_with_pumpkins:4:{user_id}"),
         InlineKeyboardButton(text="5️⃣", callback_data=f"buy_with_pumpkins:5:{user_id}"),
         InlineKeyboardButton(text="6️⃣", callback_data=f"buy_with_pumpkins:6:{user_id}")],
        [InlineKeyboardButton(text="7️⃣", callback_data=f"buy_with_pumpkins:7:{user_id}"),
         InlineKeyboardButton(text="8️⃣", callback_data=f"buy_with_pumpkins:8:{user_id}")],
        [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{user_id}")]])
    await cb.message.edit_text(f"""🛍 <b>{await gsname(name, user_id)}</b>, добро пожаловать в магазин!
{gline()}
🔄 Курс обмена в mDrops:
 • 1 тыква = 7 mDrops
🔄 Курс обмена в GGs:
 • 400 тыкв = 1 GG

{gline()}
🛒 Магазин призов:
1️⃣ 150 🎃 ➡ 1 бронзовый кейс
2️⃣ 750 🎃 ➡ 1 серебряный кейс
3️⃣ 3000 🎃 ➡ 1 золотой кейс
4️⃣ 15000 🎃 ➡ 1 алмазный кейс

5️⃣ 7500 🎃 ➡ статус "🪬 Фантом"
6️⃣ 15000 🎃 ➡ статус "🔑 Легенда"
7️⃣ 45000 🎃 ➡ статус "💎 Багач"
8️⃣ <s>100000</s> 75000 🎃 ➡ статус "🎭 Пранкер" 💎  <tg-spoiler>(УЖЕ НЕ ДОСТУПЕН В /shop)</tg-spoiler>
""", reply_markup=kb)


hw_prizes = {
    1: ["1 бронзовый кейс", 150],
    2: ["1 серебряный кейс", 750],
    3: ["1 золотой кейс", 3000],
    4: ["1 алмазный кейс", 15000],
    5: ["статус \"🪬 Фантом\"", 7500],
    6: ["статус \"🔑 Легенда\"", 15000],
    7: ["статус \"💎 Багач\"", 45000],
    8: ["статус \"🎭 Пранкер\"", 75000]
}


class TradePumpkinsStates(StatesGroup):
    waiting_in_mdrops = State()
    waiting_in_ggs = State()


@dp.callback_query(F.data.startswith("buy_with_pumpkins"))
async def buy_with_pumpkins(cb: CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    name = cb.from_user.first_name

    if int(cb.data.split(":")[2]) != int(uid):
        return await cb.answer("Это не твоя кнопка!")

    choose = cb.data.split(":")[1]

    data = await load_data(uid)

    if not choose.isdigit():
        if choose == "mdrops":
            pumpkins = data["event_halloween_factory"]["pumpkins"]
            if pumpkins < 7:
                return await cb.message.edit_text(
                    f"{UUH} {await gsname(name, uid)}, тебе не хватает тыкв для обмена!\n\n🎃 Твои тыквы: {pumpkins}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]]))

            await cb.message.edit_text(
                f"📲 Введи количетсов тыкв для обмена или напиши \"все\" для того чтобы обменять все!\n\n🎃 Твои тыквы: {pumpkins}")
            return await state.set_state(TradePumpkinsStates.waiting_in_mdrops)

        elif choose == "ggs":
            pumpkins = data["event_halloween_factory"]["pumpkins"]
            if pumpkins < 400:
                return await cb.message.edit_text(
                    f"{UUH} {await gsname(name, uid)}, тебе не хватает тыкв для обмена!\n\n🎃 Твои тыквы: {pumpkins}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]]))

            await cb.message.edit_text(
                f"📲 Введи количетсов тыкв для обмена или напиши \"все\" для того чтобы обменять все!\n\n🎃 Твои тыквы: {pumpkins}")
            return await state.set_state(TradePumpkinsStates.waiting_in_ggs)

    else:
        pumpkins = data["event_halloween_factory"]["pumpkins"]
        if pumpkins >= hw_prizes[int(choose)][1]:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_trade_pumpkins:{choose}:{uid}")],
                [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]])
            await cb.message.edit_text(
                f"✅ {await gsname(name, uid)}, подтверди покупку.\n\n🔗 Товар: {hw_prizes[int(choose)][0]}\n💰 Цена: {hw_prizes[int(choose)][1]}\n🎃 Твои тыквы: {pumpkins}",
                reply_markup=kb)
        else:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]])
            await cb.message.edit_text(
                f"❌ {await gsname(name, uid)}, тебе не хватает {abs(int(hw_prizes[int(choose)][1]) - int(pumpkins))} тыкв.\n\n🔗 Товар: {hw_prizes[int(choose)][0]}\n💰 Цена: {hw_prizes[int(choose)][1]}\n🎃 Твои тыквы: {pumpkins}",
                reply_markup=kb)


@dp.callback_query(F.data.startswith("confirm_trade_pumpkins"))
async def confirm_trade_pumpkins(cb: CallbackQuery):
    uid = cb.from_user.id
    name = cb.from_user.first_name

    # защита кнопки
    parts = cb.data.split(":")
    if int(parts[2]) != int(uid):
        return await cb.answer("Это не твоя кнопка!")

    choose = int(parts[1])

    data = await load_data(uid)
    if not data:
        await create_user_data(uid)
        data = await load_data(uid)

    pumpkins = int(data.get("event_halloween_factory", {}).get("pumpkins", 0))
    # Правильно берем цену из hw_prizes
    try:
        price = int(hw_prizes[choose][1])
    except Exception:
        return await cb.message.edit_text("❌ Ошибка: неверный выбор товара.",
                                          reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                              [InlineKeyboardButton(text=BACK,
                                                                    callback_data=f"pumpkin_factory_menu:{uid}")]
                                          ]))

    if pumpkins < price:
        return await cb.message.edit_text(
            f"{UUH} {await gsname(name, uid)}, тебе не хватает {price - pumpkins} тыкв!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]
            ])
        )

    # Списываем тыквы
    data.setdefault("event_halloween_factory", {})["pumpkins"] = pumpkins - price

    # Если выбран пункт 1..4 — выдаём кейс
    if choose <= 4:
        # соответствие пунктов -> ключи кейсов
        choose_to_case = {
            1: "bronze",
            2: "silver",
            3: "gold",
            4: "diamond"
        }
        case_key = choose_to_case.get(choose)
        if not case_key:
            return await cb.message.edit_text("❌ Ошибка при выдаче кейса.",
                                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                                  [InlineKeyboardButton(text=BACK,
                                                                        callback_data=f"pumpkin_factory_menu:{uid}")]
                                              ]))

        inventory = data.setdefault("cases", {})
        inventory[case_key] = inventory.get(case_key, 0) + 1
        await save_data(uid, data)

        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]])
        return await cb.message.edit_text(
            f"✅ Вы получили {CASES[case_key]['emoji']} <b>{CASES[case_key]['name']}</b>!\n\n"
            f"🎃 Осталось тыкв: {data['event_halloween_factory']['pumpkins']}",
            reply_markup=kb,
            parse_mode="HTML"
        )

    # Иначе — выдаём статус (пункты 5..8)
    else:
        # hw_prizes[choose][0] выглядит как 'статус \"🪬 Фантом\"'
        raw = hw_prizes.get(choose, [None, None])[0]
        # пытаемся извлечь название между кавычками
        status_name = None
        if raw:
            parts_raw = raw.split('"')
            if len(parts_raw) >= 2:
                status_name = parts_raw[1]
            else:
                # fallback — весь текст
                status_name = raw

        if not status_name:
            return await cb.message.edit_text("❌ Ошибка при выдаче статуса.",
                                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                                  [InlineKeyboardButton(text=BACK,
                                                                        callback_data=f"pumpkin_factory_menu:{uid}")]
                                              ]))

        statuses = data.setdefault("statuses", [])
        if status_name in statuses:
            msg = f"ℹ️ У вас уже есть статус {status_name}."
        else:
            statuses.append(status_name)
            await save_data(uid, data)
            msg = f"✅ Вы получили статус {status_name}!"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]])
        return await cb.message.edit_text(
            f"{msg}\n\n🎃 Осталось тыкв: {data['event_halloween_factory']['pumpkins']}",
            reply_markup=kb
        )


@dp.message(TradePumpkinsStates.waiting_in_mdrops)
async def process_mdrops_exchange(message: Message, state: FSMContext):
    uid = message.from_user.id
    text = message.text.strip().lower()

    data = await load_data(uid)
    if not data:
        await create_user_data(uid)
        data = await load_data(uid)

    pumpkins = int(data.get("event_halloween_factory", {}).get("pumpkins", 0))

    # "все" или число
    if text in ("все", "всі", "all"):
        amount = pumpkins
    else:
        if not text.isdigit():
            return await message.answer("❗ Введи число (количество тыкв для обмена) или «все».")
        amount = int(text)

    if amount <= 0:
        return await message.answer("❗ Введи положительное число.")
    if amount > pumpkins:
        return await message.answer(f"❌ У тебя нет столько тыкв. У тебя: {pumpkins} 🎃")

    # курс 1 🎃 -> 7 mDrops
    mdrops_amount = amount * 7

    data.setdefault("event_halloween_factory", {})["pumpkins"] = pumpkins - amount
    data["coins"] = data.get("coins", 0) + mdrops_amount
    await save_data(uid, data)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]
    ])

    await message.answer(
        f"✅ Обмен успешен!\n\n🎃 Потрачено: {amount} 🎃 → {mdrops_amount} mDrops\n"
        f"🎃 Осталось: {data['event_halloween_factory']['pumpkins']} 🎃\n"
        f"💰 Твои mDrops: {format_balance(data['coins'])}",
        reply_markup=kb
    )

    await state.clear()


@dp.message(TradePumpkinsStates.waiting_in_ggs)
async def process_ggs_exchange(message: Message, state: FSMContext):
    uid = message.from_user.id
    text = message.text.strip().lower()

    data = await load_data(uid)
    if not data:
        await create_user_data(uid)
        data = await load_data(uid)

    pumpkins = int(data.get("event_halloween_factory", {}).get("pumpkins", 0))

    # "все" или число
    if text in ("все", "всі", "all"):
        amount = pumpkins
    else:
        if not text.isdigit():
            return await message.answer("❗ Введи число (количество тыкв для обмена) или «все».")
        amount = int(text)

    if amount <= 0:
        return await message.answer("❗ Введи положительное число.")
    if amount > pumpkins:
        return await message.answer(f"❌ У тебя нет столько тыкв. У тебя: {pumpkins} 🎃")

    # 400 🎃 = 1 GG
    possible_ggs = amount // 400
    used_pumpkins = possible_ggs * 400
    leftover = amount - used_pumpkins

    if possible_ggs <= 0:
        return await message.answer("❌ Для обмена на GG нужно минимум 400 🎃 (или введи количество, кратное 400).")

    data["ggs"] = data.get("ggs", 0) + possible_ggs
    data.setdefault("event_halloween_factory", {})["pumpkins"] = pumpkins - used_pumpkins
    await save_data(uid, data)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BACK, callback_data=f"pumpkin_factory_menu:{uid}")]
    ])

    msg = (
        f"✅ Обмен успешен!\n\n"
        f"🎃 Потрачено: {used_pumpkins} 🎃 → {possible_ggs} GG(ов)\n"
        f"🎃 Осталось: {data['event_halloween_factory']['pumpkins']} 🎃\n"
        f"🏷️ Твои GGs: {int(data['ggs'])}"
    )
    if leftover > 0:
        msg += f"\n\nℹ️ Часть введённых тыкв ({leftover} 🎃) не была использована, потому что обмен возможен только от 400 🎃 за 1 GG."

    await message.answer(msg, reply_markup=kb)
    await state.clear()


# -------------- MAIN -------------- #

@dp.message(F.text.startswith("/start"))
async def start_command(message: Message):
    try:
        user_id = str(message.from_user.id)
        parts = message.text.split()
        arg = parts[1] if len(parts) > 1 else None

        data = await load_data(user_id)
        if not data:
            await create_user_data(message.from_user.id)
            data = await load_data(str(message.from_user.id))

        if arg and arg.startswith("check_"):
            code = arg.split("check_")[1]
            check = load_check(code)

            if not check:
                return await message.answer("❗ Неверный код чека.")
            if user_id in check.get("claimed", []):
                return await message.answer("❗ Вы уже активировали этот чек.")
            if check["remaining"] <= 0:
                return await message.answer("❗ Этот чек больше не действителен.")

            data["coins"] += check["per_user"]
            check.setdefault("claimed", []).append(user_id)
            check["remaining"] -= 1
            await save_data(user_id, data)
            save_check(code, check)

            creator_id = check.get("creator_id")
            if creator_id:
                try:
                    await bot.send_message(
                        creator_id,
                        f"✅ Пользователь {await gsname(message.from_user.first_name, message.from_user.id)} (ID: {user_id}) активировал ваш чек {code} и получил {format_balance(check['per_user'])} mDrops"
                    )
                    await send_log(
                        f"Пользователь {await gsname(message.from_user.first_name, message.from_user.id)} (ID: {user_id}) активировал чек {html.spoiler(code)} и получил {format_balance(check['per_user'])} mDrop\nАктиваций осталось: {check["remaining"]}")
                except:
                    pass

            return await message.answer(f"🤑 Вы получили {check['per_user']} mDrops по чеку!")

        referral = arg if arg and not arg.startswith("check_") else None
        if referral and referral != user_id:
            if data.get("ref_activated", False) is False:
                ref_data = await load_data(referral)
                if ref_data:
                    ref_data["coins"] = ref_data.get("coins", 0) + 2500
                    ref_data["referrals"] = ref_data.get("referrals", 0) + 1
                    await save_data(referral, ref_data)
                    try:
                        await bot.send_message(referral, "🎉 У вас новый реферал! +2500 mDrops.")
                    except:
                        pass

                data["ref_activated"] = True
                await save_data(user_id, data)

        if await load_data(user_id) != data:
            await save_data(user_id, data)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💭 Добавить бота в чат", url="https://t.me/gmegadbot?startgroup=start")],
            [InlineKeyboardButton(text="📣 Новости", url="t.me/saycursed"),
             InlineKeyboardButton(text="🟢 Наш чат", url="t.me/saycurse")],
            [InlineKeyboardButton(text="📱 WebApp", url="https://t.me/gmegadbot/gmegadapp")]])

        try:
            await message.answer_photo(
                photo="AgACAgIAAxkBAAEBnDlowCt4Vsk7wAijNaDceFUeUFahcwAC9PYxG1syAAFK6A-SJs4QhsYBAAMCAAN5AAM2BA",
                caption=f"""👋 Привет {await gsname(message.from_user.first_name, message.from_user.id)}, это МегаДроп! 📦

💎 Играй, веселись и получай пользу для своего канала и чата. Один, с друзьями или всей семьёй — выбор за тобой! 🔥

🤔 Готов? Вводи /game и начинаем!

❓Есть вопросы? Тогда 👉 /help""",
                parse_mode="HTML",
                reply_markup=kb
            )
        except BadRequest:
            try:
                await message.answer_photo(
                    photo="AgACAgIAAxkBAAIi5WjCkEk9vfYzV8RKbtogtJ4zmIWpAALh_jEb3HAZSk0ubUug8KNNAQADAgADeQADNgQ",
                    caption=f"""👋 Привет {await gsname(message.from_user.first_name, message.from_user.id)}, это МегаДроп! 📦

💎 Играй, веселись и получай пользу для своего канала и чата. Один, с друзьями или всей семьёй — выбор за тобой! 🔥

🤔 Готов? Вводи /game и начинаем!

❓Есть вопросы? Тогда 👉 /help""",
                    parse_mode="HTML",
                    reply_markup=kb
                )
            except:
                await message.answer(f"""👋 Привет {await gsname(message.from_user.first_name, message.from_user.id)}, это МегаДроп! 📦

💎 Играй, веселись и получай пользу для своего канала и чата. Один, с друзьями или всей семьёй — выбор за тобой! 🔥

🤔 Готов? Вводи /game и начинаем!

❓Есть вопросы? Тогда 👉 /help""")

    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 102)


@dp.message(F.text.lower().in_(["б", "баланс", "/balance", "/balance@gmegadbot"]))
async def handle_balance(message: Message):
    try:
        user_id = str(message.from_user.id)
        name = message.from_user.first_name

        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)
            data = await load_data(user_id)

        coins = data.get("coins", 0)
        lost = data.get("lost_coins", 0)

        # собираем клавиатуру только если баланс == 0
        kb_rows = []

        if int(coins) == 0:
            # Проверка доступности бонуса (логика повторяет hourly_bonus)
            now = datetime.now(timezone.utc)
            last_bonus = data.get("last_hourly_bonus")

            # статус
            try:
                status = int(data.get("status", 0))
            except Exception:
                status = 0

            if status == 22:
                cooldown = timedelta(minutes=30)
            else:
                cooldown = timedelta(hours=1)

            bonus_available = False
            if not last_bonus:
                bonus_available = True
            else:
                try:
                    last_time = datetime.fromisoformat(last_bonus)
                    if now - last_time >= cooldown:
                        bonus_available = True
                except Exception:
                    bonus_available = True  # на всякий — разрешаем

            if bonus_available:
                kb_rows.append([InlineKeyboardButton(text="🎁 Бонус", callback_data=f"claim_bonus:{user_id}")])

            # Проверка доступности барабана (логика похожа на baraban_handler)
            baraban_ready = False
            now_ts = time.time()
            try:
                # кулдаун в секундах (если нет — считаем доступным)
                bd = data.get("baraban_cooldown")
                if not bd or now_ts >= float(bd):
                    # проверим подписку на канал
                    try:
                        member = await bot.get_chat_member(CHANNEL_ID, int(user_id))
                        if member.status not in ("left", "kicked"):
                            baraban_ready = True
                    except Exception:
                        # если не удалось проверить — не показываем кнопку (безопаснее)
                        baraban_ready = False
            except Exception:
                baraban_ready = False

            if baraban_ready:
                kb_rows.append([InlineKeyboardButton(text="🍥 Барабан", callback_data=f"open_baraban:{user_id}")])

        # формируем markup, если есть строки
        reply_kb = None
        if kb_rows:
            reply_kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

        # отправляем сообщение с балансом и, если нужно, с кнопками
        await message.reply(
            f"{html.italic(f'💰 {await gsname(message.from_user.first_name, message.from_user.id)}, твой баланс: {format_balance(coins)} mDrops')}\n"
            f"{gline()}\n\n🎰 Слито: {format_balance(lost)} mDrops",
            reply_markup=reply_kb
        )
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 103)


@dp.callback_query(F.data.startswith("claim_bonus:"))
async def claim_bonus_cb(cb: CallbackQuery):
    try:
        parts = cb.data.split(":")
        if len(parts) < 2:
            return await cb.answer("Неверные данные.")
        uid = parts[1]
        if int(cb.from_user.id) != int(uid):
            return await cb.answer("Это не твоя кнопка!")

        data = await load_data(uid)
        if not data:
            await create_user_data(uid)
            data = await load_data(uid)

        # статус
        try:
            status = int(data.get("status", 0))
        except Exception:
            status = 0

        # определяем cooldown и диапазон бонуса
        if status == 22:
            bonus_min, bonus_max = 3000, 4500
            cooldown = timedelta(minutes=30)
            range_desc = "3000–4500 mDrops (статус 22). КД 30 мин."
        elif 17 <= status <= 21:
            step = status - 17
            bonus_min = 500 + step * 250
            bonus_max = 750 + step * 250
            bonus_min = min(bonus_min, 3000)
            bonus_max = min(bonus_max, 3000)
            cooldown = timedelta(hours=1)
            range_desc = f"{bonus_min}–{bonus_max} mDrops (статус {status}). КД 60 мин."
        else:
            bonus_min, bonus_max = 50, 250
            cooldown = timedelta(hours=1)
            range_desc = f"{bonus_min}–{bonus_max} mDrops (обычный бонус). КД 60 мин."

        now = datetime.now(timezone.utc)
        last_bonus = data.get("last_hourly_bonus")
        if last_bonus:
            try:
                last_time = datetime.fromisoformat(last_bonus)
                if now - last_time < cooldown:
                    remaining = cooldown - (now - last_time)
                    minutes = math.ceil(remaining.total_seconds() / 60)
                    return await cb.answer(f"Бонус ещё не доступен. Через {minutes} мин.", show_alert=True)
            except Exception:
                pass  # позволим получить бонус, если формат неверный

        # проверка подписки на чат (как в hourly_bonus)
        try:
            member = await bot.get_chat_member(chat_id="@saycurse", user_id=int(uid))
            if member.status in ["left", "kicked"]:
                return await cb.answer("Чтобы получить бонус, вступи в наш чат.", show_alert=True)
        except Exception:
            return await cb.answer("Ошибка проверки подписки.", show_alert=True)

        # выдаём бонус
        bonus = random.randint(bonus_min, bonus_max)
        if status not in (22,) and bonus > 3000:
            bonus = 3000

        data["coins"] = data.get("coins", 0) + bonus
        data["last_hourly_bonus"] = now.isoformat()
        await save_data(uid, data)

        await cb.message.answer(
            f"🎁 {await gsname(cb.from_user.first_name, int(uid))}, ты получил {bonus} mDrops!\n"
            f"Диапазон для твоего статуса: {range_desc}\n"
            f"Следующий бонус через {int(cooldown.total_seconds() // 60)} минут."
        )
        await cb.answer()  # убрать "крутилку" у кнопки
    except Exception as e:
        await handle_error(cb.from_user.username, e, cb.from_user.id, 104)


@dp.callback_query(F.data.startswith("open_baraban:"))
async def open_baraban_cb(cb: CallbackQuery):
    try:
        parts = cb.data.split(":")
        if len(parts) < 2:
            return await cb.answer("Неверные данные.")
        uid = parts[1]
        if int(cb.from_user.id) != int(uid):
            return await cb.answer("Это не твоя кнопка!")

        # подгружаем данные
        if not await load_data(uid):
            await create_user_data(uid)
        data = await load_data(uid)

        now = time.time()
        # статус пользователя
        try:
            status = int(data.get("status", 0))
        except Exception:
            status = 0

        # вычисляем кулдаун как в оригинале
        base_cd_seconds = 12 * 3600
        reduction_per_status = 5 * 60
        min_cd_seconds = 40 * 60

        if status == 22:
            final_cd_seconds = 30 * 60
            spins = 2
        else:
            reduction = status * reduction_per_status
            final_cd_seconds = max(min_cd_seconds, base_cd_seconds - reduction)
            spins = 1

        # проверяем кулдаун
        if "baraban_cooldown" in data and now < data["baraban_cooldown"]:
            remaining = int(data["baraban_cooldown"] - now)
            hrs = remaining // 3600
            mins = (remaining % 3600) // 60
            return await cb.answer(f"⏳ Следующий барабан через {hrs}ч {mins}м.", show_alert=True)

        # проверка подписки на канал
        try:
            member = await bot.get_chat_member(CHANNEL_ID, int(uid))
            if member.status in ("left", "kicked"):
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚜️ Перейти в канал", url="https://t.me/saycursed")]
                ])
                return await cb.message.answer(
                    f'❌ Подпишись на {html.link("канал", "https://t.me/saycursed")}, чтобы получить приз 🎁',
                    disable_web_page_preview=True,
                    reply_markup=kb
                )
        except Exception as e:
            return await cb.answer(f"⚠️ Не удалось проверить подписку: {e}", show_alert=True)

        # выбираем приз(ы)
        ptype, amount, filename = choose_prize()
        prizes = [(ptype, amount)]

        if spins == 2:
            p2, a2, f2 = choose_prize()
            prizes.append((p2, a2))

        total_coins = sum(a for t, a in prizes if t == "coins")
        total_ggs = sum(a for t, a in prizes if t != "coins")

        if total_coins:
            data["coins"] = data.get("coins", 0) + total_coins
        if total_ggs:
            data["GGs"] = data.get("GGs", 0) + total_ggs

        # обновляем кулдаун
        data["baraban_cooldown"] = now + final_cd_seconds
        await save_data(uid, data)

        # отправляем "видео" / результат — аналогично твоему handler'у
        loading_msg = await cb.message.answer("📂 Загрузка видео, ожидайте...")
        try:
            prize_video = FSInputFile(f"baraban_videos/{filename}.mp4")
            initial_caption = "🎰 Крутим барабан..."
            if spins == 2:
                initial_caption += " (2 спина за статус \"♠️ Масть\"!)"

            sent = await cb.message.answer_video(prize_video, caption=initial_caption)
            await loading_msg.delete()
        except Exception as e:
            await loading_msg.delete()
            await handle_error(cb.from_user.username, e, cb.from_user.id, 122)
            return

        # ждём для эффекта
        await asyncio.sleep(5)

        # формируем итоговый текст
        prize_lines = []
        for idx, (p, a) in enumerate(prizes, start=1):
            if p == "coins":
                prize_lines.append(f"Спин #{idx}: {format_balance(a)} mDrops")
            else:
                prize_lines.append(f"Спин #{idx}: {a} GG")
        prize_text = "\n".join(prize_lines)

        summary_parts = []
        if total_coins:
            summary_parts.append(f"{format_balance(total_coins)} mDrops")
        if total_ggs:
            summary_parts.append(f"{total_ggs} GG")
        summary = " и ".join(summary_parts) if summary_parts else "ничего"

        # пытаемся отредактировать подпись видео, как в оригинале
        try:
            await bot.edit_message_caption(
                chat_id=cb.message.chat.id,
                message_id=sent.message_id,
                caption=(
                        f"🎉 {await gsname(cb.from_user.first_name, int(uid))}, твой приз:\n{prize_text}\n\n"
                        f"Итого: {summary}\n\n"
                        f"КД барабана для твоего статуса ({get_status(status)}): {int(final_cd_seconds // 3600)}ч {int((final_cd_seconds % 3600) // 60)}м."
                        + (f"\n(Статус \"♠️ Масть\" даёт 2 спина.)" if status == 22 else "")
                )
            )
        except Exception:
            await cb.message.answer(
                f"🎉 {await gsname(cb.from_user.first_name, int(uid))}, твой приз:\n{prize_text}\n\n"
                f"Итого: {summary}\n\n"
                f"КД барабана для твоего статуса ({get_status(status)}): {int(final_cd_seconds // 3600)}ч {int((final_cd_seconds % 3600) // 60)}м."
                + (f"\n(Статус \"♠️ Масть\" даёт 2 спина.)" if status == 22 else "")
            )

        await cb.answer()
    except Exception as e:
        await handle_error(cb.from_user.username, e, cb.from_user.id, 122)


@dp.message(F.text.lower().in_(["п", "профиль", "/profile", "/profile@gmegadbot"]))
async def handle_balance(message: Message):
    try:
        user_id = message.from_user.id
        name = message.from_user.first_name

        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)
            data = await load_data(str(message.from_user.id))

        clan = data.get("clan", "нету")
        await message.reply(
            f"🆔 Профиль: {html.code(message.from_user.id)}\n{gline()}\n├ 👤 {html.italic(html.link(await gsname(message.from_user.first_name, message.from_user.id), f't.me/{message.from_user.username}'))}\n├ ⚡️ {html.italic('Статус:')} {get_status(data['status'])}\n├ 🛡 {html.italic(f'Клан: {clan}')}\n├ 🟢 {html.italic('Выиграно:')} {format_balance(data['won_coins'])} mDrops\n├ 🗿 {html.italic('Проиграно:')} {format_balance(data['lost_coins'])} mDrops\n{gline()}\n💰 {html.italic('Баланс:')} {format_balance(data['coins'])} mDrops\n💎 {html.italic('Баланс:')} {int(data['GGs'])} GGs",
            disable_web_page_preview=True)
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 104)


@dp.message(F.text.lower().in_(["бонус", "/bonus", "bonus", "/bonus@gmegadbot"]))
async def hourly_bonus(message: Message):
    try:
        user_id = str(message.from_user.id)
        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)
            data = await load_data(str(message.from_user.id))

        now = datetime.now(timezone.utc)
        last_bonus = data.get("last_hourly_bonus")

        # определяем статус (безопасно приводим к int)
        try:
            status = int(data.get("status", 0))
        except Exception:
            status = 0

        # определяем диапазон бонуса и кулдаун по статусу
        if status == 22:
            bonus_min, bonus_max = 3000, 4500
            cooldown = timedelta(minutes=30)
            range_desc = "3000–4500 mDrops (статус 22). КД 30 мин."
        elif 17 <= status <= 21:
            # прогрессия: 17 -> 500-750, 18 -> 750-1000, и т.д. (+250 к мин/макс)
            step = status - 17
            bonus_min = 500 + step * 250
            bonus_max = 750 + step * 250
            # бонус не может быть выше 3000
            bonus_min = min(bonus_min, 3000)
            bonus_max = min(bonus_max, 3000)
            cooldown = timedelta(hours=1)
            range_desc = f"{bonus_min}–{bonus_max} mDrops (статус {status}). КД 60 мин."
        else:
            bonus_min, bonus_max = 50, 250
            cooldown = timedelta(hours=1)
            range_desc = f"{bonus_min}–{bonus_max} mDrops (обычный бонус). КД 60 мин."

        # проверка времени последнего бонуса
        if last_bonus:
            last_time = datetime.fromisoformat(last_bonus)
            if now - last_time < cooldown:
                remaining = cooldown - (now - last_time)
                minutes = math.ceil(remaining.total_seconds() / 60)
                return await message.answer(
                    f"😉 {await gsname(message.from_user.first_name, message.from_user.id)}, ты уже получал бонус. Вернись за следующим через {minutes} минут."
                )

        # проверка подписки на чат
        try:
            member = await bot.get_chat_member(chat_id="@saycurse", user_id=message.from_user.id)
            if member.status in ["left", "kicked"]:
                return await message.answer(
                    'Чтобы получить бонус, вступи в <a href="https://t.me/saycurse">наш чат</a>.',
                    disable_web_page_preview=True
                )
        except Exception as e:
            return await message.answer(f"Ошибка при проверке подписки.\n{e}")

        # расчёт бонуса
        bonus = random.randint(bonus_min, bonus_max)

        # дополнительная гарантия: не более 3000 (для статусов 17–21)
        if status not in (22,) and bonus > 3000:
            bonus = 3000

        # начисляем бонус
        data["coins"] = data.get("coins", 0) + bonus
        data["last_hourly_bonus"] = now.isoformat()
        await save_data(user_id, data)

        # ответ пользователю
        cd_minutes = int(cooldown.total_seconds() // 60)
        await message.answer(
            f"🎁 {await gsname(message.from_user.first_name, message.from_user.id)}, ты получил {bonus} mDrops!\n"
            f"Диапазон для твоего статуса: {range_desc}\n"
            f"Следующий бонус можно получить через {cd_minutes} минут."
        )
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 104)


@dp.message(F.text.in_(
    ["ежедневный", "/daily", "daily", "bonus2", "бонус2", "ежедневный", "ежедневный бонус", "/daily@gmegadbot"]))
async def daily_bonus(message: Message):
    try:
        user_id = str(message.from_user.id)
        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)
            data = await load_data(str(message.from_user.id))

        now = datetime.now(timezone.utc)
        last_daily = data.get("last_daily_bonus")
        streak = data.get("daily_streak", 0)

        # Проверка времени последнего бонуса
        if last_daily:
            last_time = datetime.fromisoformat(last_daily)
            if now - last_time < timedelta(hours=24):
                remaining = timedelta(hours=24) - (now - last_time)
                hours = remaining.seconds // 3600
                minutes = (remaining.seconds % 3600) // 60
                await message.answer(
                    f"🙃 {await gsname(message.from_user.first_name, message.from_user.id)}, ты уже получил ежедневный бонус, приходи через {hours}ч. {minutes}м.")
                return

            # Сброс стрика, если прошло больше 48 часов
            if now - last_time > timedelta(hours=48):
                streak = 0

        # Получаем статус пользователя (безопасно приводим к int)
        try:
            status = int(data.get("status", 0))
        except Exception:
            status = 0

        try:
            user_profile = await bot.get_chat(user_id)
            if "@gmegadbot" not in (user_profile.bio or ""):
                await message.answer(
                    f"😉 {await gsname(message.from_user.first_name, message.from_user.id)}, чтобы получить ежедневный бонус, вставь {html.code('@gmegadbot')} в описание своего профиля (био)!\n\n☝️ Изменения вступают в силу через 30 секунд (лимит Телеграм)."
                )
                return
        except Exception as e:
            await handle_error(message.from_user.username, e, message.from_user.id, 101)
            return

        # Определяем базовый диапазон бонуса в зависимости от статуса
        if status == 22:
            base_min, base_max = 3000, 4500
            range_desc = "3000–4500 mDrops (статус 22)."
        elif 17 <= status <= 21:
            # Прогрессия: для status=17 начинаем с 1500–2250, затем прибавляем по 500 к каждому краю за шаг
            step = status - 17
            base_min = 1500 + step * 500
            base_max = 2250 + step * 500
            # Обрезаем диапазон сверху до 3000
            base_min = min(base_min, 3000)
            base_max = min(base_max, 3000)
            range_desc = f"{base_min}–{base_max} mDrops (статус {status}). Итог не больше 3000."
        else:
            base_min, base_max = 500, 1000
            range_desc = f"{base_min}–{base_max} mDrops (обычный бонус)."

        # Розыгрыш базового бонуса и умножение на стрик
        bonus_base = random.randint(base_min, base_max)
        streak = min(streak + 1, 7)
        bonus_total = bonus_base * streak

        # Ограничения итогового бонуса
        if 17 <= status <= 21:
            bonus_total = min(bonus_total, 3000)
        elif status == 22:
            bonus_total = min(bonus_total, 4500)
        # для остальных статусов оставляем без дополнительного ограничения

        # Начисляем и сохраняем
        data["coins"] = data.get("coins", 0) + bonus_total
        data["last_daily_bonus"] = now.isoformat()
        data["daily_streak"] = streak
        await save_data(user_id, data)

        # Ответ пользователю
        await message.answer(
            f"😎 {await gsname(message.from_user.first_name, message.from_user.id)}, ты получил ежедневный бонус — <b>{bonus_total} mDrops</b>!\n\n"
            f"Диапазон базового бонуса для твоего статуса: {range_desc}\n"
            f"✊ Базовое значение: {bonus_base} × {streak} (множитель стрика)\n"
            f"⏳ Приходи завтра за следующим! \n🔥 Твой стрик: {streak}/7",
            disable_web_page_preview=True
        )
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 101)


CHANNEL_ID = -1002773120685  # заміни на айді твого каналу

PRIZES = [
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 1000, "1k"),
    ("coins", 2500, "2.5k"),
    ("coins", 2500, "2.5k"),
    ("coins", 2500, "2.5k"),
    ("coins", 5000, "5k"),
    ("coins", 5000, "5k"),
    ("coins", 10000, "10k"),
    ("GGs", 1, "1gg"),
    ("GGs", 1, "1gg"),
    ("GGs", 1, "1gg"),
    ("GGs", 2, "2gg"),
    ("GGs", 2, "2gg"),
    ("GGs", 3, "3gg"),
    ("GGs", 25, "25gg"),
]


def choose_prize():
    return random.choice(PRIZES)


@dp.message(F.text.lower().in_(["/baraban", "барабан", "/baraban@gmegadbot"]))
async def baraban_handler(message: types.Message):
    user_id = str(message.from_user.id)
    name = message.from_user.first_name

    if not await load_data(user_id):
        await create_user_data(user_id)
    data = await load_data(user_id)

    now = time.time()

    # Получаем статус пользователя (безопасно приводим к int)
    try:
        status = int(data.get("status", 0))
    except Exception:
        status = 0

    # Параметры кулдауна
    base_cd_seconds = 12 * 3600  # базовый КД — 12 часов
    reduction_per_status = 5 * 60  # уменьшение КД на каждый статус — 5 минут
    min_cd_seconds = 40 * 60  # минимальный КД — 40 минут

    # Статус 22 — особый: КД 30 минут и 2 спина
    if status == 22:
        final_cd_seconds = 30 * 60
        spins = 2
    else:
        reduction = status * reduction_per_status
        final_cd_seconds = max(min_cd_seconds, base_cd_seconds - reduction)
        spins = 1

    # Проверка наличия активного кулдауна
    if "baraban_cooldown" in data and now < data["baraban_cooldown"]:
        remaining = int(data["baraban_cooldown"] - now)
        hrs = remaining // 3600
        mins = (remaining % 3600) // 60
        return await message.reply(
            f"⏳ {name}, следующий барабан можно крутить через {hrs}ч {mins}м."
        )

    # проверка подписки на канал
    try:
        member = await bot.get_chat_member(CHANNEL_ID, int(user_id))
        if member.status in ("left", "kicked"):
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚜️ Перейти в канал", url="https://t.me/saycursed")]
            ])
            return await message.reply(
                f'❌ Подпишись на {html.link("канал", "https://t.me/saycursed")}, чтобы получить приз 🎁',
                disable_web_page_preview=True,
                reply_markup=kb
            )
    except Exception as e:
        return await message.reply(f"⚠️ Не удалось проверить подписку: {e}")

    # Первый выбор приза (будем использовать его файл для видео/превью)
    ptype, amount, filename = choose_prize()

    # Отправляем "загрузку" и видео барабана (указываем, если 2 спина)
    msg = await message.reply("📂 Загрузка видео, ожидайте...")
    try:
        prize_video = FSInputFile(f"baraban_videos/{filename}.mp4")
        initial_caption = "🎰 Крутим барабан..."
        if spins == 2:
            initial_caption += " (2 спина за статус \"♠️ Масть\"!)"

        sent = await message.reply_video(prize_video, caption=initial_caption)
        await msg.delete()
    except Exception as e:
        await msg.delete()
        await handle_error(message.from_user.username, e, message.from_user.id, 122)
        return

    # Выполняем спины и собираем призы
    prizes = []  # список (тип, сумма)
    # добавляем первый приз (мы уже выбрали его выше)
    prizes.append((ptype, amount))

    # если 2 спина — выбираем второй
    if spins == 2:
        p2, a2, f2 = choose_prize()
        prizes.append((p2, a2))

    total_coins = 0
    total_ggs = 0
    for p, a in prizes:
        if p == "coins":
            total_coins += a
        else:
            total_ggs += a

    # Начисляем призы
    if total_coins:
        data["coins"] = data.get("coins", 0) + total_coins
    if total_ggs:
        data["GGs"] = data.get("GGs", 0) + total_ggs

    # Обновляем и сохраняем КД (секунды от now)
    data["baraban_cooldown"] = now + final_cd_seconds
    await save_data(user_id, data)

    # Ждём немного и показываем результат
    await asyncio.sleep(5)

    # Формируем текст итогов
    prize_lines = []
    for idx, (p, a) in enumerate(prizes, start=1):
        if p == "coins":
            prize_lines.append(f"Спин #{idx}: {format_balance(a)} mDrops")
        else:
            prize_lines.append(f"Спин #{idx}: {a} GG")
    prize_text = "\n".join(prize_lines)

    summary_parts = []
    if total_coins:
        summary_parts.append(f"{format_balance(total_coins)} mDrops")
    if total_ggs:
        summary_parts.append(f"{total_ggs} GG")
    summary = " и ".join(summary_parts) if summary_parts else "ничего"

    # Редактируем подпись видео с итогами и информацией о КД/спинах
    try:
        await bot.edit_message_caption(
            chat_id=message.chat.id,
            message_id=sent.message_id,
            caption=(
                    f"🎉 {await gsname(name, message.from_user.id)}, твой приз:\n{prize_text}\n\n"
                    f"Итого: {summary}\n\n"
                    f"КД барабана для твоего статуса ({get_status(status)}): {int(final_cd_seconds // 3600)}ч {int((final_cd_seconds % 3600) // 60)}м."
                    + (f"\n(Статус \"♠️ Масть\" даёт 2 спина.)" if status == 22 else "")
            )
        )
    except Exception:
        # если не удалось отредактировать, отправим как новый текст
        await message.reply(
            f"🎉 {await gsname(name, message.from_user.id)}, твой приз:\n{prize_text}\n\n"
            f"Итого: {summary}\n\n"
            f"КД барабана для твоего статуса ({get_status(status)}): {int(final_cd_seconds // 3600)}ч {int((final_cd_seconds % 3600) // 60)}м."
            + (f"\n(Статус \"♠️ Масть\" даёт 2 спина.)" if status == 22 else "")
        )


class DonateState(StatesGroup):
    waiting_for_amount = State()


@dp.message(F.text.lower().in_(["/donate", "/donation", "донат"]))
async def donate_menu(message: Message):
    if message.chat.type != "private":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🍓 Перейти в ЛС", url="t.me/gmegadbot")]]
        )
        return await message.reply(
            f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, донат доступен только в {html.link('ЛС с ботом', 't.me/gmegadbot')}!",
            reply_markup=kb, disable_web_page_preview=True
        )

    text = (
        "🛍 <b>Донат-меню</b> 🛒\n"
        "---------------------\n"
        "🏧 <b>Курс обмена:</b>\n"
        "⭐️ = 1 GGs \n"
        "1 GGs ≈ 3500 mDrops\n\n"
        "💎 <b>Оптовая покупка:</b>\n"
        "500 ⭐️ = 1000 GGs\n"
        "1000 ⭐️ = 3000 GGs\n"
        "5000 ⭐️ = 20'000 GGs\n"
        "---------------------\n"
        "⚡️ Спасибо что поддерживаете наш проект!\n"
        "Каждая вложенная вами копейка улучшает бота. 💫"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Купить GGs", callback_data="donate_buy")]
        # [InlineKeyboardButton(text="🔥 Топ Донатеров", callback_data="donate_top")]
    ])
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data == "donate_buy")
async def donate_buy_menu(callback: CallbackQuery):
    name = callback.from_user.first_name
    text = (
        f"💎 <b>{name}</b>, сколько GGs ты хочешь купить?\n"
        "·····················\n"
        "📊 1 ⭐️ = 1 GGs\n"
        "ℹ️ Напишите сумму в GGs, которую вы хотите задонатить,\n"
        "или выберите один из вариантов ниже 👇"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="50 GGs", callback_data="buy:50"),
            InlineKeyboardButton(text="100 GGs", callback_data="buy:100"),
            InlineKeyboardButton(text="250 GGs", callback_data="buy:250")
        ],
        [
            InlineKeyboardButton(text="🍓 -50% 1000 GGs", callback_data="buy:1000:500"),
            InlineKeyboardButton(text="🔥 -67% 3000 GGs", callback_data="buy:3000:1000")
        ],
        [
            InlineKeyboardButton(text="🌶 -75% 20000 GGs", callback_data="buy:20000:5000")
        ],
        [
            InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="donate_custom")
        ]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@dp.callback_query(F.data == "donate_custom")
async def donate_custom(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✍️ Введите количество GGs, которое хотите купить:\n\n📊 1 ⭐️ = 1 GGs")
    await state.set_state(DonateState.waiting_for_amount)


@dp.message(DonateState.waiting_for_amount)
async def process_custom_amount(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    try:
        ggs_amount = parse_bet_input(text) if not text.isdigit() else int(text)
    except Exception:
        ggs_amount = -1
    if ggs_amount <= 0:
        return await message.reply("❌ Введите корректное положительное число (например: 150 или 1к).")

    stars_price = ggs_amount
    prices = [LabeledPrice(label=f"{ggs_amount} GGs", amount=int(stars_price))]

    await bot.send_invoice(
        chat_id=message.from_user.id,
        title="Покупка GGs",
        description=f"{ggs_amount} GGs за {stars_price} ⭐️",
        payload=f"donate_{ggs_amount}_{stars_price}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="donate"
    )
    await state.clear()


@dp.callback_query(F.data.startswith("buy:"))
async def donate_process(callback: CallbackQuery):
    parts = callback.data.split(":")
    try:
        ggs_amount = int(parts[1])
    except Exception:
        return await callback.answer("❌ Неверный выбор.", show_alert=True)
    stars_price = int(parts[2]) if len(parts) > 2 else ggs_amount
    prices = [LabeledPrice(label=f"{ggs_amount} GGs", amount=int(stars_price))]

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="Покупка GGs",
        description=f"{ggs_amount} GGs за {stars_price} ⭐️",
        payload=f"donate_{ggs_amount}_{stars_price}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="donate"
    )


@dp.pre_checkout_query(lambda query: True)
async def checkout(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


async def check_and_set_top1_status(message, user_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # знайти топ-1 по total_donated
    cursor.execute("SELECT id FROM users ORDER BY total_donated DESC LIMIT 1")
    row = cursor.fetchone()

    if row and str(message.from_user.id) == str(row[0]):
        # оновити статус у БД
        cursor.execute("UPDATE users SET status = 10 WHERE id = ?", (user_id,))
        conn.commit()
        await message.answer("👑 Ты стал Топ-1 донатером! Тебе выдан статус #10.")

    conn.close()


async def get_top_donators(limit=10):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, total_donated
        FROM users
        ORDER BY total_donated DESC
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()

    text = "🔥 Топ донатеров:\n\n"
    for i, (uid, donated) in enumerate(rows, 1):
        text += f"{i}. <a href='tg://user?id={uid}'>Пользователь</a> — {donated:.2f}\n"
    return text


# ===== успішна оплата =====
@dp.message(F.content_type == ContentType.SUCCESSFUL_PAYMENT)
async def got_payment(message: Message):
    payment = message.successful_payment
    payload = payment.invoice_payload.split("_")
    ggs_amount = int(payload[1])
    user_id = str(message.from_user.id)

    data = await load_data(user_id)
    if user_id not in data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    # нараховуємо GGs
    data["GGs"] += ggs_amount
    # додаємо в статистику донатів
    data["total_donated"] = data.get("total_donated", 0) + ggs_amount
    await save_data(user_id, data)

    await message.answer(f"✅ Спасибо огромное за покупку GGs!\n\nТебе начислено {ggs_amount} GGs на баланс 🎉")

    # перевіряємо топ і видаємо топ1 донатор, треба доробити


@dp.message(F.text.in_(["/referrals", "реф", "ref", "/referrals@gmegadbot"]))
async def referrals_info(message: Message):
    user_id = str(message.from_user.id)
    data = await load_data(str(message.from_user.id))

    if not data:
        await create_user_data(user_id)
        data = await load_data(str(message.from_user.id))

    count = data.get("referrals", 0)
    link = f"https://t.me/gmegadbot?start={user_id}"
    await message.reply(
        f'✌️ {await gsname(message.from_user.first_name, message.from_user.id)}, твои реффералы: {count} человек.\n⛓ Твоя ссылка: {html.code(link)}',
        disable_web_page_preview=True)


@dp.message(F.text.lower().startswith("промо"))
async def handle_promo(message: Message):
    try:
        parts = message.text.strip().split()

        if len(parts) != 2:
            return await message.reply("❌ Использование: промо <название>")

        if not await load_data(str(message.from_user.id)):
            await create_user_data(message.from_user.id)

        name = parts[1]
        user_id = str(message.from_user.id)

        import sqlite3, json
        conn = sqlite3.connect("data.db")
        cursor = conn.cursor()

        cursor.execute("SELECT reward, claimed FROM promos WHERE name = ?", (name,))
        row = cursor.fetchone()

        if not row:
            conn.close()
            return await message.reply(
                f"⚠️ {await gsname(message.from_user.first_name, message.from_user.id)}, такого промокода не существует.")

        reward, claimed_json = row
        claimed = json.loads(claimed_json)

        if user_id in claimed:
            conn.close()
            return await message.reply(
                f"⚠️ {await gsname(message.from_user.first_name, message.from_user.id)}, вы уже активировали этот промокод!")

        # додаємо користувача в список
        claimed.append(user_id)
        cursor.execute("UPDATE promos SET claimed = ? WHERE name = ?", (json.dumps(claimed), name))

        conn.commit()
        conn.close()

        user_data = await load_data(user_id)
        user_data["coins"] += reward
        await save_data(user_id, user_data)

        await message.reply(f"✅ Вы активировали промокод <b>{name}</b>!\n🎁 Получили: {format_balance(reward)} mDrops")
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 106)


class CreateCheckStates(StatesGroup):
    awaiting_amount = State()  # ждём ввод суммы на одного (если ручной ввод)
    awaiting_count = State()  # ждём ввод количества активаций (если ручной ввод)


# ---- вспомогалки ----
PRESET_AMOUNTS = [100, 1_000, 10_000, 100_000]
PRESET_COUNTS = [1, 5, 10, 100]
MAX_TOTAL = 1_000_000
MAX_PER_USER = 100_000
UNLOCK_PRICE = 500000
ADMIN_ID = "8493326566"  # твой id, у него нет ограничений


def mk_inline_keyboard(rows: List[List[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(F.text.lower().in_(["/check", "чеки", "чек"]))
async def cmd_check_panel(message: Message):
    """
    Обработка команды /check — показывает панель чеков (создать / мои чеки / купить доступ).
    """
    # доступно только в личных сообщениях
    if message.chat.type != "private":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍓 Перейти в ЛС", url=f"https://t.me/{(await bot.get_me()).username}")]
        ])
        return await message.reply(
            f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, чеки доступны только в личных сообщениях с ботом.",
            reply_markup=kb, disable_web_page_preview=True
        )

    user_id = str(message.from_user.id)
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    has_unlock = bool(data.get("check_unlocked", False))

    kb_rows = []
    kb_rows.append([InlineKeyboardButton(text="✍️ Создать чек", callback_data="check_panel:create")])
    kb_rows.append([InlineKeyboardButton(text="📂 Мои чеки", callback_data="check_panel:my")])
    if not has_unlock:
        kb_rows.append([InlineKeyboardButton(text=f"🔒 Купить доступ ({format_balance(UNLOCK_PRICE)} mDrops)",
                                             callback_data="buy_check_unlock")])
    kb_rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="check_panel:close")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await message.answer(
        f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, панель чеков:\n\n"
        "➡️ Здесь вы можете создавать чеки, просматривать свои активные чеки и купить доступ (единовременно).",
        reply_markup=kb
    )


# ---- старт создания: открывает меню выбора суммы ----
@dp.callback_query(F.data == "check_panel:create")
async def check_panel_create_start(query: CallbackQuery, state: FSMContext):
    user_id = str(query.from_user.id)
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if not bool(data.get("check_unlocked", False)):
        kb = mk_inline_keyboard([[InlineKeyboardButton(text=f"Купить доступ ({format_balance(UNLOCK_PRICE)} mDrops)",
                                                       callback_data="buy_check_unlock")]])
        try:
            await query.message.edit_text(
                f"🔒 Чтобы создавать чеки, нужно купить доступ — {format_balance(UNLOCK_PRICE)} mDrops.",
                reply_markup=kb
            )
        except:
            pass
        return await query.answer()

    balance = int(data.get("coins", 0))

    # Кнопки сумм — показываем только те, на которые хватит баланса (хотя это сумма на одного; всё равно показываем,
    # потому что пользователь может потом выбрать количество 1)
    kb_rows = []
    for amt in PRESET_AMOUNTS:
        if balance >= amt:
            kb_rows.append([InlineKeyboardButton(text=f"{format_balance(amt)}", callback_data=f"check_amount:{amt}")])

    # всегда добавляем ручной ввод и отмену
    kb_rows.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="check_amount:custom")])
    kb_rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="check_cancel")])

    kb = mk_inline_keyboard(kb_rows)
    try:
        await query.message.edit_text(
            f"✍️ <b>Создание чека — шаг 1/2</b>\n\nВыберите сумму на одного (можно кнопкой или ввести вручную):",
            parse_mode="HTML",
            reply_markup=kb
        )
    except:
        await query.answer()

    # очищаем стейт на всякий случай
    await state.clear()


# ---- callback: выбранная сумма (preset или custom) ----
@dp.callback_query(F.data.startswith("check_amount:"))
async def check_amount_callback(query: CallbackQuery, state: FSMContext):
    _, payload = query.data.split(":", 1)
    user_id = str(query.from_user.id)
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)
    balance = int(data.get("coins", 0))

    if payload == "custom":
        await state.set_state(CreateCheckStates.awaiting_amount)
        try:
            await query.message.edit_text("✏️ Введите сумму на одного (например: 1000 или 2.5к).")
        except:
            pass
        return await query.answer()

    # preset amount chosen
    try:
        per_user = int(payload)
    except:
        return await query.answer("Неверная сумма.", show_alert=True)

    # проверки per_user
    if str(user_id) != ADMIN_ID and per_user > MAX_PER_USER:
        return await query.answer(f"Максимум на одного — {format_balance(MAX_PER_USER)} mDrops.", show_alert=True)
    if per_user <= 0:
        return await query.answer("Неверная сумма.", show_alert=True)

    # переходим к выбору количества
    await state.update_data(per_user=int(per_user))
    await show_counts_menu(query, state, per_user, balance)
    return await query.answer()


# ---- helper: показывает меню выбора количества (кнопки + ручной ввод) ----
async def show_counts_menu(query: CallbackQuery, state: FSMContext, per_user: int, balance: int):
    # максимум по балансу и лимиту
    max_by_balance = balance // per_user if per_user > 0 else 0
    max_by_limit = MAX_TOTAL // per_user if per_user > 0 else 0
    max_count = min(max_by_balance, max_by_limit)
    if max_count <= 0:
        try:
            await query.message.edit_text("❗ Недостаточно средств для любой активации с выбранной суммой.")
        except:
            pass
        await state.clear()
        return

    # собираем кнопки: берём PRESET_COUNTS, фильтруем <= max_count, и добавляем кнопку с макс (если он не в списке)
    kb_rows = []
    added = set()
    for c in PRESET_COUNTS:
        if c <= max_count:
            kb_rows.append([InlineKeyboardButton(text=str(c), callback_data=f"check_count:{c}")])
            added.add(c)

    # если max_count небольшой и не равен preset, добавим кнопку с max_count
    if max_count not in added:
        kb_rows.append([InlineKeyboardButton(text=str(max_count), callback_data=f"check_count:{max_count}")])

    kb_rows.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="check_count:custom")])
    kb_rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="check_cancel")])
    kb = mk_inline_keyboard(kb_rows)

    try:
        await query.message.edit_text(
            f"✍️ <b>Создание чека — шаг 2/2</b>\n\n"
            f"На одного: <b>{format_balance(per_user)}</b> mDrops\n"
            f"Выберите количество активаций (быстрые кнопки):",
            parse_mode="HTML",
            reply_markup=kb
        )
    except:
        pass


# ---- callback: выбранное количество (preset или custom) ----
@dp.callback_query(F.data.startswith("check_count:"))
async def check_count_callback(query: CallbackQuery, state: FSMContext):
    _, payload = query.data.split(":", 1)
    user_id = str(query.from_user.id)
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)
    balance = int(data.get("coins", 0))

    if payload == "custom":
        await state.set_state(CreateCheckStates.awaiting_count)
        try:
            await query.message.edit_text("✏️ Введите количество активаций (целое число).")
        except:
            pass
        return await query.answer()

    try:
        count = int(payload)
    except:
        return await query.answer("Неверное количество.", show_alert=True)

    # берём per_user из state
    st = await state.get_data()
    per_user = st.get("per_user")
    if per_user is None:
        # На случай, если стейт потерян — просим снова выбрать сумму
        await state.clear()
        return await query.answer("Ошибка: сначала выберите сумму.", show_alert=True)

    # валидация: total <= MAX_TOTAL и <= balance
    total = count * per_user
    if total > MAX_TOTAL and user_id != ADMIN_ID:
        return await query.answer(f"Сумма чека не может превышать {format_balance(MAX_TOTAL)} mDrops.", show_alert=True)
    if total > balance:
        return await query.answer("Недостаточно средств для выбранного количества.", show_alert=True)

    # всё ок — показываем подтверждение (используем confirm_check callback)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Подтвердить создание", callback_data=f"confirm_check:{count}:{per_user}")
    ], [
        InlineKeyboardButton(text="❌ Отмена", callback_data="check_cancel")
    ]])
    try:
        await query.message.edit_text(
            f"Подтверждение создания чека:\n\n"
            f"⛳ Количество: <b>{count}</b>\n"
            f"💸 На одного: <b>{format_balance(per_user)}</b>\n"
            f"🔢 Итого: <b>{format_balance(total)}</b>\n\n"
            "Нажмите «Подтвердить создание», чтобы списать средства и создать чек.",
            parse_mode="HTML",
            reply_markup=kb
        )
    except:
        pass

    # очищаем стейт
    await state.clear()
    return await query.answer()


# ---- message handler: ручной ввод суммы (state awaiting_amount) ----
@dp.message(CreateCheckStates.awaiting_amount)
async def manual_amount_input(msg: Message, state: FSMContext):
    user_id = str(msg.from_user.id)
    text = (msg.text or "").strip()
    try:
        if "к" in text.lower():
            per_user = parse_bet_input(text)
        else:
            per_user = int(text)
    except Exception:
        await msg.reply("Неверный формат суммы. Пример: 1000 или 2.5к")
        return

    if per_user <= 0:
        await msg.reply("Сумма должна быть положительной.")
        return

    if user_id != ADMIN_ID and per_user > MAX_PER_USER:
        await msg.reply(f"Максимум на одного — {format_balance(MAX_PER_USER)} mDrops.")
        await state.clear()
        return

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)
    balance = int(data.get("coins", 0))

    if per_user > balance:
        await msg.reply("У вас недостаточно средств даже для одной активации с этой суммой.")
        await state.clear()
        return

    # сохраняем per_user и переходим к выбору количества: показываем counts меню
    await state.update_data(per_user=int(per_user))
    # используем вспомогательную функцию - но query у нас нет, поэтому отправим новое сообщение
    # строим клавиатуру counts аналогично show_counts_menu
    max_by_balance = balance // per_user
    max_by_limit = MAX_TOTAL // per_user
    max_count = min(max_by_balance, max_by_limit)
    if max_count <= 0:
        await msg.reply("Недостаточно средств для любой активации с выбранной суммой.")
        await state.clear()
        return

    kb_rows = []
    added = set()
    for c in PRESET_COUNTS:
        if c <= max_count:
            kb_rows.append([InlineKeyboardButton(text=str(c), callback_data=f"check_count:{c}")])
            added.add(c)
    if max_count not in added:
        kb_rows.append([InlineKeyboardButton(text=str(max_count), callback_data=f"check_count:{max_count}")])
    kb_rows.append([InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="check_count:custom")])
    kb_rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="check_cancel")])
    kb = mk_inline_keyboard(kb_rows)

    await msg.reply(
        f"Вы выбрали сумму: <b>{format_balance(per_user)}</b> mDrops.\n\n"
        f"Выберите количество активаций:",
        parse_mode="HTML",
        reply_markup=kb
    )
    await state.clear()  # считывание завершено; остальные шаги работают через callback'и


# ---- message handler: ручной ввод количества (state awaiting_count) ----
@dp.message(CreateCheckStates.awaiting_count)
async def manual_count_input(msg: Message, state: FSMContext):
    user_id = str(msg.from_user.id)
    text = (msg.text or "").strip()
    if not text.isdigit():
        await msg.reply("Неверный формат количества. Введите целое число.")
        await state.clear()
        return
    count = int(text)
    if count <= 0:
        await msg.reply("Количество должно быть положительным.")
        await state.clear()
        return

    st = await state.get_data()
    per_user = st.get("per_user")
    if per_user is None:
        await msg.reply("Ошибка: сначала выберите сумму на одного.")
        await state.clear()
        return

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)
    balance = int(data.get("coins", 0))

    total = count * per_user
    if total > MAX_TOTAL and user_id != ADMIN_ID:
        await msg.reply(f"Сумма чека не может превышать {format_balance(MAX_TOTAL)} mDrops.")
        await state.clear()
        return
    if total > balance:
        await msg.reply("Недостаточно средств для выбранного количества.")
        await state.clear()
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Подтвердить создание", callback_data=f"confirm_check:{count}:{per_user}")
    ], [
        InlineKeyboardButton(text="❌ Отмена", callback_data="check_cancel")
    ]])
    await msg.reply(
        f"Подтверждение создания чека:\n\n"
        f"⛳ Количество: <b>{count}</b>\n"
        f"💸 На одного: <b>{format_balance(per_user)}</b>\n"
        f"🔢 Итого: <b>{format_balance(total)}</b>\n\n"
        "Нажмите «Подтвердить создание», чтобы списать средства и создать чек.",
        parse_mode="HTML",
        reply_markup=kb
    )
    await state.clear()


@dp.callback_query(F.data == "buy_check_unlock")
async def buy_check_unlock_callback(query: CallbackQuery):
    """
    Хендлер покупки доступа к чекам.
    Попытка 1: оплатить картой через try_pay_card (если функция доступна).
    Попытка 2: списать с основного баланса user['coins'].
    """
    await query.answer()  # убираем "часики" в UI

    # проверяем, что это ЛС — покупка доступна только в приватном чате
    try:
        if query.message and getattr(query.message, "chat", None) and query.message.chat.type != "private":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🍓 Перейти в ЛС", url=f"https://t.me/{(await bot.get_me()).username}")]
            ])
            return await query.message.edit_text("🔒 Покупка доступа доступна только в личных сообщениях с ботом.",
                                                 reply_markup=kb)
    except Exception:
        # если не можем определить тип чата, продолжаем, но будем работать аккуратно
        pass

    user_id = str(query.from_user.id)

    # загрузка данных пользователя, создание при отсутствии
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    # если уже есть доступ — сообщаем
    if bool(data.get("check_unlocked", False)):
        try:
            await query.message.edit_text("🔓 У вас уже есть доступ к созданию чеков.")
        except:
            await query.answer("У вас уже есть доступ к созданию чеков.", show_alert=True)
        return

    price = float(UNLOCK_PRICE)

    # 1) Попытка оплаты картой (если есть функция try_pay_card)
    paid_by_card = False
    card_used = None
    card_msg = None
    try:
        if "try_pay_card" in globals():
            # try_pay_card возвращает dict {"success": True/False, "comment": "...", "card": "..."}
            res = await try_pay_card(user_id, price, note="Покупка доступа к чекам")
            if isinstance(res, dict) and res.get("success"):
                paid_by_card = True
                card_used = res.get("card")
                card_msg = res.get("comment") or "Оплата картой успешна"
    except Exception:
        # если что-то пошло не так при оплате картой — продолжим попытку списания с баланса
        paid_by_card = False

    if paid_by_card:
        # Успешная оплата картой — даём доступ и сохраняем
        data["check_unlocked"] = True
        await save_data(user_id, data)

        # Маскируем номер карты для вывода (если есть)
        masked = None
        if card_used:
            try:
                masked = f"•••• •••• •••• {str(card_used)[-4:]}"
            except Exception:
                masked = str(card_used)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Создать чек", callback_data="check_panel:create")],
            [InlineKeyboardButton(text="📂 Мои чеки", callback_data="check_panel:my")]
        ])
        msg_lines = [f"✅ Доступ к созданию чеков куплен за {format_balance(price)} mDrops (оплата картой)."]
        if masked:
            msg_lines.append(f"Карта: <code>{masked}</code>")
        if card_msg:
            msg_lines.append(f"Комментарий: {card_msg}")
        try:
            await query.message.edit_text("\n".join(msg_lines), parse_mode="HTML", reply_markup=kb)
        except:
            await query.answer("Доступ куплен (оплата картой).", show_alert=True)
        return

    # 2) Попытка списания с основного баланса
    balance = float(data.get("coins", 0))
    if balance >= price:
        data["coins"] = balance - price
        data["check_unlocked"] = True
        await save_data(user_id, data)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Создать чек", callback_data="check_panel:create")],
            [InlineKeyboardButton(text="📂 Мои чеки", callback_data="check_panel:my")]
        ])
        try:
            await query.message.edit_text(
                f"✅ Доступ к созданию чеков куплен за {format_balance(price)} mDrops (списано с основного баланса).",
                parse_mode="HTML",
                reply_markup=kb
            )
        except:
            await query.answer("Доступ куплен. Сумма списана с основного баланса.", show_alert=True)
        return

    # 3) Недостаточно средств: показываем сообщение и кнопки для пополнения/отмены
    need = price - balance
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пополнить баланс", callback_data="bank")],
        [InlineKeyboardButton(text="Отмена", callback_data="check_panel:close")]
    ])
    try:
        await query.message.edit_text(
            f"❗ Недостаточно средств для покупки доступа.\n"
            f"Нужно: {format_balance(price)} mDrops\n"
            f"У вас: {format_balance(balance)} mDrops\n"
            f"Не хватает: {format_balance(need)} mDrops\n\n"
            "Вы можете пополнить баланс или оплатить с карты (если у вас есть карта).",
            parse_mode="HTML",
            reply_markup=kb
        )
    except:
        await query.answer("Недостаточно средств для покупки доступа.", show_alert=True)


# ---- cancel callback (очистка состояний и возврат сообщения) ----
@dp.callback_query(F.data == "check_cancel")
async def check_cancel_callback(query: CallbackQuery, state: FSMContext):
    try:
        await state.clear()
    except:
        pass
    try:
        await query.message.edit_text("❌ Действие отменено.")
    except:
        pass
    return await query.answer()


@dp.callback_query(F.data.startswith("confirm_check:"))
async def confirm_check_callback_panel(query: CallbackQuery):
    """Робочий confirm_check handler з логуванням та захистом від помилок."""
    try:
        # Швидко підтверджуємо, щоб убрати "часики" в UI
        await query.answer()

        # Лог для дебагу

        # Розбираємо дані
        parts = query.data.split(":")
        if len(parts) != 3:
            await query.message.edit_text("❗ Неверные данные для подтверждения создания чека.")
            return

        _, count_s, per_user_s = parts
        try:
            count = int(count_s)
            per_user = int(per_user_s)
        except ValueError:
            await query.message.edit_text("❗ Неверный формат данных (число/сумма).")
            return

        user_id = str(query.from_user.id)
        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)
            data = await load_data(user_id)

        # Всі перевірки перед списанням
        total_cost = count * per_user
        needs_unlock = not bool(data.get("check_unlocked", False))
        total_with_unlock = total_cost + (UNLOCK_PRICE if needs_unlock else 0)

        if int(data.get("coins", 0)) < total_with_unlock:
            await query.message.edit_text(
                f"❗ Недостаточно средств. Нужна сумма: {format_balance(total_with_unlock)} mDrops.")
            return

        # Списываем деньги
        data["coins"] = int(data.get("coins", 0)) - int(total_with_unlock)
        if needs_unlock:
            data["check_unlocked"] = True
        await save_data(user_id, data)

        # Генерируем код
        code = ''.join(random.choices("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=8))
        check_data = {
            "creator_id": user_id,
            "per_user": per_user,
            "remaining": count,
            "claimed": []
        }

        # Поддержка sync/async save_check
        try:
            if asyncio.iscoroutinefunction(save_check):
                await save_check(code, check_data)
            else:
                save_check(code, check_data)
        except Exception as e:
            # Откат в случае ошибки сохранения
            # Вернуть деньги пользователю
            data["coins"] = int(data.get("coins", 0)) + int(total_with_unlock)
            await save_data(user_id, data)
            await query.message.edit_text("⚠ Ошибка при сохранении чека. Ставка возвращена.")
            return

        # Логи для админов
        try:
            creator_chat = await bot.get_chat(int(user_id))
            username = getattr(creator_chat, "username", None)
            await send_log(
                f"Игрок @{username if username else '-'} ({user_id}) создал чек.\n"
                f"На одного: {per_user} mDrops\nАктиваций: {count}\nВсего списано: {total_cost} mDrops"
                + (f"\nПлата за доступ: {UNLOCK_PRICE} mDrops" if needs_unlock else "")
                + f"\nКод: {html.spoiler(code)}"
            )
        except Exception:
            pass

        # Отправляем пользователю результат (ссылкой на /start=check_code)
        bot_username = (await bot.get_me()).username
        link = f"https://t.me/{bot_username}?start=check_{code}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Поделиться", url=f"https://t.me/share/url?url={link}")],
            [InlineKeyboardButton(text="📂 Мои чеки", callback_data="check_panel:my")]
        ])

        try:
            await query.message.edit_text(
                f"🎉 Чек создан!\n\n"
                f"Код: <b>{code}</b>\n"
                f"Ссылка: {link}\n"
                f"Осталось активаций: {count}",
                parse_mode="HTML",
                reply_markup=kb
            )
        except Exception:
            # Если редактирование не удалось — просто ответим alert'ом
            await query.answer("Чек создан.", show_alert=True)

    except Exception as exc:
        pass
        # Если что-то пошло не так — стараемся уведомить пользователя
        try:
            await query.answer("Ошибка при создании чека.", show_alert=True)
        except:
            pass


def _sync_load_all_checks() -> Dict[str, Dict[str, Any]]:
    """
    Синхронно загрузить все чеки из таблицы checks.
    Возвращает dict code -> check_data (claimed уже как list).
    """
    out: Dict[str, Dict[str, Any]] = {}
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Убедимся, что таблица существует (как у вас в load_check)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checks (
                code TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                per_user REAL NOT NULL,
                remaining INTEGER NOT NULL,
                claimed TEXT DEFAULT '[]'
            )
        """)
        cursor.execute("SELECT * FROM checks")
        rows = cursor.fetchall()
        for row in rows:
            d = dict(row)
            # превратить claimed JSON в список Python
            try:
                d["claimed"] = json.loads(d.get("claimed", "[]"))
            except Exception:
                d["claimed"] = []
            # нормализуем типы/поля при необходимости
            out[str(d.get("code"))] = d
    return out


def _sync_list_checks_by_creator(user_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Синхронно получить все чеки, у которых creator_id == user_id.
    Возвращает dict code->check_data.
    """
    res: Dict[str, Dict[str, Any]] = {}
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checks (
                code TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                per_user REAL NOT NULL,
                remaining INTEGER NOT NULL,
                claimed TEXT DEFAULT '[]'
            )
        """)
        cursor.execute("SELECT * FROM checks WHERE creator_id = ?", (str(user_id),))
        rows = cursor.fetchall()
        for row in rows:
            d = dict(row)
            try:
                d["claimed"] = json.loads(d.get("claimed", "[]"))
            except Exception:
                d["claimed"] = []
            res[str(d.get("code"))] = d
    return res


# ----------------- Асинхронные обёртки для использования в хендлерах -----------------

async def load_all_checks() -> Dict[str, Dict[str, Any]]:
    """
    Асинхронная обёртка над _sync_load_all_checks.
    Возвращает dict code->check_data.
    """
    return await asyncio.to_thread(_sync_load_all_checks)


async def list_checks_by_creator(user_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Асинхронная обёртка над _sync_list_checks_by_creator.
    Возвращает dict code->check_data только для данного user_id.
    """
    return await asyncio.to_thread(_sync_list_checks_by_creator, str(user_id))


# ----------------- Универсальная, робастная функция, совместимая с вашим кодом -----------------

async def list_checks_by_creator_or_empty(user_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Универсальный helper: пытается получить все чеки, принадлежащие user_id.
    Логика:
      1) Если в globals есть кастомная функция list_checks_by_creator (и она НЕ наша),
         вызовет её (совместимость).
      2) Иначе использует sqlite-реализацию (load_all_checks / list_checks_by_creator).
      3) В любом случае возвращает dict {code: check_data} или {}.
    """
    try:
        # 1) Если у вас где-то переопределена собственная list_checks_by_creator (в globals),
        #    используем её (только если она не эта самая функция).
        gl = globals()
        if "list_checks_by_creator" in gl and gl["list_checks_by_creator"] is not list_checks_by_creator:
            fn = gl["list_checks_by_creator"]
            try:
                res = fn(user_id)  # type: ignore
                if asyncio.iscoroutine(res):
                    res = await res
                # нормализуем результат
                if isinstance(res, dict):
                    return {str(k): v for k, v in res.items()}
                elif isinstance(res, (list, tuple)):
                    out = {}
                    for item in res:
                        if isinstance(item, dict) and item.get("code"):
                            out[str(item["code"])] = item
                        elif isinstance(item, tuple) and len(item) >= 2:
                            out[str(item[0])] = item[1]
                    return out
            except Exception:
                # если внешняя функция сломалась — проигнорируем и продолжим локальный fallback
                pass

        # 2) Попробуем нашу sqlite-функцию (быстрая и надёжная)
        res = await list_checks_by_creator(user_id)
        if isinstance(res, dict):
            return res

        # 3) Если вдруг вышло не то — загрузим всё и отфильтруем (доп. защита)
        all_checks = await load_all_checks()
        out = {}
        for code, ch in (all_checks or {}).items():
            if not isinstance(ch, dict):
                continue
            creator = ch.get("creator_id") or ch.get("creator") or ch.get("owner")
            if str(creator) == str(user_id):
                entry = dict(ch)
                if not entry.get("code"):
                    entry["code"] = code
                out[str(code)] = entry
        return out

    except Exception:
        # молча возвращаем пустое множество — вызывающий код корректно обработает отсутствие чеков
        return {}


def _sync_delete_check(code: str) -> bool:
    """Синхронно удаляет чек из таблицы checks. Возвращает True если удалено."""
    code = str(code)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checks (
                code TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                per_user REAL NOT NULL,
                remaining INTEGER NOT NULL,
                claimed TEXT DEFAULT '[]'
            )
        """)
        cursor.execute("SELECT 1 FROM checks WHERE code = ?", (code,))
        if cursor.fetchone() is None:
            return False
        cursor.execute("DELETE FROM checks WHERE code = ?", (code,))
        conn.commit()
        return True


# Асинхронные обёртки
async def async_load_check(code: str) -> dict:
    """Асинхронно вызывает sync load_check через to_thread."""
    return await asyncio.to_thread(load_check, code)


async def async_save_check(code: str, data: dict) -> None:
    """Асинхронно вызывает sync save_check через to_thread."""
    return await asyncio.to_thread(save_check, code, data)


async def async_delete_check(code: str) -> bool:
    """Асинхронно удаляет чек через to_thread."""
    return await asyncio.to_thread(_sync_delete_check, code)


# ----------------- view_check handler: показать детальную информацию -----------------
@dp.callback_query(F.data.startswith("view_check:"))
async def view_check_callback(query: CallbackQuery):
    await query.answer()  # убрать "часики"
    parts = query.data.split(":", 1)
    if len(parts) != 2:
        try:
            await query.message.edit_text("❗ Неверные данные для просмотра чека.")
        except:
            pass
        return

    code = parts[1]
    try:
        check = await async_load_check(code)
    except Exception:
        check = {}

    if not check:
        try:
            await query.message.edit_text("❗ Чек не найден или уже использован/удален.")
        except:
            await query.answer("Чек не найден.", show_alert=True)
        return

    # Нормализация полей
    creator_id = str(check.get("creator_id", "—"))
    per_user = check.get("per_user", check.get("amount", "—"))
    remaining = check.get("remaining", check.get("left", 0))
    created_at = check.get("created_at")  # если есть unix timestamp
    claimed = check.get("claimed", [])

    # Собираем строку с информацией
    lines = []
    lines.append(f"🔖 Чек: <b>{code}</b>")
    lines.append(f"👤 Создатель: <code>{creator_id}</code>")
    # Попробуем получить имя создателя (без падения при ошибке)
    try:
        chat = await bot.get_chat(int(creator_id))
        creator_name = getattr(chat, "first_name", None) or getattr(chat, "username", None) or str(creator_id)
        lines[-1] = f"👤 Создатель: <b>{creator_name}</b> (<code>{creator_id}</code>)"
    except Exception:
        # оставляем id если не удалось
        pass

    lines.append(f"💸 На одного: <b>{per_user}</b> mDrops")
    lines.append(f"🔢 Осталось активаций: <b>{remaining}</b>")

    if created_at:
        try:
            import datetime
            ts = int(created_at)
            dt = datetime.datetime.utcfromtimestamp(ts)
            # выводим в локальном формате (UTC) — при необходимости измените
            lines.append(f"🕒 Создан: {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        except Exception:
            pass

    # Разбираем список claimed — может быть список id или список dict
    activators_lines = []
    if claimed:
        # claimed может быть json-строкой, но наши load_check уже парсит json -> list.
        # Обрабатываем элементы: str/int (user_id) или dict с полем 'user_id'/'id'
        for idx, item in enumerate(claimed, start=1):
            user_id = None
            extra = ""
            if isinstance(item, (str, int)):
                user_id = str(item)
            elif isinstance(item, dict):
                user_id = str(item.get("user_id") or item.get("id") or item.get("uid") or item.get("from"))
                # есть ли отметка времени или сумма?
                if "at" in item:
                    extra += f" (at: {item['at']})"
                if "amount" in item:
                    extra += f" (amount: {item['amount']})"
            else:
                # неизвестный формат — просто показать repr
                activators_lines.append(f"{idx}. {repr(item)}")
                continue

            # Получаем имя активатора через bot.get_chat
            name_display = user_id
            try:
                ch = await bot.get_chat(int(user_id))
                name_display = getattr(ch, "first_name", None) or getattr(ch, "username", None) or str(user_id)
            except Exception:
                # если не получилось — оставим id
                pass

            activators_lines.append(f"{idx}. {name_display} (<code>{user_id}</code>){extra}")

    # клавиатура: кнопка удалить (удалять может только создатель или админ) и назад
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ Удалить чек", callback_data=f"delete_check:{code}")],
        [InlineKeyboardButton(text="◀️ Назад (Мои чеки)", callback_data="check_panel:my")]
    ])

    text = "\n".join(lines)
    if activators_lines:
        text += "\n\n👥 Активаторы:\n" + "\n".join(activators_lines)
    else:
        text += "\n\n👥 Активаторов пока нет."

    try:
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        # если не можем редактировать — отправим новое сообщение
        await query.message.answer(text, parse_mode="HTML", reply_markup=kb)


# ----------------- delete_check handler: удаление чека -----------------
@dp.callback_query(F.data.startswith("delete_check:"))
async def delete_check_callback(query: CallbackQuery):
    """
    Удаление чека.
    - Проверяет права (только создатель или ADMIN_ID).
    - Удаляет чек из БД.
    - Возвращает оставшиеся средства (per_user * remaining) создателю на основной баланс.
    - Логирует всё через append_log.
    """
    await query.answer()

    parts = query.data.split(":", 1)
    if len(parts) != 2:
        try:
            await query.message.edit_text("❗ Неверные данные для удаления чека.")
        except Exception:
            pass
        return

    code = parts[1]

    # Загружаем чек, чтобы проверить его создателя и посчитать сумму возврата
    try:
        check = await async_load_check(code)
    except Exception as exc:
        append_log({"event": "delete_check_load_error", "code": code, "error": repr(exc)}, add_timestamp=True)
        check = {}

    if not check:
        try:
            await query.message.edit_text("❗ Чек не найден или уже удалён.")
        except Exception:
            await query.answer("Чек не найден.", show_alert=True)
        return

    creator_id = str(check.get("creator_id", ""))
    user_id = str(query.from_user.id)

    # проверка прав: только создатель или админ может удалить
    if user_id != str(creator_id) and user_id != str(ADMIN_ID):
        try:
            await query.answer("У вас нет прав на удаление этого чека.", show_alert=True)
        except Exception:
            pass
        return

    # вычисляем сумму, которую нужно вернуть (остаток)
    try:
        per_user_raw = check.get("per_user", 0)
        remaining_raw = check.get("remaining", 0)
        # допускаем, что per_user может быть float или строкой
        per_user = float(per_user_raw)
        remaining = int(remaining_raw)
    except Exception:
        per_user = 0.0
        try:
            remaining = int(check.get("remaining", 0))
        except Exception:
            remaining = 0

    total_back = per_user * remaining
    # округлим до целого (или до 2 знаков), в зависимости от вашей логики — здесь округляем до целого
    try:
        total_back_amt = int(round(total_back))
    except Exception:
        total_back_amt = int(total_back) if total_back else 0

    # Удаляем чек из БД
    try:
        deleted = await async_delete_check(code)
    except Exception as exc:
        deleted = False
        append_log({"event": "delete_check_delete_error", "code": code, "creator_id": creator_id, "error": repr(exc)},
                   add_timestamp=True)

    if not deleted:
        try:
            await query.message.edit_text("❗ Не удалось удалить чек. Попробуйте позже.")
        except Exception:
            await query.answer("Ошибка при удалении.", show_alert=True)
        return

    # Если есть что вернуть — добавляем на баланс создателя
    refunded = False
    refund_note = ""
    if total_back_amt > 0:
        try:
            # загрузим данные создателя (создадим, если их нет)
            data = await load_data(creator_id)
            if not data:
                await create_user_data(creator_id)
                data = await load_data(creator_id)
            # увеличиваем основной баланс
            prev = int(data.get("coins", 0))
            data["coins"] = int(prev + total_back_amt)
            await save_data(creator_id, data)
            refunded = True
            refund_note = f"Возвращено {format_balance(total_back_amt)} mDrops создателю ({creator_id})."
            # логируем
            append_log({
                "event": "delete_check_refund",
                "code": code,
                "creator_id": creator_id,
                "refunded_amount": total_back_amt,
                "balance_before": prev,
                "balance_after": data["coins"],
                "operator_id": user_id
            }, add_timestamp=True)
        except Exception as exc:
            refunded = False
            append_log({
                "event": "delete_check_refund_error",
                "code": code,
                "creator_id": creator_id,
                "attempted_refund": total_back_amt,
                "error": repr(exc),
                "operator_id": user_id
            }, add_timestamp=True)

    # Сообщаем пользователю, который удалял чек
    try:
        if total_back_amt > 0 and refunded:
            await query.message.edit_text(f"✅ Чек успешно удалён.\n{refund_note}")
        elif total_back_amt > 0 and not refunded:
            await query.message.edit_text(
                "✅ Чек удалён, но не удалось вернуть средства создателю. Администратор уведомлён.")
        else:
            await query.message.edit_text("✅ Чек успешно удалён. Возврат не требуется (нет оставшихся активаций).")
    except Exception:
        # fallback: короткий alert
        if total_back_amt > 0 and refunded:
            await query.answer(f"Чек удалён. {format_balance(total_back_amt)} возвращено создателю.", show_alert=True)
        elif total_back_amt > 0 and not refunded:
            await query.answer("Чек удалён, но возврат не выполнен.", show_alert=True)
        else:
            await query.answer("Чек удалён.", show_alert=True)

    # дополнительно: можно уведомить создателя в ЛС о возврате (опционально)
    try:
        if total_back_amt > 0 and refunded:
            try:
                # попытаемся отправить личное сообщение создателю
                await bot.send_message(int(creator_id),
                                       f"🔔 Ваш чек ({code}) был удалён. На ваш баланс возвращено {format_balance(total_back_amt)} mDrops.")
            except Exception:
                # не критично, просто логируем неудачную отправку
                append_log({"event": "notify_creator_failed", "creator_id": creator_id, "code": code},
                           add_timestamp=True)
    except Exception:
        pass


@dp.callback_query(F.data == "check_panel:my")
async def check_panel_my(query: CallbackQuery):
    """
    Надійний хендлер 'Мої чеки' — намагається отримати список чеків і коректно відобразити їх.
    Застосовуйте цей варіант замість старого.
    """
    try:
        await query.answer()  # підтвердження, щоб прибрати "часики"

        user_id = str(query.from_user.id)
        checks = await list_checks_by_creator_or_empty(user_id)

        if not checks:
            # Якщо не знайшли — інформуємо
            try:
                await query.message.edit_text("📂 У вас немає активних чеків.")
            except Exception:
                # якщо не можемо відредагувати — відповідаємо алертом
                await query.answer("📂 У вас немає активних чеків.", show_alert=True)
            return

        # будуємо текст і клавіатуру
        lines = []
        kb_rows = []
        i = 0
        # гарантуємо deterministic order
        for code in sorted(checks.keys()):
            if i >= 20:  # захист від занадто довгих списків
                break
            ch = checks[code] or {}
            per = ch.get("per_user", ch.get("amount", "—"))
            rem = ch.get("remaining", ch.get("left", 0))
            lines.append(
                f"• <b>{code}</b> — {format_balance(int(per)) if isinstance(per, int) else per} mDrops, осталось: {rem}")
            kb_rows.append([
                InlineKeyboardButton(text=f"🔍 {code}", callback_data=f"view_check:{code}"),
                InlineKeyboardButton(text="🗑️ Отменить", callback_data=f"delete_check:{code}")
            ])
            i += 1

        kb_rows.append([InlineKeyboardButton(text="Закрыть", callback_data="check_panel:close")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

        text = "📂 Ваши чеки:\n\n" + "\n".join(lines)
        try:
            await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            # якщо не вдалося відредагувати — надсилаємо нове повідомлення
            await query.message.answer(text, parse_mode="HTML", reply_markup=kb)

    except Exception as e:
        pass
        try:
            await query.answer("Ошибка при получении чеков.", show_alert=True)
        except:
            pass


try:
    # Python 3.9+
    from zoneinfo import ZoneInfo

    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    # fallback — використовуємо UTC, якщо zoneinfo відсутній
    from datetime import timezone

    KYIV_TZ = timezone.utc

DAILY_TRANSFER_LIMIT = 1_000_000  # щоденний ліміт на передачі (грубо, до комісії)


@dp.message(F.text.lower().startswith("дать"))
async def transfer_command(message: Message):
    try:
        if not message.reply_to_message:
            return await message.reply(
                f"🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, чтобы передать mDrops — ответь сообщением пользователю, которому хочешь передать!")

        sender_id = str(message.from_user.id)
        recipient_id = str(message.reply_to_message.from_user.id)

        # Инициализация аккаунтов, если их нет
        if not await load_data(sender_id):
            await create_user_data(sender_id)
        if not await load_data(recipient_id):
            await create_user_data(recipient_id)

        if sender_id == recipient_id:
            return await message.reply(
                f"🫵 {await gsname(message.from_user.first_name, message.from_user.id)}, ты не можешь передать mDrops самому себе!")

        # Запрещённые получатели (как в твоём коде) — оставил как есть
        if int(recipient_id) in (8257726098, 8375492513):
            return await message.reply(
                f"😁 {await gsname(message.from_user.first_name, message.from_user.id)}, я не принимаю подарки!")

        # Загружаем данные отправителя
        data = await load_data(sender_id)
        if not data:
            return await message.reply("⚠️ Ошибка: данные отправителя не найдены.")

        # Текущий баланс
        user_balance = int(data.get("coins", 0))

        # Разбор суммы в сообщении
        text = message.text.strip().lower()
        parts = text.split()
        if len(parts) < 2:
            return await message.reply("Неверный формат. Пример: «дать 500» или «дать все» (в ответе).")

        amount_text = parts[1]

        # Получаем сегодняшнюю дату в Kyiv timezone в формате YYYY-MM-DD
        try:
            now = datetime.now(KYIV_TZ)
            today_str = now.strftime("%Y-%m-%d")
        except Exception:
            now = datetime.utcnow()
            today_str = now.strftime("%Y-%m-%d")

        # Инициализируем структуру суточных переводов, если нужно
        daily = data.get("daily_transfers")
        if not isinstance(daily, dict) or daily.get("date") != today_str:
            # Если дата отличается — сбрасываем счётчик
            data["daily_transfers"] = {"date": today_str, "sent": 0}
            daily_sent = 0
        else:
            daily_sent = int(daily.get("sent", 0))

        remaining_limit = DAILY_TRANSFER_LIMIT - daily_sent
        if remaining_limit < 0:
            remaining_limit = 0

        # Определяем желаемую сумму (gross — сумма до комиссии)
        if amount_text in ["все", "всё"]:
            if remaining_limit <= 0:
                return await message.reply(
                    f"⚠️ {await gsname(message.from_user.first_name, message.from_user.id)}, дневной лимит переводов исчерпан.")
            # "все" означает максимум, который можно отправить сейчас — минимум из баланса и остатка лимита
            amount = min(user_balance, remaining_limit)
            if amount <= 0:
                return await message.reply(
                    f"⚠️ {await gsname(message.from_user.first_name, message.from_user.id)}, нет доступных mDrops для отправки.")
        else:
            try:
                amount = parse_bet_input(amount_text) if "к" in amount_text else int(amount_text)
                amount = int(amount)
                if amount <= 0:
                    return await message.reply("Неверное значение суммы для перевода.")
            except Exception:
                return await message.reply("Неверное значение! Используй число или 'все'.")

            # Проверка лимита
            if remaining_limit <= 0:
                return await message.reply(
                    f"⚠️ {await gsname(message.from_user.first_name, message.from_user.id)}, дневной лимит переводов ( {format_balance(DAILY_TRANSFER_LIMIT)} mDrops ) уже исчерпан.")
            if amount > remaining_limit:
                return await message.reply(
                    f"⚠️ {await gsname(message.from_user.first_name, message.from_user.id)}, сегодня можно передать ещё {format_balance(remaining_limit)} mDrops. Попробуй меньшую сумму."
                )

        # Проверка баланса
        if amount > user_balance:
            short = amount - user_balance
            return await message.reply(
                f"☹️ {await gsname(message.from_user.first_name, message.from_user.id)}, тебе не хватает {format_balance(short)} mDrops!")

        # Рассчитываем комиссию 15%
        fee = amount * 15 // 100
        net_amount = amount - fee

        # Выполняем перевод: списываем у отправителя, добавляем получателю
        data["coins"] = int(data.get("coins", 0)) - int(amount)
        # Обновляем daily_sent и сохраняем
        data["daily_transfers"]["sent"] = int(data["daily_transfers"].get("sent", 0)) + int(amount)
        await save_data(sender_id, data)

        recipient_data = await load_data(recipient_id)
        if not recipient_data:
            # В редком случае создаём и добавляем
            await create_user_data(recipient_id)
            recipient_data = await load_data(recipient_id)

        recipient_data["coins"] = int(recipient_data.get("coins", 0)) + int(net_amount)
        await save_data(recipient_id, recipient_data)

        # Логирование события (как в твоём коде)
        try:
            sender_chat = await bot.get_chat(int(sender_id))
            sender_username = getattr(sender_chat, "username", None)
            recipient_chat = await bot.get_chat(int(recipient_id))
            recipient_username = getattr(recipient_chat, "username", None)
            await send_log(
                f"Игрок @{sender_username if sender_username else '-'} ({sender_id}) успешно передал {format_balance(net_amount)} mDrops игроку @{recipient_username if recipient_username else '-'} ({recipient_id}).\n"
                f"Сумма: {format_balance(amount)} mDrops (комиссия {format_balance(fee)})."
            )
        except Exception:
            # не критично — продолжаем
            pass

        sender_name = message.from_user.first_name
        recipient_name = message.reply_to_message.from_user.first_name

        await message.answer(
            f"✅ {await gsname(sender_name, int(sender_id))} успешно перевёл {format_balance(net_amount)} mDrops {recipient_name}\n"
            f"(💰 Комиссия: {format_balance(fee)} mDrops)\n\n"
            f"📅 Сегодня отправлено: {format_balance(data['daily_transfers']['sent'])} / {format_balance(DAILY_TRANSFER_LIMIT)} mDrops"
        )
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 108)


# кеш топу
TOP_CACHE = {
    "time": 0,
    "text": "",
}

CACHE_TTL = 10  # сек, скільки живе кеш (можеш змінити)


@dp.message(F.text.lower().in_(["/top", "топ", "/top@gmegadbot"]))
async def top_players(message: Message, bot: Bot):
    try:
        import time
        now = time.time()

        # якщо кеш ще актуальний -> віддаємо його
        if now - TOP_CACHE["time"] < CACHE_TTL and TOP_CACHE["text"]:
            await message.answer(TOP_CACHE["text"])
            return

        # перерахунок топу
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS json_data (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """)
            cursor.execute("SELECT key, value FROM json_data")
            rows = cursor.fetchall()

        players = []
        for key, value in rows:
            try:
                data = json.loads(value)
                coins = float(data.get("coins", 0))
            except Exception:
                coins = 0.0
            players.append((key, coins))

        players.sort(key=lambda x: x[1], reverse=True)
        top = players[:10]
        total_players = len(players)

        if total_players == 0:
            text = "<b>🏆 Топ игроков:</b>\nПока нет игроков."
        else:
            lines = ["<b>🏆 Топ игроков:</b>\n"]
            for i, (uid, coins) in enumerate(top, 1):
                try:
                    chat = await bot.get_chat(int(uid))
                    name = f"<a href=\"tg://user?id=0\">{await gsname(chat.first_name)}</a>" or await gsname(
                        chat.username) or f"ID {uid}"
                except Exception:
                    name = f"ID {uid}"

                if i == 1:
                    i = "🥇 1."
                elif i == 2:
                    i = "🥈 2."
                elif i == 3:
                    i = "🥉 3."
                else:
                    i = f"🏅 {i}."
                lines.append(f"{i} {name} | <code>{format_balance(coins)} mDrops</code>")

            lines.append(f"\n<blockquote>Всего игроков: {total_players}</blockquote>")
            text = "\n".join(lines)

        # зберігаємо в кеш
        TOP_CACHE["time"] = now
        TOP_CACHE["text"] = text

        await message.answer(text)
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 109)


@dp.message(F.text.lower().in_(["/help", "помощь", "поддержка", "/help@gmegadbot"]))
async def handle_help(message: Message):
    uid = message.from_user.id
    kb = InlineKeyboardMarkup(inline_keyboard=(
    [[InlineKeyboardButton(text="📦 ИГРЫ", callback_data=f"callback_games_from_help:{uid}"),
      InlineKeyboardButton(text="📕 ПРАВИЛА",
                           url="sleet-windflower-6df.notion.site/GMEGADBOT-256118305d388059ae2ff01896163f6a?pvs=73")]]))

    await message.reply(
        f"📖 {await gsname(message.from_user.first_name, message.from_user.id)}, ты в меню помощи!\n\n📌 {html.bold("Существующие команды")}:{html.blockquote(f"{html.bold("/balance")} - ваш баланс\n{html.bold("/profile")} - ваш профиль\n{html.bold("/bonus")} - получить бонус (раз в час)\n{html.bold("/daily")} - получить ежедневный бонус (раз в 24 часа)\n{html.bold("/referrals")} - ваша реферальная ссылка\n{html.bold("/check")} - меню для создания чеков"
                                                                                                                                                            f"\n{html.bold("/cases")} - кейсы\n{html.bold("/top")} - мировой топ по mDrops\n{html.bold("/exchange")} - обменник GGs/mDrops\n{html.bold("/baraban")} - барабан бонусов (раз в 12 часов)\n{html.bold("/donation")} - донат меню\n{html.bold("/earn")} - заработать GGs\n{html.bold("/promotion")} - реклама канала/группы\n{html.bold("/bank")} - банк\n{html.bold("/partners")} - партнерская программа\n{html.bold("/shop")} - магазин")}\n\n🛡 Контакты: {html.blockquote(f"{html.bold("Владалец/разработчик:")} t.me/sollamon\n{html.bold("Канал:")} t.me/saycursed\n{html.bold("Чат:")} t.me/saycurse\n{html.bold("Бот Поддержки")}: @gmegasupbot\n")}",
        reply_markup=kb, disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("help_callback"))
async def handle_help(callback: CallbackQuery):
    uid = callback.from_user.id
    if int(callback.data.split(":")[1]) != int(uid):
        await callback.answer("Это не твоя кнопка!")

    kb = InlineKeyboardMarkup(inline_keyboard=(
    [[InlineKeyboardButton(text="📦 ИГРЫ", callback_data=f"callback_games_from_help:{uid}"),
      InlineKeyboardButton(text="📕 ПРАВИЛА",
                           url="sleet-windflower-6df.notion.site/GMEGADBOT-256118305d388059ae2ff01896163f6a?pvs=73")]]))

    await callback.message.edit_text(
        f"📖 {await gsname(callback.from_user.first_name, callback.from_user.id)}, ты в меню помощи!\n\n📌 {html.bold("Существующие команды")}:{html.blockquote(f"{html.bold("/balance")} - ваш баланс\n{html.bold("/profile")} - ваш профиль\n{html.bold("/bonus")} - получить бонус (раз в час)\n{html.bold("/daily")} - получить ежедневный бонус (раз в 24 часа)\n{html.bold("/referrals")} - ваша реферальная ссылка\n{html.bold("/check")} - меню для создания чеков"
                                                                                                                                                              f"\n{html.bold("/cases")} - кейсы\n{html.bold("/top")} - мировой топ по mDrops\n{html.bold("/exchange")} - обменник GGs/mDrops\n{html.bold("/baraban")} - барабан бонусов (раз в 12 часов)\n{html.bold("/donation")} - донат меню\n{html.bold("/earn")} - заработать GGs\n{html.bold("/promotion")} - реклама канала/группы\n{html.bold("/bank")} - банк")}\n\n🛡 Контакты: {html.blockquote(f"{html.bold("Владалец/разработчик:")} t.me/sollamon\n{html.bold("Канал:")} t.me/saycursed\n{html.bold("Чат:")} t.me/saycurse\n{html.bold("Бот Поддержки")}: @gmegasupbot\n")}",
        reply_markup=kb, disable_web_page_preview=True)


# -------------- CLANES -------------- #

async def add_clan_request(user_id: str, clan_name: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Перевіряємо, чи немає вже активної заявки
    cursor.execute("SELECT id FROM clan_requests WHERE user_id = ? AND status = 'pending'", (user_id,))
    if cursor.fetchone():
        conn.close()
        return False

    cursor.execute("INSERT INTO clan_requests (user_id, clan_name) VALUES (?, ?)", (user_id, clan_name))
    conn.commit()
    conn.close()
    return True


async def get_clan_requests(clan_name: str) -> list:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM clan_requests WHERE clan_name = ? AND status = 'pending'", (clan_name,))
    requests = [row[0] for row in cursor.fetchall()]
    conn.close()
    return requests


async def update_clan_request(user_id: str, clan_name: str, status: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE clan_requests SET status = ? WHERE user_id = ? AND clan_name = ? AND status = 'pending'",
                   (status, user_id, clan_name))
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def get_all_clans():
    """Отримує усі клани з бази"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name, level, members FROM clans")
    rows = cursor.fetchall()
    conn.close()
    return [{"name": r[0], "level": r[1], "members": json.loads(r[2])} for r in rows]


async def create_clan(name: str, owner_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Перевіряємо, чи існує вже клан з такою назвою
    cursor.execute("SELECT id FROM clans WHERE name = ?", (name,))
    if cursor.fetchone():
        conn.close()
        return False  # Клан з такою назвою вже є

    # Створюємо новий клан
    members = json.dumps([owner_id])
    admins = json.dumps([owner_id])

    cursor.execute("""
    INSERT INTO clans (name, coffres, level, rating, members, admins, owner)
    VALUES (?, 0, 1, 0, ?, ?, ?)
    """, (name, members, admins, owner_id))

    conn.commit()
    conn.close()
    return True


async def load_clan(name: str) -> dict | None:
    """Завантажує дані клану за його назвою"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name, coffres, level, rating, members, admins, owner FROM clans WHERE name = ?", (name,))
    row = cursor.fetchone()

    conn.close()

    if not row:
        return None

    return {
        "name": row[0],
        "coffres": row[1],
        "level": row[2],
        "rating": row[3],
        "members": json.loads(row[4]),
        "admins": json.loads(row[5]),
        "owner": row[6]
    }


async def save_clan(name: str, data: dict) -> bool:
    """Зберігає оновлені дані клану"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE clans 
            SET coffres = ?, level = ?, rating = ?, members = ?, admins = ?, owner = ?
            WHERE name = ?
        """, (
            data.get("coffres", 0),
            data.get("level", 1),
            data.get("rating", 0),
            json.dumps(data.get("members", [])),
            json.dumps(data.get("admins", [])),
            data.get("owner"),
            name
        ))

        conn.commit()
        return cursor.rowcount > 0

    finally:
        conn.close()


def contains_emoji(text: str) -> bool:
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F"  # смайли
        "\U0001F300-\U0001F5FF"  # символи та піктограми
        "\U0001F680-\U0001F6FF"  # транспорт та символи
        "\U0001F1E0-\U0001F1FF"  # прапори
        "\U00002700-\U000027BF"  # різні символи
        "\U0001F900-\U0001F9FF"  # додаткові символи
        "\U0001FA70-\U0001FAFF"  # ще додаткові символи
        "]+", flags=re.UNICODE
    )
    return bool(emoji_pattern.search(text))


class ClanStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_anount_rating_buy = State()
    waiting_for_amount_to_add_coffer = State()
    waiting_for_delete_confirm = State()
    waiting_for_amount_to_give_from_coffer = State()
    waiting_for_id_to_give_from_coffer = State()


@dp.message(F.text.lower().in_(["кланы", "клан", "/clan"]))
@flood_protect(min_delay=0.5)
async def handle_clan_command(message: Message):
    if message.chat.type != "private":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🍓 Перейти в ЛС", url="t.me/gmegadbot")]]
        )
        return await message.reply(
            f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, кланы доступны только в {html.link('ЛС с ботом', 't.me/gmegadbot')}!",
            reply_markup=kb, disable_web_page_preview=True
        )

    user_id = message.from_user.id
    name = message.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        kb = InlineKeyboardMarkup(inline_keyboard=(
        [[InlineKeyboardButton(text="🔥 Создать Клан", callback_data="create_clan")],
         [InlineKeyboardButton(text="🔍 Найти Клан", callback_data="find_clan")]]))
        return await message.reply(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!", reply_markup=kb)

    clan_data = await load_clan(str(clan_name))
    clan_level = clan_data["level"]
    player_accept = None

    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(message.from_user.username, "error when checking clan level (player)", user_id, 123)

    total_members = int(len(clan_data["members"]))
    total_members_available = 5 * int(clan_level)
    kb = None
    if player_accept == "Владелец" or player_accept == "Админ":
        kb = InlineKeyboardMarkup(inline_keyboard=(
            [[InlineKeyboardButton(text="Казна", callback_data="clan_coffer")],
             [InlineKeyboardButton(text="Участники", callback_data="show_clan_members")],
             [InlineKeyboardButton(text="Заявки на вступление", callback_data="clan_requests")],
             [InlineKeyboardButton(text="Купить рейтинг", callback_data="clan_buy_rating"),
              InlineKeyboardButton(text="Топ кланов", callback_data="top_clans:1")],
             [InlineKeyboardButton(text="Увеличить уровень", callback_data="upgrade_clan")],
             [InlineKeyboardButton(text="Покинуть клан", callback_data=f"leave_clan:{clan_name}")]]
        ))
        if str(user_id) == str(clan_data["owner"]):
            kb.inline_keyboard.append(
                [InlineKeyboardButton(text="‼️ Удалить клан", callback_data=f"delete_clan:{clan_name}")])
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="Казна", callback_data="clan_coffer")],
                              [InlineKeyboardButton(text="Участники", callback_data="show_clan_members")],
                              [InlineKeyboardButton(text="Купить рейтинг", callback_data="clan_buy_rating"),
                               InlineKeyboardButton(text="Топ кланов", callback_data="top_clans:1")],
                              [InlineKeyboardButton(text="Покинуть клан", callback_data=f"leave_clan:{clan_name}")]]))

    await message.reply(
        f"{await gsname(name, user_id)} твой клан:\n🛡 Название: {clan_name}\n💰 Казна: {format_balance(clan_data["coffres"])} mDrops\n⚜️ Уровень Клана: {clan_data["level"]}\n🏆 Рейтинг: {clan_data["rating"]}\n👤 Твоя роль: {player_accept}\n👥 Участников: {total_members}/{total_members_available}",
        reply_markup=kb)


@dp.callback_query(F.data == "upgrade_clan")
@flood_protect(min_delay=0.5)
async def handle_upgrade_clan(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        kb = InlineKeyboardMarkup(inline_keyboard=(
        [[InlineKeyboardButton(text="🔥 Создать Клан", callback_data="create_clan")],
         [InlineKeyboardButton(text="🔍 Найти Клан", callback_data="find_clan")]]))
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!",
                                                reply_markup=kb)

    clan_data = await load_clan(str(clan_name))
    clan_level = clan_data["level"]
    player_accept = None

    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(callback.from_user.username, "error when checking clan level (player)", user_id, 123)

    if player_accept == "Участник":
        return await callback.message.edit_text(f"{await gsname(name, user_id)} ты не имеешь доступа сюда!")

    if clan_level >= 5:
        return await callback.message.edit_text(
            f"{await gsname(name, user_id)} уровень твоего клана уже максимальный (5 ур.)!")

    price = clan_level * 250000
    if price > clan_data["coffres"]:
        return await callback.answer(
            f"Не достаточно mDrops для улучшения клана!\n\nЦена улучшения: {format_balance(price)} mDrops",
            show_alert=True)

    clan_data["coffres"] -= price
    clan_data["level"] += 1
    await save_clan(clan_name, clan_data)
    await callback.message.edit_text(
        f"{await gsname(name, user_id)}, ты успешно увеличил уровень клана до {clan_data["level"]}!")


@dp.callback_query(F.data == "clan_callback")
@flood_protect(min_delay=0.5)
async def handle_clan_command(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        kb = InlineKeyboardMarkup(inline_keyboard=(
        [[InlineKeyboardButton(text="🔥 Создать Клан", callback_data="create_clan")],
         [InlineKeyboardButton(text="🔍 Найти Клан", callback_data="find_clan")]]))
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!",
                                                reply_markup=kb)

    clan_data = await load_clan(str(clan_name))
    clan_level = clan_data["level"]
    player_accept = None

    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(callback.from_user.username, "error when checking clan level (player)", user_id, 123)

    total_members = int(len(clan_data["members"]))
    total_members_available = 5 * int(clan_level)
    kb = None
    if player_accept == "Владелец" or player_accept == "Админ":
        kb = InlineKeyboardMarkup(inline_keyboard=(
            [[InlineKeyboardButton(text="Казна", callback_data="clan_coffer")],
             [InlineKeyboardButton(text="Участники", callback_data="show_clan_members")],
             [InlineKeyboardButton(text="Заявки на вступление", callback_data="clan_requests")],
             [InlineKeyboardButton(text="Купить рейтинг", callback_data="clan_buy_rating"),
              InlineKeyboardButton(text="Топ кланов", callback_data="top_clans:1")],
             [InlineKeyboardButton(text="Увеличить уровень", callback_data="upgrade_clan")],
             [InlineKeyboardButton(text="Покинуть клан", callback_data=f"leave_clan:{clan_name}")]]
        ))
        if str(user_id) == str(clan_data["owner"]):
            kb.inline_keyboard.append(
                [InlineKeyboardButton(text="‼️ Удалить клан", callback_data=f"delete_clan:{clan_name}")])
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="Казна", callback_data="clan_coffer")],
                              [InlineKeyboardButton(text="Участники", callback_data="show_clan_members")],
                              [InlineKeyboardButton(text="Купить рейтинг", callback_data="clan_buy_rating"),
                               InlineKeyboardButton(text="Топ кланов", callback_data="top_clans:1")],
                              [InlineKeyboardButton(text="Покинуть клан", callback_data=f"leave_clan:{clan_name}")]]))

    await callback.message.edit_text(
        f"{await gsname(name, user_id)} твой клан:\n🛡 Название: {clan_name}\n💰 Казна: {format_balance(clan_data["coffres"])} mDrops\n⚜️ Уровень Клана: {clan_data["level"]}\n🏆 Рейтинг: {clan_data["rating"]}\n👤 Твоя роль: {player_accept}\n👥 Участников: {total_members}/{total_members_available}",
        reply_markup=kb)


@dp.callback_query(F.data.startswith("delete_clan:"))
async def handle_delete_clan_request(callback: CallbackQuery, state: FSMContext):
    caller_id = str(callback.from_user.id)
    _, clan_name = callback.data.split(":", 1)

    clan = await load_clan(clan_name)
    if not clan:
        return await callback.answer("❌ Клан не найден.", show_alert=True)

    if caller_id != str(clan.get("owner")):
        return await callback.answer("❌ Только владелец может удалить клан!", show_alert=True)

    # Попросим подтвердить полным вводом названия
    try:
        await callback.message.edit_text(
            f"⚠️ Внимание! Для подтверждения удаления клана введите точное его название:\n\n<b>{clan_name}</b>\n\nЭто действие необратимо.",
            parse_mode="HTML"
        )
    except Exception:
        # если нельзя редактировать — просто отправим alert
        await callback.answer("Подтвердите удаление: введите название клана в чат.", show_alert=True)

    await state.update_data(clan_to_delete=clan_name, owner_id=caller_id)
    await state.set_state(ClanStates.waiting_for_delete_confirm)


@dp.message(ClanStates.waiting_for_delete_confirm)
async def handle_confirm_delete(message: Message, state: FSMContext):
    data = await state.get_data()
    clan_name = data.get("clan_to_delete")
    owner_id = data.get("owner_id")
    sender_id = str(message.from_user.id)

    # Тільки власник може підтвердити
    if sender_id != owner_id:
        await state.clear()
        return await message.answer("❌ Только владелец может подтвердить удаление.")

    if message.text.strip() != clan_name:
        await state.clear()
        return await message.answer("❌ Название не совпадает. Удаление отменено.")

    clan = await load_clan(clan_name)
    if not clan:
        await state.clear()
        return await message.answer("❌ Клан не найден или уже удалён.")

    members = clan.get("members", []) or []

    # Оновимо дані кожного учасника у вашій БД (якщо є)
    for mid in members:
        try:
            udata = await load_data(str(mid))
            if udata:
                udata["clan"] = None
                await save_data(str(mid), udata)
        except Exception:
            pass

    # Видаляємо клан і заявки з БД
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM clans WHERE name = ?", (clan_name,))
        cursor.execute("DELETE FROM clan_requests WHERE clan_name = ?", (clan_name,))
        conn.commit()
    finally:
        conn.close()

    await state.clear()

    # Уведомлення в чат і учасникам (якщо можна)
    await message.answer(f"✅ Клан \"{clan_name}\" успешно удалён.")

    for mid in members:
        try:
            await bot.send_message(int(mid), f"❗️ Клан \"{clan_name}\" был распущен владельцем.")
        except Exception:
            pass


@dp.callback_query(F.data == "clan_coffer")
async def handle_clan_coffer(query: CallbackQuery):
    user_id = query.from_user.id
    name = query.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        return await query.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    clan_data = await load_clan(str(clan_name))
    clan_level = clan_data["level"]
    player_accept = None
    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(query.from_user.username, "error when checking clan level (player)", user_id, 123)

    if player_accept == "Владелец" or player_accept == "Админ":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Пополнить Казну", callback_data="add_money_to_clan_coffer"),
             InlineKeyboardButton(text="Выдать Деньги", callback_data="give_money_to_player_clan")]])
        return await query.message.edit_text(f"{await gsname(name, user_id)}, ты в казне клана, выбери действие:",
                                             reply_markup=kb)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Пополнить Казну", callback_data="add_money_to_clan_coffer")]])
        return await query.message.edit_text(f"{await gsname(name, user_id)}, ты в казне клана, выбери действие:",
                                             reply_markup=kb)


@dp.callback_query(F.data == "give_money_to_player_clan")
async def handle_give_money_to_player_clan(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    name = query.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        return await query.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    clan_data = await load_clan(str(clan_name))
    player_accept = None
    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(query.from_user.username, "error when checking clan level (player)", user_id, 123)

    if player_accept == "Участник":
        return await query.message.edit_text(f"{await gsname(name, user_id)} ты не имеешь доступа сюда!")

    await query.message.edit_text(
        f"{await gsname(name, user_id)}, введи количество mDrops для выдачи (1к, 50к, 100к)\n\nБаланс казны: {format_balance(clan_data["coffres"])} mDrops")
    await state.set_state(ClanStates.waiting_for_amount_to_give_from_coffer)


@dp.message(ClanStates.waiting_for_amount_to_give_from_coffer)
async def handle_waiting_for_amount_to_give_from_coffer(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        await state.clear()
        return await msg.answer(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    clan_data = await load_clan(str(clan_name))
    player_accept = None
    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(msg.from_user.username, "error when checking clan level (player)", user_id, 123)

    if player_accept == "Участник":
        await state.clear()
        return await msg.answer(f"{await gsname(name, user_id)} ты не имеешь доступа сюда!")

    try:
        amount = parse_bet_input(msg.text)
    except:
        return await msg.answer(
            f"{await gsname(name, user_id)}, введи количество mDrops для выдачи (1к, 50к, 100к)\n\nБаланс казны: {format_balance(clan_data["coffres"])} mDrops")

    if int(clan_data["coffres"]) < int(amount):
        return await msg.answer(
            f"{await gsname(name, user_id)}, недостаточно mDrops. Введи количество mDrops для выдачи (1к, 50к, 100к)\n\nБаланс казны: {format_balance(clan_data["coffres"])} mDrops")

    await state.update_data(amount=amount)

    await msg.answer(f"{await gsname(name, user_id)}, введи ID получателя")
    await state.set_state(ClanStates.waiting_for_id_to_give_from_coffer)


@dp.message(ClanStates.waiting_for_id_to_give_from_coffer)
async def handle_waiting_for_amount_to_give_from_coffer(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        await state.clear()
        return await msg.answer(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    clan_data = await load_clan(str(clan_name))
    player_accept = None
    if str(user_id) == str(clan_data["owner"]):
        player_accept = "Владелец"
    elif str(user_id) in clan_data["admins"]:
        player_accept = "Админ"
    elif str(user_id) in clan_data["members"]:
        player_accept = "Участник"
    else:
        return await handle_error(msg.from_user.username, "error when checking clan level (player)", user_id, 123)

    if player_accept == "Участник":
        await state.clear()
        return await msg.answer(f"{await gsname(name, user_id)} ты не имеешь доступа сюда!")

    target_id = int(msg.text)
    if not str(target_id) in clan_data["members"]:
        await state.clear()
        return await msg.answer(f"{await gsname(name, user_id)} данный игрок отсутствует в твоем клане!")

    state_data = await state.get_data()

    target_data = await load_data(target_id)
    if not target_data:
        await state.clear()
        return await msg.answer(f"{await gsname(name, user_id)} данный игрок еще не зарегистрировался!")

    amount = state_data["amount"]
    clan_data["coffres"] -= amount
    await save_clan(clan_name, clan_data)

    target_data["coins"] += amount
    await save_data(target_id, target_data)

    chat = await bot.get_chat(target_id)
    target_name = chat.first_name

    await msg.answer(
        f"{await gsname(name, user_id)} ты успешно выдал {format_balance(amount)} mDrops игроку {await gsname(target_name)} ({html.code(target_id)})!")
    append_log(
        f"{await gsname(name, user_id)} ({html.code(user_id)}) успешно выдал {format_balance(amount)} mDrops игроку {await gsname(target_name)} ({html.code(target_id)})!",
        add_timestamp=True)
    try:
        await bot.send_message(target_id, f"Админ клана \"{clan_name}\" выдал вам {format_balance(amount)} mDrops!")
    except:
        pass
    await state.clear()


@dp.callback_query(F.data == "add_money_to_clan_coffer")
async def handle_clan_coffer(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    name = query.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        return await query.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    await query.message.edit_text(
        f"{await gsname(name, user_id)}, введи на сколько mDrops ты хочешь пополнить казну (1к, 5к, 100к или все)")
    await state.set_state(ClanStates.waiting_for_amount_to_add_coffer)


@dp.message(ClanStates.waiting_for_amount_to_add_coffer)
async def handle_waiting_for_amount_to_add_coffer(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        await state.clear()
        return await msg.answer(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    amount = 0
    try:
        if msg.text in ["все", "всё"]:
            if data["coins"] >= 10:
                amount = int(data["coins"])
            else:
                return await msg.answer(f"{await gsname(name, user_id)}, минимальная сумма - 10 mDrops!",
                                        reply_markup=ckb(user_id))
        elif "к" in msg.text:
            amount = parse_bet_input(msg.text)
            if int(amount) < 10:
                return await msg.answer(f"{await gsname(name, user_id)}, минимальная сумма - 10 mDrops!",
                                        reply_markup=ckb(user_id))
        elif msg.text.isdigit():
            amount = int(msg.text)
            if amount < 10:
                return await msg.answer(f"{await gsname(name, user_id)}, минимальная сумма - 10 mDrops!",
                                        reply_markup=ckb(user_id))
        else:
            return await msg.answer(f"{await gsname(name, user_id)}, введи нормальное число!",
                                    reply_markup=ckb(user_id))
    except Exception:
        return await msg.answer(f"{await gsname(name, user_id)}, введи нормальное число!", reply_markup=ckb(user_id))

    if amount < 10:
        return await msg.answer(f"{await gsname(name, user_id)}, минимальная сумма - 10 mDrops!",
                                reply_markup=ckb(user_id))

    await msg.answer(f"{PAY_TEXT}", reply_markup=get_buy_kb(user_id, f"clan_pay_coffres_add_wb:{amount}",
                                                            f"clan_pay_coffres_add_wc:{amount}"))


@dp.callback_query(F.data.startswith("clan_pay_coffres_add_wb:"))
async def handle_add_mdrops_to_coffres_wb(qc: CallbackQuery, state: FSMContext):
    user_id = qc.from_user.id
    data = await load_data(user_id)
    name = qc.from_user.first_name

    clan_name = data.get("clan", None)
    clan_data = await load_clan(clan_name)

    _, amount, _ = qc.data.split(":")
    amount = int(amount)

    if int(amount) > int(data["coins"]):
        return await qc.message.edit_text(f"{await gsname(name, user_id)}, тебе не хватает mDrops!",
                                          reply_markup=ckb(user_id))

    clan_data = await load_clan(clan_name)
    clan_data["coffres"] += amount
    await save_clan(clan_name, clan_data)

    data["coins"] -= amount
    await save_data(user_id, data)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В клан", callback_data="clan_callback")]])
    await qc.message.edit_text(f"{await gsname(name, user_id)}, ты пополнил казну на {format_balance(amount)} mDrops",
                               reply_markup=kb)


@dp.callback_query(F.data.startswith("clan_pay_coffres_add_wc:"))
async def handle_add_mdrops_to_coffres_wc(qc: CallbackQuery, state: FSMContext):
    user_id = qc.from_user.id
    data = await load_data(user_id)
    name = qc.from_user.first_name

    clan_name = data.get("clan", None)
    clan_data = await load_clan(clan_name)

    _, amount, _ = qc.data.split(":")
    amount = int(amount)

    buy = await pay_with_card(str(user_id), float(amount), note="Пополнение клана")
    if not buy[0]:
        return await qc.message.edit_text(f"<b>Ошибка оплаты!</b>\n\nКомментарий: {buy[1]}")

    clan_data = await load_clan(clan_name)
    clan_data["coffres"] += amount
    await save_clan(clan_name, clan_data)

    data["coins"] -= amount
    await save_data(user_id, data)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В клан", callback_data="clan_callback")]])
    await qc.message.edit_text(f"{await gsname(name, user_id)}, ты пополнил казну на {format_balance(amount)} mDrops",
                               reply_markup=kb)


@dp.callback_query(F.data == "show_clan_members")
async def show_clan_members(callback: CallbackQuery):
    user_id = callback.from_user.id
    caller_str = str(user_id)
    caller_name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)
    if not clan_name:
        kb = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="🔥 Создать Клан", callback_data="create_clan")],
                              [InlineKeyboardButton(text="🔍 Найти Клан", callback_data="find_clan")]]))
        return await callback.message.edit_text(f"😕 {await gsname(caller_name, user_id)}, ты не состоишь в клане!",
                                                reply_markup=kb)

    clan = await load_clan(clan_name)
    if clan is None:
        return await callback.message.answer("❌ Ошибка: клан не найден.")

    members = clan.get("members", [])
    admins = clan.get("admins", [])
    owner = str(clan.get("owner"))

    # Определяем роль вызывающего
    if caller_str == owner:
        caller_role = "owner"
    elif caller_str in admins:
        caller_role = "admin"
    elif caller_str in members:
        caller_role = "member"
    else:
        return await handle_error(callback.from_user.username, "error when checking clan level (player)", user_id, 123)

    # Построим отображение участников (имена) и словарь id -> display
    id_to_display = {}
    operated_members = []
    for m in members:
        try:
            user = await bot.get_chat(m)
            display = user.full_name or (f"@{user.username}" if getattr(user, "username", None) else str(m))
        except Exception:
            display = str(m)
        # помечаем владельца/админов
        tag = ""
        if str(m) == owner:
            tag = " (Владелец)"
        elif str(m) in admins:
            tag = " (Админ)"
        operated_members.append(f"{display}{tag} — {m}")
        id_to_display[str(m)] = display

    members_text = "• " + "\n• ".join(operated_members) if operated_members else "Пока нет участников."

    # Построим клавиатуру: для каждого участника добавляем строку с кнопками действий,
    # которые доступны текущему пользователю. В текстах кнопок используем им'я (display).
    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for m in members:
        mid = str(m)
        # не даём действия над владельцем
        if mid == owner:
            continue

        display = id_to_display.get(mid, mid)
        # укоротим довгі імена до 24 символів у кнопках
        short_display = (display[:21] + "...") if len(display) > 24 else display

        row = []
        if caller_role in ("owner", "admin"):
            row.append(
                InlineKeyboardButton(text=f"Выгнать {short_display}", callback_data=f"kick_member:{mid}:{clan_name}"))
        if caller_role == "owner":
            if mid in admins:
                row.append(InlineKeyboardButton(text=f"Снять админ {short_display}",
                                                callback_data=f"demote_admin:{mid}:{clan_name}"))
            else:
                row.append(InlineKeyboardButton(text=f"Назначить админ {short_display}",
                                                callback_data=f"promote_admin:{mid}:{clan_name}"))

        if row:
            kb.inline_keyboard.append(row)

    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="clan_callback")])

    await callback.message.edit_text(
        f"{await gsname(caller_name, user_id)}, все участники клана \"{clan_name}\":\n\n{members_text}",
        reply_markup=kb)


@dp.callback_query(F.data.startswith("kick_member:"))
async def handle_kick_member(callback: CallbackQuery):
    try:
        caller_id = str(callback.from_user.id)
        _, target_id, clan_name = callback.data.split(":", 2)
        target_id = str(target_id)

        clan = await load_clan(clan_name)
        if not clan:
            return await callback.message.answer("❌ Клан не найден.")

        # Проверки прав
        if caller_id != str(clan.get("owner")) and caller_id not in clan.get("admins", []):
            return await callback.message.answer("❌ У тебя нет прав выгнать пользователя!")

        if target_id == str(clan.get("owner")):
            return await callback.message.answer("❌ Нельзя выгнать владельца клана!")

        members = clan.get("members", [])
        admins = clan.get("admins", [])

        if target_id not in members:
            return await callback.message.answer("❌ Пользователь не в клане.")

        # Удаляем из участников и (если есть) из админов
        members = [m for m in members if str(m) != target_id]
        admins = [a for a in admins if str(a) != target_id]

        clan["members"] = members
        clan["admins"] = admins
        await save_clan(clan_name, clan)

        # Обновляем данные пользователя
        try:
            udata = await load_data(target_id)
            if udata:
                udata["clan"] = None
                await save_data(target_id, udata)
        except Exception:
            pass

        # Уведомления
        await callback.message.answer(f"✅ Пользователь {target_id} выгнан из клана \"{clan_name}\".")
        try:
            await bot.send_message(target_id, f"❗️ Вы были исключены из клана \"{clan_name}\".")
        except Exception:
            # возможно приватный чат — игнорируем
            pass

        # Попробуем обновить окно со списком участников (если нужно)
        try:
            await show_clan_members(callback)
        except Exception:
            pass

    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 999)


@dp.callback_query(F.data.startswith("promote_admin:"))
async def handle_promote_admin(callback: CallbackQuery):
    try:
        caller_id = str(callback.from_user.id)
        _, target_id, clan_name = callback.data.split(":", 2)
        target_id = str(target_id)

        clan = await load_clan(clan_name)
        if not clan:
            return await callback.message.answer("❌ Клан не найден.")

        if caller_id != str(clan.get("owner")):
            return await callback.message.answer("❌ Только владелец может назначать админов!")

        if target_id == str(clan.get("owner")):
            return await callback.message.answer("❌ Владелец уже является админом по умолчанию.")

        if target_id not in clan.get("members", []):
            return await callback.message.answer("❌ Пользователь не состоит в клане.")

        admins = clan.get("admins", [])
        if target_id in admins:
            return await callback.message.answer("❌ Пользователь уже админ.")

        admins.append(target_id)
        clan["admins"] = admins
        await save_clan(clan_name, clan)

        await callback.message.answer(f"✅ Пользователь {target_id} назначен админом в клане \"{clan_name}\".")
        try:
            await bot.send_message(target_id, f"🎖️ Вас назначили админом в клане \"{clan_name}\".")
        except Exception:
            pass

        try:
            await show_clan_members(callback)
        except Exception:
            pass

    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 1000)


@dp.callback_query(F.data.startswith("demote_admin:"))
async def handle_demote_admin(callback: CallbackQuery):
    try:
        caller_id = str(callback.from_user.id)
        _, target_id, clan_name = callback.data.split(":", 2)
        target_id = str(target_id)

        clan = await load_clan(clan_name)
        if not clan:
            return await callback.message.answer("❌ Клан не найден.")

        if caller_id != str(clan.get("owner")):
            return await callback.message.answer("❌ Только владелец может снимать админов!")

        if target_id == str(clan.get("owner")):
            return await callback.message.answer("❌ Нельзя снять владельца с прав (он владелец).")

        admins = clan.get("admins", [])
        if target_id not in admins:
            return await callback.message.answer("❌ Пользователь не является админом.")

        admins = [a for a in admins if str(a) != target_id]
        clan["admins"] = admins
        await save_clan(clan_name, clan)

        await callback.message.answer(f"✅ Пользователь {target_id} больше не админ в клане \"{clan_name}\".")
        try:
            await bot.send_message(target_id, f"⚠️ Вас лишили прав админа в клане \"{clan_name}\".")
        except Exception:
            pass

        try:
            await show_clan_members(callback)
        except Exception:
            pass

    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 1001)


@dp.callback_query(F.data.startswith("leave_clan:"))
async def handle_leave_clan(callback: CallbackQuery):
    try:
        caller_id = str(callback.from_user.id)
        # callback.data формат: "leave_clan:<clan_name>"
        _, clan_name = callback.data.split(":", 1)

        clan = await load_clan(clan_name)
        if not clan:
            await callback.answer("❌ Клан не найден.", show_alert=True)
            return

        owner = str(clan.get("owner"))
        members = clan.get("members", [])
        admins = clan.get("admins", [])

        # Проверяем роль
        if caller_id == owner:
            # Владельцу нельзя просто так уйти — нужно передать права или распустить клан
            await callback.answer("❌ Владелец не может покинуть клан. Передай права (не реализовано) или удали клан!",
                                  show_alert=True)
            return

        if caller_id not in members:
            await callback.answer("❌ Ты не состоишь в этом клане.", show_alert=True)
            return

        # Удаляем из участников и из админов (если был админом)
        members = [m for m in members if str(m) != caller_id]
        admins = [a for a in admins if str(a) != caller_id]

        clan["members"] = members
        clan["admins"] = admins

        await save_clan(clan_name, clan)

        # Обновляем данные пользователя
        try:
            user_data = await load_data(caller_id)
            if user_data:
                user_data["clan"] = None
                await save_data(caller_id, user_data)
        except Exception:
            # не критично — продолжаем
            pass

        # Уведомления в чат
        display_name = None
        try:
            u = await bot.get_chat(int(caller_id))
            display_name = u.full_name or (f"@{u.username}" if getattr(u, "username", None) else caller_id)
        except Exception:
            display_name = caller_id

        await callback.message.answer(f"✅ {display_name} покинул(а) клан \"{clan_name}\".")

        # Попробуем уведомить владельца в ЛС (если возможно)
        try:
            await bot.send_message(int(owner), f"❗ Пользователь {display_name} покинул клан \"{clan_name}\".")
        except Exception:
            # приватный чат — игнорируем
            pass

        # Обновим текущее окно (если это окно с инфой о клане/списком участников)
        try:
            # если текущее сообщение — окно участников, обновим его
            await show_clan_members(callback)
        except Exception:
            # просто отредактируем сообщение с сообщением об успехе
            try:
                await callback.message.edit_text(f"✅ Вы покинули клан \"{clan_name}\".")
            except Exception:
                pass

        await callback.answer("Вы успешно покинули клан.", show_alert=False)

    except Exception as e:
        # лог ошибки и уведомление
        await handle_error(callback.from_user.username, e, callback.from_user.id, 1100)
        try:
            await callback.answer("❌ Произошла ошибка при выходе из клана.", show_alert=True)
        except Exception:
            pass


@dp.callback_query(F.data == "find_clan")
async def find_clan_callback(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    clans = get_all_clans()

    available_clans = []
    for clan in clans:
        max_members = clan["level"] * 5
        if len(clan["members"]) < max_members:
            available_clans.append(clan)

    if not available_clans:
        return await callback.message.edit_text("😕 Нет доступных кланов для присоединения.")

    text = "🔍 Доступные кланы:\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])

    for clan in available_clans:
        text += f"⚜️ {clan['name']} | Уровень {clan['level']} | {len(clan['members'])}/{clan['level'] * 5} мест\n"
        kb.inline_keyboard.append([InlineKeyboardButton(
            text=f"Присоединиться к {clan['name']}",
            callback_data=f"join_clan:{clan['name']}"
        )])

    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "create_clan")
async def handle_create_new_clan(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if clan_name:
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, ты уже состоишь в клане!")

    kb = InlineKeyboardMarkup(inline_keyboard=(
    [[InlineKeyboardButton(text="✅ Да, создать клан", callback_data="so_create_clan")],
     [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_clan_creation")]]))
    if int(data["GGs"]) >= 150:
        await callback.message.edit_text(
            f"💸 Цена создания клана: 150 GGs\n\n💎 Твой баланс: {data["GGs"]} GGs\n❓ Ты уверен что хочешь создать клан?",
            reply_markup=kb)
    else:
        await callback.message.edit_text(
            f"💸 {await gsname(name, user_id)}, цена создания клана: 150 GGs\n\n💎 Твой баланс: {data["GGs"]} GGs\n❌ Тебе не хватает {abs(150 - int(data["GGs"]))} GGs чтобы создать клан!")


@dp.callback_query(F.data == "cancel_clan_creation")
async def handle_cancel_clan_creation(callback: CallbackQuery):
    await callback.message.edit_text("❌ Создание клана отменено!")


@dp.callback_query(F.data == "so_create_clan")
async def handle_clan_creation(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if clan_name:
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, ты уже состоишь в клане!")
    if int(data["GGs"]) < 150:
        await callback.message.edit_text(
            f"💸 {await gsname(name, user_id)}, цена создания клана: 150 GGs\n\n💎 Твой баланс: {data["GGs"]} GGs\n❌ Тебе не хватает {abs(150 - int(data["GGs"]))} GGs чтобы создать клан!")

    await callback.message.edit_text(
        f"{await gsname(name, user_id)}, введи название клана\n\n1. Оно не может содержать емодзи\n2. Длинна должна составлять не более 20 символов")
    await state.set_state(ClanStates.waiting_for_name)


@dp.message(ClanStates.waiting_for_name)
async def handle_create_clan_name(message: Message, state: FSMContext):
    user_id = message.from_user.id
    name = message.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)
    clan_name = message.text
    if await load_clan(clan_name):
        return await message.reply(
            f"❌ {await gsname(name, user_id)}, клан с таким именем уже существует!")

    data["GGs"] -= 150
    await save_data(user_id, data)

    p_clan_name_player = data.get("clan", None)

    if p_clan_name_player:
        return await message.reply(f"😕 {await gsname(name, user_id)}, ты уже состоишь в клане!")
    if int(data["GGs"]) < 150:
        await message.answer(
            f"💸 {await gsname(name, user_id)}, цена создания клана: 150 GGs\n\n💎 Твой баланс: {data["GGs"]} GGs\n❌ Тебе не хватает {abs(150 - int(data["GGs"]))} GGs чтобы создать клан!")

    if len(clan_name) > 20:
        return await message.reply(
            f"❌ {await gsname(name, user_id)}, название клана должна составлять не более 20 символов")
    elif contains_emoji(clan_name):
        return await message.reply(f"❌ {await gsname(name, user_id)}, название клана не должно содержать емодзи")

    await create_clan(clan_name, str(user_id))
    data["clan"] = clan_name
    await save_data(user_id, data)
    await state.clear()
    return await message.reply(f"🔥 {await gsname(name, user_id)}, ты успешно создал клан \"{clan_name}\"")


@dp.callback_query(F.data.startswith("join_clan:"))
async def handle_join_clan(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    clan_name = callback.data.split(":")[1]

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if data.get("clan"):
        return await callback.message.answer("❌ Ты уже состоишь в клане!")

    success = await add_clan_request(user_id, clan_name)
    if success:
        await callback.message.answer(f"📨 Заявка в клан \"{clan_name}\" отправлена!")
    else:
        await callback.message.answer("❌ У тебя уже есть активная заявка!")


@dp.callback_query(F.data == "clan_requests")
async def handle_clan_requests(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    data = await load_data(user_id)

    clan_name = data.get("clan")
    if not clan_name:
        return await callback.message.answer("❌ Ты не состоишь в клане!")

    clan = await load_clan(clan_name)
    if user_id not in clan["admins"] and user_id != clan["owner"]:
        return await callback.message.answer("❌ У тебя нет прав!")

    requests = await get_clan_requests(clan_name)
    if not requests:
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text="Назад",
                callback_data=f"clan_callback"
            )
        ])
        return await callback.message.edit_text("Нет активных заявок.", reply_markup=kb)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for req in requests:
        try:
            user = await bot.get_chat(req)
            name = user.full_name
        except:
            name = f"Unknown ({req})"

        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"Принять {name}",
                callback_data=f"accept_request:{req}:{clan_name}"
            ),
            InlineKeyboardButton(
                text="Отклонить",
                callback_data=f"reject_request:{req}:{clan_name}"
            )
        ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text="Назад",
            callback_data=f"clan_callback"
        )
    ])

    await callback.message.edit_text(f"Заявки в клан \"{clan_name}\":", reply_markup=kb)


@dp.callback_query(F.data.startswith("accept_request:"))
async def handle_accept_request(callback: CallbackQuery):
    user_id = callback.from_user.id
    _, target_id, clan_name = callback.data.split(":")

    clan = await load_clan(clan_name)
    if str(user_id) not in clan["admins"] and str(user_id) != clan["owner"]:
        return await callback.message.answer("❌ У тебя нет прав!")

    target_data = await load_data(target_id)

    if target_data.get("clan", None):
        return await callback.answer("Этот игрок уже состоит в клане!")

    # Додаємо користувача у клан
    clan["members"].append(target_id)
    await save_clan(clan_name, clan)

    await update_clan_request(target_id, clan_name, "accepted")

    # Оновлюємо дані користувача
    data = await load_data(target_id)
    data["clan"] = clan_name
    await save_data(target_id, data)

    await callback.message.answer(f"✅ Пользователь {target_id} принят в клан {clan_name}!")
    await bot.send_message(target_id, f"✅ Вас приняли в клан \"{clan_name}\"!")


@dp.callback_query(F.data.startswith("reject_request:"))
async def handle_reject_request(callback: CallbackQuery):
    user_id = callback.from_user.id
    _, target_id, clan_name = callback.data.split(":")

    clan = await load_clan(clan_name)
    if str(user_id) not in clan["admins"] and str(user_id) != clan["owner"]:
        return await callback.message.answer("❌ У тебя нет прав!")

    await update_clan_request(target_id, clan_name, "rejected")
    await callback.message.answer(f"❌ Заявка {target_id} отклонена!")


@dp.callback_query(F.data == "clan_buy_rating")
async def handle_clan_buy_rating(callback: CallbackQuery):
    user_id = callback.from_user.id
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    if not clan_name:
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!")

    cland = await load_clan(clan_name)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    ggs = data["GGs"]
    sums = []

    if ggs >= 10:
        sums.append(
            InlineKeyboardButton(
                text=f"10",
                callback_data=f"buy_clan_rating:10"
            )
        )
    if ggs >= 50:
        sums.append(
            InlineKeyboardButton(
                text=f"50",
                callback_data=f"buy_clan_rating:50"
            )
        )
    if ggs >= 100:
        sums.append(
            InlineKeyboardButton(
                text=f"100",
                callback_data=f"buy_clan_rating:100"
            )
        )

    if sums:
        kb.inline_keyboard.append(sums)
    if ggs > 0:
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"Все",
                callback_data=f"buy_clan_rating:{ggs}"
            )
        ])
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"Ввести вручную",
                callback_data=f"buy_clan_rating:own"
            )
        ])

    kb.inline_keyboard.append([
        InlineKeyboardButton(
            text=f"Назад",
            callback_data=f"clan_callback"
        )
    ])

    await callback.message.edit_text(
        f"🏆 {await gsname(name, user_id)}, ты в меню покупки рейтинга!\n\n💸 Курс: 1 🏆 = 1 GGs\n🏆 Рейтинг клана сейчас: {cland["rating"]}\n💎 Твой баланс: {ggs} GGs\n❓ Сколько ты хочешь купить?",
        reply_markup=kb)


@dp.callback_query(F.data.startswith("buy_clan_rating:"))
async def handle_reject_request(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    _, amount = callback.data.split(":")
    name = callback.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    back_button = InlineKeyboardMarkup(
        inline_keyboard=([[InlineKeyboardButton(text="Назад", callback_data="clan_callback")]]))
    cancel = InlineKeyboardMarkup(inline_keyboard=([[InlineKeyboardButton(text="Отмена", callback_data="cancel")]]))

    if not clan_name:
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!",
                                                reply_markup=back_button)

    clan = await load_clan(clan_name)
    if int(data["GGs"]) < 1:
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, у тебя нету GGs!",
                                                reply_markup=back_button)

    if str(amount) == "own":
        await callback.message.edit_text(
            f"🏆 {await gsname(name, user_id)}, введи количетсво рейтинга которое ты хочешь купить:",
            reply_markup=cancel)
        return await state.set_state(ClanStates.waiting_for_anount_rating_buy)

    if int(amount) > int(data["GGs"]):
        return await callback.message.edit_text(f"😕 {await gsname(name, user_id)}, у тебя не хватает GGs!",
                                                reply_markup=back_button)

    data["GGs"] -= int(amount)
    await save_data(user_id, data)

    clan["rating"] += int(amount)
    await save_clan(clan_name, clan)

    await callback.message.edit_text(f"🏆 {await gsname(name, user_id)}, ты пополнил рейтинг клана на {amount}!",
                                     reply_markup=back_button)


@dp.message(ClanStates.waiting_for_anount_rating_buy)
async def handle_buy_rating_own(message: Message, state: FSMContext):
    user_id = message.from_user.id
    amount = message.text
    name = message.from_user.first_name

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    clan_name = data.get("clan", None)

    cancel = InlineKeyboardMarkup(inline_keyboard=([[InlineKeyboardButton(text="Отмена", callback_data="cancel")]]))

    if not clan_name:
        return await message.answer(f"😕 {await gsname(name, user_id)}, ты не состоишь в клане!", reply_markup=cancel)

    clan = await load_clan(clan_name)
    if int(data["GGs"]) < 1:
        return await message.answer(f"😕 {await gsname(name, user_id)}, у тебя нету GGs!", reply_markup=cancel)

    if int(amount) > 0:
        data["GGs"] -= int(amount)
        await save_data(user_id, data)

        clan["rating"] += int(amount)
        await save_clan(str(clan_name), clan)
        back_button = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="Назад", callback_data="clan_callback")]]))
        await message.answer(f"🏆 {await gsname(name, user_id)}, ты успешно пополнил рейтинг клана на {amount}!",
                             reply_markup=back_button)
        await state.clear()


def _safe_load_list(s):
    try:
        if isinstance(s, list):
            return s
        if isinstance(s, str):
            return json.loads(s)
    except Exception:
        return []
    return []


@dp.callback_query(F.data.startswith("top_clans:"))
async def callback_top_clans(query: CallbackQuery):
    """
    Callback data формат: "top_clans:<page>"
    Полная функция с кнопкой "Назад" (callback_data="clan_callback"),
    а также логикой обновления, которая пытается редактировать текущее сообщение.
    """
    try:
        await query.answer()  # убираем спиннер у кнопки

        per_page = 10

        # Разбираем страницу
        try:
            page = int(query.data.split(":", 1)[1])
        except Exception:
            page = 1
        if page < 1:
            page = 1

        # Получаем данные из БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM clans")
        total = cursor.fetchone()[0] or 0
        last_page = max(1, math.ceil(total / per_page))

        if page > last_page:
            page = last_page

        offset = (page - 1) * per_page

        cursor.execute(
            "SELECT name, rating, members, owner, level, coffres FROM clans "
            "ORDER BY rating DESC, name ASC LIMIT ? OFFSET ?",
            (per_page, offset)
        )
        rows = cursor.fetchall()
        conn.close()

        # Формируем текст
        if not rows:
            text = "Пока что нет кланов."
        else:
            lines = [f"🏆 Топ кланов — страница {page}/{last_page}\n"]
            rank = offset + 1
            for name, rating, members_raw, owner, level, coffres in rows:
                members = _safe_load_list(members_raw)
                members_count = len(members)
                lines.append(f"{rank}. {name} — {rating} 🏆 ({members_count} 👥)")
                rank += 1
            text = "\n".join(lines)

        # Строим клавиатуру вручную, чтобы гарантированно добавить кнопку "Назад" в отдельный ряд
        rows_kb = []
        if page > 1:
            rows_kb.append([InlineKeyboardButton(text="⏮️ Назад", callback_data=f"top_clans:{page - 1}")])
        rows_kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"top_clans:{page}")])
        if page < last_page:
            rows_kb.append([InlineKeyboardButton(text="Вперед ⏭️", callback_data=f"top_clans:{page + 1}")])
        # Кнопка "Назад" к меню клана
        rows_kb.append([InlineKeyboardButton(text="Назад", callback_data="clan_callback")])

        kb = InlineKeyboardMarkup(inline_keyboard=rows_kb)

        # Пытаемся отредактировать исходное сообщение. Если нельзя — показываем alert.
        try:
            await query.message.edit_text(text, reply_markup=kb)
        except Exception:
            try:
                await query.answer("Не удалось обновить это сообщение. Откройте топ заново.", show_alert=True)
            except Exception:
                pass

    except Exception:
        try:
            await query.answer("❌ Произошла ошибка при загрузке топа кланов.", show_alert=True)
        except Exception:
            pass


@dp.callback_query(F.data == "cancel")
async def handle_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(f"Действие отменено")


# -------------- CASES -------------- #

CASES = {
    "bronze": {"emoji": "🟢", "price": 1000, "name": "Бронзовый кейс"},
    "silver": {"emoji": "🔵", "price": 5000, "name": "Серебряный кейс"},
    "gold": {"emoji": "🟣", "price": 20000, "name": "Золотой кейс"},
    "platinum": {"emoji": "⚪", "price": 50000, "name": "Платиновый кейс"},
    "diamond": {"emoji": "💎", "price": 100000, "name": "Алмазный кейс"},
    "emerald": {"emoji": "🟩", "price": 200000, "name": "Изумрудный кейс"},
    "ruby": {"emoji": "🔴", "price": 500000, "name": "Рубиновый кейс"},
    "aquamaine": {"emoji": "💠", "price": 1000000, "name": "Аквамариновый кейс"},
}


@dp.message(F.text.lower().in_(["/cases", "кейсы", "кейс", "/case", "/cases@gmegadbot", "/case@gmegadbot"]))
async def case_menu(message: Message):
    try:
        kb = InlineKeyboardBuilder()
        for key, case in CASES.items():
            kb.row(InlineKeyboardButton(
                text=f"{case['emoji']} {case['name']} – {case['price']} mDrops",
                callback_data=f"buy_case:{key}:{message.from_user.id}"
            ))
        kb.row(InlineKeyboardButton(text="📂 Мои кейсы", callback_data=f"my_cases:{message.from_user.id}"))
        await message.reply("🎁 Добро пожаловать в магазин кейсов! Выберите кейс для покупки:",
                            reply_markup=kb.as_markup())
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 110)


@dp.callback_query(F.data.startswith("case_back"))
@flood_protect(min_delay=0.5)
async def case_back(callback: CallbackQuery):
    try:
        parts = callback.data.split(":")
        uid = parts[1]
        if int(uid) != int(callback.from_user.id):
            return await callback.answer("Это не твоя кнопка!")
        kb = InlineKeyboardBuilder()
        for key, case in CASES.items():
            kb.row(InlineKeyboardButton(
                text=f"{case['emoji']} {case['name']} – {case['price']} mDrops",
                callback_data=f"buy_case:{key}:{callback.from_user.id}"
            ))
        kb.row(InlineKeyboardButton(text="📂 Мои кейсы", callback_data=f"my_cases:{callback.from_user.id}"))
        await callback.message.edit_text("🎁 Добро пожаловать в магазин кейсов! Выберите кейс для покупки:",
                                         reply_markup=kb.as_markup())
    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 110)


@dp.callback_query(F.data.startswith("buy_case:"))
@flood_protect(min_delay=0.5)
async def buy_case(callback: CallbackQuery):
    try:
        user_id = str(callback.from_user.id)
        parts = callback.data.split(":")
        if int(parts[2]) != int(user_id):
            return await callback.answer("Это не твоя кнопка!")
        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)

        case_key = callback.data.split(":")[1]

        await callback.message.edit_text(f"{PAY_TEXT}",
                                         reply_markup=get_buy_kb(int(user_id), f"bcwb:{case_key}:{user_id}",
                                                                 f"bcwc:{case_key}:{user_id}"))
    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 112)


@dp.callback_query(F.data.startswith("bcwc:"))
async def buy_case_wc(qc: CallbackQuery):
    user_id = qc.from_user.id
    parts = qc.data.split(":")
    if int(parts[2]) != int(user_id):
        return await qc.answer("Это не твоя кнопка!")

    case_key = parts[1]
    case = CASES[case_key]

    buy = await pay_with_card(str(user_id), float(case["price"]))
    if not buy[0]:
        return await qc.message.edit_text(f"<b>Ошибка покупки!</b>\n\nКомментарий: {buy[1]}")

    data = await load_data(user_id)
    inventory = data.setdefault("cases", {})
    inventory[case_key] = inventory.get(case_key, 0) + 1
    await save_data(user_id, data)

    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"case_back:{user_id}"))
    await qc.message.edit_text(
        f"✅ Вы купили {case['emoji']} <b>{case['name']}</b> за {case['price']} mDrops!\n\nПерейдите в 📂 <b>Мои кейсы</b>, чтобы открыть его.",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data.startswith("bcwb:"))
async def buy_case_wb(qc: CallbackQuery):
    user_id = str(qc.from_user.id)
    parts = qc.data.split(":")
    if int(parts[2]) != int(user_id):
        return await qc.answer("Это не твоя кнопка!")

    case_key = parts[1]
    case = CASES[case_key]

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if data["coins"] < case["price"]:
        return await qc.message.edit_text("❌ Недостаточно средств!", reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data=f"case_back:{user_id}")]]))

    # списание средств и добавление кейса
    data["coins"] -= case["price"]
    inventory = data.setdefault("cases", {})
    inventory[case_key] = inventory.get(case_key, 0) + 1
    await save_data(user_id, data)
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"case_back:{user_id}"))
    await qc.message.edit_text(
        f"✅ Вы купили {case['emoji']} <b>{case['name']}</b> за {case['price']} mDrops!\n\nПерейдите в 📂 <b>Мои кейсы</b>, чтобы открыть его.",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data.startswith("my_cases:"))
@flood_protect(min_delay=0.5)
async def my_cases(callback: CallbackQuery):
    try:
        user_id = str(callback.from_user.id)
        if int(callback.data.split(":")[1]) != int(user_id):
            return await callback.answer(f"Это не твоя кнопка!")

        data = await load_data(user_id)
        if not data:
            await create_user_data(user_id)
            data = await load_data(user_id)

        inventory = data.get("cases", {})
        kb = InlineKeyboardBuilder()

        if not inventory:
            return await callback.message.edit_text("📂 У вас нет кейсов.")

        for key, amount in inventory.items():
            if amount > 0:
                case = CASES[key]
                kb.row(InlineKeyboardButton(
                    text=f"{case['emoji']} {case['name']} ×{amount}",
                    callback_data=f"open_case:{key}:{user_id}"
                ))

        kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"case_back:{user_id}"))
        await callback.message.edit_text("🎒 Ваши кейсы:", reply_markup=kb.as_markup())
    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 113)


@dp.callback_query(F.data.startswith("open_case:"))
@flood_protect(min_delay=0.5)
async def open_case(callback: CallbackQuery):
    try:
        user_id = str(callback.from_user.id)
        if int(callback.data.split(":")[2]) != int(user_id):
            return await callback.answer("Это не твоя кнопка!")

        data = await load_data(user_id)
        if not data:
            return

        case_key = callback.data.split(":")[1]
        inventory = data.setdefault("cases", {})
        if inventory.get(case_key, 0) <= 0:
            return await callback.answer("❌ У вас нет такого кейса.", show_alert=True)

        # списываем кейс
        inventory[case_key] -= 1

        # создаем сетку
        grid = ["💰" if random.random() < 0.7 else "❌" for _ in range(9)]
        random.shuffle(grid)

        data["current_case"] = {
            "key": case_key,
            "opened": [],
            "grid": grid,
            "owner_id": user_id,
        }
        await save_data(user_id, data)

        await show_case_grid(callback.message, user_id)
    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 114)


async def show_case_grid(message: Message, user_id: str):
    data = await load_data(user_id)
    case_data = data.get("current_case", {})
    opened = case_data.get("opened", [])
    grid = case_data.get("grid", [])

    kb = InlineKeyboardBuilder()
    for i in range(9):
        text = grid[i] if i in opened else "❓"
        kb.button(text=text, callback_data=f"case_click:{i}:{user_id}")

    buttons = list(kb.buttons)
    kb = InlineKeyboardBuilder()
    for i in range(0, 9, 3):
        kb.row(*buttons[i:i + 3])

    await message.edit_text("🎮 Нажмите на 3 ячейки, чтобы открыть их:", reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("case_click:"))
@flood_protect(min_delay=0.5)
async def case_click(callback: CallbackQuery):
    try:
        _, index_str, owner_id = callback.data.split(":")
        index = int(index_str)
        clicker_id = str(callback.from_user.id)

        if clicker_id != owner_id:
            return await callback.answer("⛔ Это не ваш кейс!", show_alert=True)

        data = await load_data(clicker_id)
        case_data = data.get("current_case", {})
        if not case_data or index in case_data["opened"]:
            return await callback.answer("❌ Неверный ход.", show_alert=True)

        case_data["opened"].append(index)
        await save_data(clicker_id, data)

        if len(case_data["opened"]) >= 3:
            total_money = 0
            reward_text = ""
            for i in case_data["opened"]:
                reward = case_data["grid"][i]
                if reward == "💰":
                    base_price = CASES[case_data["key"]]["price"]
                    amount = random.randint(int(base_price * 0.1), int(base_price * 0.6))
                    total_money += amount
                    reward_text += f"💰 +{amount} mDrops\n"
                else:
                    reward_text += "❌ Пусто\n"

            data["coins"] = data.get("coins", 0) + total_money
            data["current_case"] = None
            await save_data(clicker_id, data)

            kb = InlineKeyboardBuilder()
            kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"case_back:{callback.from_user.id}"))
            await callback.message.edit_text(
                f"🎉 Вы получили:\n{reward_text} mDrops\n💵 Всего: {total_money} mDrops",
                reply_markup=kb.as_markup()
            )
        else:
            await show_case_grid(callback.message, clicker_id)
    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 115)


# -------------- EXCHANGE -------------- #

TRADE_WINDOW_SECONDS = 60  # вікно (сек) для rate-limit
MAX_TRADES_PER_WINDOW = 5  # макс угод в цьому вікні для одного юзера
TRADE_COOLDOWN_SECONDS = 2  # мінімум секунд між окремими угодами (додатково)
CONFIRM_EXPIRE_SECONDS = 120  # скільки дійсна кнопка підтвердження
CONFIRM_THRESHOLD_GGS = 10.0  # якщо кількість GGs >= цього - потрібне підтвердження
CONFIRM_THRESHOLD_MCOINS = 50000  # якщо сума mDrops >= цього - потрібне підтвердження


def init_trade_protection_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_activity (
                uid TEXT PRIMARY KEY,
                window_start REAL,
                trades_count INTEGER,
                last_trade_ts REAL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_confirms (
                token TEXT PRIMARY KEY,
                uid TEXT,
                order_id INTEGER,
                amount REAL,
                created_at REAL,
                expires_at REAL
            )
        """)
        conn.commit()


# Викликай при ініціалізації бота
init_trade_protection_db()


class ExchangeForm(StatesGroup):
    waiting_price = State()
    waiting_amount = State()
    waiting_deal_amount = State()


def init_exchange_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS exchange_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price REAL NOT NULL,
                amount REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


init_exchange_db()


def can_start_trade(uid: str):
    """Повертає (can: bool, reason: str_or_None)."""
    now = time.time()
    act = _get_trade_activity(uid)
    if not act:
        return True, None
    window_start = act["window_start"] or 0.0
    trades_count = int(act["trades_count"] or 0)
    last_trade_ts = float(act["last_trade_ts"] or 0.0)

    # cooldown між окремими угодами
    if now - last_trade_ts < TRADE_COOLDOWN_SECONDS:
        return False, f"Потрібно почекати {TRADE_COOLDOWN_SECONDS} с. між угодами."

    # якщо вікно протягом WINDOW_SECONDS
    if now - window_start <= TRADE_WINDOW_SECONDS:
        if trades_count >= MAX_TRADES_PER_WINDOW:
            return False, f"Ліміт угод: максимум {MAX_TRADES_PER_WINDOW} угод за {TRADE_WINDOW_SECONDS} сек."
        else:
            return True, None
    else:
        # вікно/лічильник скидається
        return True, None


def record_trade(uid: str):
    """Запис успішної угоди — збільшує лічильник/оновлює last_trade_ts."""
    now = time.time()
    act = _get_trade_activity(uid)
    if not act:
        _save_trade_activity(uid, now, 1, now)
        return
    window_start = act["window_start"] or 0.0
    trades_count = int(act["trades_count"] or 0)
    last_trade_ts = float(act["last_trade_ts"] or 0.0)

    if now - window_start <= TRADE_WINDOW_SECONDS:
        trades_count += 1
        _save_trade_activity(uid, window_start, trades_count, now)
    else:
        # нове вікно
        _save_trade_activity(uid, now, 1, now)


def create_trade_confirmation(uid: str, order_id: int, amount: float):
    token = uuid.uuid4().hex
    now = time.time()
    expires = now + CONFIRM_EXPIRE_SECONDS
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trade_confirms (token, uid, order_id, amount, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (token, uid, order_id, float(amount), now, expires))
        conn.commit()
    return token


def get_trade_confirmation(token: str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT token, uid, order_id, amount, created_at, expires_at FROM trade_confirms WHERE token = ?", (token,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "token": row[0], "uid": row[1], "order_id": row[2],
            "amount": row[3], "created_at": row[4], "expires_at": row[5]
        }


def delete_trade_confirmation(token: str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trade_confirms WHERE token = ?", (token,))
        conn.commit()


def _get_trade_activity(uid: str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT window_start, trades_count, last_trade_ts FROM trade_activity WHERE uid = ?", (uid,))
        row = cursor.fetchone()
        if row:
            return {"window_start": row[0], "trades_count": row[1], "last_trade_ts": row[2]}
        return None


def _save_trade_activity(uid: str, window_start: float, trades_count: int, last_trade_ts: float):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trade_activity (uid, window_start, trades_count, last_trade_ts)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET window_start=excluded.window_start, trades_count=excluded.trades_count, last_trade_ts=excluded.last_trade_ts
        """, (uid, window_start, trades_count, last_trade_ts))
        conn.commit()


def get_exchange_menu(user_first_name: str):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    kb.inline_keyboard.append([InlineKeyboardButton(text="💸 Купить", callback_data="exchange_buy")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="💳 Продать", callback_data="exchange_sell")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="📝 Управление курсом", callback_data="exchange_rates_menu")])
    return kb


def get_exchange_rates_menu():
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    kb.inline_keyboard.append(
        [InlineKeyboardButton(text="✍ Изменить курс покупки", callback_data="exchange_setprice:buy")])
    kb.inline_keyboard.append(
        [InlineKeyboardButton(text="✍ Изменить курс продажи", callback_data="exchange_setprice:sell")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    return kb


def get_back_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    return kb


def amount_quick_kb_for_create(order_type: str, user_data: dict, price: float):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    if order_type == "buy":
        balance = user_data.get("coins", 0)
        row = []
        for v in (1000, 5000, 10000):
            if balance >= v:
                row.append(InlineKeyboardButton(text=f"{format_balance(v)}", callback_data=f"fill_amount:coins:{v}"))
        if balance > 0:
            row.append(InlineKeyboardButton(text="Все", callback_data=f"fill_amount:coins:all"))
        if row:
            kb.inline_keyboard.append(row)
    else:
        GGs = user_data.get("GGs", 0)
        row = []
        for v in (1, 3, 5):
            if GGs >= v:
                row.append(InlineKeyboardButton(text=str(v), callback_data=f"fill_amount:GGs:{v}"))
        if GGs > 0:
            row.append(InlineKeyboardButton(text="Все", callback_data=f"fill_amount:GGs:all"))
        if row:
            kb.inline_keyboard.append(row)
    kb.inline_keyboard.append([InlineKeyboardButton(text="✏ Ввести вручную", callback_data="manual_amount")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    return kb


def deal_amount_kb(order_id: int, max_amount: float):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    quick = []
    if max_amount >= 1:
        quick.append(InlineKeyboardButton(text="1", callback_data=f"deal_fill:{order_id}:1"))
    quick.append(InlineKeyboardButton(text="Все", callback_data=f"deal_fill:{order_id}:all"))
    if quick:
        kb.inline_keyboard.append(quick)
    kb.inline_keyboard.append([InlineKeyboardButton(text="✏ Ввести вручную", callback_data=f"deal_manual:{order_id}")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    return kb


def get_orders(order_type: str, limit: int = 9, offset: int = 0):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        if order_type == "sell":
            cursor.execute("""
                SELECT id, user_id, price, amount FROM exchange_orders
                WHERE order_type = ?
                ORDER BY price DESC
                LIMIT ? OFFSET ?
            """, (order_type, limit, offset))
        else:
            cursor.execute("""
                SELECT id, user_id, price, amount FROM exchange_orders
                WHERE order_type = ?
                ORDER BY price ASC
                LIMIT ? OFFSET ?
            """, (order_type, limit, offset))
        return cursor.fetchall()


def get_order_by_id(order_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, order_type, price, amount FROM exchange_orders WHERE id = ?", (order_id,))
        return cursor.fetchone()


def update_order_amount(order_id: int, new_amount: float):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        if new_amount <= 0:
            cursor.execute("DELETE FROM exchange_orders WHERE id = ?", (order_id,))
        else:
            cursor.execute("UPDATE exchange_orders SET amount = ? WHERE id = ?", (new_amount, order_id))
        conn.commit()


def save_order(user_id: str, order_type: str, price: float, amount: float):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO exchange_orders (user_id, order_type, price, amount) VALUES (?, ?, ?, ?)",
            (user_id, order_type, price, amount)
        )
        conn.commit()
        return cursor.lastrowid


def user_orders_summary(user_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, order_type, price, amount FROM exchange_orders WHERE user_id = ?", (user_id,))
        return cursor.fetchall()


async def _get_display_name(bot, user_id: str) -> str:
    try:
        ud = await load_data(str(user_id)) or {}
        name = ud.get("name") or ud.get("first_name") or ud.get("username")
        if name:
            return await gsname(name, user_id)
    except Exception:
        pass
    try:
        user = await bot.get_chat(int(user_id))
        if getattr(user, "first_name", None):
            return await gsname(user.first_name)
        if getattr(user, "username", None):
            return user.username
    except Exception:
        pass
    return f"User_{str(user_id)[-4:]}"


def my_orders_kb(orders):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for oid, otype, price, amount in orders:
        kb.inline_keyboard.append([InlineKeyboardButton(text=f"Управлять {oid}", callback_data=f"manage_order:{oid}")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    return kb


@dp.callback_query(F.data.startswith("manage_order:"))
@flood_protect(min_delay=0.5)
async def manage_order_cb(query: types.CallbackQuery, state: FSMContext):
    try:
        oid = int(query.data.split(":")[1])
    except Exception:
        await query.answer("Неверный ID.", show_alert=True)
        return
    order = get_order_by_id(oid)
    if not order:
        await query.answer("Заявка не найдена.", show_alert=True)
        return
    oid, owner_id, order_type, price, amount = order
    if str(query.from_user.id) != str(owner_id):
        await query.answer("Это не ваша заявка.", show_alert=True)
        return
    owner_name = await _get_display_name(query.bot, owner_id)
    header = (
        f"📝 Управление заявкой ID: {oid}\n\n👤 {owner_name}\nТип: {'Покупка' if order_type.upper() == 'BUY' else 'Продажа'}\nКурс: {format_balance(price)} mDrops / 1 GGs\nОсталось: {format_balance(amount)} GGs")
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    kb.inline_keyboard.append([InlineKeyboardButton(text="❌ Отменить и вернуть", callback_data=f"cancel_order:{oid}")])
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_rates_menu")])
    try:
        await query.message.edit_text(header, reply_markup=kb)
    except Exception:
        await query.message.answer(header, reply_markup=kb)
    await query.answer()


@dp.callback_query(F.data.startswith("cancel_order:"))
@flood_protect(min_delay=0.5)
async def cancel_order_cb(query: types.CallbackQuery):
    """
    Отмена заявки и возврат резерва (locked_coins или locked_GGs).
    """
    try:
        oid = int(query.data.split(":")[1])
    except Exception:
        await query.answer("Неверный ID.", show_alert=True)
        return

    order = get_order_by_id(oid)
    if not order:
        await query.answer("Заявка уже не найдена.", show_alert=True)
        return

    oid, owner_id, order_type, price, amount = order
    uid = str(query.from_user.id)
    if uid != str(owner_id):
        await query.answer("Вы не являетесь владельцем этой заявки.", show_alert=True)
        return

    user_data = await load_data(uid) or {}

    if order_type == "buy":
        # вернуть coins из locked_coins в основной баланс
        coins_back = float(price) * float(amount)
        user_data["locked_coins"] = max(0.0, float(user_data.get("locked_coins", 0)) - coins_back)
        user_data["coins"] = float(user_data.get("coins", 0)) + coins_back
        await save_data(uid, user_data)
        returned_text = f"💸 Возвращено: {format_balance(coins_back)} mDrops"
    else:  # sell
        # вернуть GGs из locked_GGs
        GGs_back = float(amount)
        user_data["locked_GGs"] = max(0.0, float(user_data.get("locked_GGs", 0)) - GGs_back)
        user_data["GGs"] = float(user_data.get("GGs", 0)) + GGs_back
        await save_data(uid, user_data)
        returned_text = f"📦 Возвращено: {format_balance(GGs_back)} GGs"

    # удаляем заявку
    update_order_amount(oid, 0)

    text = f"✅ Заявка ID: {oid} отменена.\n{returned_text}"
    try:
        await query.message.edit_text(text, reply_markup=get_back_kb())
    except Exception:
        await query.message.answer(text, reply_markup=get_back_kb())
    await query.answer()


@dp.message(F.text.lower().in_(["/exchange", "exchange", "обменник"]))
async def exchange_menu(message: types.Message):
    if getattr(message.chat, "type", None) != "private":
        kb = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="🍓 Перейти в ЛС", url="t.me/gmegadbot")]]))
        return await message.reply(
            f"🍓 Обменник доступен только в {html.link("ЛС с ботом", "t.me/gmegadbot")}!", reply_markup=kb,
            disable_web_page_preview=True
        )

    if not await load_data(str(message.from_user.id)):
        await create_user_data(message.from_user.id)

    await message.answer(
        f"💱 P2P ОБМЕННИК\n\nℹ Здесь ты можешь купить и продать GGs.\n👇 {await gsname(message.from_user.first_name, message.from_user.id)}, что ты хочешь?",
        reply_markup=get_exchange_menu(message.from_user.first_name)
    )


@dp.callback_query(F.data == "exchange_menu")
async def exchange_menu_callback(query: types.CallbackQuery):
    try:
        await query.message.edit_text(
            f"💱 P2P ОБМЕННИК\n\nℹ Здесь ты можешь купить и продать GGs.\n👇 {await gsname(query.from_user.first_name)}, что ты хочешь?",
            reply_markup=get_exchange_menu(query.from_user.first_name)
        )
    except Exception:
        await query.message.answer(
            f"💱 P2P ОБМЕННИК\n\nℹ Здесь ты можешь купить и продать GGs.\n👇 {await gsname(query.from_user.first_name)}, что ты хочешь?",
            reply_markup=get_exchange_menu(query.from_user.first_name)
        )
    await query.answer()


@dp.callback_query(F.data == "exchange_rates_menu")
@flood_protect(min_delay=0.5)
async def exchange_rates_menu(query: types.CallbackQuery):
    uid = str(query.from_user.id)
    orders = user_orders_summary(uid)
    lines = ["📝 Управление курсом\n\nВаши активные заявки:"]
    if orders:
        for oid, otype, price, amount in orders:
            lines.append(
                f"🔸 ID: {oid} | {'Покупка' if otype.upper() == 'BUY' else 'Продажа'} | {format_balance(amount)} GGs @ {format_balance(price)} mDrops")
        text = "\n".join(lines)
        try:
            await query.message.edit_text(text, reply_markup=my_orders_kb(orders))
        except Exception:
            await query.message.answer(text, reply_markup=my_orders_kb(orders))
    else:
        lines.append("— У вас нет активных заявок.")
        text = "\n".join(lines)
        try:
            await query.message.edit_text(text, reply_markup=get_exchange_rates_menu())
        except Exception:
            await query.message.answer(text, reply_markup=get_exchange_rates_menu())
    await query.answer()


@dp.callback_query(F.data.startswith("exchange_setprice:"))
@flood_protect(min_delay=0.5)
async def set_price(query: types.CallbackQuery, state: FSMContext):
    order_type = query.data.split(":")[1]
    await state.update_data(order_type=order_type)
    await state.set_state(ExchangeForm.waiting_price)
    uid = str(query.from_user.id)
    my_orders = user_orders_summary(uid)
    lines = ["💰 Ввод цены за 1 GG (в mDrops)."]
    relevant = [o for o in my_orders if o[1] == order_type]
    if relevant:
        lines.append("🔸 Ваши текущие заявки этого типа:")
        for oid, otype, price, amount in relevant:
            lines.append(f"ID:{oid} | {format_balance(amount)} GGs @ {format_balance(price)} mDrops")
    text = "\n".join(lines)
    try:
        await query.message.edit_text(text)
    except Exception:
        await query.message.answer(text)
    await query.answer()


@dp.message(ExchangeForm.waiting_price, F.text.regexp(r"^\d+(\.\d+)?$"))
async def set_price_value(message: types.Message, state: FSMContext):
    price = float(message.text)
    await state.update_data(price=price)
    await state.set_state(ExchangeForm.waiting_amount)
    data = await state.get_data()
    order_type = data.get("order_type", "buy")
    uid = str(message.from_user.id)
    user_data = await load_data(uid) or {}
    if order_type == "buy":
        await message.answer("✍ Выберите, сколько mDrops вы хотите вложить (быстро-кнопки) или введите вручную:",
                             reply_markup=amount_quick_kb_for_create("buy", user_data, price))
    else:
        await message.answer("✍ Выберите, сколько GGs выставить на продажу (быстро-кнопки) или введите вручную:",
                             reply_markup=amount_quick_kb_for_create("sell", user_data, price))


@dp.callback_query(F.data.startswith("fill_amount:"))
@flood_protect(min_delay=0.5)
async def fill_amount_callback(query: types.CallbackQuery, state: FSMContext):
    """
    callback data: fill_amount:<currency>:<amt_or_all>
    currency = 'coins' или 'GGs'
    """
    try:
        _, currency, amt = query.data.split(":")
    except Exception:
        await query.answer("Неверный формат.", show_alert=True)
        return

    data = await state.get_data()
    order_type = data.get("order_type")
    price = float(data.get("price", 0) or 0)
    uid = str(query.from_user.id)

    user_data = await load_data(uid) or {}

    if currency == "coins":
        # создание BUY-заявки — резервируем coins
        if amt == "all":
            coins = float(user_data.get("coins", 0))
        else:
            try:
                coins = float(amt)
            except Exception:
                await query.answer("Неверная сумма.", show_alert=True)
                return

        if coins <= 0 or float(user_data.get("coins", 0)) + 1e-9 < coins:
            await query.answer("❗ Недостаточно mDrops.", show_alert=True)
            return

        # резервируем coins
        user_data["coins"] = float(user_data.get("coins", 0)) - coins
        user_data["locked_coins"] = float(user_data.get("locked_coins", 0)) + coins
        await save_data(uid, user_data)

        amount_gg = coins / price if price > 0 else 0.0
        order_id = save_order(uid, order_type, price, amount_gg)

        text = (
            f"✅ Заявка создана!\n\nID: {order_id}\nТип: {order_type.upper()}\n"
            f"Курс: {format_balance(price)} mDrops / 1 GGs\nКол-во: {format_balance(amount_gg)} GGs"
        )
        try:
            await query.message.edit_text(text, reply_markup=get_back_kb())
        except Exception:
            await query.message.answer(text, reply_markup=get_back_kb())

    else:
        # currency == "GGs" -> создание SELL-заявки — резервируем GGs
        if amt == "all":
            ggs = float(user_data.get("GGs", 0))
        else:
            try:
                ggs = float(amt)
            except Exception:
                await query.answer("Неверная сумма.", show_alert=True)
                return

        if ggs <= 0 or float(user_data.get("GGs", 0)) + 1e-9 < ggs:
            await query.answer("❗ Недостаточно GGs.", show_alert=True)
            return

        # резервируем GGs
        user_data["GGs"] = float(user_data.get("GGs", 0)) - ggs
        user_data["locked_GGs"] = float(user_data.get("locked_GGs", 0)) + ggs
        await save_data(uid, user_data)

        order_id = save_order(uid, order_type, price, ggs)
        text = (
            f"✅ Заявка создана!\n\nID: {order_id}\nТип: {order_type.upper()}\n"
            f"Курс: {format_balance(price)} mDrops / 1 GGs\nКол-во: {format_balance(ggs)} GGs"
        )
        try:
            await query.message.edit_text(text, reply_markup=get_back_kb())
        except Exception:
            await query.message.answer(text, reply_markup=get_back_kb())

    await state.clear()
    await query.answer()


@dp.callback_query(F.data == "manual_amount")
async def manual_amount_cb(query: types.CallbackQuery):
    try:
        await query.message.edit_text(
            "✏ Введи число:\n(для BUY — сумма в mDrops для депозита; для SELL — количество GGs):",
            reply_markup=get_back_kb())
    except Exception:
        await query.message.answer(
            "✏ Введи число:\n(для BUY — сумма в mDrops для депозита; для SELL — количество GGs):")
    await query.answer()


@dp.message(ExchangeForm.waiting_amount, F.text.regexp(r"^\d+(\.\d+)?$"))
async def set_amount_value(message: types.Message, state: FSMContext):
    """
    Ручной ввод суммы при создании заявки.
    Если order_type == 'buy' — вводится сумма в mDrops (coins).
    Если order_type == 'sell' — вводится количество GGs.
    """
    uid = str(message.from_user.id)
    data = await state.get_data()
    price = float(data.get("price", 0) or 0)
    order_type = data.get("order_type", "buy")
    amount_input = float(message.text)
    user_data = await load_data(uid) or {}

    if amount_input <= 0:
        return await message.answer("Неверная сумма.")

    if order_type == "buy":
        coins_to_deposit = float(amount_input)
        if float(user_data.get("coins", 0)) + 1e-9 < coins_to_deposit:
            return await message.answer("❗ Недостаточно mDrops.")
        # резервируем coins
        user_data["coins"] = float(user_data.get("coins", 0)) - coins_to_deposit
        user_data["locked_coins"] = float(user_data.get("locked_coins", 0)) + coins_to_deposit
        await save_data(uid, user_data)

        amount_gg = coins_to_deposit / price if price > 0 else 0.0
        order_id = save_order(uid, order_type, price, amount_gg)
        text = (
            f"✅ Заявка создана!\n\nID: {order_id}\nКурс: {format_balance(price)} mDrops / 1 GGs\n"
            f"Кол-во: {format_balance(amount_gg)} GGs"
        )
        await message.answer(text, reply_markup=get_back_kb())

    else:
        # sell
        GGs_to_sell = float(amount_input)
        if float(user_data.get("GGs", 0)) + 1e-9 < GGs_to_sell:
            return await message.answer("❗ Недостаточно GGs.")
        # резервируем GGs
        user_data["GGs"] = float(user_data.get("GGs", 0)) - GGs_to_sell
        user_data["locked_GGs"] = float(user_data.get("locked_GGs", 0)) + GGs_to_sell
        await save_data(uid, user_data)

        order_id = save_order(uid, order_type, price, GGs_to_sell)
        text = (
            f"✅ Заявка создана!\n\nID: {order_id}\nКурс: {format_balance(price)} mDrops / 1 GGs\n"
            f"Кол-во: {format_balance(GGs_to_sell)} GGs"
        )
        await message.answer(text, reply_markup=get_back_kb())

    await state.clear()


@dp.callback_query(F.data == "exchange_buy")
@flood_protect(min_delay=0.5)
async def market_buy(query: types.CallbackQuery):
    orders = get_orders("sell", 9, 0)
    if not orders:
        try:
            await query.message.edit_text("❗ На рынке нет заявок на продажу.", reply_markup=get_back_kb())
        except Exception:
            await query.message.answer("❗ На рынке нет заявок на продажу.", reply_markup=get_back_kb())
        await query.answer()
        return

    lines = ["📊 <b>Заявки на продажу GGs</b>\n"]
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for oid, uid, price, amount in orders:
        name = await _get_display_name(query.bot, uid)
        total_m = price * amount
        btn_text = f"{name} • {int(price)} mDrops"
        kb.inline_keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"deal:{oid}")])
        lines.append(f"🔹 ID: {oid} | {name} | {format_balance(amount)} GGs @ {format_balance(price)} mDrops")
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    text = "\n".join(lines)
    try:
        await query.message.edit_text(text, reply_markup=kb)
    except Exception:
        await query.message.answer(text, reply_markup=kb)
    await query.answer()


@dp.callback_query(F.data == "exchange_sell")
@flood_protect(min_delay=0.5)
async def market_sell(query: types.CallbackQuery):
    orders = get_orders("buy", 9, 0)
    if not orders:
        try:
            await query.message.edit_text("❗ На рынке нет заявок на покупку.", reply_markup=get_back_kb())
        except Exception:
            await query.message.answer("❗ На рынке нет заявок на покупку.", reply_markup=get_back_kb())
        await query.answer()
        return

    lines = ["📊 <b>Заявки на покупку GGs</b>\n", f"{gline()}\n",
             f"{html.blockquote(html.italic("ℹ️ GGs можно купить только у других игроков. Выгодное предложение всегда размещается сверху."))}"]
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for oid, uid, price, amount in orders:
        name = await _get_display_name(query.bot, uid)
        reserved = price * amount
        btn_text = f"{name} • {int(price)} mDrops"
        kb.inline_keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"deal:{oid}")])
        # lines.append(f"🔹 ID: {oid} | 👤 Продавец: {name} | 💰 Запрос: {amount} GGs | 💸 Курс: {price} mDrops = 1 GG | ❇️ Доступно: {format_balance(reserved)} mDrops)")
    kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="exchange_menu")])
    text = "\n".join(lines)
    try:
        await query.message.edit_text(text, reply_markup=kb)
    except Exception:
        await query.message.answer(text, reply_markup=kb)
    await query.answer()


@dp.callback_query(F.data.startswith("deal:"))
@flood_protect(min_delay=0.5)
async def deal_order_prompt(query: types.CallbackQuery, state: FSMContext):
    try:
        order_id = int(query.data.split(":")[1])
    except:
        await query.answer("Неверный ID.", show_alert=True)
        return
    order = get_order_by_id(order_id)
    if not order:
        await query.answer("Заявка не найдена.", show_alert=True)
        return
    oid, owner_id, order_type, price, amount = order
    owner_name = await _get_display_name(query.bot, owner_id)
    if order_type == "sell":
        reserved_GGs = amount
        reserved_m = price * amount
        header = (f"👤 {owner_name}\n💸 Курс: {int(price)} mDrops = 1 GGs\n📦 Доступно: {int(reserved_GGs)} GGs")
    else:
        reserved_coins = price * amount
        header = (
            f"👤 {owner_name}\n💰 Пополнено: {format_balance(reserved_coins)} mDrops\n📦 Готов купить: {int(amount)} GGs")

    text = (f"\n🚀 КУПИТЬ GGs\n{html.code(gline())}\n{header}\n"
            f"{html.code(gline())}\n"
            "✏ Введите количество GGs для сделки (или выберите быстрый вариант):")
    try:
        await query.message.edit_text(text, reply_markup=deal_amount_kb(oid, amount))
    except Exception:
        await query.message.answer(text, reply_markup=deal_amount_kb(oid, amount))
    await state.update_data(deal_order_id=oid)
    await state.set_state(ExchangeForm.waiting_deal_amount)
    await query.answer()


@dp.callback_query(F.data.startswith("deal_fill:"))
@flood_protect(min_delay=0.5)
async def deal_fill_callback(query: types.CallbackQuery, state: FSMContext):
    # format: deal_fill:<order_id>:<amt_or_all>
    try:
        _, oid_s, amt_s = query.data.split(":")
        oid = int(oid_s)
    except Exception:
        await query.answer("Неверный формат.", show_alert=True)
        return

    order = get_order_by_id(oid)
    if not order:
        await query.answer("Заявка не найдена.", show_alert=True)
        return

    _, owner_id, order_type, price, amount_available = order
    taker_id = str(query.from_user.id)

    # Запрет трейда с собственной заявкой
    if str(owner_id) == taker_id:
        await query.answer("❗ Нельзя совершать сделку с собственной заявкой.", show_alert=True)
        return

    # Rate-limit check (если используется защита)
    ok, reason = can_start_trade(taker_id) if 'can_start_trade' in globals() else (True, None)
    if not ok:
        await query.answer(reason, show_alert=True)
        return

    # Если "all" — вычисляем сколько можно
    if amt_s == "all":
        taker_data = await load_data(taker_id) or {}

        if order_type == "sell":
            buyer_coins = float(taker_data.get("coins", 0))
            max_affordable = buyer_coins / float(price) if float(price) > 0 else float(amount_available)
            take_amount = min(float(amount_available), max_affordable)
            if take_amount <= 0:
                await query.answer("❗ У вас недостаточно mDrops для покупки.", show_alert=True)
                return

        elif order_type == "buy":
            seller_ggs = float(taker_data.get("GGs", 0))
            take_amount = min(float(amount_available), seller_ggs)
            if take_amount <= 0:
                await query.answer("❗ У вас недостаточно GGs для продажи.", show_alert=True)
                return

        else:
            await query.answer("Неподдерживаемый тип заявки.", show_alert=True)
            return

        require_confirm = True

    else:
        # числовая сумма
        try:
            take_amount = float(amt_s)
        except Exception:
            await query.answer("Неверная сумма.", show_alert=True)
            return

        total_cost = float(price) * take_amount
        require_confirm = (
                    take_amount >= CONFIRM_THRESHOLD_GGS or total_cost >= CONFIRM_THRESHOLD_MCOINS) if 'CONFIRM_THRESHOLD_GGS' in globals() else False

    # дополнительные проверки
    if take_amount > float(amount_available) + 1e-9:
        await query.answer("Запрошено больше, чем доступно.", show_alert=True)
        return
    if take_amount <= 0:
        await query.answer("Неверное количество.", show_alert=True)
        return

    # Если требуется подтверждение — создаём токен и кнопку
    if require_confirm and 'create_trade_confirmation' in globals():
        token = create_trade_confirmation(taker_id, oid, take_amount)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить сделку ✅", callback_data=f"deal_confirm:{token}")],
            [InlineKeyboardButton(text="Отменить ❌", callback_data="exchange_menu")]
        ])
        try:
            await query.message.edit_text(
                f"🔒 Требуется подтверждение сделки:\nID: {oid}\nСумма: {format_balance(take_amount)} GGs\nНажмите «Подтвердить сделку», чтобы завершить.",
                reply_markup=kb
            )
        except Exception:
            await query.message.answer(
                f"🔒 Требуется подтверждение сделки:\nID: {oid}\nСумма: {format_balance(take_amount)} GGs\nНажмите «Подтвердить сделку», чтобы завершить.",
                reply_markup=kb
            )
        await query.answer()
        return

    # Нет подтверждения — выполняем сразу
    try:
        await execute_trade(query, state, oid, take_amount)
        if 'record_trade' in globals():
            record_trade(taker_id)
    except Exception as e:
        await query.answer("Ошибка при выполнении сделки.", show_alert=True)
        await send_log(f"execute_trade error: {e}")


@dp.callback_query(F.data.startswith("deal_manual:"))
@flood_protect(min_delay=0.5)
async def deal_manual_cb(query: types.CallbackQuery, state: FSMContext):
    oid = int(query.data.split(":")[1])
    order = get_order_by_id(oid)
    if not order:
        await query.answer("Заявка не найдена.", show_alert=True)
        return

    oid, owner_id, order_type, price, amount = order
    taker_id = str(query.from_user.id)

    # запрет самотрейда
    if str(owner_id) == taker_id:
        await query.answer("❗ Нельзя совершать сделку с собственной заявкой.", show_alert=True)
        return

    # Rate-limit
    ok, reason = can_start_trade(taker_id) if 'can_start_trade' in globals() else (True, None)
    if not ok:
        await query.answer(reason, show_alert=True)
        return

    await state.update_data(deal_order_id=oid)
    await state.set_state(ExchangeForm.waiting_deal_amount)
    try:
        await query.message.edit_text("✏ Введите количество GGs для сделки (число):", reply_markup=get_back_kb())
    except Exception:
        await query.message.answer("✏ Введите количество GGs для сделки (число):")
    await query.answer()


@dp.message(ExchangeForm.waiting_deal_amount, F.text.regexp(r"^\d+(\.\d+)?$"))
async def deal_manual_amount_entered(message: types.Message, state: FSMContext):
    data = await state.get_data()
    oid = int(data.get("deal_order_id"))
    take_amount = float(message.text)

    order = get_order_by_id(oid)
    if not order:
        await message.answer("Заявка не найдена.")
        await state.clear()
        return

    _, owner_id, order_type, price, amount_available = order
    taker_id = str(message.from_user.id)

    # запрет самотрейда
    if str(owner_id) == taker_id:
        await message.answer("❗ Нельзя совершать сделку с собственной заявкой.")
        await state.clear()
        return

    # Rate-limit
    ok, reason = can_start_trade(taker_id) if 'can_start_trade' in globals() else (True, None)
    if not ok:
        await message.answer(reason)
        await state.clear()
        return

    if take_amount <= 0 or take_amount > float(amount_available) + 1e-9:
        await message.answer("Неверное количество.")
        await state.clear()
        return

    total_cost = float(price) * take_amount
    require_confirm = (
                take_amount >= CONFIRM_THRESHOLD_GGS or total_cost >= CONFIRM_THRESHOLD_MCOINS) if 'CONFIRM_THRESHOLD_GGS' in globals() else False

    if require_confirm and 'create_trade_confirmation' in globals():
        token = create_trade_confirmation(taker_id, oid, take_amount)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить сделку ✅", callback_data=f"deal_confirm:{token}")],
            [InlineKeyboardButton(text="Отменить ❌", callback_data="exchange_menu")]
        ])
        try:
            await message.reply(
                f"🔒 Подтвердите сделку ID:{oid} на {format_balance(take_amount)} GGs — нажмите кнопку ниже.",
                reply_markup=kb
            )
        except Exception:
            await message.answer(
                f"🔒 Подтвердите сделку ID:{oid} на {format_balance(take_amount)} GGs — нажмите кнопку ниже.",
                reply_markup=kb
            )
        await state.clear()
        return

    # Нет подтверждения — вызываем execute_trade
    class Q:
        pass

    q = Q()
    q.message = message
    q.bot = message.bot
    q.from_user = message.from_user

    try:
        await execute_trade(q, state, oid, take_amount)
        if 'record_trade' in globals():
            record_trade(taker_id)
    except Exception as e:
        await message.answer("Ошибка при выполнении сделки.")
        print("execute_trade error:", e)
    await state.clear()


@dp.callback_query(F.data.startswith("deal_confirm:"))
@flood_protect(min_delay=0.5)
async def deal_confirm_cb(query: types.CallbackQuery, state: FSMContext):
    # callback_data: deal_confirm:<token>
    try:
        token = query.data.split(":")[1]
    except Exception:
        await query.answer("Неверный токен.", show_alert=True)
        return

    conf = get_trade_confirmation(token) if 'get_trade_confirmation' in globals() else None
    if not conf:
        await query.answer("Токен недействителен или просрочен.", show_alert=True)
        return

    now = time.time()
    if now > float(conf["expires_at"]):
        if 'delete_trade_confirmation' in globals():
            delete_trade_confirmation(token)
        await query.answer("Срок подтверждения истёк.", show_alert=True)
        return

    uid = str(query.from_user.id)
    if uid != str(conf["uid"]):
        await query.answer("Вы не можете подтвердить эту сделку.", show_alert=True)
        return

    order_id = int(conf["order_id"])
    amount = float(conf["amount"])

    order = get_order_by_id(order_id)
    if not order:
        if 'delete_trade_confirmation' in globals():
            delete_trade_confirmation(token)
        await query.answer("Заявка уже не найдена.", show_alert=True)
        return

    _, owner_id, order_type, price, amount_available = order

    # запрет самотрейда (доп. защита)
    if str(owner_id) == uid:
        if 'delete_trade_confirmation' in globals():
            delete_trade_confirmation(token)
        await query.answer("❗ Нельзя совершать сделку с собственной заявкой.", show_alert=True)
        return

    ok, reason = can_start_trade(uid) if 'can_start_trade' in globals() else (True, None)
    if not ok:
        if 'delete_trade_confirmation' in globals():
            delete_trade_confirmation(token)
        await query.answer(reason, show_alert=True)
        return

    try:
        await execute_trade(query, state, order_id, amount)
        if 'record_trade' in globals():
            record_trade(uid)
        if 'delete_trade_confirmation' in globals():
            delete_trade_confirmation(token)
    except Exception as e:
        await query.answer("Ошибка при выполнении сделки.", show_alert=True)
        print("execute_trade error:", e)


async def execute_trade(event, state: FSMContext, order_id: int, take_amount: float):
    # message / user extraction
    message = event.message if hasattr(event, "message") else event
    user = event.from_user if hasattr(event, "from_user") else message.from_user

    async def safe_answer(text=None, show_alert: bool = False):
        if not text:
            return
        if hasattr(event, "answer"):
            await event.answer(text, show_alert=show_alert)
        else:
            await message.answer(text)

    order = get_order_by_id(order_id)
    if not order:
        await safe_answer("Заявка не найдена.", show_alert=True)
        await state.clear()
        return

    oid, owner_id, order_type, price, amount_available = order
    taker_id = str(user.id)

    # guards
    take_amount = float(take_amount)
    amount_available = float(amount_available)
    if take_amount <= 0:
        await safe_answer("Неверное количество.", show_alert=True)
        return
    if take_amount > amount_available + 1e-9:
        await safe_answer("Запрошено больше, чем доступно.", show_alert=True)
        return

    total_cost = float(price) * take_amount

    if order_type == "sell":
        # order owner = продавець (вони вже мали locked_GGs)
        seller_id = str(owner_id)
        buyer_id = taker_id

        # Завантажуємо обох (один раз кожного)
        buyer_data = await load_data(buyer_id) or {}
        seller_data = await load_data(seller_id) or {}

        # Перевірка купівельної спроможності
        if float(buyer_data.get("coins", 0)) < total_cost - 1e-9:
            await safe_answer("❗ У вас недостаточно mDrops.", show_alert=True)
            return

        # Переводи:
        # - у покупця списуємо coins і додаємо GGs
        buyer_data["coins"] = float(buyer_data.get("coins", 0)) - total_cost
        buyer_data["GGs"] = float(buyer_data.get("GGs", 0)) + take_amount

        # - у продавця зменшуємо reserved (locked_GGs) і додаємо coins
        locked = float(seller_data.get("locked_GGs", 0))
        # на всякий випадок -- не робимо negative
        seller_data["locked_GGs"] = max(0.0, locked - take_amount)
        seller_data["coins"] = float(seller_data.get("coins", 0)) + total_cost

        # Зберігаємо обидва
        await save_data(buyer_id, buyer_data)
        await save_data(seller_id, seller_data)

        # Оновлюємо заявку
        new_amount = amount_available - take_amount
        update_order_amount(oid, new_amount)

        text = f"✅ Вы купили {format_balance(take_amount)} GGs за {format_balance(total_cost)} mDrops."
        try:
            await message.edit_text(text, reply_markup=get_back_kb())
        except Exception:
            await message.answer(text, reply_markup=get_back_kb())

        await state.clear()
        await safe_answer()

    elif order_type == "buy":
        # order owner = покупець (вони вже мали locked_coins)
        buyer_id_reserved = str(owner_id)  # той, хто створив BUY заявку (резервував coins)
        seller_id = taker_id  # теперішній користувач — продавець

        seller_data = await load_data(seller_id) or {}
        buyer_data = await load_data(buyer_id_reserved) or {}

        # Перевіряємо, чи є у продавця достатньо GGs (не locked, бо продавець не резервував)
        if float(seller_data.get("GGs", 0)) < take_amount - 1e-9:
            await safe_answer("❗ У вас недостаточно GGs.", show_alert=True)
            return

        # Переводи:
        # - у продавця списуємо GGs і додаємо coins
        seller_data["GGs"] = float(seller_data.get("GGs", 0)) - take_amount
        seller_data["coins"] = float(seller_data.get("coins", 0)) + total_cost

        # - у покупця зменшуємо locked_coins і додаємо GGs
        locked_coins = float(buyer_data.get("locked_coins", 0))
        buyer_data["locked_coins"] = max(0.0, locked_coins - total_cost)
        buyer_data["GGs"] = float(buyer_data.get("GGs", 0)) + take_amount

        # Зберігаємо обох
        await save_data(seller_id, seller_data)
        await save_data(buyer_id_reserved, buyer_data)

        # Оновлюємо заявку
        new_amount = amount_available - take_amount
        update_order_amount(oid, new_amount)

        text = f"✅ Вы продали {format_balance(take_amount)} GGs и получили {format_balance(total_cost)} mDrops."
        try:
            await message.edit_text(text, reply_markup=get_back_kb())
        except Exception:
            await message.answer(text, reply_markup=get_back_kb())

        await state.clear()
        await safe_answer()

    else:
        await safe_answer("Неподдерживаемый тип заявки.", show_alert=True)
        await state.clear()


# -------------- GAMES -------------- #

@dp.callback_query(F.data.startswith("games_page3:"))
async def games_second_page(query: CallbackQuery):
    uid = query.from_user.id
    caller_id = query.data.split(":")[1]

    if int(uid) != int(caller_id):
        return query.answer("Это не твоя кнопка!")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="◀️ Страница 2", callback_data=f"games_page2:{uid}")]])
    await query.message.edit_text(
        f"🎮 {html.italic(await gsname(query.from_user.first_name, query.from_user.id))}, список игр (страница 3):\n{gline()}\n"
        f" • 🛕 {html.bold('Башня')}. Пример: башня 1к\n"
        f"<i>9 уровней, множитель в конце - х7.1</i>\n\n"
        f" • 📦 {html.bold('Сундуки')}. Пример: сундуки 1к\n"
        f"<i>4 варианта и 1 верный</i>\n\n"
        f" • 🪙 {html.bold('Монетка')}. Пример: монетка 10к\n"
        f"<i>Орел или решка?</i>\n\n"
        f" • 🎰 {html.bold('Слоты')}. Пример: слоты 2.5к\n"
        f"<i>Малый шанс - большой выигрыш!</i>\n\n", reply_markup=kb)


@dp.callback_query(F.data.startswith("callback_games_from_help:"))
async def handle_callback_games_question(callback: CallbackQuery):
    uid = callback.from_user.id
    ci = callback.data.split(":")[1]
    if int(uid) != int(ci):
        return callback.answer("Это не твоя кнопка!")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="▶️ Страница 2", callback_data=f"games_page2:{uid}")],
                         [InlineKeyboardButton(text="📕 Помощь", callback_data=f"help_callback:{uid}")]])
    await callback.message.edit_text(
        f"🎮 {html.italic(await gsname(callback.from_user.first_name, callback.from_user.id))}, список игр:\n{gline()}\n"
        f" • 🎰 {html.bold('Рулетка')}. Пример: рул 500 кра\n"
        f"<i>Типы ставок: чел, нечет, кра, чер, зел, зеро, 0-11, 12-22, 23-35</i>\n\n"
        f" • 🚀 {html.bold('Краш')}. Пример: краш 200 3\n"
        f"<i>Не стоит ставить на 10)</i>\n\n"
        f" • ⛏️ {html.bold('Золото')}. Пример: золото 2.5к\n"
        f"<i>50 на 50</i>\n\n"
        f" • 💣 {html.bold('Мины')}. Пример: мины 250 3\n"
        f"<i>Можно выбирать количество мин от 1 до 6, и поставить после ставки</i>\n\n"
        f" • 💠 {html.bold('Алмазы')}. Пример: алмазы 5к 1\n"
        f"<i>Максимальное количество мин: 2</i>\n\n", reply_markup=kb)


@dp.callback_query(F.data.startswith("callback_games:"))
async def handle_callback_games_question(callback: CallbackQuery):
    uid = callback.from_user.id
    ci = callback.data.split(":")[1]
    if int(uid) != int(ci):
        return callback.answer("Это не твоя кнопка!")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="▶️ Страница 2", callback_data=f"games_page2:{uid}")],
                         [InlineKeyboardButton(text="📕 Помощь", callback_data=f"help_callback:{uid}")]])
    await callback.message.edit_text(
        f"🎮 {html.italic(await gsname(callback.from_user.first_name, callback.from_user.id))}, список игр:\n{gline()}\n"
        f" • 🎰 {html.bold('Рулетка')}. Пример: рул 500 кра\n"
        f"<i>Типы ставок: чел, нечет, кра, чер, зел, зеро, 0-11, 12-22, 23-35</i>\n\n"
        f" • 🚀 {html.bold('Краш')}. Пример: краш 200 3\n"
        f"<i>Не стоит ставить на 10)</i>\n\n"
        f" • ⛏️ {html.bold('Золото')}. Пример: золото 2.5к\n"
        f"<i>50 на 50</i>\n\n"
        f" • 💣 {html.bold('Мины')}. Пример: мины 250 3\n"
        f"<i>Можно выбирать количество мин от 1 до 6, и поставить после ставки</i>\n\n"
        f" • 💠 {html.bold('Алмазы')}. Пример: алмазы 5к 1\n"
        f"<i>Максимальное количество мин: 2</i>\n\n", reply_markup=kb)


@dp.message(F.text.lower().in_(["игры", "/games", "играть", "/game", "/game@swagametrbot", "/games@swagametrbot"]))
async def handle_games_question(message: Message):
    uid = message.from_user.id
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="▶️ Страница 2", callback_data=f"games_page2:{uid}")]])
    await message.reply(
        f"🎮 {html.italic(await gsname(message.from_user.first_name, message.from_user.id))}, список игр (страница 1):\n{gline()}\n"
        f" • 🎰 {html.bold('Рулетка')}. Пример: рул 500 кра\n"
        f"<i>Типы ставок: чел, нечет, кра, чер, зел, зеро, 0-11, 12-22, 23-35</i>\n\n"
        f" • 🚀 {html.bold('Краш')}. Пример: краш 200 3\n"
        f"<i>Не стоит ставить на 10)</i>\n\n"
        f" • ⛏️ {html.bold('Золото')}. Пример: золото 2.5к\n"
        f"<i>50 на 50</i>\n\n"
        f" • 💣 {html.bold('Мины')}. Пример: мины 250 3\n"
        f"<i>Можно выбирать количество мин от 1 до 6, и поставить после ставки</i>\n\n"
        f" • 💠 {html.bold('Алмазы')}. Пример: алмазы 5к 1\n"
        f"<i>Максимальное количество мин: 2</i>\n\n", reply_markup=kb)


@dp.callback_query(F.data.startswith("games_page2:"))
async def games_second_page(query: CallbackQuery):
    uid = query.from_user.id
    caller_id = query.data.split(":")[1]

    if int(uid) != int(caller_id):
        return query.answer("Это не твоя кнопка!")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Страница 1", callback_data=f"callback_games:{uid}"),
         InlineKeyboardButton(text="▶️ Страница 3", callback_data=f"games_page3:{uid}")]])
    await query.message.edit_text(
        f"🎮 {html.italic(await gsname(query.from_user.first_name, query.from_user.id))}, список игр (страница 2):\n{gline()}\n"
        f" • 🎳 {html.bold('Боул')} (боулинг). Пример: боул 500\n"
        f"<i>Игра между игроками</i>\n\n"
        f" • 🔫 {html.bold('РР')} (русская рулетка). Пример: рр 3к\n"
        f"<i>С каждым выстрелом множитель и шанс поражения увеличиваются пропорционально</i>\n\n"
        f" • 🏀 {html.bold('Баскетболл')}. Пример: баск 2.5к\n"
        f"<i>Проверим твою меткость?</i>\n\n"
        f" • 🎲 {html.bold('Кубик')}. Пример: кубик 10к нечет\n"
        f"<i>Чет/нечет - х1.9, числа 1-6 - х3.5</i>\n\n"
        f" • ⚔️ {html.bold('Дуэль')}. Пример: дуэль 5к\n"
        f"<i>Бросайте кубики, победитель забирает все!</i>\n\n", reply_markup=kb)


@dp.message(F.text.lower().startswith("краш"))
async def handle_crash(message: types.Message):
    try:
        text = message.text.strip()
        args = text.split()
        if len(args) != 3:
            return await message.reply(
                "❌ Использование: краш (сумма) (множитель)\nПример: краш 100 2.5"
            )

        user_id = str(message.from_user.id)
        data = await load_data(user_id)

        if not data:
            await create_user_data(user_id)
            data = await load_data(user_id)

        balance = data["coins"]

        try:
            if args[1].lower() in ["все", "всё"]:
                amount = balance
            else:
                amount = args[1]
                amount = parse_bet_input(amount)
            multiplier = float(args[2])
        except ValueError:
            return await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n"
                f"{html.code(gline())}\n<b>Пример:</b> {html.code('краш 100 2.5')}\n<b>Пример:</b> {html.code('краш все 5')}"
            )

        if amount <= 9:
            return await message.reply("⚠️ Минимальная ставка - 10 mDrops")
        if balance < amount:
            return await message.reply("❌ Недостаточно денег для этой ставки!")
        if multiplier < 1.01 or multiplier > 10:
            return await message.reply("❌ Точка краша должна быть от 1.01 до 10!")

        # Нормализуем amount в int (на случай, если parse вернул str)
        amount = int(parse_bet_input(amount))

        # Списываем ставку заранее
        data["coins"] -= amount

        # ----------------- НОВАЯ ЛОГИКА РАСПРЕДЕЛЕНИЯ -----------------
        # Цель: чаще низкие краши, реже большие — игроки иногда выигрывают, но в долгой перспективе проигрывают.
        r = random.random()

        if r < 0.15:
            # 15% — миттєвий краш (1.00)
            crash_multiplier = 1.00
        elif r < 0.65:
            # 70% — 1.01–1.99
            crash_multiplier = round(random.uniform(1.01, 1.99), 2)
        elif r < 0.85:
            # 10% — 2.00–2.99
            crash_multiplier = round(random.uniform(2.00, 2.99), 2)
        elif r < 0.95:
            # 4% — 3.00–5.99
            crash_multiplier = round(random.uniform(3.00, 5.99), 2)
        else:
            # 1% — 6.00–10.00
            crash_multiplier = round(random.uniform(6.00, 10.00), 2)
        # ---------------------------------------------------------------

        # Опционально: если хочешь задержку/анимацию, можно поставить asyncio.sleep(1) перед ответом

        if crash_multiplier >= multiplier:
            win = round(amount * multiplier)
            data["coins"] += win
            result_text = (
                f"🚀 Результат: {crash_multiplier}x\n"
                f"✅ Вы выиграли {format_balance(win)} mDrops (ставка: {format_balance(amount)} mDrops × {multiplier}x)"
            )
            data["won_coins"] = data.get("won_coins", 0) + int(amount)
        else:
            result_text = (
                f"💥 Результат: {crash_multiplier}x\n"
                f"❌ Вы проиграли {format_balance(amount)} mDrops (цель: {multiplier}x)"
            )
            data["lost_coins"] = data.get("lost_coins", 0) + int(amount)

        await save_data(user_id, data)
        await message.reply(result_text)
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 116)


@dp.message(F.text.lower().startswith("кости"))
async def play_dice(message: Message):
    try:
        user_id = str(message.from_user.id)
        parts = message.text.strip().lower().split()

        # Ожидаем формат: кости <количество> <м|б|равно>
        if len(parts) != 3:
            return await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n{html.code("➼ ➼ ➼ ➼ ➼ ➼ ➼ ➼ ➼ ➼ ➼")}\n<b>Пример:</b> {html.code('кости 250 м')}\n<b>Пример:</b> {html.code('кости все равно')}")

        # Получаем баланс из глобального data
        data = await load_data(user_id)

        if not data:
            await create_user_data(user_id)
            data = await load_data(user_id)

        balance = data["coins"]
        bet = 0
        # Парсим ставку
        bet_str = parts[1]
        if bet_str in ["все", "всё"]:
            bet = balance
        elif "к" in bet_str:
            bet = parse_bet_input(bet_str)
        elif parts[1].isdigit():
            bet = int(parts[1])
        else:
            return await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n{gline()}\n<b>Пример:</b> {html.code('кости 250 м')}\n<b>Пример:</b> {html.code('кости все равно')}")

        if bet <= 0 or balance < bet:
            return await message.reply("❌ Недостаточно средств!")

        # Парсим выбор «м», «б» или «равно»
        choice = parts[2]
        if choice not in ["м", "б", "равно"]:
            return await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n{gline()}\n<b>Пример:</b> {html.code('кости 250 м')}\n<b>Пример:</b> {html.code('кости все равно')}")

        # Выполняем бросок
        d1_raw = await message.reply_dice(emoji="🎲")
        d2_raw = await message.reply_dice(emoji="🎲")
        d1 = d1_raw.dice.value
        d2 = d2_raw.dice.value
        total = d1 + d2

        # Снимаем ставку
        data["coins"] = balance - bet

        result_lines = [f"🎲 Кости: {d1} + {d2} = {total}"]
        win = 0

        # Проверяем условие выигрыша
        if choice == "м" and total < 7:
            win = round(bet * 2.25)
            result_lines.append(f"✅ Вы угадали «меньше»! Вы выиграли {format_balance(win)} mDrops")
            data["won_coins"] += int(bet)
        elif choice == "б" and total > 7:
            win = round(bet * 2.25)
            result_lines.append(f"✅ Вы угадали «больше»! Вы выиграли {format_balance(win)} mDrops")
            data["won_coins"] += int(bet)
        elif choice == "равно" and total == 7:
            win = round(bet * 5)
            result_lines.append(f"🎯 Вы угадали «равно»! Вы выиграли {format_balance(win)} mDrops")
            data["won_coins"] += int(bet)
        else:
            result_lines.append(f"❌ Вы проиграли {format_balance(bet)} mDrops.")
            data["lost_coins"] += int(bet)

        # Начисляем выигрыш, если он есть
        if win > 0:
            data["coins"] += win

        # Сохраняем изменения глобального data
        await save_data(user_id, data)
        await asyncio.sleep(4)
        await message.reply("\n".join(result_lines))
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 117)


# Персональні блокування для обробки івентів по кожному гравцю (в пам'яті процесу)
user_gold_locks = {}  # user_id -> asyncio.Lock()


def _get_user_lock(user_id):
    """Отримати або створити asyncio.Lock для user_id (лениво, без явних імпортів)."""
    lock = user_gold_locks.get(user_id)
    if lock is None:
        lock = __import__("asyncio").Lock()
        user_gold_locks[user_id] = lock
    return lock


def generate_game_id():
    try:
        return __import__("uuid").uuid4().hex
    except Exception:
        generate_game_id._cnt = getattr(generate_game_id, "_cnt", 0) + 1
        return f"g{generate_game_id._cnt}"


# ---- Рендери ----
def render_gold_game(game):
    stake = int(game["stake"])
    level = int(game["current_level"])
    levels = len(GOLD_MULTIPLIERS)

    current_multiplier = GOLD_MULTIPLIERS[level - 1] if level > 0 and (level - 1) < levels else 0
    next_multiplier = GOLD_MULTIPLIERS[level] if level < levels else GOLD_MULTIPLIERS[-1]

    current_amount = int(round(stake * current_multiplier))
    next_amount = int(round(stake * next_multiplier))

    rows = []
    for i in reversed(range(levels)):
        if i < len(game["path"]):
            left = "💰" if game["path"][i] == 0 else "❓"
            right = "💰" if game["path"][i] == 1 else "❓"
        else:
            left = right = "❓"

        value = f" {format_balance(int(round(stake * GOLD_MULTIPLIERS[i])))}"
        rows.append(f"|{left}|{right}|{value} mDrops ({GOLD_MULTIPLIERS[i]}x)")

    text = (
            "Выбери ячейку чтобы продолжить игру Золото!\n"
            f"{gline()}\n"
            f"💰 Текущий приз: {current_multiplier}x / {format_balance(current_amount)} mDrops\n"
            f"⚡️ Следующая ячейка: {next_multiplier}x / {format_balance(next_amount)} mDrops\n\n"
            + "\n".join(rows)
    )
    return text


async def render_gold_result(game, lost=False, collected=False, winnings=None, multiplier=None):
    stake = int(game["stake"])
    bad_cells = game.get("bad_cells", [])
    path = game.get("path", [])
    levels = len(GOLD_MULTIPLIERS)

    rows = []
    for i in reversed(range(levels)):
        bad = bad_cells[i] if i < len(bad_cells) else 0
        value = f" {format_balance(int(round(stake * GOLD_MULTIPLIERS[i])))}"

        if i < len(path):
            picked = path[i]
            if picked == bad:
                # вибрана бомба
                left = "💥" if picked == 0 else "💸"
                right = "💸" if picked == 0 else "💥"
            else:
                left = "💰" if picked == 0 else "🧨"
                right = "💰" if picked == 1 else "🧨"
        else:
            # ВАЖЛИВО: коли програш — індекс програної клітинки = len(path) - 1
            if lost and i == len(path) - 1:
                # покажемо саме ту бомбу, яка вибилась
                if bad == 0:
                    left, right = "💥", "💸"
                else:
                    left, right = "💸", "💥"
            else:
                left = "💸" if bad == 0 else "🧨"
                right = "💸" if bad == 1 else "🧨"

        rows.append(f"|{left}|{right}| {value} mDrops ({GOLD_MULTIPLIERS[i]}x)")

    if lost:
        head = "💥 Ты проиграл!\nПопробуй еще раз!"
        plyr_data = await load_data(game["player_id"])
        plyr_data["lost_coins"] = int(plyr_data.get("lost_coins", 0)) + int(game["stake"])
        # видаляємо гру після поразки
        if "gold_games" in plyr_data:
            # якщо використовуєте gold_games -> видаляємо по game_id (якщо є)
            gid = game.get("game_id")
            if gid and gid in plyr_data["gold_games"]:
                del plyr_data["gold_games"][gid]
            # сумісність зі старим полем:
            if "gold_game" in plyr_data:
                del plyr_data["gold_game"]
        await save_data(game["player_id"], plyr_data)
    elif collected:
        head = f"🤑 Ты забрал выигрыш!\nСтавка: {format_balance(stake)} mDrops\nВыигрыш: {format_balance(winnings)} mDrops (x{multiplier})"
        plyr_data = await load_data(game["player_id"])
        if "gold_games" in plyr_data:
            gid = game.get("game_id")
            if gid and gid in plyr_data["gold_games"]:
                del plyr_data["gold_games"][gid]
            if "gold_game" in plyr_data:
                del plyr_data["gold_game"]
        await save_data(game["player_id"], plyr_data)
    else:
        head = "🎮 Результаты игры:"

    return head + f"\n{gline()}\n" + "\n".join(rows)


# ---- Клавіатура (повертає InlineKeyboardMarkup; припускається, що InlineKeyboardMarkup/InlineKeyboardButton доступні) ----
def gold_keyboard(player_id: str, game_id: str, level: int):
    """
    Повертає InlineKeyboardMarkup (створіть InlineKeyboardButton з callback_data у вигляді:
    gold_choose:{player_id}:{game_id}:{level}:{choice}, gold_collect:{player_id}:{game_id}, gold_cancel:{player_id}:{game_id})
    """
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

    if level == 0:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="❓", callback_data=f"gold_choose:{player_id}:{game_id}:{level}:0"),
                    InlineKeyboardButton(text="❓", callback_data=f"gold_choose:{player_id}:{game_id}:{level}:1")
                ],
                [
                    InlineKeyboardButton(text="❌ Отменить", callback_data=f"gold_cancel:{player_id}:{game_id}")
                ]
            ]
        )
    else:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="❓", callback_data=f"gold_choose:{player_id}:{game_id}:{level}:0"),
                    InlineKeyboardButton(text="❓", callback_data=f"gold_choose:{player_id}:{game_id}:{level}:1")
                ],
                [
                    InlineKeyboardButton(text="💰 Забрать приз", callback_data=f"gold_collect:{player_id}:{game_id}")
                ]
            ]
        )


# ---- Обробники (вставте як handlers в вашому боті) ----
@dp.message(F.text.lower().startswith("золото"))
async def start_gold_game_handler(message: Message):
    try:
        parts = (message.text or "").strip().split()
        user_id = str(message.from_user.id)
        data = await load_data(user_id)

        if len(parts) != 2:
            return await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n{gline()}\n<b>Пример:</b> {html.code('золото 25к')}\n<b>Пример:</b> {html.code('золото все')}")
        # парсим ставку
        arg = parts[1].lower()
        if arg in ["все", "всё"]:
            stake = int(data.get("coins", 0))
        elif "к" in arg or arg.endswith("k"):
            stake = parse_bet_input(arg)
        elif arg.replace(".", "", 1).isdigit():
            stake = int(float(arg))
        else:
            await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n{gline()}\n<b>Пример:</b> {html.code('золото 25к')}\n<b>Пример:</b> {html.code('золото все')}")

        if stake <= 0 or stake > int(data.get("coins", 0)):
            return await message.reply("❗ Недостаточно средств!")

        levels = len(GOLD_MULTIPLIERS)
        if levels <= 0:
            return await message.reply("⚠️ Игра временно недоступна (разраб даун).")

        rnd = __import__("random")
        bad_cells = [rnd.randint(0, 1) for _ in range(levels)]
        game_id = generate_game_id()

        game_data = {
            "game_id": game_id,
            "stake": int(stake),
            "bad_cells": bad_cells,
            "current_level": 0,
            "path": [],
            "state": "playing",
            "player_id": user_id,
            "started_at": int(__import__("time").time())
        }

        # списуємо ставку і зберігаємо гру у словнику gold_games
        data["coins"] = int(data.get("coins", 0)) - int(stake)
        if "gold_games" not in data or not isinstance(data["gold_games"], dict):
            data["gold_games"] = {}
        data["gold_games"][game_id] = game_data
        await save_data(user_id, data)

        msg_wait = await message.reply("🔗 Игра загружается, ожидайте...")
        game = await message.reply(
            render_gold_game(game_data),
            reply_markup=gold_keyboard(user_id, game_id, game_data["current_level"])
        )
        await asyncio.sleep(0.5)
        if game:
            return await msg_wait.delete()
        for i in range(8):
            await asyncio.sleep(0.5)
            if game:
                return await msg_wait.delete()
        await asyncio.sleep(1)
        if not game:
            await msg_wait.edit_text("❌ Ошибка при запуске игры, ставка возвращена")
            data["coins"] += stake
            await save_data(user_id, data)

    except Exception as e:
        await handle_error(getattr(message.from_user, "username", None), e, message.from_user.id, 118)


@dp.callback_query(F.data.startswith("gold_choose"))
@flood_protect(min_delay=0.5)
async def handle_gold_choice(callback: CallbackQuery):
    try:
        parts = (callback.data or "").split(":")  # ["gold_choose", player_id, game_id, level, choice]
        if len(parts) != 5:
            return await callback.answer("⚠️ Неверные данные.", show_alert=True)

        _, player_id, game_id, level_str, choice_str = parts
        user_id = str(callback.from_user.id)

        if user_id != player_id:
            return await callback.answer("❌ Это не твоя игра или ты уже проиграл!", show_alert=True)

        lock = _get_user_lock(player_id)
        async with lock:
            data = await load_data(player_id)
            games = data.get("gold_games", {}) or {}
            game = games.get(game_id)

            if not game or game.get("state") != "playing":
                return await callback.answer("Игра не активна.", show_alert=True)

            try:
                choice = int(choice_str)
                level = int(level_str)
            except ValueError:
                return await callback.answer("⚠️ Ошибка данных.", show_alert=True)

            if game["current_level"] != level:
                return await callback.answer("Неверный уровень игры.", show_alert=True)

            levels = len(GOLD_MULTIPLIERS)
            if level < 0 or level >= levels:
                return await callback.answer("⚠️ Неверный уровень.", show_alert=True)

            # перевірка на бомбу
            if game["bad_cells"][level] == choice:
                game["state"] = "lost"
                game["path"].append(choice)
                data["lost_coins"] = int(data.get("lost_coins", 0)) + int(game["stake"])
                # видаляємо гру
                if "gold_games" in data and game_id in data["gold_games"]:
                    del data["gold_games"][game_id]
                await save_data(player_id, data)
                return await callback.message.edit_text(await render_gold_result(game, lost=True))

            # вдалий вибір
            game["path"].append(choice)
            game["current_level"] += 1

            # перемога — пройшов всі рівні
            if game["current_level"] >= levels:
                game["state"] = "won"
                multiplier = GOLD_MULTIPLIERS[levels - 1]
                winnings = int(round(game["stake"] * multiplier))
                data["coins"] = int(data.get("coins", 0)) + int(winnings)
                data["won_coins"] = int(data.get("won_coins", 0)) + int(winnings)
                # видаляємо гру після виграшу
                if "gold_games" in data and game_id in data["gold_games"]:
                    del data["gold_games"][game_id]
                await save_data(player_id, data)
                return await callback.message.edit_text(
                    f"🎉 Ты прошел все уровни!\nВыигрыш: {format_balance(winnings)} mDrops"
                )

            # продовжуємо гру
            data["gold_games"][game_id] = game
            await save_data(player_id, data)
            await callback.message.edit_text(
                render_gold_game(game),
                reply_markup=gold_keyboard(player_id, game_id, game["current_level"])
            )

    except Exception as e:
        await handle_error(getattr(callback.from_user, "username", None), e, callback.from_user.id, 119)


@dp.callback_query(F.data.startswith("gold_collect"))
@flood_protect(min_delay=0.5)
async def collect_prize(callback: CallbackQuery):
    try:
        parts = (callback.data or "").split(":")
        if len(parts) != 3:
            return await callback.answer("⚠️ Неверные данные.", show_alert=True)
        _, player_id, game_id = parts
        user_id = str(callback.from_user.id)

        if user_id != player_id:
            return await callback.answer("Это не твоя кнопка!", show_alert=True)

        lock = _get_user_lock(player_id)
        async with lock:
            data = await load_data(player_id)
            games = data.get("gold_games", {}) or {}
            game = games.get(game_id)

            if not game or game.get("state") != "playing":
                return await callback.answer("Игра завершена.", show_alert=True)

            level = int(game["current_level"])
            if level <= 0:
                return await callback.answer("⚠️ Слишком рано — нужно сделать хотя бы один ход.", show_alert=True)

            multiplier = GOLD_MULTIPLIERS[level - 1]
            winnings = int(round(game["stake"] * multiplier))

            data["coins"] = int(data.get("coins", 0)) + int(winnings)
            data["won_coins"] = int(data.get("won_coins", 0)) + int(winnings)
            game["state"] = "collected"

            if "gold_games" in data and game_id in data["gold_games"]:
                del data["gold_games"][game_id]
            await save_data(player_id, data)

            return await callback.message.edit_text(
                await render_gold_result(game, collected=True, winnings=winnings, multiplier=multiplier))
    except Exception as e:
        await handle_error(getattr(callback.from_user, "username", None), e, callback.from_user.id, 120)


@dp.callback_query(F.data.startswith("gold_cancel"))
@flood_protect(min_delay=0.5)
async def cancel_gold_game(callback: CallbackQuery):
    try:
        parts = (callback.data or "").split(":")
        if len(parts) != 3:
            return await callback.answer("⚠️ Неверные данные.", show_alert=True)
        _, player_id, game_id = parts
        user_id = str(callback.from_user.id)

        if user_id != player_id:
            return await callback.answer("❌ Это не твоя игра или ты уже проиграл!", show_alert=True)

        lock = _get_user_lock(player_id)
        async with lock:
            data = await load_data(player_id)
            games = data.get("gold_games", {}) or {}
            game = games.get(game_id)

            if not game or game.get("state") != "playing":
                return await callback.answer("Игра уже завершена.", show_alert=True)

            # повертаємо ставку і видаляємо гру
            data["coins"] = int(data.get("coins", 0)) + int(game["stake"])
            game["state"] = "cancelled"
            if "gold_games" in data and game_id in data["gold_games"]:
                del data["gold_games"][game_id]
            await save_data(player_id, data)

            await callback.message.edit_text("❌ Игра отменена. Ставку вернули на баланс.")
    except Exception as e:
        await handle_error(getattr(callback.from_user, "username", None), e, callback.from_user.id, 121)


user_tower_locks: Dict[str, asyncio.Lock] = {}


def _get_tower_lock(user_id: str) -> asyncio.Lock:
    lock = user_tower_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_tower_locks[user_id] = lock
    return lock


# --- допоміжні функції ---

def _migrate_single_tower_to_multi(data: dict) -> None:
    """Якщо у data є старий ключ 'tower', мігруємо його в 'towers' з новим id."""
    if not data:
        return
    if 'towers' not in data and 'tower' in data:
        single = data.pop('tower')
        gid = uuid.uuid4().hex[:8]
        data.setdefault('towers', {})[gid] = single


# ---------- START GAME ----------
async def start_tower_game(user_id: str, bet: int, game_id: str = None) -> str:
    """
    Створюємо гру і повертаємо game_id (рядок). Якщо щось пішло не так — повертаємо пустий рядок.
    Більше не блокуємо старт нової гри якщо вже є активні — можна мати будь-яку кількість ігор.
    """
    if bet is None:
        raise ValueError("bet is required")

    data = await load_data(user_id) or {}

    # міграція для сумісності
    _migrate_single_tower_to_multi(data)

    bombs: List[List[int]] = []
    for _ in range(9):
        row = [0] * 5
        bomb_index = random.randint(0, 4)
        row[bomb_index] = 1
        bombs.append(row)

    game_id = game_id or uuid.uuid4().hex[:8]

    tower = {
        "bet": int(bet),
        "level": 0,
        "bombs": bombs,
        "selected": [],
        "lost": False,
        "state": "playing",
        "started_at": int(time.time()),
        "last_action_ts": 0,
        "game_id": game_id,
    }

    data.setdefault('towers', {})[game_id] = tower
    await save_data(user_id, data)
    return game_id


# ---------- BUILD KEYBOARD ----------
async def build_tower_keyboard(user_id: str, game_id: str) -> InlineKeyboardMarkup:
    data = await load_data(user_id) or {}
    _migrate_single_tower_to_multi(data)
    towers = data.get('towers', {})
    tower = towers.get(game_id)
    if not tower:
        return InlineKeyboardMarkup(inline_keyboard=[])

    level = int(tower.get("level", 0))
    selected = tower.get("selected", [])
    lost = bool(tower.get("lost", False))

    if lost or level >= 9:
        return InlineKeyboardMarkup(inline_keyboard=[])

    kb_rows = []

    # Поточний рівень
    kb_rows.append([
        InlineKeyboardButton(text="❓", callback_data=f"tower:{game_id}:choose:{j}") for j in range(5)
    ])

    # Пройдені рівні (вгору)
    for i in range(level - 1, -1, -1):
        choice = selected[i] if i < len(selected) else None
        row_buttons = []
        for j in range(5):
            emoji = "💰" if (choice is not None and j == choice) else "❓"
            row_buttons.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        kb_rows.append(row_buttons)

    # Кнопки управління
    if level == 0:
        kb_rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"tower:{game_id}:cancel")])
    else:
        kb_rows.append([InlineKeyboardButton(text="🎁 Забрать приз", callback_data=f"tower:{game_id}:collect")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# ---------- BUILD FINAL ----------
async def build_final_tower_keyboard(user_id: str, game_id: str) -> InlineKeyboardMarkup:
    data = await load_data(user_id) or {}
    _migrate_single_tower_to_multi(data)
    towers = data.get('towers', {})
    tower = towers.get(game_id)
    if not tower:
        return InlineKeyboardMarkup(inline_keyboard=[])

    bombs = tower.get("bombs", [])
    selected = tower.get("selected", [])

    if tower.get("lost"):
        last = len(selected) - 1
    else:
        last = min(len(selected) - 1, 8)

    if last < 0:
        return InlineKeyboardMarkup(inline_keyboard=[])

    reveal_mines = tower.get("state") in ("collected", "won")

    kb_rows = []
    for i in range(last, -1, -1):
        row = bombs[i] if i < len(bombs) else [0, 0, 0, 0, 0]
        choice = selected[i] if i < len(selected) else None
        buttons_row = []
        for j in range(5):
            if tower.get("lost"):
                if row[j] == 1:
                    if choice is not None and j == choice:
                        emoji = "💥"
                    else:
                        emoji = "💣"
                else:
                    if choice is not None and j == choice:
                        emoji = "💰"
                    else:
                        emoji = "💼"
            elif reveal_mines:
                if row[j] == 1:
                    emoji = "💣"
                else:
                    if choice is not None and j == choice:
                        emoji = "💰"
                    else:
                        emoji = "💼"
            else:
                if choice is not None and j == choice:
                    emoji = "💰"
                else:
                    emoji = "💼"
            buttons_row.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        kb_rows.append(buttons_row)

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# ---------- UNIVERSAL CALLBACK HANDLER ----------
@dp.callback_query(F.data.startswith("tower:"))
@flood_protect(min_delay=0.5)
async def on_tower_callback(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    lock = _get_tower_lock(user_id)
    async with lock:
        try:
            parts = callback.data.split(":")
            # expected patterns:
            # tower:<game_id>:choose:<index>
            # tower:<game_id>:collect
            # tower:<game_id>:cancel
            # tower:<game_id>:noop (or noop by itself)
            if len(parts) < 3:
                # fallback for noop or malformed
                return await callback.answer()

            _, game_id, action = parts[0:3]
            extra = parts[3] if len(parts) > 3 else None

            data = await load_data(user_id) or {}
            _migrate_single_tower_to_multi(data)
            towers = data.get('towers', {})
            tower = towers.get(game_id)

            if not tower:
                await callback.answer("❌ Это не твоя игра или она уже завершена!", show_alert=True)
                return

            if tower.get('state') != 'playing':
                await callback.answer("⚠️ Игра уже завершена", show_alert=True)
                return

            # simple server-side throttle
            now = int(time.time() * 1000)
            last_ts = tower.get("last_action_ts", 0)
            if now - last_ts < 200:
                await callback.answer()
                return
            tower['last_action_ts'] = now

            # CANCEL
            if action == 'cancel':
                if int(tower.get('level', 0)) != 0 or tower.get('selected'):
                    return await callback.answer("⚠️ Нельзя отменить после первого хода", show_alert=True)
                bet = int(tower.get('bet', 0))
                data['coins'] = int(data.get('coins', 0)) + bet
                towers.pop(game_id, None)
                if not towers:
                    data.pop('towers', None)
                await save_data(user_id, data)
                return await callback.message.edit_text(
                    f"❌ Игра отменена, ставка {format_balance(bet)} mDrops возвращена.", reply_markup=None)

            # COLLECT
            if action == 'collect':
                if int(tower.get('level', 0)) == 0:
                    return await callback.answer("⚠️ Слишком рано, вы ещё не сделали ход!", show_alert=True)
                if tower.get('lost', False):
                    return await callback.answer("⚠️ Вы уже проиграли", show_alert=True)

                lvl = int(tower.get('level', 0))
                mult = TOWER_MULTIPLIERS[lvl - 1]
                win = round(int(tower.get('bet', 0)) * mult)

                tower['state'] = 'collected'
                data['coins'] = int(data.get('coins', 0)) + int(win)
                data['won_coins'] = int(data.get('won_coins', 0)) + int(win)
                towers[game_id] = tower
                # remove finished game
                towers.pop(game_id, None)
                if not towers:
                    data.pop('towers', None)
                await save_data(user_id, data)

                final_kb = await build_final_tower_keyboard(user_id, game_id)
                await callback.message.edit_text(f"💸 Вы забрали {format_balance(win)} mDrops!", reply_markup=final_kb)
                return

            # NOOP
            if action == 'noop':
                return await callback.answer()

            # OTHERWISE — action should be 'choose' with extra index OR numeric index (legacy)
            choice = None
            if action == 'choose' and extra is not None:
                try:
                    choice = int(extra)
                except Exception:
                    return await callback.answer("⚠️ Ошибка выбора", show_alert=True)
            else:
                # legacy: tower:<index> (not expected in new format)
                try:
                    choice = int(action)
                except Exception:
                    return await callback.answer("⚠️ Ошибка", show_alert=True)

            # валідації
            if choice is None or choice < 0 or choice > 4:
                return await callback.answer("⚠️ Неверный выбор", show_alert=True)

            level = int(tower.get('level', 0))
            if level < 0 or level > 8:
                return await callback.answer("⚠️ Неверный уровень игры", show_alert=True)

            if len(tower.get('selected', [])) > level:
                return await callback.answer("⚠️ Вы уже выбрали на этом уровне", show_alert=True)

            # додати вибір
            tower.setdefault('selected', []).append(choice)

            bombs = tower.get('bombs', [])
            if level >= len(bombs):
                tower['lost'] = True
                tower['state'] = 'lost'
                towers[game_id] = tower
                await save_data(user_id, data)
                await callback.message.edit_text("⚠️ Ошибка игры — неверный внутренний индекс.")
                # прибрати гру
                towers.pop(game_id, None)
                if not towers:
                    data.pop('towers', None)
                await save_data(user_id, data)
                return

            if bombs[level][choice] == 1:
                # поразка
                tower['lost'] = True
                tower['state'] = 'lost'
                data['lost_coins'] = int(data.get('lost_coins', 0)) + int(tower.get('bet', 0))
                towers[game_id] = tower
                await save_data(user_id, data)

                final_kb = await build_final_tower_keyboard(user_id, game_id)
                await callback.message.edit_text(
                    f"💥 Вы проиграли!\n\n💸 Ставка: {format_balance(int(tower.get('bet', 0)))} mDrops",
                    reply_markup=final_kb
                )
                # прибираємо гру
                towers.pop(game_id, None)
                if not towers:
                    data.pop('towers', None)
                await save_data(user_id, data)
                return

            # безпечна клітинка
            tower['level'] = level + 1
            towers[game_id] = tower
            data['towers'] = towers
            await save_data(user_id, data)

            # дійшли до вершини?
            if tower['level'] >= 9:
                mult = TOWER_MULTIPLIERS[8]
                win = round(int(tower.get('bet', 0)) * mult)
                if tower.get('state') == 'playing':
                    tower['state'] = 'won'
                    data['coins'] = int(data.get('coins', 0)) + int(win)
                    data['won_coins'] = int(data.get('won_coins', 0)) + int(win)
                    towers[game_id] = tower
                    await save_data(user_id, data)

                    final_kb = await build_final_tower_keyboard(user_id, game_id)
                    await callback.message.edit_text(
                        f"🏆 Поздравляем! Вы дошли до вершины и забрали {format_balance(win)} mDrops!",
                        reply_markup=final_kb
                    )

                # прибрати гру
                towers.pop(game_id, None)
                if not towers:
                    data.pop('towers', None)
                await save_data(user_id, data)
                return

            # інакше — показуємо наступний рівень
            new_level = tower['level']
            mult = TOWER_MULTIPLIERS[new_level - 1]
            await callback.message.edit_text(
                f"📈 Уровень: {new_level}/9\n"
                f"💰 Возможный выигрыш: {format_balance(int(tower.get('bet', 0) * mult))} mDrops\n"
                f"🔥 Множитель: x{mult:.1f}",
                reply_markup=await build_tower_keyboard(user_id, game_id)
            )

        except Exception as e:
            await handle_error(callback.from_user.username, e, callback.from_user.id, 201)


# ---------- START MESSAGE ----------
@dp.message(F.text.lower().startswith("башня"))
async def start_tower(message: Message):
    uid = str(message.from_user.id)
    args = message.text.split()

    if len(args) != 2:
        return await message.reply(
            f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n<b>Пример:</b> {html.code('башня 300')}\n<b>Пример:</b> {html.code('башня все')}")

    user_data = await load_data(uid) or {"coins": 0}
    balance = int(user_data.get("coins", 0))

    # Ставка
    if args[1].lower() == "все":
        bet = balance
    elif "к" in args[1].lower():
        bet = parse_bet_input(args[1].lower())
    elif args[1].isdigit():
        bet = int(args[1])
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n<b>Пример:</b> {html.code('башня 300')}\n<b>Пример:</b> {html.code('башня все')}")

    if bet > balance:
        return await message.reply("❌ Недостаточно средств для ставки.")

    if bet <= 9:
        return await message.reply("⚠️ Минимальная ставка - 10 mDrops")

    # Забронювати монети
    user_data["coins"] = int(user_data.get("coins", 0)) - int(bet)
    await save_data(uid, user_data)

    game_id = await start_tower_game(uid, bet)
    if not game_id:
        # повернути ставку у випадку помилки
        user_data = await load_data(uid) or {}
        user_data["coins"] = int(user_data.get("coins", 0)) + int(bet)
        await save_data(uid, user_data)
        return await message.reply("⚠️ Не удалось начать игру. Попробуйте позже.")

    msg_wait = await message.reply("🔗 Игра загружается...")
    game = await message.reply(
        f"🎮 Игра «Башня» началась!\n"
        f"📈 Уровень: 1/9\n"
        f"💵 Ставка: {format_balance(bet)} mDrops\n",
        reply_markup=await build_tower_keyboard(uid, game_id)
    )
    await asyncio.sleep(0.5)
    if game:
        return await msg_wait.delete()
    for i in range(8):
        await asyncio.sleep(0.5)
        if game:
            return await msg_wait.delete()
    await asyncio.sleep(1)
    if not game:
        await msg_wait.edit_text("❌ Ошибка при запуске игры, ставка возвращена")
        user_data["coins"] += bet
        await save_data(uid, user_data)


# noop handler (на випадок, якщо callback_data == 'noop')
@dp.callback_query(F.data == "noop")
async def do_nothing(callback: CallbackQuery):
    await callback.answer()


MINES_GAMES: Dict[str, dict] = {}

# per-user locks to avoid race conditions (lock by owner id)
user_mines_locks: Dict[str, asyncio.Lock] = {}


def _get_mines_lock(user_id: str) -> asyncio.Lock:
    lock = user_mines_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_mines_locks[user_id] = lock
    return lock


# ---------------- multipliers ----------------
def get_multipliers_for_mines_count(mines_count: int, house_edge: float = 1.0) -> List[float]:
    CELLS = 25
    if not (1 <= mines_count <= 24):
        raise ValueError("mines_count must be between 1 and 24")
    if not (0.0 < house_edge <= 1.0):
        raise ValueError("house_edge must be in (0.0, 1.0]")

    safe_cells = CELLS - mines_count
    multipliers: List[float] = []
    p_survive = 1.0
    multipliers.append(round(1.0 * house_edge, 4))  # k = 0

    for k in range(1, safe_cells + 1):
        numerator = safe_cells - (k - 1)
        denominator = CELLS - (k - 1)
        if denominator <= 0:
            p_step = 0.0
        else:
            p_step = numerator / denominator
        p_survive *= p_step
        if p_survive <= 0:
            mult = float('inf')
        else:
            mult = (1.0 / p_survive) * house_edge
        multipliers.append(round(mult, 4))
    return multipliers


# ---------- helpers ----------
def _new_game_id() -> str:
    return uuid.uuid4().hex


def _cb_cell(game_id: str, idx: int) -> str:
    return f"mines:cell:{game_id}:{idx}"


def _cb_collect(game_id: str) -> str:
    return f"mines:collect:{game_id}"


def _cb_cancel(game_id: str) -> str:
    return f"mines:cancel:{game_id}"


# ---------------- start handler ----------------
@dp.message(F.text.lower().startswith("мины"))
async def start_mines(message: Message):
    try:
        parts = message.text.strip().split()
        if len(parts) < 2:
            return await message.reply(
                f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n"
                f"{gline()}\n"
                f"<b>Пример:</b> {html.code('мины 300')}\n"
                f"<b>Пример:</b> {html.code('мины 300 3')}  # вторая цифра - количество мин (1-6)\n"
                f"<b>Пример:</b> {html.code('мины все')}"
            )

        user_id = str(message.from_user.id)
        data = await load_data(user_id) or {"coins": 0}
        balance = int(data.get("coins", 0))

        # Ставка
        bet_str = parts[1].lower()
        if bet_str in ("все", "всё"):
            bet = balance
        elif "к" in bet_str:
            bet = parse_bet_input(bet_str)
        elif bet_str.isdigit():
            bet = int(bet_str)
        else:
            return await message.reply("⚠️ Неверный формат ставки.")

        if bet < 10:
            return await message.reply("⚠️ Минимальная ставка - 10 mDrops")
        if bet > balance:
            return await message.reply("❌ Недостаточно средств на балансе!")

        # mines_count (опционально)
        mines_count = 1
        if len(parts) >= 3:
            try:
                mcnt = int(parts[2])
            except ValueError:
                return await message.reply("⚠️ Неверный формат количества мин. Используй число от 1 до 6.")
            if not (1 <= mcnt <= 6):
                return await message.reply("⚠️ Количество мин должно быть от 1 до 6.")
            mines_count = mcnt

        # reserve bet (списание) — как и раньше
        data["coins"] = int(data.get("coins", 0)) - int(bet)
        await save_data(user_id, data)

        msg_wait = await message.reply("🔗 Игра загружается, ожидайте")

        # створюємо поле та міни
        field = ["❓"] * 25
        mines = random.sample(range(25), mines_count)
        print(f"{user_id} (mines {mines_count}): {mines}")

        # створюємо нову гру в пам'яті
        game_id = _new_game_id()
        now_ms = int(time.time() * 1000)
        MINES_GAMES[game_id] = {
            "game_id": game_id,
            "bet": int(bet),
            "field": field,
            "opened": [],  # list of opened indices
            "mines": mines,  # list of mine indices (int)
            "mines_count": mines_count,
            "owner": user_id,
            "state": "playing",  # playing | lost | won | collected | cancelled
            "started_at": now_ms,
            "last_action_ts": 0
        }

        multipliers = get_multipliers_for_mines_count(mines_count)
        initial_multiplier = multipliers[1] if len(multipliers) > 1 else multipliers[0]
        potential_prize = round(bet * initial_multiplier)

        text = (
            "💣 Игра началась!\n"
            f"💵 Ставка: {format_balance(bet)} mDrops\n"
            f"💰 Возможный приз (за 1 открытие): {format_balance(potential_prize)} mDrops\n"
            f"📈 Количество мин: {mines_count}  |  x{round(initial_multiplier, 2)}"
        )

        keyboard = build_mines_keyboard(game_id)
        game = await message.reply(text, reply_markup=keyboard)
        await asyncio.sleep(0.5)
        if game:
            return await msg_wait.delete()
        for i in range(8):
            await asyncio.sleep(0.5)
            if game:
                return await msg_wait.delete()
        await asyncio.sleep(1)
        if not game:
            await msg_wait.edit_text("❌ Ошибка при запуске игры, ставка возвращена")
            data["coins"] += bet
            await save_data(user_id, data)

    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 300)


# ---------------- keyboard builder ----------------
def build_mines_keyboard(game_id: str) -> InlineKeyboardMarkup:
    game = MINES_GAMES.get(game_id)
    if not game:
        return InlineKeyboardMarkup(inline_keyboard=[])

    buttons = []
    for row in range(5):
        row_buttons = []
        for col in range(5):
            i = row * 5 + col
            text = game["field"][i]
            row_buttons.append(InlineKeyboardButton(text=text, callback_data=_cb_cell(game_id, i)))
        buttons.append(row_buttons)

    opened = len(game.get("opened", []))
    multipliers = get_multipliers_for_mines_count(game["mines_count"])
    idx = opened if opened < len(multipliers) else (len(multipliers) - 1)
    prize_next = round(game["bet"] * multipliers[idx])

    if opened > 0:
        buttons.append([InlineKeyboardButton(text=f"💰 Забрать ({format_balance(prize_next)} mDrops)",
                                             callback_data=_cb_collect(game_id))])
    else:
        buttons.append([InlineKeyboardButton(text="❌ Отменить", callback_data=_cb_cancel(game_id))])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ---------------- final keyboard builder ----------------
def build_final_mines_keyboard(field: List[str], include_noop: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for r in range(5):
        row_buttons = []
        for c in range(5):
            i = r * 5 + c
            txt = field[i]
            row_buttons.append(InlineKeyboardButton(text=txt, callback_data="noop"))
        rows.append(row_buttons)
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------- callback handler ----------------
@dp.callback_query(F.data.startswith("mines:"))
@flood_protect(min_delay=0.5)
async def handle_mines(callback: CallbackQuery):
    # format: mines:action:game_id[:idx]
    try:
        parts = callback.data.split(":")
        if len(parts) < 3:
            return await callback.answer("⚠️ Неверные данные.", show_alert=True)
        _, action, game_id = parts[:3]
        caller_id = str(callback.from_user.id)

        game = MINES_GAMES.get(game_id)
        if not game:
            return await callback.answer("⚠️ Игра не найдена или уже завершена.", show_alert=True)

        owner_id = game.get("owner")
        # якщо не власник — тихо відповідаємо
        if caller_id != owner_id:
            return await callback.answer("❌ Это не твоя игра или ты уже проиграл!", show_alert=True)

        lock = _get_mines_lock(owner_id)
        async with lock:
            # reload game inside lock
            game = MINES_GAMES.get(game_id)
            if not game or game.get("state") != "playing":
                return await callback.answer("⚠️ Игра не активна.", show_alert=True)

            data = await load_data(owner_id) or {"coins": 0, "won_coins": 0, "lost_coins": 0}

            # server-side quick throttle (200ms)
            now_ms = int(time.time() * 1000)
            last_ts = game.get("last_action_ts", 0)
            if now_ms - last_ts < 200:
                await callback.answer()
                return
            game["last_action_ts"] = now_ms

            bet = int(game["bet"])
            mines_count = int(game.get("mines_count", 1))
            mines_list = [int(x) for x in game.get("mines", [])]
            opened_list = [int(x) for x in game.get("opened", [])]
            multipliers = get_multipliers_for_mines_count(mines_count)

            # COLLECT
            if action == "collect":
                opened = len(opened_list)
                if opened == 0:
                    return await callback.answer("⚠️ Нечего забирать!")

                coef_index = opened if opened < len(multipliers) else (len(multipliers) - 1)
                coef = multipliers[coef_index]
                reward = round(bet * coef)

                # Побудуємо фінальне поле (показати мiни)
                final_field = ["❓"] * 25
                for i in opened_list:
                    final_field[i] = "💰"
                for m in mines_list:
                    final_field[m] = "💣"

                final_keyboard = build_final_mines_keyboard(final_field)

                # позначаємо collected і нараховуємо
                game["state"] = "collected"
                data["coins"] = int(data.get("coins", 0)) + int(reward)
                data["won_coins"] = int(data.get("won_coins", 0)) + int(reward)
                await save_data(owner_id, data)

                await callback.message.edit_text(f"💰 Вы забрали {format_balance(reward)} mDrops! Поздравляем!",
                                                 reply_markup=final_keyboard)
                MINES_GAMES.pop(game_id, None)
                return

            # CANCEL
            if action == "cancel":
                # повернути ставку
                data["coins"] = int(data.get("coins", 0)) + bet
                await save_data(owner_id, data)
                await callback.message.edit_text("❌ Игра отменена. Ставка возвращена.")
                MINES_GAMES.pop(game_id, None)
                return

            # CELL CLICK
            if action == "cell":
                if len(parts) != 4:
                    return await callback.answer("⚠️ Неверные данные.", show_alert=True)
                try:
                    index = int(parts[3])
                except ValueError:
                    return await callback.answer("⚠️ Неверный индекс.", show_alert=True)

                if index < 0 or index >= 25:
                    return await callback.answer("⚠️ Неверный индекс.", show_alert=True)

                if index in opened_list:
                    return await callback.answer("⛔ Уже открыто")

                # LOSS
                if index in mines_list:
                    # build final field (opened are 💰, mines are 💣 with exploded as 💥)
                    final_field = ["❓"] * 25
                    for i in opened_list:
                        final_field[i] = "💰"
                    for m in mines_list:
                        final_field[m] = "💣"
                    final_field[index] = "💥"

                    final_keyboard = build_final_mines_keyboard(final_field)

                    # облік програшу
                    data["lost_coins"] = int(data.get("lost_coins", 0)) + bet
                    await save_data(owner_id, data)

                    game["state"] = "lost"
                    await callback.message.edit_text(f"💥 Вы попали на мину!\n\n💸 Ставка: {format_balance(bet)} mDrops",
                                                     reply_markup=final_keyboard)
                    MINES_GAMES.pop(game_id, None)
                    return

                # SUCCESSFUL OPEN
                opened_list.append(index)
                game["opened"] = opened_list
                game["field"][index] = "💰"

                opened_now = len(opened_list)
                safe_needed = 25 - mines_count

                # WIN (all safe opened)
                if opened_now >= safe_needed:
                    coef_index = opened_now if opened_now < len(multipliers) else (len(multipliers) - 1)
                    reward = round(bet * multipliers[coef_index])

                    # build final field (show all mines)
                    final_field = ["❓"] * 25
                    for i in opened_list:
                        final_field[i] = "💰"
                    for m in mines_list:
                        final_field[m] = "💣"

                    final_keyboard = build_final_mines_keyboard(final_field)

                    game["state"] = "won"
                    data["coins"] = int(data.get("coins", 0)) + int(reward)
                    data["won_coins"] = int(data.get("won_coins", 0)) + int(reward)
                    await save_data(owner_id, data)

                    await callback.message.edit_text(f"🏆 Вы прошли все мины! Награда: {format_balance(reward)} mDrops",
                                                     reply_markup=final_keyboard)
                    MINES_GAMES.pop(game_id, None)
                    return

                # CONTINUE: show updated info and keyboard
                idx = opened_now if opened_now < len(multipliers) else (len(multipliers) - 1)
                multiplier = multipliers[idx]
                potential = round(bet * multiplier)

                text = (
                    "💣 Игра продолжается!\n"
                    f"💵 Ставка: {format_balance(bet)} mDrops\n"
                    f"💰 Возможный приз: {format_balance(potential)} mDrops\n"
                    f"📈 Множитель: x{round(multiplier, 2)}"
                )

                await callback.message.edit_text(text, reply_markup=build_mines_keyboard(game_id))
                return

            return await callback.answer("⚠️ Неверное действие.", show_alert=True)

    except Exception as e:
        try:
            await handle_error(callback.from_user.username, e, callback.from_user.id, 301)
        except Exception:
            pass


@dp.message(F.text.lower().startswith("рул"))
async def roulette_game(message: Message):
    uid = str(message.from_user.id)
    args = message.text.lower().split()

    # Шаблон помилки/прикладу (щоб не дублювати багато рядків)
    usage = (
        f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
        f"{html.code(gline())}\n"
        f"<b>Пример:</b> {html.code('рул 300 чет')}\n"
        f"<b>Пример:</b> {html.code('рул все чер')}\n"
        f"Возможные ставки: {html.code('кра')}, {html.code('чер')}, {html.code('чет')}, {html.code('нечет')}, "
        f"{html.code('0-11')}, {html.code('12-22')}, {html.code('23-35')}, {html.code('зеро')}, {html.code('зел')}."
    )

    if len(args) < 3:
        return await message.reply(usage, parse_mode="HTML")

    bet_raw, target = args[1], args[2]
    data = await load_data(uid)
    if not data:
        # Якщо користувача нема - створимо
        await create_user_data(uid)
        data = await load_data(uid)

    balance = data["coins"]

    bets = ["кра", "чер", "чет", "нечет", "0-11", "12-22", "23-35", "зеро", "зел"]

    # Розбір ставки
    if bet_raw == "все":
        bet = balance
    elif "к" in bet_raw.lower():
        bet = parse_bet_input(bet_raw)
    elif bet_raw.isdigit():
        bet = int(bet_raw)
    else:
        return await message.reply(usage, parse_mode="HTML")

    # Валідація
    if bet > balance:
        return await message.reply("❌ Недостаточно средств для ставки.", parse_mode="HTML")

    if bet <= 9:
        return await message.reply("⚠️ Минимальная ставка - 10 mDrops!", parse_mode="HTML")

    if target not in bets:
        return await message.reply(usage, parse_mode="HTML")

    # Списуємо ставку відразу
    data["coins"] -= bet

    # Генеруємо число рулетки 0..35
    number = random.randint(0, 35)

    # Визначаємо колір і парність (type -> parity)
    color = ""
    parity = ""
    if number == 0:
        color = "зеленый"
        parity = ""
    else:
        if number % 2 == 0:
            color = "чер"
            parity = "чет"
        else:
            color = "кра"
            parity = "нечет"

    # Визначаємо перемогу та множник
    multiplier = 2

    def is_win():
        nonlocal multiplier
        # зеро / зел
        if target in ("зеро", "зел"):
            if number == 0:
                multiplier = 35
                return True
            return False

        # чер/кра/чет/нечет
        if target == "чер":
            return number != 0 and number % 2 == 0
        if target == "кра":
            return number % 2 == 1
        if target == "чет":
            return number != 0 and number % 2 == 0
        if target == "нечет":
            return number % 2 == 1

        # діапазони
        if target == "0-11":
            multiplayer = 2.8
            return 0 <= number <= 11
        if target == "12-22":
            multiplayer = 2.8
            return 12 <= number <= 22
        if target == "23-35":
            multiplayer = 2.8
            return 23 <= number <= 35

        return False

    win = is_win()

    # Підбір емодзі для кольору
    if number == 0:
        color_emoji = "🟢"
        color_label = "ЗЕЛЁНЫЙ"
    elif color == "чер":
        color_emoji = "⬛"
        color_label = "ЧЁРНЫЙ"
    else:
        color_emoji = "🔴"
        color_label = "КРАСНЫЙ"

    # Формуємо стильний результат
    # Верхній заголовок
    header = (
            html.bold("🎰 РУЛЕТКА — РЕЗУЛЬТАТ")
            + "\n"
            + html.code("────────────────────────────")
            + "\n"
    )

    # Основна «карта» випавшого числа
    number_block = (
            f"{color_emoji}  {html.bold(str(number))}  —  {html.bold(color_label)}"
            + (f", {html.bold(parity.upper())}" if parity else "")
            + "\n"
    )

    # Підсумкова цитата (використовуємо blockquote для акценту)
    if win:
        reward = bet * multiplier
        data["coins"] += reward
        data["won_coins"] = data.get("won_coins", 0) + int(reward)

        quote = html.blockquote(f"🎉 Вы угадали ставку: {target} ({color}{', ' + parity if parity else ''})")
        result_label = html.bold("ВЫИГРЫШ")
        result_value = format_balance(reward)
    else:
        reward = 0
        data["lost_coins"] = data.get("lost_coins", 0) + int(bet)

        quote = html.blockquote(f"😔 Вы не угадали ставку: {target} ({color}{', ' + parity if parity else ''})")
        result_label = html.bold("ПРОИГРЫШ")
        result_value = format_balance(bet)

    # Таблиця результатів у фіксованому шрифті для вирівнювання
    # Використовуємо <pre> щоб зберегти вирівнювання; теги безпечні при parse_mode="HTML"
    table = (
        "<pre>"
        f"Ставка: {format_balance(bet)} mDrops\n"
        f"Множитель:{' x' + str(multiplier)}\n"
        f"{result_label}: {result_value} mDrops\n"
        f"Баланс: {format_balance(data['coins'])} mDrops\n"
        "</pre>"
    )

    # Додатковий підпис/порада
    footer = (
        "\n"
    )

    # Склеюємо повний текст
    result_text = header + number_block + "\n" + quote + "\n" + table + footer

    # Зберігаємо дані та відправляємо
    await save_data(uid, data)
    await message.reply(result_text, parse_mode="HTML")


# ----------- КНБ -----------
RPS_CHOICES = ["камень", "ножницы", "бумага"]
RPS_WIN = {
    "камень": "ножницы",  # камень beats ножницы
    "ножницы": "бумага",
    "бумага": "камень"
}

# store active games: chat_id -> game dict
# game structure: {"user1": uid, "user2": uid_or_None, "bet": int, "choice1": None|str, "choice2": None|str}
rps_challenges: Dict[str, Dict] = {}


def build_rps_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🪨 Камень", callback_data="rps_choice_камень")],
        [InlineKeyboardButton(text="✂️ Ножницы", callback_data="rps_choice_ножницы")],
        [InlineKeyboardButton(text="📄 Бумага", callback_data="rps_choice_бумага")],
    ])


def build_rps_choice_with_cancel() -> InlineKeyboardMarkup:
    kb = build_rps_choice_keyboard()
    kb.inline_keyboard.append([InlineKeyboardButton(text="❌ Отменить", callback_data="rps_cancel")])
    return kb


def build_rps_accept_keyboard(user_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👊 Принять вызов", callback_data=f"rps_accept_{user_id}")]
    ])


def build_rps_accept_with_cancel(user_id: str) -> InlineKeyboardMarkup:
    kb = build_rps_accept_keyboard(user_id)
    kb.inline_keyboard.append([InlineKeyboardButton(text="❌ Отменить", callback_data="rps_cancel")])
    return kb


# ---------- START CHALLENGE ----------
# @dp.message(F.text.lower().startswith("кнб"))
async def start_rps_challenge(message: Message):
    # Only in groups
    if message.chat.type == "private":
        return await message.reply("⚠️ Эта игра доступна только в групповом чате.")

    uid = str(message.from_user.id)
    chat_id = str(message.chat.id)
    parts = (message.text or "").split()

    if len(parts) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {message.from_user.first_name}, ты ввел что-то неправильно!')}\n"
            f"{html.code(gline())}\n"
            f"<b>Пример:</b> {html.code('кнб 300')}\n"
            f"<b>Пример:</b> {html.code('кнб все')}"
        )

    bet_str = parts[1].lower()

    # load user balance
    user_data = await load_data(uid) or {}
    balance = int(user_data.get("coins", 0))

    if bet_str == "все":
        bet = balance
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply("❌ Неверный формат ставки.")

    if bet <= 9:
        return await message.reply("⚠️ Минимальная ставка - 10 mDrops!")

    if bet > balance:
        return await message.reply("❌ Недостаточно средств для ставки.")

    # deduct stake from user1
    user_data["coins"] = balance - bet
    await save_data(uid, user_data)

    # create challenge
    rps_challenges[chat_id] = {
        "user1": uid,
        "bet": bet,
        "choice1": None,
        "choice2": None,
        "user2": None
    }

    await message.reply(
        f"🎮 {await gsname(message.from_user.first_name, message.from_user.id)} начал игру КНБ на {format_balance(bet)} mDrops!\nВыберите ход:",
        reply_markup=build_rps_choice_with_cancel()
    )


# ---------- PLAYER CHOICE ----------
@dp.callback_query(lambda c: c.data and c.data.startswith("rps_choice_"))
@flood_protect(min_delay=0.5)
async def handle_rps_choice(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    chat_id = str(callback.message.chat.id)
    game = rps_challenges.get(chat_id)

    if not game:
        return await callback.answer("😕 Игра не найдена", show_alert=True)

    choice = callback.data.split("_", 2)[2]  # after prefix

    # If user1 selects first (and hasn't yet)
    if uid == game.get("user1") and game["choice1"] is None:
        game["choice1"] = choice
        # If no second player yet, present accept button with cancel
        if not game.get("user2"):
            return await callback.message.edit_text(
                f"👊 Игрок 1 выбрал ход. Кто примет вызов на {format_balance(game['bet'])} mDrops?",
                reply_markup=build_rps_accept_with_cancel(game["user1"])
            )
        else:
            # user2 exists but hasn't chosen -> wait for user2 to choose
            return await callback.message.answer("Вы выбрали ход. Ожидаем второго игрока.")

    # If choosing as user2 (joined)
    if uid == game.get("user2") and game["choice2"] is None:
        game["choice2"] = choice
    # If user2 not set, maybe the challenger pressed a choice second time: treat accordingly
    elif uid == game.get("user1") and not game.get("choice2") and game.get("user2"):
        # this case is rare: user1 attempts to set choice2 when user2 is set; disallow
        return await callback.answer("🤨 Это не твоя кнопка!", show_alert=True)
    # If someone else (not participant) pressed and user2 not set, disallow
    elif uid != game.get("user1") and uid != game.get("user2"):
        return await callback.answer("🤨 Это не твоя кнопка!", show_alert=True)

    # Both choices present?
    if game.get("choice1") and game.get("choice2"):
        c1 = game["choice1"]
        c2 = game["choice2"]
        bet = game["bet"]

        # Tie
        if c1 == c2:
            # return stakes to both players
            for u_key in ("user1", "user2"):
                u = game.get(u_key)
                if u:
                    d = await load_data(u) or {}
                    d["coins"] = d.get("coins", 0) + bet
                    await save_data(u, d)
            # reset choices for new round (user1 starts again)
            game["choice1"] = None
            game["choice2"] = None
            # If user2 still present, keep him; otherwise keep game waiting
            return await callback.message.edit_text(
                f"⚖️ Ничья! Ходы: {c1} vs {c2}\n🔁 Новый раунд: игрок 1 выбирает ход...",
                reply_markup=build_rps_choice_with_cancel()
            )

        # determine winner: if RPS_WIN[c1] == c2 -> user1 wins, else user2 wins
        winner_key, loser_key = ("user1", "user2") if RPS_WIN.get(c1) == c2 else ("user2", "user1")
        total_win = bet * 2

        winner_id = game.get(winner_key)
        loser_id = game.get(loser_key)

        # update winner
        if winner_id:
            winner_data = await load_data(winner_id) or {}
            winner_data["won_coins"] = winner_data.get("won_coins", 0) + total_win
            winner_data["coins"] = winner_data.get("coins", 0) + total_win
            await save_data(winner_id, winner_data)

        # update loser stats
        if loser_id:
            loser_data = await load_data(loser_id) or {}
            loser_data["lost_coins"] = loser_data.get("lost_coins", 0) + bet
            await save_data(loser_id, loser_data)

        # remove game
        rps_challenges.pop(chat_id, None)

        winner_num = 1 if winner_key == "user1" else 2
        await callback.message.edit_text(
            f"🏆 Победил игрок {winner_num}!\nХоды: {c1} vs {c2}\n💰 Выигрыш: {format_balance(total_win)} mDrops"
        )
        return

    # If not both choices yet, just confirm the click
    await callback.answer("Ход сохранён.", show_alert=False)


# ---------- ACCEPT CHALLENGE ----------
@dp.callback_query(lambda c: c.data and c.data.startswith("rps_accept_"))
@flood_protect(min_delay=0.5)
async def player_two_accept(callback: CallbackQuery):
    chat_id = str(callback.message.chat.id)
    uid = str(callback.from_user.id)
    # extract target user id from callback (the challenger)
    parts = callback.data.split("_", 2)
    if len(parts) < 3:
        return await callback.answer("Ошибка данных.", show_alert=True)
    target_user = parts[2]

    game = rps_challenges.get(chat_id)
    if not game:
        return await callback.answer("😕 Игра не найдена", show_alert=True)

    if uid == game.get("user1"):
        return await callback.answer("🤨 Ты не можешь принять свой вызов!", show_alert=True)

    user_data = await load_data(uid) or {}
    balance = int(user_data.get("coins", 0))

    if balance < game["bet"]:
        return await callback.answer("❌ У тебя недостаточно средств!", show_alert=True)

    # deduct stake from user2
    user_data["coins"] = balance - game["bet"]
    await save_data(uid, user_data)

    # set user2 and prompt choices
    game["user2"] = uid
    # Show choice keyboard to both (edit message)
    try:
        await callback.message.edit_text(
            f"✊ Игрок 2 ({await gsname(callback.from_user.first_name, callback.from_user.id)}) присоединился! Выберите ваш ход:",
            reply_markup=build_rps_choice_with_cancel()
        )
    except Exception:
        # fallback: send as new message
        await callback.message.answer(
            f"✊ Игрок 2 ({await gsname(callback.from_user.first_name, callback.from_user.id)}) присоединился! Выберите ваш ход:",
            reply_markup=build_rps_choice_with_cancel()
        )


# ---------- CANCEL GAME ----------
@dp.callback_query(F.data == "rps_cancel")
@flood_protect(min_delay=0.5)
async def handle_rps_cancel(callback: CallbackQuery):
    chat_id = str(callback.message.chat.id)
    caller_id = str(callback.from_user.id)
    game = rps_challenges.get(chat_id)

    if not game:
        await callback.answer("😕 Игра не найдена", show_alert=True)
        return

    # allow cancel by: creator (user1), joined user (user2), or chat admin
    allowed = False
    if caller_id == game.get("user1") or caller_id == game.get("user2"):
        allowed = True

    # check admin privileges in chat
    try:
        member = await bot.get_chat_member(int(chat_id), int(caller_id))
        if getattr(member, "status", None) in ("administrator", "creator"):
            allowed = True
    except Exception:
        pass

    if not allowed:
        await callback.answer("🚫 Вы не можете отменить эту игру.", show_alert=True)
        return

    bet = int(game.get("bet", 0))

    # return stakes to user1
    u1 = game.get("user1")
    if u1:
        d1 = await load_data(u1) or {}
        d1["coins"] = d1.get("coins", 0) + bet
        await save_data(u1, d1)

    # return stake to user2 if joined
    u2 = game.get("user2")
    if u2:
        d2 = await load_data(u2) or {}
        d2["coins"] = d2.get("coins", 0) + bet
        await save_data(u2, d2)

    # remove game
    rps_challenges.pop(chat_id, None)

    # edit message to inform cancellation
    try:
        await callback.message.edit_text("❌ Игра отменена")
    except Exception:
        # ignore edit errors
        pass

    await callback.answer("Игра отменена", show_alert=False)


# --------- БОУЛИНГ -----------

MIN_BET = 10

bowling_games: Dict[str, Dict] = {}


def build_bowling_accept_keyboard(user_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👊 Принять вызов", callback_data=f"bowling_accept_{user_id}")],
        [InlineKeyboardButton(text="❌ Отменить игру", callback_data="bowling_cancel")]
    ])


def build_bowling_initial_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👊 Принять вызов", callback_data="bowling_noop")],
        # noop placeholder (не используется)
        [InlineKeyboardButton(text="❌ Отменить игру", callback_data="bowling_cancel")]
    ])


# Хендлер запуска игры (триггер: сообщение, начинающееся с "боул")
@dp.message(F.text.lower().startswith("боул"))
async def bowling_start(message: Message):
    if message.chat.type == "private":
        return await message.reply("⚠️ Эта игра доступна только в групповом чате.")

    uid = str(message.from_user.id)
    chat_id = str(message.chat.id)
    parts = (message.text or "").split()

    user_data = await load_data(uid)
    if not user_data:
        await create_user_data(uid)
        user_data = await load_data(uid)

    if len(parts) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"Пример: {html.code('боул 300')}\n"
            f"Пример: {html.code('боул все')}"
        )

    bet_str = parts[1].lower()
    balance = int(user_data.get("coins", 0))

    if bet_str == "все":
        bet = balance
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{html.code(gline())}\n"
            f"Пример: {html.code('боул 300')}\n"
            f"Пример: {html.code('боул все')}"
        )

    if bet <= 0 or bet > balance:
        return await message.reply("❌ Недостаточно средств для ставки.")

    if bet < MIN_BET:
        return await message.reply(f"⚠️ Минимальная ставка — {MIN_BET} mDrops!")

    # списываем ставку у первого игрока
    user_data["coins"] = balance - bet
    await save_data(uid, user_data)

    # генерируем результат броска первого игрока
    score1 = random.randint(1, 10)
    bowling_games[chat_id] = {
        "user1": uid,
        "score1": score1,
        "bet": bet,
        "user2": None,
        "score2": None
    }

    await message.reply(
        f"🎳 {await gsname(message.from_user.first_name, message.from_user.id)} бросил шар и выбил {html.spoiler("?")} очков!\n"
        f"Кто примет вызов на {format_balance(bet)} mDrops?",
        reply_markup=build_bowling_accept_keyboard(uid)
    )


# Обработчик принятия вызова
@dp.callback_query(lambda c: c.data and c.data.startswith("bowling_accept_"))
@flood_protect(min_delay=0.5)
async def bowling_accept(callback: CallbackQuery):
    chat_id = str(callback.message.chat.id)
    uid = str(callback.from_user.id)
    game = bowling_games.get(chat_id)

    if not game:
        return await callback.answer("❌ Игра не найдена!", show_alert=True)

    if uid == game["user1"]:
        return await callback.answer("🤨 Ты не можешь бросать сам против себя!", show_alert=True)

    user2_data = await load_data(uid)
    # проверяем баланс второго игрока
    if not user2_data:
        await create_user_data(uid)
        user2_data = await load_data(uid)

    balance2 = int(user2_data.get("coins", 0))
    bet = int(game["bet"])

    if balance2 < bet:
        return await callback.answer("❌ У тебя недостаточно средств!", show_alert=True)

    # списываем ставку второго игрока
    user2_data["coins"] = balance2 - bet
    await save_data(uid, user2_data)

    # генерируем результат второго игрока
    score2 = random.randint(1, 10)
    game["user2"] = uid
    game["score2"] = score2

    user1 = game["user1"]
    user2 = game["user2"]
    score1 = game["score1"]

    # удаляем игру из активных
    bowling_games.pop(chat_id, None)

    # обработка ничьи
    if score1 == score2:
        # вернуть ставки обоим
        p1 = await load_data(user1) or {}
        p1["coins"] = p1.get("coins", 0) + bet
        await save_data(user1, p1)

        p2 = await load_data(user2) or {}
        p2["coins"] = p2.get("coins", 0) + bet
        await save_data(user2, p2)

        return await callback.message.edit_text(
            f"🎳 Ничья! Оба выбили по {score1} очков. Ставки возвращены."
        )

    # определяем победителя
    if score1 > score2:
        winner = user1
        loser = user2
    else:
        winner = user2
        loser = user1

    total_win = bet * 2

    # начисляем выигрыш и статистику
    wdata = await load_data(winner) or {}
    wdata["coins"] = wdata.get("coins", 0) + total_win
    wdata["won_coins"] = wdata.get("won_coins", 0) + total_win
    await save_data(winner, wdata)

    ldata = await load_data(loser) or {}
    ldata["lost_coins"] = ldata.get("lost_coins", 0) + bet
    await save_data(loser, ldata)

    # итоговое сообщение
    winner_num = "Игрок 1" if winner == user1 else "Игрок 2"
    await callback.message.edit_text(
        "🎳 Результаты:\n"
        f"Игрок 1: {score1} очков\n"
        f"Игрок 2: {score2} очков\n"
        f"🏆 Победа за {winner_num}! 💰 +{format_balance(total_win)} mDrops"
    )


# Обработчик отмены игры (возврат ставок)
@dp.callback_query(F.data == "bowling_cancel")
@flood_protect(min_delay=0.5)
async def bowling_cancel(callback: CallbackQuery):
    chat_id = str(callback.message.chat.id)
    caller_id = str(callback.from_user.id)
    game = bowling_games.get(chat_id)

    if not game:
        await callback.answer("❌ Игру не найдено или она уже завершена.", show_alert=True)
        return

    # разрешено отменять: организатор (user1), присоединившийся игрок (user2), или админ/владелец чата
    allowed = False
    if caller_id == game.get("user1") or caller_id == game.get("user2"):
        allowed = True

    try:
        member = await bot.get_chat_member(int(chat_id), int(caller_id))
        if getattr(member, "status", None) in ("administrator", "creator"):
            allowed = True
    except Exception:
        # если не получилось проверить — оставить allowed как есть
        pass

    if not allowed:
        await callback.answer("🚫 Вы не можете отменить эту игру.", show_alert=True)
        return

    bet = int(game.get("bet", 0))

    # вернуть ставку первому игроку
    u1 = game.get("user1")
    if u1:
        d1 = await load_data(u1) or {}
        d1["coins"] = d1.get("coins", 0) + bet
        await save_data(u1, d1)

    # вернуть ставку второму игроку, если он присоединился
    u2 = game.get("user2")
    if u2:
        d2 = await load_data(u2) or {}
        d2["coins"] = d2.get("coins", 0) + bet
        await save_data(u2, d2)

    # удалить игру
    bowling_games.pop(chat_id, None)

    # редактируем сообщение
    try:
        await callback.message.edit_text("❌ Игра отменена. Ставки возвращены.")
    except Exception:
        pass

    await callback.answer("Игра отменена. Ставки возвращены.", show_alert=False)


chest_games = {}  # chat_id: {user, bet, correct}


@dp.message(F.text.lower().startswith("сундуки"))
async def chest_start(message: Message):
    uid = str(message.from_user.id)
    chat_id = str(message.chat.id)
    parts = message.text.split()

    data = await load_data(uid)

    if len(parts) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('сундуки 300')}\n"
            f"<b>Пример:</b> {html.code('сундуки все')}"
        )

    bet_str = parts[1].lower()
    balance = data["coins"]

    if bet_str == "все":
        bet = balance
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('сундуки 300')}\n"
            f"<b>Пример:</b> {html.code('сундуки все')}"
        )

    if bet <= 0 or bet > balance:
        return await message.reply("❌ Недостаточно средств!")

    # списываем ставку
    data["coins"] -= bet
    await save_data(uid, data)

    # генерируем правильный сундук
    correct = random.randint(1, 4)
    chest_games[chat_id] = {"user": uid, "bet": bet, "correct": correct}

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Сундук 1", callback_data=f"chest_1")],
        [InlineKeyboardButton(text="📦 Сундук 2", callback_data=f"chest_2")],
        [InlineKeyboardButton(text="📦 Сундук 3", callback_data=f"chest_3")],
        [InlineKeyboardButton(text="📦 Сундук 4", callback_data=f"chest_4")],
    ])

    await message.reply(
        f"📦 {await gsname(message.from_user.first_name, message.from_user.id)} сделал ставку {format_balance(bet)} mDrops!\n"
        "Выбери один из 4 сундуков, в одном из них спрятан приз 💰",
        reply_markup=kb
    )


@dp.callback_query(F.data.startswith("chest_"))
@flood_protect(min_delay=0.5)
async def chest_pick(callback: CallbackQuery):
    chat_id = str(callback.message.chat.id)
    uid = str(callback.from_user.id)
    game = chest_games.get(chat_id)

    if not game:
        return await callback.answer("❌ Игра не найдена или ты уже проиграл!", show_alert=True)

    if uid != game["user"]:
        return await callback.answer("❌ Это не твоя игра или ты уже проиграл!", show_alert=True)

    pick = int(callback.data.split("_")[1])
    bet = game["bet"]
    correct = game["correct"]

    chest_games.pop(chat_id)

    if pick == correct:
        win = int(bet * 2.5)
        data = await load_data(uid)
        data["coins"] += win
        data["won_coins"] += win
        await save_data(uid, data)

        await callback.message.edit_text(
            f"🎉 Ты выбрал сундук {pick} и угадал! Приз: +{format_balance(win)} mDrops 💰"
        )
    else:
        looser_data = await load_data(uid)
        looser_data["lost_coins"] += bet
        await save_data(uid, looser_data)

        await callback.message.edit_text(
            f"😢 Ты выбрал сундук {pick}, но правильный был сундук {correct}.\n\n💸 Ставка: {format_balance(bet)} mDrops"
        )


# --------- РР -----------
active_rr_games = {}


def build_rr_keyboard(user_id: int, first_round: bool = False):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔫 Выстрелить", callback_data=f"rr_shoot:{user_id}")
    if first_round:  # только на первом шаге показываем "Отменить"
        kb.button(text="❌ Отменить", callback_data=f"rr_cancel:{user_id}")
    else:
        kb.button(text="🛑 Стоп", callback_data=f"rr_stop:{user_id}")
    return kb.as_markup()


def compute_rr_multiplier(game: dict) -> float:
    """
    Обчислює множник по формулі: 6 / (6 - used_count)
    Повертає округлений до 2 знаків float.
    Безпечний на випадок дивних даних.
    """
    used_count = len(set(game.get("used", [])))
    remaining = 6 - used_count
    if remaining <= 0:
        return 6.0  # теоретично не має трапитись, але на всяк випадок
    return round(6.0 / remaining, 2)


def render_baraban(game: dict, reveal: bool = False):
    total = 6
    used_set = set(game.get("used", []))

    if not reveal:
        # показуємо хрестики для невідомих комірок, кількість хрестиків = залишок
        return "[ " + "  ".join(["×"] * (total - len(used_set))) + " ]"

    # при reveal показуємо всі слоти: використані — "○", порожні — "×", куля — "⁍"
    slots = ["×"] * total
    for idx in used_set:
        if 0 <= idx < total:
            slots[idx] = "○"
    bullet_idx = game.get("bullet_index")
    if isinstance(bullet_idx, int) and 0 <= bullet_idx < total:
        slots[bullet_idx] = "⁍"
    return "[ " + "  ".join(slots) + " ]"


@dp.message(F.text.lower().startswith("рр"))
async def handle_rr(message: Message):
    user_id = str(message.from_user.id)
    name = message.from_user.first_name

    if not await load_data(user_id):
        await create_user_data(user_id)
    data = await load_data(user_id)

    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('рр 2к')}\n"
            f"<b>Пример:</b> {html.code('рр все')}"
        )

    bet_str = parts[1].lower()
    balance = data["coins"]

    if bet_str == "все":
        bet = balance
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{html.code(gline())}\n"
            f"Пример: {html.code('рр 2к')}\n"
            f"Пример: {html.code('рр все')}"
        )

    if bet <= 0 or bet > balance:
        return await message.reply("❌ Недостаточно средств для ставки.")

    # списуємо ставку
    data["coins"] -= bet
    await save_data(user_id, data)

    bullet_index = random.randint(0, 5)

    active_rr_games[user_id] = {
        "bet": bet,
        "multiplier": 1.0,  # тимчасово, одразу нижче перерахуємо
        "bullet_index": bullet_index,
        "current_slot": 0,
        "shots": 0,
        "used": [],
        "finished": False,
        "first_round": True  # позначка для кнопки "Отменить"
    }

    # одразу перераховуємо реальний множник
    active_rr_games[user_id]["multiplier"] = compute_rr_multiplier(active_rr_games[user_id])

    await message.answer(
        f"🔫 {name}, вы запустили игру\n\n"
        f"🔫 Барабан: {render_baraban(active_rr_games[user_id])}\n"
        f"🤑 Выигрыш: x{active_rr_games[user_id]['multiplier']} (+{int(bet * (active_rr_games[user_id]['multiplier'] - 1))} mDrops)",
        reply_markup=build_rr_keyboard(message.from_user.id, first_round=True)
    )


@dp.callback_query(F.data.startswith("rr_"))
@flood_protect(min_delay=0.5)
async def rr_callback(call: CallbackQuery):
    action, owner_id = call.data.split(":")
    owner_id = int(owner_id)
    key = str(owner_id)

    if call.from_user.id != owner_id:
        return await call.answer("❌ Это не твоя игра или ты уже проиграл!", show_alert=True)

    game = active_rr_games.get(key)
    if not game:
        return await call.answer("Игра не найдена.", show_alert=True)

    name = call.from_user.first_name
    bet = game["bet"]

    if action == "rr_cancel":
        # повертаємо ставку без множника
        data = await load_data(key)
        data["coins"] += bet
        await save_data(key, data)

        await call.message.edit_text(
            f"❌ {name}, вы отменили игру и получили обратно {bet} mDrops."
        )
        active_rr_games.pop(key, None)
        return

    if action == "rr_shoot":
        # після першого пострілу кнопку "Отменить" прибираємо
        game["first_round"] = False

        slot = game["current_slot"]
        game["shots"] += 1

        if slot == game["bullet_index"]:
            text = (
                f"❌ {name}, вы проиграли!\n\n"
                f"🔫 {game['shots']}-й патрон оказался настоящим\n\n"
                f"Проиграно: {format_balance(bet)} mDrops\n\n"
                f"🔫 Барабан: {render_baraban(game, reveal=True)}"
            )
            await call.message.edit_text(text)
            active_rr_games.pop(key, None)
            return

        # якщо холостий — додаємо в used, пересуваємо слот і перераховуємо множник по формулі
        game.setdefault("used", []).append(slot)
        game["current_slot"] = (slot + 1) % 6
        # перерахунок множника:
        game["multiplier"] = compute_rr_multiplier(game)
        # показуємо потенційний виграш-прибуток (прибуток = bet * (multiplier - 1))
        win_amount = int(bet * (game["multiplier"] - 1))

        await call.message.edit_text(
            f"🔫 {name}, {game['shots']}-й выстрел был холостым\n\n"
            f"🔫 Барабан: {render_baraban(game)}\n"
            f"🤑 Выигрыш: x{game['multiplier']} (+{win_amount} mDrops)",
            reply_markup=build_rr_keyboard(owner_id, first_round=False)
        )
        return

    elif action == "rr_stop":
        # виплата: повертаємо bet * multiplier (включаючи ставку)
        win_amount = int(bet * game["multiplier"])
        data = await load_data(key)
        data["coins"] += win_amount
        await save_data(key, data)

        text = (
            f"✅ {name}, вы выиграли {win_amount} mDrops (x{game['multiplier']})\n\n"
            f"🔫 Барабан: {render_baraban(game, reveal=True)}"
        )
        await call.message.edit_text(text)
        active_rr_games.pop(key, None)
        return


@dp.message(F.text.lower().startswith("баск"))
async def handle_basketball(message: Message):
    user_id = message.from_user.id
    args = message.text.split()

    if len(args) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('баскетбол 5к')}\n"
            f"<b>Пример:</b> {html.code('баскетбол все')}"
        )

    bet_str = args[1]
    data = await load_data(str(user_id))
    balance = data["coins"]

    if bet_str == "все":
        bet = balance
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('баскетбол 5к')}\n"
            f"<b>Пример:</b> {html.code('баскетбол все')}"
        )

    if bet > balance:
        return await message.reply(
            f"🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, тебе не хватает mDrops!")

    if bet < 10:
        return await message.reply(
            f"🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, минимальная ставка - 10 mDrops!")

    # Списуємо ставку
    data["coins"] -= bet
    await save_data(str(user_id), data)

    # Кидаємо анімований емодзі 🏀
    dice = await message.answer_dice(emoji="🏀")

    # Чекаємо результат
    result = dice.dice.value
    if result == 5 or result == 4:  # перемога
        win_amount = int(bet * 2.2)
        data["coins"] += win_amount
        await save_data(str(user_id), data)
        await message.reply(
            f"🏀 {await gsname(message.from_user.first_name, message.from_user.id)}, гол! Ты выиграл {format_balance(win_amount)} mDrops\nБаланс: {format_balance(data["coins"])} mDrops")
    else:
        await message.reply(
            f"❌ {await gsname(message.from_user.first_name, message.from_user.id)}, промах! Ты програл {format_balance(bet)} mDrops\nБаланс: {format_balance(data["coins"])} mDrops")


FOOTBALL_MULTIPLIERS = {
    "gol": 1.6,
    "mimo": 2.2
}


@dp.message(F.text.lower().startswith("футбол"))
async def handle_football(message: Message):
    user_id = message.from_user.id
    args = message.text.split()

    if len(args) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, user_id)}, ти ввів(ла) щось неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Приклад:</b> {html.code('футбол 5к')}\n"
            f"<b>Приклад:</b> {html.code('футбол 2к мимо')}"
        )

    bet_str = args[1]
    # парсим ставку
    if bet_str == "все":
        data = await load_data(str(user_id))
        if not data:
            await create_user_data(str(user_id))
            data = await load_data(str(user_id))
        bet = data.get("coins", 0)
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, user_id)}, ти ввів(ла) щось неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Приклад:</b> {html.code('футбол 5к')}\n"
            f"<b>Приклад:</b> {html.code('футбол 2к мимо')}"
        )

    # завантажуємо баланс
    data = await load_data(str(user_id))
    if not data:
        await create_user_data(str(user_id))
        data = await load_data(str(user_id))

    balance = data.get("coins", 0)

    if bet > balance:
        return await message.reply(f"🤨 {await gsname(message.from_user.first_name, user_id)}, тобі бракує mDrops!")

    if bet < 10:
        return await message.reply(
            f"🤨 {await gsname(message.from_user.first_name, user_id)}, мінімальна ставка - 10 mDrops!")

    # списуємо ставку відразу (як і раніше) — якщо користувач відміняє, повернемо назад
    data["coins"] = balance - bet
    await save_data(str(user_id), data)

    # якщо користувач одразу вказав результат
    chosen = None
    if len(args) >= 3:
        chosen_raw = args[2].lower()
        if chosen_raw in ("гол", "gol", "goal"):
            chosen = "gol"
        elif chosen_raw in ("мимо", "mimo", "miss"):
            chosen = "mimo"
        else:
            chosen = None

    # випадок: користувач указав вибір — показуємо анімацію, робимо паузу, видаляємо анімацію і надсилаємо результат у новому повідомленні
    if chosen:
        dice_msg = None
        try:
            dice_msg = await message.answer_dice(emoji="⚽")
        except Exception:
            dice_msg = None

        # затримка перед результатом
        await asyncio.sleep(3)

        # видаляємо перше бот-повідомлення (анімацію), якщо воно є
        if dice_msg is not None:
            try:
                await dice_msg.delete()
            except Exception:
                pass

        # визначаємо результат (можна змінити ваги)
        outcome = random.choices(["gol", "mimo"], weights=[50, 50], k=1)[0]

        # оновлюємо дані та виплати
        data = await load_data(str(user_id))
        if not data:
            await create_user_data(str(user_id))
            data = await load_data(str(user_id))

        if outcome == chosen:
            mult = FOOTBALL_MULTIPLIERS[chosen]
            win_amount = int(bet * mult)
            data["coins"] = data.get("coins", 0) + win_amount
            await save_data(str(user_id), data)

            await message.answer(
                f"⚽️ {await gsname(message.from_user.first_name, user_id)}, подія: <b>{'Гол' if outcome == 'gol' else 'Мимо'}</b> — ти вгадав(ла)! 🎉\n"
                f"Ти виграв(ла) {format_balance(win_amount)} mDrops\n"
                f"Баланс: {format_balance(data['coins'])} mDrops",
                parse_mode="HTML"
            )
        else:
            # програш — ставка вже списана
            await message.answer(
                f"❌ {await gsname(message.from_user.first_name, user_id)}, подія: <b>{'Гол' if outcome == 'gol' else 'Мимо'}</b>\n"
                f"Ти програв(ла) ставку {format_balance(bet)} mDrops\n"
                f"Баланс: {format_balance(data.get('coins', 0))} mDrops",
                parse_mode="HTML"
            )

        return

    # інакше — показуємо панель вибору (кнопки)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"⚽️ Гол - {FOOTBALL_MULTIPLIERS['gol']}х",
                                  callback_data=f"football_play:{user_id}:{bet}:gol")],
            [InlineKeyboardButton(text=f"🥅 Мимо - {FOOTBALL_MULTIPLIERS['mimo']}х",
                                  callback_data=f"football_play:{user_id}:{bet}:mimo")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"football_cancel:{user_id}:{bet}")]
        ])

        await message.reply(
            f"⚽️ Футбол · вибери результат!\n"
            f"{gline()}\n"
            f"💸 Ставка: {format_balance(bet)} mDrops",
            reply_markup=kb
        )


@dp.callback_query(F.data.startswith("football_play:"))
async def football_play(cb: CallbackQuery):
    parts = cb.data.split(":")
    if len(parts) < 4:
        uid = int(parts[1])
        if cb.from_user.id != uid:
            return await cb.answer("Это не твоя кнопка!")
        bet = int(parts[2])
        data = await load_data()
        data["coins"] += bet
        await save_data(uid, data)
        await cb.message.delete()
        return await cb.answer("Ошибка, ставку возвращено, игра отменена", show_alert=True)

    uid = int(parts[1])
    bet = int(parts[2])
    choice = parts[3]  # 'gol' або 'mimo'

    # захист — кнопку натиснув не той користувач
    if cb.from_user.id != uid:
        return await cb.answer("Это не твоя кнопка!")

    # спочатку видаляємо повідомлення з кнопками (перше повідомлення)
    try:
        await cb.message.delete()
    except Exception:
        pass

    # для фану показуємо анімацію (нове тимчасове повідомлення)
    dice_msg = None
    try:
        dice_msg = await cb.message.answer_dice(emoji="⚽")
    except Exception:
        dice_msg = None

    # пауза 3 секунди
    await asyncio.sleep(3)

    # видаляємо анімацію, якщо вона була
    if dice_msg is not None:
        try:
            await dice_msg.delete()
        except Exception:
            pass

    # визначаємо результат
    if int(dice_msg.dice.value) <= 2:
        outcome = "mimo"
    else:
        outcome = "gol"

    data = await load_data(str(uid))
    if not data:
        await create_user_data(str(uid))
        data = await load_data(str(uid))

    if outcome == choice:
        mult = FOOTBALL_MULTIPLIERS[choice]
        win_amount = int(bet * mult)
        data["coins"] = data.get("coins", 0) + win_amount
        await save_data(str(uid), data)

        await cb.message.answer(
            f"🔥 {await gsname(cb.from_user.first_name, uid)} | <b>Футбол · Победа!</b> ✅\n"
            f"{gline()}\n"
            f"💸 Ставка: {format_balance(bet)} mDrops\n"
            f"🎲 Выбрано: {'Гол' if outcome == 'gol' else 'Мимо'}\n"
            f"💰 Выигрыш: x{FOOTBALL_MULTIPLIERS[choice]} / {format_balance(win_amount)} mDrops\n"
            f"{gline()}\n"
            f"{html.blockquote(f"⚡️ Итог: {'Гол' if outcome == 'gol' else 'Мимо'}")}",
            parse_mode="HTML"
        )
    else:
        await cb.message.answer(
            f"💥 {await gsname(cb.from_user.first_name, uid)} | <b>Футбол · Проигрыш!</b>\n"
            f"{gline()}\n"
            f"💸 Ставка: {format_balance(bet)} mDrops\n"
            f"🎲 Выбрано: <b>{'Мимо' if outcome == 'gol' else 'Гол'}</b>\n"
            f"{gline()}\n"
            f"{html.blockquote(f"⚡️ Итог: {'Гол' if outcome == 'gol' else 'Мимо'}")}",
            parse_mode="HTML"
        )


@dp.callback_query(F.data.startswith("football_cancel:"))
async def football_cancel(cb: CallbackQuery):
    parts = cb.data.split(":")
    if len(parts) < 3:
        return await cb.answer("Ошибка")

    uid = int(parts[1])
    bet = int(parts[2])

    if cb.from_user.id != uid:
        return await cb.answer("Это не твоя кнопка!")

    # повертаємо ставку
    data = await load_data(str(uid))
    if not data:
        await create_user_data(str(uid))
        data = await load_data(str(uid))

    data["coins"] = data.get("coins", 0) + bet
    await save_data(str(uid), data)

    # видаляємо повідомлення з кнопками і надсилаємо підтвердження (без затримки)
    try:
        await cb.message.delete()
    except Exception:
        pass

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=BACK, callback_data=f"back_to_menu:{uid}")]])
    await cb.message.answer(
        f"ℹ️ {await gsname(cb.from_user.first_name, uid)}, ставка {format_balance(bet)} mDrops возвращена.\n"
        f"💰 Баланс: {format_balance(data['coins'])} mDrops",
        reply_markup=kb
    )


active_duels = {}


@dp.message(F.text.lower().startswith("дуэль"))
async def start_duel(message: Message):
    user_id = message.from_user.id
    args = message.text.split()

    if len(args) < 2:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('дуэль 3к')}\n"
            f"<b>Пример:</b> {html.code('дуэль все')}"
        )

    bet_str = args[1]
    data = await load_data(str(user_id))
    balance = data["coins"]

    if bet_str == "все":
        bet = balance
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await message.reply(
            f"{html.italic(f'🤨 {await gsname(message.from_user.first_name, message.from_user.id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('дуэль 3к')}\n"
            f"<b>Пример:</b> {html.code('дуэль все')}"
        )

    if bet <= 0 or bet > balance:
        return await message.reply("❌ Недостаточно средств для дуэли!")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять дуэль", callback_data=f"accept_duel:{bet}:{user_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_duel:{user_id}")]
        ]
    )
    msg = await message.reply(
        f"⚔️ {await gsname(message.from_user.first_name, user_id)} вызывает на дуэль!\n"
        f"💰 Ставка: {format_balance(bet)} mDrops",
        reply_markup=kb
    )

    active_duels[msg.message_id] = {"initiator": user_id, "bet": bet}
    active_duels[msg.message_id] = {"initiator": user_id, "bet": bet}


@dp.callback_query(F.data.startswith("accept_duel:"))
@flood_protect(min_delay=0.5)
async def accept_duel(callback: CallbackQuery):
    _, bet_str, initiator_id = callback.data.split(":")
    bet = int(bet_str)
    initiator_id = int(initiator_id)
    opponent_id = callback.from_user.id

    if not await load_data(str(opponent_id)):
        await create_user_data(opponent_id)

    if initiator_id == opponent_id:
        return await callback.answer("❌ Нельзя принять свою же дуэль!", show_alert=True)

    # Загружаем данные игроков
    initiator_data = await load_data(str(initiator_id))
    opponent_data = await load_data(str(opponent_id))

    chatg = await bot.get_chat(initiator_id)
    initiator_name = chatg.first_name
    opponent_name = callback.from_user.first_name

    if initiator_data["coins"] < bet:
        return await callback.answer("❌ У инициатора уже нет денег!", show_alert=True)
    if opponent_data["coins"] < bet:
        return await callback.answer("❌ У вас недостаточно денег!", show_alert=True)

    # Списываем ставки
    initiator_data["coins"] -= bet
    opponent_data["coins"] -= bet
    await save_data(str(initiator_id), initiator_data)
    await save_data(str(opponent_id), opponent_data)

    await callback.message.edit_text(
        f"⚔️ Дуэль началась!\n"
        f"💰 Ставка: {format_balance(bet)} mDrops\n\n"
        f"👤 {await gsname(opponent_name, opponent_id)} принял вызов от {await gsname(initiator_name, initiator_id)}!"
    )

    # Кидаем кубики
    msg = await callback.message.answer(f"🎲 Бросаем кубик для {await gsname(initiator_name, initiator_id)}...")
    dice1 = await callback.message.answer_dice(emoji="🎲")
    await asyncio.sleep(3)

    msg2 = await callback.message.answer(f"🎲 Бросаем кубик для {await gsname(opponent_name, opponent_id)}...")
    dice2 = await callback.message.answer_dice(emoji="🎲")

    await asyncio.sleep(4)

    val1, val2 = dice1.dice.value, dice2.dice.value

    if val1 > val2:
        initiator_data["coins"] += bet * 2
        initiator_data["won_coins"] += bet * 2
        opponent_data["lost_coins"] += bet
        await save_data(str(opponent_id), opponent_data)
        await save_data(str(initiator_id), initiator_data)
        await callback.message.answer(
            f"🏆 Победил {await gsname(initiator_name, initiator_id)}!\nВыигрыш: {format_balance(bet * 2)} mDrops"
        )
    elif val2 > val1:
        opponent_data["coins"] += bet * 2
        opponent_data["won_coins"] += bet * 2
        initiator_data["lost_coins"] += bet
        await save_data(str(initiator_id), initiator_data)
        await save_data(str(opponent_id), opponent_data)
        await callback.message.answer(
            f"🏆 Победил {await gsname(opponent_name, opponent_id)}!\nВыигрыш: {format_balance(bet * 2)} mDrops"
        )
    else:
        initiator_data["coins"] += bet
        opponent_data["coins"] += bet
        await save_data(str(initiator_id), initiator_data)
        await save_data(str(opponent_id), opponent_data)
        await callback.message.answer("🤝 Ничья! Ставки возвращены.")


@dp.callback_query(F.data.startswith("cancel_duel:"))
@flood_protect(min_delay=0.5)
async def cancel_duel(callback: CallbackQuery):
    _, initiator_id = callback.data.split(":")
    initiator_id = int(initiator_id)

    if callback.from_user.id != initiator_id:
        return await callback.answer("❌ Отменить дуэль может только её создатель!", show_alert=True)

    # Удаляем дуэль
    if callback.message.message_id in active_duels:
        del active_duels[callback.message.message_id]

    await callback.message.edit_text("⚔️ Дуэль была отменена ❌")
    await callback.answer("Вы отменили дуэль ✅")


coin_games: Dict[Tuple[int, int], Dict[str, Any]] = {}

# TTL для ігор (секунд) — по бажанню, щоб не зберігати вічно
GAME_TTL = 300  # 5 хвилин


@dp.message(F.text.lower().startswith("монетка"))
async def handle_coin_game(msg: Message):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(msg.from_user.first_name, user_id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('монетка 2.5к')}\n"
            f"<b>Пример:</b> {html.code('монетка все')}"
        )

    bet_str = parts[1].strip()

    # Загружаємо дані користувача
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    # Парсинг ставки
    if bet_str.lower() == "все":
        bet = data["coins"]
    elif "к" in bet_str.lower():
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(msg.from_user.first_name, user_id)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('монетка 2.5к')}\n"
            f"<b>Пример:</b> {html.code('монетка все')}"
        )

    # Валідація ставки
    if bet <= 0:
        return await msg.reply(f"❌ {await gsname(name, user_id)}, неверная ставка.")
    if bet < 10:
        return await msg.reply(f"❌ {await gsname(name, user_id)}, минимальная ставка - 10 mDrops")
    if bet > data["coins"]:
        return await msg.reply(
            f"❌ {await gsname(name, user_id)}, ставка не может быть больше баланса!\n\n💰 Твой баланс: {format_balance(data['coins'])}")

    # Снимаем ставку відразу
    data["coins"] -= bet
    await save_data(user_id, data)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔼 Орел", callback_data="coin_game:eagle"),
            InlineKeyboardButton(text="🔽 Решка", callback_data="coin_game:tails")
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="coin_game:cancel")]
    ])

    sent = await msg.reply(
        f"🪙 {await gsname(name, user_id)}, выбери сторону! Ставка: {format_balance(bet)} mDrops",
        reply_markup=kb
    )

    # Зберігаємо гру в словнику по ключу (chat_id, message_id)
    game_key = (sent.chat.id, sent.message_id)
    coin_games[game_key] = {
        "owner_id": user_id,
        "bet": int(bet),
        "created_at": asyncio.get_event_loop().time()
    }

    # Опціонально — плануємо автоматичне очищення через GAME_TTL
    async def expire_game_after_ttl(key):
        await asyncio.sleep(GAME_TTL)
        g = coin_games.get(key)
        if not g:
            return
        # повертаємо ставку гравцю і інформуємо (якщо повідомлення доступне)
        try:
            owner = g["owner_id"]
            # завантажимо дані і повернемо ставку
            data_owner = await load_data(owner)
            if data_owner is None:
                await create_user_data(owner)
                data_owner = await load_data(owner)
            data_owner["coins"] += g["bet"]
            await save_data(owner, data_owner)
            # намагаємось відредагувати повідомлення
            try:
                # якщо повідомлення ще існує — повідомимо про автоматичне скасування
                await bot.edit_message_text(
                    chat_id=key[0],
                    message_id=key[1],
                    text=f"❌ Игра автоматически отменена (время ожидания истекло). Ставка {format_balance(g['bet'])} возвращена."
                )
            except Exception:
                pass
        except Exception:
            pass
        # видаляємо гру
        coin_games.pop(key, None)

    # запускаємо таск для TTL (не блокуючи)
    asyncio.create_task(expire_game_after_ttl(game_key))


@dp.callback_query(F.data.startswith("coin_game:"))
@flood_protect(min_delay=0.5)
async def coin_game_callback(query: CallbackQuery):
    # callback_data у форматі coin_game:action
    parts = query.data.split(":")
    if len(parts) != 2:
        return await query.answer("Ошибка данных.", show_alert=True)

    _, action = parts

    # Нам потрібне повідомлення, в якому була клавіатура — використовуємо його id як ключ
    if not query.message:
        return await query.answer("Ошибка: сообщение не найдено.", show_alert=True)

    key = (query.message.chat.id, query.message.message_id)
    game = coin_games.get(key)
    if not game:
        return await query.answer("Эта игра устарела или уже завершена.", show_alert=True)

    owner_id = game["owner_id"]
    bet = int(game["bet"])

    # Только владелец может нажимать
    if query.from_user.id != owner_id:
        return await query.answer("❌ Это не твоя ставка!", show_alert=True)

    # Обработка отмены
    if action == "cancel":
        # вернуть ставку
        data = await load_data(owner_id)
        if not data:
            await create_user_data(owner_id)
            data = await load_data(owner_id)
        data["coins"] += bet
        await save_data(owner_id, data)

        # удалить игру и отредактировать сообщение
        coin_games.pop(key, None)
        try:
            await query.message.edit_text(f"❌ Игра отменена. Ставка {format_balance(bet)} возвращена.")
        except Exception:
            pass
        return await query.answer()

    # Подтверждаем принятие нажатия (убирает "часики")
    await query.answer()

    # Редактируем сообщение — показываем, что бросаем
    try:
        await query.message.edit_text("🪙 Подбрасываем монетку...")
    except:
        pass

    await asyncio.sleep(3)

    # Генерируем результат 50/50
    result_side = random.choice(["eagle", "tails"])
    nice_result = "🔼 Орел" if result_side == "eagle" else "🔽 Решка"

    # Если игрок угадал
    if action == result_side:
        reward = bet * 2
        data = await load_data(owner_id)
        if not data:
            await create_user_data(owner_id)
            data = await load_data(owner_id)
        data["coins"] += reward
        data["won_coins"] = data.get("won_coins", 0) + int(reward)
        await save_data(owner_id, data)

        # удаляем игру
        coin_games.pop(key, None)

        try:
            await query.message.edit_text(
                f"🪙 Результат: {nice_result}\n\n"
                f"🎉 {await gsname(query.from_user.first_name, owner_id)}, ты победил! Выигрыш: {format_balance(reward)} mDrops\n"
                f"💰 Баланс: {format_balance(data['coins'])}"
            )
        except:
            pass
        return await query.answer("Вы выиграли!", show_alert=False)

    # Игрок проиграл (ставка уже списана при старте)
    data = await load_data(owner_id)
    if not data:
        await create_user_data(owner_id)
        data = await load_data(owner_id)
    data["lost_coins"] = data.get("lost_coins", 0) + int(bet)
    await save_data(owner_id, data)

    # удаляем игру
    coin_games.pop(key, None)

    try:
        await query.message.edit_text(
            f"🪙 Результат: {nice_result}\n\n"
            f"😔 {await gsname(query.from_user.first_name, owner_id)}, ты проиграл. Ставка {format_balance(bet)} mDrops потеряна.\n"
            f"💰 Баланс: {format_balance(data['coins'])}"
        )
    except:
        pass
    return await query.answer("Вы проиграли.", show_alert=False)


@dp.message(F.text.lower().startswith("кубик"))
async def handle_cube_game(msg: Message):
    uid = msg.from_user.id
    name = msg.from_user.first_name

    _, bet_str, bet_type = msg.text.split(" ")
    bets_types = ["1", "2", "3", "4", "5", "6", "чет", "нечет", "б", "м"]
    if not bet_str:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(name, uid)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('кубик 500 5')}\n"
            f"<b>Пример:</b> {html.code('кубик все нечет')}\n"
            f"<b>Типы ставок:</b> {", ".join(bets_types)}"
        )

    if not bet_type or not bet_type in bets_types:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(name, uid)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('кубик 500 5')}\n"
            f"<b>Пример:</b> {html.code('кубик все нечет')}\n"
            f"<b>Типы ставок:</b> {", ".join(bets_types)}"
        )

    multiplayer = 2
    data = await load_data(uid)

    bet = 0
    if bet_str.lower() == "все":
        bet = int(data["coins"])
    elif bet_str.isdigit():
        bet = int(bet_str)
    elif "к" in bet_str:
        try:
            bet = parse_bet_input(bet_str)
        except Exception:
            return await msg.reply(
                f"{html.italic(f'🤨 {await gsname(name, uid)}, ты ввел что-то неправильно!')}\n"
                f"{gline()}\n"
                f"<b>Пример:</b> {html.code('кубик 500 5')}\n"
                f"<b>Пример:</b> {html.code('кубик все нечет')}\n"
                f"<b>Типы ставок:</b> {", ".join(bets_types)}"
            )

    else:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(name, uid)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('кубик 500 5')}\n"
            f"<b>Пример:</b> {html.code('кубик все нечет')}\n"
            f"<b>Типы ставок:</b> {", ".join(bets_types)}"
        )

    if bet < 10:
        return await msg.reply(f"❌ {await gsname(name, uid)}, минимальная ставка - 10 mDrops")
    if bet > data["coins"]:
        return await msg.reply(f"❌ {await gsname(name, uid)}, недостаточно mDrops для ставки")

    data["coins"] -= bet
    await save_data(uid, data)

    number = random.randint(1, 6)
    win = False

    if str(bet_type) == str(number):
        multiplayer = 3.5
        win = True
    elif str(bet_type) == "чет" and number % 2 == 0:
        multiplayer = 1.9
        win = True
    elif str(bet_type) == "нечет" and number % 2 == 1:
        multiplayer = 1.9
        win = True
    elif str(bet_type) == "б" and number >= 4:
        multiplayer = 1.9
        win = True
    elif str(bet_type) == "м" and number <= 3:
        multiplayer = 1.9
        win = True
    else:
        win = False

    b_or_m = "меньше" if number <= 3 else "больше"
    c_or_n = "чет" if number % 2 == 0 else "нечет"

    win_sum = bet * multiplayer
    if win:
        data["won_coins"] += bet
        data["coins"] += win_sum
        await save_data(uid, data)
        return await msg.reply(
            f"🎉 {await gsname(name, uid)} ты выиграл!\n{gline()}\n🎲 Число: {html.bold(number)} ({html.code(f"{b_or_m}")}, {html.code(f"{c_or_n}")})\n\n💰 Выигрыш: {format_balance(win_sum)} mDrops (x{multiplayer})")
    else:
        data["lost_coins"] += bet
        await save_data(uid, data)
        return await msg.reply(
            f"😢 {await gsname(name, uid)} ты проиграл!\n{gline()}\n🎲 Число: {html.bold(number)} ({html.code(f"{b_or_m}")}, {html.code(f"{c_or_n}")})\n\n💸 Ставка: {format_balance(bet)} mDrops")


DIAMOND_GAMES: Dict[str, dict] = {}  # game_id -> state
user_diamond_locks: Dict[str, asyncio.Lock] = {}  # locks per user (to avoid races)

TOTAL_ROWS = 50
COLUMNS = 3
SHOW_PREV_ROWS = 8
HOUSE_EDGE = 0.985


def _get_diamond_lock(user_id: str) -> asyncio.Lock:
    lock = user_diamond_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_diamond_locks[user_id] = lock
    return lock


# ---------- Multipliers ----------
def calc_next_multiplier(state: dict) -> float:
    level = int(state.get("level", 0))
    bombs = state.get("bombs", [])
    if level < 0 or level >= len(bombs):
        return 0.0
    row = bombs[level]
    mines_in_row = sum(1 for x in row if int(x) == 1)
    cells_in_row = len(row)
    safe = max(0, cells_in_row - mines_in_row)
    if cells_in_row <= 0 or safe <= 0:
        return 0.0
    p_safe = safe / cells_in_row
    mult = (1.0 / p_safe) * HOUSE_EDGE
    return round(mult, 6)


def product_multipliers(state: dict) -> float:
    prod = 1.0
    for m in state.get("multipliers_history", []):
        prod *= m
    return round(prod, 6)


# ---------- Game lifecycle (in-memory) ----------
def _new_game_id() -> str:
    return uuid.uuid4().hex


async def start_diamonds_game(user_id: str, bet: int, mines_amount: int = 1) -> str:
    """
    Создаёт игру в оперативной памяти, возвращает game_id.
    """
    if bet is None:
        raise ValueError("bet is required")
    if mines_amount < 1 or mines_amount > min(2, COLUMNS - 1):
        raise ValueError("mines_amount must be 1..2")

    bombs: List[List[int]] = []
    for _ in range(TOTAL_ROWS):
        row = [0] * COLUMNS
        mine_positions = random.sample(range(COLUMNS), mines_amount)
        for p in mine_positions:
            row[p] = 1
        bombs.append(row)

    game_id = _new_game_id()
    DIAMOND_GAMES[game_id] = {
        "game_id": game_id,
        "uid": str(user_id),
        "bet": int(bet),
        "mines_amount": int(mines_amount),
        "level": 0,
        "bombs": bombs,
        "selected": [],
        "lost": False,
        "state": "playing",
        "started_at": int(time.time()),
        "last_action_ts": 0,
        "multipliers_history": []
    }
    return game_id


def _get_state_for_game(game_id: str) -> Optional[dict]:
    return DIAMOND_GAMES.get(game_id)


def _end_and_cleanup(game_id: str):
    DIAMOND_GAMES.pop(game_id, None)


# ---------- Keyboard builder ----------
def build_diamonds_keyboard_for_game(game_id: str) -> InlineKeyboardMarkup:
    state = _get_state_for_game(game_id)
    if not state:
        return InlineKeyboardMarkup(inline_keyboard=[])

    level = int(state.get("level", 0))
    selected = state.get("selected", [])
    lost = bool(state.get("lost", False))

    if lost or level >= TOTAL_ROWS:
        return InlineKeyboardMarkup(inline_keyboard=[])

    kb_rows = []

    start_prev = max(0, level - SHOW_PREV_ROWS)
    for i in range(start_prev, level):
        choice = selected[i] if i < len(selected) else None
        row_buttons = []
        for j in range(COLUMNS):
            emoji = "💠" if (choice is not None and j == choice) else "❓"
            row_buttons.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        kb_rows.append(row_buttons)

    # current row (clickable)
    kb_rows.append([InlineKeyboardButton(text="❓", callback_data=f"diam_choose:{game_id}:{j}") for j in range(COLUMNS)])

    # actions (attach game_id so each game has its own action buttons)
    if level == 0:
        kb_rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"diam_cancel:{game_id}")])
    else:
        kb_rows.append([InlineKeyboardButton(text="💰 Забрать", callback_data=f"diam_collect:{game_id}")])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# ---------- Handlers ----------
@dp.callback_query(F.data.startswith("diam_choose:"))
@flood_protect(min_delay=0.5)
async def on_diam_choose(cb: CallbackQuery):
    # parse game_id and idx
    try:
        _, game_id, idx_s = cb.data.split(":")
        idx = int(idx_s)
    except Exception:
        await cb.answer("⚠️ Некорректные данные", show_alert=True)
        return

    state = _get_state_for_game(game_id)
    if not state:
        await cb.answer("❌ Игра не найдена или уже завершена", show_alert=True)
        return

    uid = state["uid"]
    if str(cb.from_user.id) != uid:
        await cb.answer("❌ Это не ваша игра", show_alert=True)
        return

    lock = _get_diamond_lock(uid)
    async with lock:
        try:
            if state.get("state") != "playing":
                await cb.answer("⚠️ Игра уже завершена", show_alert=True)
                return

            now = int(time.time() * 1000)
            last_ts = state.get("last_action_ts", 0)
            if now - last_ts < 200:
                await cb.answer()
                return
            state["last_action_ts"] = now

            if idx < 0 or idx >= COLUMNS:
                await cb.answer("⚠️ Неверный выбор", show_alert=True)
                return

            level = int(state.get("level", 0))
            if level < 0 or level >= TOTAL_ROWS:
                await cb.answer("⚠️ Неверный уровень игры", show_alert=True)
                return

            if state.get("lost", False):
                await cb.answer("⚠️ Вы уже проиграли", show_alert=True)
                return

            # вычисляем множитель ДО открытия
            mult_for_move = calc_next_multiplier(state)
            if mult_for_move <= 0:
                mult_for_move = 1.0

            # сохраняем выбор, если ещё не добавлен
            if len(state.get("selected", [])) <= level:
                state.setdefault("selected", []).append(idx)

            # проверяем мину
            bombs = state.get("bombs", [])
            if bombs[level][idx] == 1:
                # поражение
                state["lost"] = True
                state["state"] = "lost"

                # обновим статистику в БД: lost_coins += bet
                try:
                    data = await load_data(uid) or {}
                    data["lost_coins"] = int(data.get("lost_coins", 0)) + int(state.get("bet", 0))
                    await save_data(uid, data)
                except Exception:
                    # не критично — логирование внешним handle_error
                    pass

                final_kb = build_final_diamonds_keyboard_for_game(game_id)
                await cb.message.edit_text(
                    f"💥 <b>{await gsname(cb.from_user.first_name, cb.from_user.id)}, ты попал на мину!</b>\n{gline()}\n"
                    f"<b>🧨 Мин</b>: {state.get('mines_amount')}\n"
                    f"<b>💸 Ставка</b>: {format_balance(state.get('bet', 0))}\n"
                    f"{html.blockquote(f'<b>🪜 Ряд:</b> {level + 1}')}",
                    reply_markup=final_kb
                )
                _end_and_cleanup(game_id)
                return

            # безопасно
            state.setdefault("multipliers_history", []).append(mult_for_move)
            state["level"] = level + 1

            # если дошли до конца
            if state["level"] >= TOTAL_ROWS:
                current_total_mult = product_multipliers(state)
                win = int(state.get("bet", 0) * current_total_mult)

                # начисляем выигрыш в БД
                try:
                    data = await load_data(uid) or {}
                    data["coins"] = int(data.get("coins", 0)) + int(win)
                    data["won_coins"] = int(data.get("won_coins", 0)) + int(win)
                    await save_data(uid, data)
                except Exception:
                    pass

                state["state"] = "won"
                final_kb = build_final_diamonds_keyboard_for_game(game_id)
                await cb.message.edit_text(
                    f"🏆 Поздравляем! Вы прошли все {TOTAL_ROWS} уровней и забрали {format_balance(win)} mDrops!",
                    reply_markup=final_kb
                )
                _end_and_cleanup(game_id)
                return

            # иначе — показываем следующий уровень и текущие множители (произведение)
            current_total_mult = product_multipliers(state)
            bet = state.get('bet', 0)
            available_prize = round(current_total_mult, 2) * bet

            next_mult = calc_next_multiplier(state)
            kb = build_diamonds_keyboard_for_game(game_id)
            await cb.message.edit_text(
                f"💠<b> {await gsname(cb.from_user.first_name, uid)}, продолжаем игру!</b>\n{gline()}\n"
                f"<b>🧨 Мин:</b> {state.get('mines_amount', 1)}\n"
                f"<b>💸 Ставка:</b> {format_balance(state.get('bet', 0))} mDrops\n"
                f"<b>📊 Выигрыш:</b> x{round(current_total_mult, 2)} / {format_balance(available_prize)} mDrops\n"
                f"{html.blockquote(f'<b>🪜 Ряд:</b> {state['level']}')}",
                reply_markup=kb
            )
        except Exception as e:
            try:
                await handle_error(cb.from_user.username, e, cb.from_user.id, 600)
            except Exception:
                await cb.answer("⚠️ Внутренняя ошибка", show_alert=True)


@dp.callback_query(F.data.startswith("diam_cancel:"))
@flood_protect(min_delay=0.5)
async def on_diam_cancel(cb: CallbackQuery):
    try:
        _, game_id = cb.data.split(":")
    except Exception:
        await cb.answer();
        return
    state = _get_state_for_game(game_id)
    if not state:
        await cb.answer("❌ Игра не найдена", show_alert=True);
        return
    uid = state["uid"]
    if str(cb.from_user.id) != uid:
        await cb.answer("❌ Это не ваша игра", show_alert=True);
        return

    lock = _get_diamond_lock(uid)
    async with lock:
        if int(state.get("level", 0)) != 0 or state.get("selected"):
            await cb.answer("⚠️ Нельзя отменить после первого хода", show_alert=True)
            return
        # вернуть ставку в DB
        try:
            data = await load_data(uid) or {}
            bet = int(state.get("bet", 0))
            data["coins"] = int(data.get("coins", 0)) + bet
            await save_data(uid, data)
        except Exception:
            pass

        _end_and_cleanup(game_id)
        await cb.message.edit_text(f"❌ Игра отменена, ставка возвращена.", reply_markup=None)


@dp.callback_query(F.data.startswith("diam_collect:"))
@flood_protect(min_delay=0.5)
async def on_diam_collect(cb: CallbackQuery):
    try:
        _, game_id = cb.data.split(":")
    except Exception:
        await cb.answer();
        return
    state = _get_state_for_game(game_id)
    if not state:
        await cb.answer("❌ Игра не найдена", show_alert=True);
        return
    uid = state["uid"]
    if str(cb.from_user.id) != uid:
        await cb.answer("❌ Это не ваша игра", show_alert=True);
        return

    lock = _get_diamond_lock(uid)
    async with lock:
        if int(state.get("level", 0)) == 0:
            await cb.answer("⚠️ Слишком рано, вы ещё не сделали ход!", show_alert=True)
            return
        if state.get("lost", False):
            await cb.answer("⚠️ Вы уже проиграли", show_alert=True)
            return

        bet = state.get('bet', 0)
        current_total_mult = product_multipliers(state)
        win = int(state.get("bet", 0) * current_total_mult)

        # начисление монет в БД
        try:
            data = await load_data(uid) or {}
            data["coins"] = int(data.get("coins", 0)) + int(win)
            data["won_coins"] = int(data.get("won_coins", 0)) + int(win)
            await save_data(uid, data)
        except Exception:
            pass

        # отметим состояние как collected, чтобы финал показал мины
        state["state"] = "collected"
        final_kb = build_final_diamonds_keyboard_for_game(game_id)

        await cb.message.edit_text(
            f"🎉 <b>{await gsname(cb.from_user.first_name, cb.from_user.id)}, ты забрал выигрыш!</b> ✅\n{gline()}\n"
            f"<b>🧨 Мин:</b> {state.get('mines_amount')}\n"
            f"<b>💸 Ставка:</b> {format_balance(int(state.get('bet', 0)))} mDrops\n"
            f"<b>💰 Выигрыш:</b> x{round(current_total_mult, 2)} / {format_balance(win)} mDrops\n",
            reply_markup=final_kb
        )

        _end_and_cleanup(game_id)


@dp.callback_query(F.data == "noop")
async def do_nothing(cb: CallbackQuery):
    await cb.answer()


# ---------- Final keyboard builder (read-only, раскрываем мины при collected или lost) ----------
def build_final_diamonds_keyboard_for_game(game_id: str) -> InlineKeyboardMarkup:
    state = _get_state_for_game(game_id)
    if not state:
        return InlineKeyboardMarkup(inline_keyboard=[])

    bombs = state.get("bombs", [])
    selected = state.get("selected", [])
    lost = state.get("lost", False)
    collected = state.get("state") == "collected" or state.get("state") == "won"

    if lost:
        last = len(selected) - 1
    else:
        last = min(len(selected) - 1, TOTAL_ROWS - 1)

    if last < 0:
        return InlineKeyboardMarkup(inline_keyboard=[])

    kb_rows = []
    # отображаем ряды сверху вниз: 0..last
    for i in range(0, last + 1):
        row = bombs[i] if i < len(bombs) else [0] * COLUMNS
        choice = selected[i] if i < len(selected) else None
        buttons_row = []
        for j in range(COLUMNS):
            if lost:
                # при проигрыше: показываем мины, и если пользователь выбрал мину — 💥
                if row[j] == 1:
                    emoji = "💥" if (choice is not None and j == choice) else "🧨"
                else:
                    emoji = "💠" if (choice is not None and j == choice) else "💰"
            elif collected:
                # при сборе: показываем где были мины (🧨), выбранную безопасную — 💠, остальные — 💰
                if row[j] == 1:
                    emoji = "🧨"
                else:
                    emoji = "💠" if (choice is not None and j == choice) else "💰"
            else:
                # обычный финал без раскрытия (не должно обычно попадать сюда)
                emoji = "💠" if (choice is not None and j == choice) else "💰"
            buttons_row.append(InlineKeyboardButton(text=emoji, callback_data="noop"))
        kb_rows.append(buttons_row)
    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# ---------- Start command (создаёт новую игру и возвращает клаву с game_id) ----------
@dp.message(F.text.lower().startswith("алмазы"))
async def start_diamonds(msg: Message):
    uid = str(msg.from_user.id)
    parts = msg.text.split()
    if len(parts) != 3:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(msg.from_user.first_name, uid)}, ты ввел что-то неправильно!')}\n"
            f"{gline()}\n"
            f"<b>Пример:</b> {html.code('алмазы 5к 2')}\n"
            f"<b>Пример:</b> {html.code('алмазы все 1')}"
        )

    _, bet_str, mines_str = parts
    user_data = await load_data(uid) or {"coins": 0}
    balance = int(user_data.get("coins", 0))

    # parse bet
    if bet_str.lower() == "все":
        bet = balance
    elif "к" in bet_str:
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await msg.reply("❌ Неправильный формат ставки.")

    if bet > balance:
        return await msg.reply("❌ Недостаточно средств для ставки.")
    if bet <= 9:
        return await msg.reply("⚠️ Минимальная ставка - 10 mDrops")

    # parse mines_amount
    if not mines_str.isdigit():
        return await msg.reply("❌ Неправильный формат параметра мин.")
    mines_amount = int(mines_str)
    if mines_amount < 1 or mines_amount > 2:
        return await msg.reply("❌ Количество мин в ряду должно быть 1 или 2.")

    # reserve bet in DB
    user_data["coins"] = int(user_data.get("coins", 0)) - int(bet)
    await save_data(uid, user_data)

    # start game (in-memory)
    game_id = await start_diamonds_game(uid, bet, mines_amount)

    kb = build_diamonds_keyboard_for_game(game_id)
    state = _get_state_for_game(game_id)
    next_mult = calc_next_multiplier(state) if state else 0.0

    await msg.reply(
        f"🍀<b> {await gsname(msg.from_user.first_name, uid)}, выбери ячейку!</b>\n{gline()}\n"
        f"🧨<b> Мин:</b> {mines_amount}\n"
        f"<b>💸 Ставка</b>: {format_balance(bet)} mDrops\n",
        reply_markup=kb
    )


@dp.message(F.text.startswith("слоты"))
async def handle_slots_game(msg: Message):
    uid = msg.from_user.id
    name = msg.from_user.first_name

    data = await load_data(uid)
    if not data:
        await create_user_data(uid)
        data = await load_data(uid)

    parts = msg.text.split(" ")

    if len(parts) < 2:
        return await msg.reply(
            f"{html.italic(f'🤨 {await gsname(name, uid)}, ты ввёл неправильно!')}\n"
            f"{gline()}\n<b>Пример:</b> {html.code('слоты 1к')}"
        )

    bet_str = parts[1]

    # parse bet
    if bet_str.lower() == "все":
        bet = data["coins"]
    elif "к" in bet_str:
        bet = parse_bet_input(bet_str)
    elif bet_str.isdigit():
        bet = int(bet_str)
    else:
        return await msg.reply("❌ Неправильный формат ставки.")

    if bet > data["coins"]:
        return await msg.reply("❌ Недостаточно средств для ставки.")
    if bet <= 9:
        return await msg.reply("⚠️ Минимальная ставка - 10 mDrops")

    data["coins"] -= bet

    slot = await msg.answer_dice(emoji="🎰")
    result = int(slot.dice.value)
    idx = result - 1

    symbols = ["🔲", "🍇", "🍋", "7️⃣"]

    left = idx % 4
    middle = (idx // 4) % 4
    right = (idx // 16) % 4

    s_left = symbols[left]
    s_middle = symbols[middle]
    s_right = symbols[right]

    combo_str = f"{s_left}{s_middle}{s_right}"

    await asyncio.sleep(2)
    if result == 64:
        win = bet * 16
        await msg.reply(
            f"<b>🎉 {await gsname(name, id)}, ты выиграл!</b>\n{gline()}\n<b>Результат:</b> 7️⃣7️⃣7️⃣\n<b>Выигрыш:</b> x16/{format_balance(win)} mDrops")

        data["coins"] += win
        data["won_coins"] += win
        await save_data(uid, data)
    elif result == 22:
        win = bet * 16
        await msg.reply(
            f"<b>🎉 {await gsname(name, id)}, ты выиграл!</b>\n{gline()}\n<b>Результат:</b> 🍇🍇🍇\n<b>Выигрыш:</b> x16/{format_balance(win)} mDrops")

        data["coins"] += win
        data["won_coins"] += win
        await save_data(uid, data)
    elif result == 43:
        win = bet * 16
        await msg.reply(
            f"<b>🎉 {await gsname(name, id)}, ты выиграл!</b>\n{gline()}\n<b>Результат:</b> 🍋🍋🍋\n<b>Выигрыш:</b> x16/{format_balance(win)} mDrops")

        data["coins"] += win
        data["won_coins"] += win
        await save_data(uid, data)
    elif result == 1:
        win = bet * 16
        await msg.reply(
            f"<b>🎉 {await gsname(name, id)}, ты выиграл!</b>\n{gline()}\n<b>Результат:</b> 🔲🔲🔲\n<b>Выигрыш:</b> x16/{format_balance(win)} mDrops")

        data["coins"] += win
        data["won_coins"] += win
        await save_data(uid, data)
    else:
        await msg.reply(
            f"<b>😔 {await gsname(name, id)}, ты проиграл!</b>\n{gline()}\n<b>Результат:</b> {combo_str}\n<b>Ставка:</b> {format_balance(bet)} mDrops")

        data["lost_coins"] += bet
        await save_data(uid, data)


# -------------- PROMOTION -------------- #
import urllib.parse

TASKS_FILE = "tasks.json"  # file for promo tasks
_tasks_lock = asyncio.Lock()
CHECK_INTERVAL_SECONDS = 60 * 10  # background loop check interval
GRACE_PERIOD = timedelta(hours=24)
REQUIRED_DAYS = 7
PRICE_PER_SUB = 1


def _read_tasks_sync() -> List[Dict]:
    if not os.path.exists(TASKS_FILE):
        return []
    try:
        with open(TASKS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_tasks_sync(tasks: List[Dict]):
    os.makedirs(os.path.dirname(TASKS_FILE) or ".", exist_ok=True)
    with open(TASKS_FILE, "w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)


async def init_tasks_file():
    async with _tasks_lock:
        if not os.path.exists(TASKS_FILE):
            await asyncio.to_thread(_write_tasks_sync, [])


async def load_tasks() -> List[Dict]:
    return await asyncio.to_thread(_read_tasks_sync)


async def save_tasks(tasks: List[Dict]):
    await asyncio.to_thread(_write_tasks_sync, tasks)


async def add_task(channel_id: int, owner_id: str, count: int, title: Optional[str] = None) -> Dict:
    if count <= 0:
        raise ValueError("count must be positive")
    task = {
        "channel_id": int(channel_id),
        "owner": str(owner_id),
        "remaining": int(count),
        "price_per_sub": PRICE_PER_SUB,
        "title": title or str(channel_id),
        "created_at": datetime.utcnow().isoformat()
    }
    async with _tasks_lock:
        tasks = await load_tasks()
        tasks.append(task)
        await save_tasks(tasks)
    return task


async def find_task(channel_id: int, created_at: Optional[str] = None) -> Optional[Dict]:
    async with _tasks_lock:
        tasks = await load_tasks()
        for t in tasks:
            if int(t.get("channel_id")) == int(channel_id):
                if created_at is None or t.get("created_at") == created_at:
                    return t
    return None


async def decrement_task(channel_id: int, created_at: str) -> Tuple[Optional[Dict], bool]:
    async with _tasks_lock:
        tasks = await load_tasks()
        for idx, t in enumerate(tasks):
            if int(t.get("channel_id")) == int(channel_id) and t.get("created_at") == created_at:
                t["remaining"] = max(0, int(t.get("remaining", 0)) - 1)
                if t["remaining"] <= 0:
                    removed = tasks.pop(idx)
                    await save_tasks(tasks)
                    return removed, True
                else:
                    tasks[idx] = t
                    await save_tasks(tasks)
                    return t, False
    return None, False


async def remove_task_exact(channel_id: int, created_at: str) -> Optional[Dict]:
    async with _tasks_lock:
        tasks = await load_tasks()
        for idx, t in enumerate(tasks):
            if int(t.get("channel_id")) == int(channel_id) and t.get("created_at") == created_at:
                removed = tasks.pop(idx)
                await save_tasks(tasks)
                return removed
    return None


# ------------------ FSM States ------------------
class PromoStates(StatesGroup):
    waiting_for_count = State()


# ------------------ PROMOTION HANDLERS ------------------
@dp.message(F.text.lower().in_(["/promotion", "продвижение", "продвигать", "реклама", "/promotion@gmegadbot"]))
async def cmd_promotion(message: Message):
    if message.chat.type != "private":
        kb = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="🍓 Перейти в ЛС", url="t.me/gmegadbot")]]))
        return await message.reply(
            f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, продвижение доступно только в {html.link("ЛС с ботом", "t.me/gmegadbot")}!",
            reply_markup=kb, disable_web_page_preview=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Отправить канал", callback_data="promo_add_channel")]
    ])
    await message.answer(
        "📢 Продвижение: нажмите кнопку и перешлите сообщение из вашего канала (бот должен быть админом).",
        reply_markup=kb)


@dp.callback_query(lambda c: c.data == "promo_add_channel")
async def promo_add_channel_cb(query: CallbackQuery):
    await query.answer()
    await query.message.answer("👉 Перешлите любой пост из своего канала, где бот добавлен как администратор.")


@dp.message(lambda m: getattr(m, "forward_from_chat", None) is not None)
async def handle_forward_from_channel(message: Message, state: FSMContext):
    channel = message.forward_from_chat
    if not channel or getattr(channel, "type", None) != "channel":
        return
    user_id = str(message.from_user.id)

    # проверяем что бот админ
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(channel.id, me.id)
        if getattr(member, "status", None) not in ("administrator", "creator"):
            return await message.answer("❌ Добавьте бота в администраторы канала и повторите.")
    except Exception:
        return await message.answer("⚠ Не удалось проверить права бота в канале. Добавьте бота и повторите.")

    user = await load_data(user_id) or {}
    pending = user.setdefault("pending", {})
    pending["promo_channel_id"] = int(channel.id)
    pending["promo_channel_title"] = channel.title or str(channel.id)
    await save_data(user_id, user)

    await message.answer(
        f"✅ Канал распознан: <b>{pending['promo_channel_title']}</b>\n"
        "Сколько подписок заказать? (введите целое число)"
    )

    # ---- Ось правильно встановлюємо стан через FSMContext ----
    await state.set_state(PromoStates.waiting_for_count)


@dp.message(StateFilter(PromoStates.waiting_for_count))
async def receive_count_for_promo(message: Message, state: FSMContext):
    user_id = str(message.from_user.id)
    text = (message.text or "").strip()
    try:
        count = int(text)
        if count <= 0:
            raise ValueError()
    except Exception:
        return await message.answer("Введите положительное целое число подписок.")

    user = await load_data(user_id) or {}
    pending = user.get("pending", {})
    channel_id = pending.get("promo_channel_id")
    channel_title = pending.get("promo_channel_title")
    if not channel_id:
        await state.clear()
        return await message.answer("Ошибка: информация о канале не найдена. Повторите /promotion.")

    # проверяем опять права
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(int(channel_id), me.id)
        if getattr(member, "status", None) not in ("administrator", "creator"):
            await state.clear()
            return await message.answer("❌ Бот больше не администратор в канале. Добавьте бота и повторите.")
    except Exception:
        await state.clear()
        return await message.answer("⚠ Не удалось проверить права бота в канале. Повторите позже.")

    # списываем GGs
    user_ = await load_data(user_id) or {}
    ggs = int(user_.get("GGs", 0))
    cost = count * PRICE_PER_SUB
    if ggs < cost:
        await state.clear()
        return await message.answer(f"❌ У вас недостаточно GGs. Нужно {cost}, у вас {ggs}.")

    user_["GGs"] = ggs - cost
    user_.setdefault("pending", {}).pop("promo_channel_id", None)
    user_.setdefault("pending", {}).pop("promo_channel_title", None)
    await save_data(user_id, user_)

    # add task
    await init_tasks_file()
    task = await add_task(channel_id=int(channel_id), owner_id=user_id, count=count, title=channel_title)

    await state.clear()
    await message.answer(f"✅ Задача создана: <b>{channel_title}</b> — {count} подписок. Списано {cost} GGs.")


# ------------------ EARN HANDLERS ------------------
import urllib.parse
from math import ceil

PAGE_SIZE = 5  # кількість задач на сторінку


@dp.message(F.text.lower().in_(["/earn", "заработать", "заработок", "/earn@gmegadbot"]))
async def cmd_earn(message: Message):
    if message.chat.type != "private":
        kb = InlineKeyboardMarkup(
            inline_keyboard=([[InlineKeyboardButton(text="🍓 Перейти в ЛС", url="t.me/gmegadbot")]]))
        return await message.reply(
            f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, заработок доступен только в {html.link('ЛС с ботом', 't.me/gmegadbot')}!",
            reply_markup=kb, disable_web_page_preview=True
        )

    user_id = str(message.from_user.id)
    user = await load_data(user_id) or {}
    if user.get("earn_ban"):
        return await message.answer("🚫 Вы заблокированы в системе заработка.")

    tasks = await load_tasks()
    available = [t for t in tasks if t.get("remaining", 0) > 0]
    if not available:
        return await message.answer("😔 Сейчас нет доступных каналов для подписки.")

    # Параметри пагінації
    total = len(available)
    last_page = max(1, ceil(total / PAGE_SIZE))
    page = 1

    # Формуємо і відправляємо першу сторінку як одне повідомлення
    await send_earn_page(message.chat.id, available, page, last_page)


async def send_earn_page(chat_id: int | str, available: list, page: int, last_page: int,
                         edit_message: Message | None = None):
    """
    Формує і відправляє (або редагує) сторінку з доступними задачами.
    Якщо edit_message задано — редагуємо його, інакше — відправляємо нове.
    (Версія БЕЗ html.escape)
    """
    # обчислення індексів
    page = max(1, page)
    offset = (page - 1) * PAGE_SIZE
    page_tasks = available[offset: offset + PAGE_SIZE]

    # Формуємо текст
    header = f"📋 Доступные задачи — Страница {page}/{last_page}\n\n"
    lines = []
    for i, t in enumerate(page_tasks, start=offset + 1):
        ch_id = int(t["channel_id"])
        title = t.get("title") or str(ch_id)
        # намагаємось отримати назву каналу
        try:
            chat = await bot.get_chat(ch_id)
            title = getattr(chat, "title", title) or title
        except Exception as e:
            pass

        remaining = t.get("remaining", 0)
        reward = t.get("price_per_sub", PRICE_PER_SUB)
        # Без ескейпінгу — підставляємо назву прямо
        lines.append(f"{i}. <b>{str(title)}</b>\n   Осталось: {remaining} — Награда: {reward} GGs/подписка")

    text = header + ("\n\n".join(lines) if lines else "Пока что нет задач на этой странице.")

    # Формуємо клавіатуру
    kb_rows = []

    for t in page_tasks:
        ch_id = int(t["channel_id"])
        username = None
        try:
            chat = await bot.get_chat(ch_id)
            username = getattr(chat, "username", None)
        except Exception:
            pass

        created_encoded = urllib.parse.quote_plus(str(t.get("created_at", "")))
        row = []
        if username:
            # кнопка url
            row.append(InlineKeyboardButton(text="Открыть канал", url=f"https://t.me/{username}"))
        # кнопка перевірки підписки
        row.append(InlineKeyboardButton(text="✅ Я подписался", callback_data=f"check_sub:{ch_id}:{created_encoded}"))

        kb_rows.append(row)

    # Навігація
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"earn_page:{page - 1}"))
    if last_page > 1 and page < last_page:
        nav_row.append(InlineKeyboardButton(text="Дальше ➡️", callback_data=f"earn_page:{page + 1}"))
    if nav_row:
        kb_rows.append(nav_row)

    kb_rows.append([InlineKeyboardButton(text="Закрыть", callback_data="earn_close")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    try:
        if edit_message:
            await edit_message.edit_text(text, reply_markup=kb, disable_web_page_preview=True, parse_mode="HTML")
        else:
            await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True, parse_mode="HTML")
    except Exception:
        if not edit_message:
            await bot.send_message(chat_id, text, disable_web_page_preview=True, parse_mode="HTML")


@dp.callback_query(F.data.startswith("earn_page:"))
async def handle_earn_page(callback: CallbackQuery):
    # Пагінація — перепрораховуємо список заново, щоб відобразити актуальний стан
    try:
        page = int(callback.data.split(":", 1)[1])
    except Exception:
        page = 1

    tasks = await load_tasks()
    available = [t for t in tasks if t.get("remaining", 0) > 0]
    if not available:
        return await callback.message.edit_text("😔 Сейчас нет доступных каналов для подписки.")

    total = len(available)
    last_page = max(1, ceil(total / PAGE_SIZE))
    page = max(1, min(page, last_page))

    await send_earn_page(callback.message.chat.id, available, page, last_page, edit_message=callback.message)
    await callback.answer()  # закриває "завантаження" у клієнта


@dp.callback_query(F.data == "earn_close")
async def handle_earn_close(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        try:
            await callback.message.edit_text("Меню закрыто.")
        except Exception:
            pass
    await callback.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith("check_sub:"))
@flood_protect(min_delay=0.5)
async def cb_check_sub(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        return await callback.message.answer("Ошибка данных.")
    try:
        channel_id = int(parts[1])
    except Exception:
        return await callback.message.answer("Ошибка данных: неверный id канала.")
    created_at = urllib.parse.unquote_plus(parts[2])
    user_id = str(callback.from_user.id)

    user = await load_data(user_id) or {}
    if user.get("earn_ban"):
        return await callback.message.answer("🚫 Вы заблокированы в системе заработка.")

    try:
        try:
            member = await bot.get_chat_member(channel_id, int(user_id))
        except ChatNotFound:
            return await callback.message.answer("❗ Канал не найден или он приватный. Невозможно проверить подписку.")
        except BadRequest as e:
            # Например: "USER_ID_INVALID" или "CHAT_ADMIN_REQUIRED" и т.п.
            return await callback.message.answer(
                "❗ Не удалось проверить — бот не имеет доступа к информации по этому каналу.")
        except Unauthorized:
            return await callback.message.answer("❗ Бот не имеет доступа к этому каналу.")
        except RetryAfter as e:

            await callback.answer("Сервер Telegram временно перегружен — попробуйте чуть позже.", show_alert=True)
            return
        except Exception as e:

            return await callback.message.answer(
                "⚠ Не удалось проверить подписку (ошибка API). Попробуйте снова позже.")

        status = getattr(member, "status", None)
        if status in ("member", "administrator", "creator"):
            subs = user.setdefault("subscriptions", {})
            sub_key = str(channel_id)
            now = datetime.utcnow()

            if subs.get(sub_key, {}).get("active"):
                return await callback.message.answer(
                    "Вы уже выполняли это задание или у вас уже активная подписка на этот канал.")

            user["GGs"] = user.get("GGs", 0) + PRICE_PER_SUB
            subs[sub_key] = {
                "start": now.isoformat(),
                "end": (now + timedelta(days=REQUIRED_DAYS)).isoformat(),
                "active": True,
                "grace_until": None,
                "task_created_at": created_at
            }
            await save_data(user_id, user)

            removed_or_updated, deleted = await decrement_task(channel_id, created_at)
            if deleted:
                owner_id = removed_or_updated.get("owner")
                try:
                    await bot.send_message(int(owner_id),
                                           f"✅ Ваша задача для каналу <b>{removed_or_updated.get('title')}</b> выполнена и удалена.")
                except Exception:
                    pass
            else:
                if removed_or_updated:
                    owner_id = removed_or_updated.get("owner")
                    try:
                        await bot.send_message(int(owner_id),
                                               f"ℹ️ У задачи для <b>{removed_or_updated.get('title')}</b> осталось {removed_or_updated.get('remaining')} подписок.")
                    except Exception:
                        pass

            await callback.message.answer(
                f"🎉 Подписка подтверждена! +{PRICE_PER_SUB} GGs. Не отписывайтесь в течение {REQUIRED_DAYS} дней.")
        else:
            return await callback.message.answer("❗ Вы не подписаны на канал.")
    except Exception:
        # Логируем будь-яку іншу несподівану помилку і віддаємо дружнє повідомлення

        return await callback.message.answer("⚠ Не удалось проверить подписку (внутренняя ошибка). Попробуйте позже.")


# ------------------ BACKGROUND CHECKER ------------------
async def check_subscriptions_loop():
    # wait until bot ready
    await bot.get_me()
    while True:
        try:
            now = datetime.utcnow()
            # iterate all numeric keys in sqlite json_data
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute("SELECT key, value FROM json_data")
                rows = cur.fetchall()

            for row in rows:
                key = row["key"]
                if not key.isdigit():
                    continue
                user_id = key
                try:
                    user = json.loads(row["value"])
                except Exception:
                    continue
                subs = user.get("subscriptions", {})
                if user.get("earn_ban"):
                    continue

                user_changed = False
                for ch_id, s in list(subs.items()):
                    try:
                        ch_int = int(ch_id)
                    except Exception:
                        continue

                    end = None
                    grace = None
                    try:
                        end = datetime.fromisoformat(s.get("end")) if s.get("end") else None
                    except Exception:
                        end = None
                    try:
                        grace = datetime.fromisoformat(s.get("grace_until")) if s.get("grace_until") else None
                    except Exception:
                        grace = None

                    # finished allowed period
                    if end and now >= end:
                        subs[ch_id]["active"] = False
                        subs[ch_id].pop("grace_until", None)
                        user_changed = True
                        continue

                    # still within protected period
                    if end and now < end:
                        try:
                            member = await bot.get_chat_member(ch_int, int(user_id))
                            is_member = getattr(member, "status", None) in ("member", "administrator", "creator")
                        except ChatNotFound:
                            # Канал видалено або приватний — вважаємо, що юзер не в каналі
                            is_member = False
                        except BadRequest as e:

                            is_member = False
                        except Unauthorized:

                            is_member = False
                        except RetryAfter as e:
                            # Якщо телеграм просить почекати — чекаємо і продовжуємо цикл

                            await asyncio.sleep(getattr(e, "timeout", 1))
                            # після паузи — повторимо перевірку в наступному проході
                            is_member = False
                        except Exception as e:

                            is_member = False

                        if is_member:
                            if s.get("grace_until"):
                                subs[ch_id]["grace_until"] = None
                                user_changed = True
                        else:
                            if not s.get("grace_until"):
                                subs[ch_id]["grace_until"] = (now + GRACE_PERIOD).isoformat()
                                user_changed = True
                                try:
                                    await bot.send_message(int(user_id),
                                                           f"⚠ Вы отписались от канала {ch_id}. У вас есть 24 часа, чтобы вернуться, иначе вы будете заблокированы в системе заработка.")
                                except Exception:
                                    pass
                            else:
                                grace_dt = None
                                try:
                                    grace_dt = datetime.fromisoformat(s.get("grace_until"))
                                except Exception:
                                    grace_dt = None
                                if grace_dt and now >= grace_dt:
                                    user["earn_ban"] = True
                                    user_changed = True
                                    try:
                                        await bot.send_message(int(user_id),
                                                               "🚫 Вы заблокированы в системе заработка за отписку и невозвращение в течение 24 часов.")
                                    except Exception:
                                        pass
                                    for kk in list(subs.keys()):
                                        subs[kk]["active"] = False
                                    break
                if user_changed:
                    await save_data(user_id, user)
        except Exception as e:
            print("Error in subscription checker:", e)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


def format_duration(future_dt: datetime) -> str:
    """
    Возвращает строку типа '3d 4h' или '5h 12m' относительно текущего времени.
    """
    now = datetime.now()
    if future_dt <= now:
        return "завершено"
    delta = future_dt - now
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    parts = []
    if days > 0:
        parts.append(f"{days}д")
    if hours > 0:
        parts.append(f"{hours}ч")
    if minutes > 0 and days == 0:  # показываем минуты только если мало времени
        parts.append(f"{minutes}м")
    return " ".join(parts) if parts else "меньше минуты"


# ---------------- SHOP ----------------

@dp.message(F.text.in_(["/shop", "магаз", "магазин", "шоп", "маркет"]))
async def handle_shop_command(msg: Message):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    kb = InlineKeyboardBuilder()
    kb.button(
        text="🪅 Статусы",
        callback_data=f"shop_statuses_view:{user_id}"
    )

    await msg.answer(
        f"🛍 {await gsname(name, user_id)}, ты в магазине!\n\nВыбери раздел:",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data.startswith("shop_callback"))
async def handle_shop_command(query: CallbackQuery):
    user_id = query.from_user.id
    name = query.from_user.first_name
    data = query.data
    caller_id = data.split(":", 1)[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Это не твоя кнопка!")

    kb = InlineKeyboardBuilder()
    kb.button(
        text="🪅 Статусы",
        callback_data=f"shop_statuses_view:{user_id}"
    )

    await query.message.edit_text(
        f"🛍 {await gsname(name, user_id)}, ты в магазине!\n\nВыбери раздел:",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data.startswith("shop_statuses_view"))
async def handle_view_shop_statuses(query: CallbackQuery):
    user_id = query.from_user.id
    name = query.from_user.first_name
    data = query.data
    caller_id = data.split(":", 1)[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Это не твоя кнопка!")

    statuses_line = " • " + "\n • ".join(BUYABLE_STATUSES)

    kb = InlineKeyboardBuilder()
    for i, status in enumerate(BUYABLE_STATUSES):
        kb.button(
            text=status,
            callback_data=f"statuses_view:{i}:{caller_id}"
        )

    # Робимо по 2 кнопки в ряд
    kb.adjust(2)

    await query.message.edit_text(
        f"{await gsname(name, user_id)}, доступные для покупки статусы:\n{statuses_line}",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data.startswith("statuses_view"))
async def handle_view_shop_statuses(query: CallbackQuery):
    user_id = query.from_user.id
    name = query.from_user.first_name
    data = query.data
    status_id = int(data.split(":")[1])
    caller_id = data.split(":")[2]

    int_price = 0
    price = ""
    price_type = ""

    if int(caller_id) != int(user_id):
        return await query.answer("Это не твоя кнопка!")

    if BUYABLE_STATUSES_PRICES[status_id][0] == "m":
        int_price = int(BUYABLE_STATUSES_PRICES[status_id][1:])
        price = f"{format_balance(int(BUYABLE_STATUSES_PRICES[status_id][1:]))} mDrops"
        price_type = "m"
    if BUYABLE_STATUSES_PRICES[status_id][0] == "l":
        int_price = int(BUYABLE_STATUSES_PRICES[status_id][1:])
        price = f"недоступен"
        price_type = "l"
    elif BUYABLE_STATUSES_PRICES[status_id][0] == "s":
        int_price = int(BUYABLE_STATUSES_PRICES[status_id][1:])
        price = f"{BUYABLE_STATUSES_PRICES[status_id][1:]} телеграм звезд"
        price_type = "s"

    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    kb = None
    if price_type == "m" and int_price < data["coins"]:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"💸 Купить {BUYABLE_STATUSES[status_id]}",
                                                                         callback_data=f"sbuy_status:{status_id}:{user_id}")],
                                                   [InlineKeyboardButton(text="Назад",
                                                                         callback_data=f"shop_statuses_view:{user_id}")]])
    elif price_type == "s":
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"💸 Купить {BUYABLE_STATUSES[status_id]}",
                                                                         callback_data=f"buy_status:{status_id}:{user_id}")],
                                                   [InlineKeyboardButton(text="Назад",
                                                                         callback_data=f"shop_statuses_view:{user_id}")]])
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data=f"shop_statuses_view:{user_id}")]])

    await query.message.edit_text(
        f"🔗 {await gsname(name, user_id)}, информация про статус \"{BUYABLE_STATUSES[status_id]}\":\n"
        f"\n💰 Цена: {price}\n"
        f"{"\nДанный статус из лимитированной коллекции!" if price_type == "l" else ""}", reply_markup=kb)


def get_buy_kb(id: int, handler_balance: str, handler_card: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="💰 С баланса", callback_data=f"{handler_balance}:{id}")],
                         [InlineKeyboardButton(text="💳 С карты", callback_data=f"{handler_card}:{id}")]])


PAY_TEXT = "⭐️ Выбери способ оплаты:"


@dp.callback_query(F.data.startswith("sbuy_status"))
async def buy_status_choose(cq: CallbackQuery):
    user_id = cq.from_user.id
    name = cq.from_user.first_name
    data_str = cq.data
    status_id = int(data_str.split(":")[1])
    caller_id = data_str.split(":")[2]

    if int(caller_id) != int(user_id):
        return await cq.answer("Это не твоя кнопка!")

    raw_price = BUYABLE_STATUSES_PRICES[status_id]
    if raw_price[0] == "m":
        int_price = int(raw_price[1:])
        price_type = "m"
    elif raw_price[0] == "l":
        return cq.answer("Данный статус не доступен для покупки!")
    elif raw_price[0] == "s":
        int_price = int(raw_price[1:])
        price_type = "s"
    else:
        return await cq.answer("Неверная цена.", show_alert=True)

    # отримуємо дані користувача
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    await cq.message.edit_text(f"{PAY_TEXT}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 С баланса", callback_data=f"buy_status_wb:{status_id}:{user_id}")],
        [InlineKeyboardButton(text="💳 С карты", callback_data=f"buy_status_wc:"
                                                              f"{status_id}:{user_id}")]]))


@dp.callback_query(F.data.startswith("buy_status_wb"))
async def handle_buy_status_coins(query: CallbackQuery):
    user_id = query.from_user.id
    name = query.from_user.first_name
    data_str = query.data
    status_id = int(data_str.split(":")[1])
    caller_id = data_str.split(":")[2]

    if int(caller_id) != int(user_id):
        return await query.answer("Это не твоя кнопка!")

    raw_price = BUYABLE_STATUSES_PRICES[status_id]
    if raw_price[0] == "m":
        int_price = int(raw_price[1:])
        price_type = "m"
    elif raw_price[0] == "l":
        return query.answer("Данный статус не доступен для покупки!")
    elif raw_price[0] == "s":
        int_price = int(raw_price[1:])
        price_type = "s"
    else:
        return await query.answer("Неверная цена.", show_alert=True)

    # отримуємо дані користувача
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if price_type == "m":
        # миттєва покупка (mDrops)
        if data.get("coins", 0) < int_price:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data=f"shop_statuses_view:{user_id}")]])
            return await query.message.edit_text(
                f"{await gsname(name, user_id)}, тебе не хватает mDrops для покупки этого статуса", reply_markup=kb)
        data["coins"] = data.get("coins", 0) - int_price
        data["status"] = status_id + len(STATUSES) - len(BUYABLE_STATUSES)
        await save_data(user_id, data)
        await query.message.edit_text(
            f"{await gsname(name, user_id)}, ти купил статус \"{BUYABLE_STATUSES[status_id]}\" за {format_balance(int_price)} mDrops!",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="В Магазин", callback_data=f"shop_callback:{user_id}")]])
        )
        return

    # --- Якщо тут, то price_type == "s" -> відправляємо інвойс для оплати зірками ---
    status_name = BUYABLE_STATUSES[status_id]
    stars_price = int_price  # скільки зірок просимо

    prices = [LabeledPrice(label=f"{status_name}", amount=int(stars_price))]

    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="Покупка статуса",
            description=f"{status_name} за {stars_price} ⭐️",
            payload=f"buy_status:{user_id}:{status_id}:{stars_price}",
            provider_token="",
            currency="XTR",  # залишив як у вашому прикладі
            prices=prices,
            start_parameter=f"buy_status_{user_id}_{status_id}"
        )
    except Exception as e:
        # помилка відправки інвойсу
        await query.answer("Не удалось отправить форму оплаты, попробуйте позже!", show_alert=True)
        # можна залогувати e
        return

    # повідомляємо користувача, що чекати інвойс у чаті
    await query.answer("Форма отрпавлена, оплатите для получения статуса", show_alert=False)


@dp.callback_query(F.data.startswith("buy_status_wc"))
async def handle_buy_status_card(query: CallbackQuery):
    user_id = query.from_user.id
    name = query.from_user.first_name
    data_str = query.data
    status_id = int(data_str.split(":")[1])
    caller_id = data_str.split(":")[2]

    if int(caller_id) != int(user_id):
        return await query.answer("Это не твоя кнопка!")

    raw_price = BUYABLE_STATUSES_PRICES[status_id]
    if raw_price[0] == "m":
        int_price = int(raw_price[1:])
        price_type = "m"
    elif raw_price[0] == "l":
        return query.answer("Данный статус не доступен для покупки!")
    elif raw_price[0] == "s":
        int_price = int(raw_price[1:])
        price_type = "s"
    else:
        return await query.answer("Неверная цена.", show_alert=True)

    # отримуємо дані користувача
    data = await load_data(user_id)
    if not data:
        await create_user_data(user_id)
        data = await load_data(user_id)

    if price_type == "m":
        buy = await pay_with_card(str(user_id), float(int_price), note="Покупка статуса")
        if not buy[0]:
            return await query.message.edit_text(f"Ошибка оплаты!\n\nКомментарий: {buy[1]}")
        data["status"] = status_id + len(STATUSES) - len(BUYABLE_STATUSES)
        await save_data(user_id, data)
        await query.message.edit_text(
            f"{await gsname(name, user_id)}, ти купил статус \"{BUYABLE_STATUSES[status_id]}\" за {format_balance(int_price)} mDrops!",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="В Магазин", callback_data=f"shop_callback:{user_id}")]])
        )
        return

    # --- Якщо тут, то price_type == "s" -> відправляємо інвойс для оплати зірками ---
    status_name = BUYABLE_STATUSES[status_id]
    stars_price = int_price  # скільки зірок просимо

    prices = [LabeledPrice(label=f"{status_name}", amount=int(stars_price))]

    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="Покупка статуса",
            description=f"{status_name} за {stars_price} ⭐️",
            payload=f"buy_status:{user_id}:{status_id}:{stars_price}",
            provider_token="",
            currency="XTR",  # залишив як у вашому прикладі
            prices=prices,
            start_parameter=f"buy_status_{user_id}_{status_id}"
        )
    except Exception as e:
        # помилка відправки інвойсу
        await query.answer("Не удалось отправить форму оплаты, попробуйте позже!", show_alert=True)
        # можна залогувати e
        return

    # повідомляємо користувача, що чекати інвойс у чаті
    await query.answer("Форма отрпавлена, оплатите для получения статуса", show_alert=False)


@dp.pre_checkout_query(lambda q: True)
async def process_pre_checkout(pre_checkout_q: PreCheckoutQuery):
    # Можна перевіряти payload тут, якщо потрібно
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)


@dp.message(F.successful_payment)
async def handle_successful_payment(message: Message):
    success: SuccessfulPayment = message.successful_payment
    payload = success.invoice_payload  # наш payload

    # Обробляємо payload формату: buy_status:{user_id}:{status_id}:{stars_price}
    if not payload:
        return

    if payload.startswith("buy_status:"):
        try:
            parts = payload.split(":")
            # parts: ["buy_status", "<user_id>", "<status_id>", "<stars_price>"]
            payload_user_id = int(parts[1])
            payload_status_id = int(parts[2])
            payload_price = int(parts[3])
        except Exception:
            # некоректний payload
            await message.reply("Ошибка обработки оплаты (payload). Свяжитесь с админом.",
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="Написать админу", url="t.me/sollamon")]]))
            return

        # Переконаємось, що платник - це той самий user_id (іноді можна порівняти id)
        payer_id = message.from_user.id
        if payer_id != payload_user_id:
            # Можна або дозволити, або відхилити — вирішуйте самі. Ми просто пишемо попередження.
            await message.reply("Случилась ошибка при оплате")
            # Ми все одно дамо статус тому, хто оплатив:

        # Завантажуємо дані користувача і присвоюємо статус
        data = await load_data(payload_user_id)
        if not data:
            await create_user_data(payload_user_id)
            data = await load_data(payload_user_id)

        # Присвоюємо статус (логіка зсуву та ж сама)
        data["status"] = payload_status_id + len(STATUSES) - len(BUYABLE_STATUSES)
        await save_data(payload_user_id, data)

        # Відправляємо підтвердження (редагуємо / відправляємо повідомлення)
        try:
            # edit message where інвойс був (можна спробувати)
            await message.reply(
                f"{await gsname(message.from_user.first_name, payload_user_id)}, оплата пройдена ✅\nВам выдано статус \"{BUYABLE_STATUSES[payload_status_id]}\".")
        except:
            pass

    elif payload.startswith("donate_"):
        # Ваша існуюча логіка доната (додати GGs, або що ви робите після оплати)
        # наприклад: payload = donate_<ggs>_<stars_price>
        parts = payload.split("_")
        # реалізуйте під свої потреби
        return


# ---------------- PARTNERS PROGRAM ----------------
ADMIN_PARTNER_ID = 8493326566
PARTNERS_FILE = "partners.json"
PENDING_FILE = "partners_pending.json"
USERS_FILE = "users.json"
PARTNERS_LOG = "partners_log.json"

# выплаты по уровням (mDrops)
LEVEL_PAYOUT = {
    1: 100_000,
    2: 150_000,
    3: 250_000,
    4: 350_000,
    5: 500_000,
}


# ====== FSM ======
class PartnerApproveStates(StatesGroup):
    waiting_for_subs = State()


class PartnerCreateStates(StatesGroup):
    waiting_for_channel_link = State()
    waiting_for_user_confirm = State()


# ====== Утилиты для JSON ======

def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_partners():
    return load_json(PARTNERS_FILE, {"channels": {}})


def save_partners(data) -> None:
    save_json(PARTNERS_FILE, data)


def load_pending():
    return load_json(PENDING_FILE, {})


def save_pending(data) -> None:
    save_json(PENDING_FILE, data)


# ====== Логика уровня по подписчикам ======

def subs_to_level(subs: int) -> int:
    if subs <= 150:
        return 1
    if 151 <= subs <= 250:
        return 2
    if 251 <= subs <= 500:
        return 3
    if 501 <= subs <= 1000:
        return 4
    return 5


# ====== Хелперы поиска контрактов ======
def find_contract_by_owner(owner_id: int) -> Tuple[Optional[str], Optional[dict]]:
    partners = load_partners()
    for key, v in partners.get("channels", {}).items():
        try:
            if int(v.get("owner_id", -1)) == int(owner_id):
                return key, v
        except Exception:
            continue
    return None, None


def find_contract_by_id(contract_id: int) -> Tuple[Optional[str], Optional[dict]]:
    partners = load_partners()
    for key, v in partners.get("channels", {}).items():
        try:
            if int(v.get("id", -1)) == int(contract_id):
                return key, v
        except Exception:
            continue
    return None, None


# ====== Генерация 7-символьного кода ======
def gen_verify_code(length: int = 7) -> str:
    import string
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


# ====== Хендлеры партнерки ======

@dp.message(F.text.lower().in_(["партенраская программа", "пп", "партнерка", "/partners"]))
async def handle_partners_program(msg: Message):
    """
    Основная команда /partners — показ контракта или начало создания заявки.
    Процесс теперь последовательный: "Создать контракт" -> ввод ссылки -> подтверждение.
    """
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    # Получаем username бота (если нужно)
    BOT_USERNAME = None
    try:
        if bot is not None:
            me = await bot.get_me()
            BOT_USERNAME = me.username or me.first_name
    except Exception:
        BOT_USERNAME = None

    if msg.chat.type != "private":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍓 Перейти в ЛС",
                                  url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "https://t.me/yourbot")]
        ])
        return await msg.reply(
            f"🍓 {await gsname(name, user_id)}, команда доступна только в ЛС с ботом!",
            reply_markup=kb
        )

    key, user_contract = find_contract_by_owner(user_id)
    if user_contract:
        last_ts = int(user_contract.get("last_payment", 0) or 0)
        last_dt = datetime.utcfromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M:%S") if last_ts else "никогда"
        level = user_contract.get("level", 1)
        subs = user_contract.get("subs", 0)
        link = user_contract.get("link", "—")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Получить выплату",
                                  callback_data=f"partners_claim:{user_contract.get('id')}:{user_id}")],
            [InlineKeyboardButton(text="Удалить контракт",
                                  callback_data=f"partners_delete:{user_contract.get('id')}:{user_id}")]
        ])
        await msg.answer(
            f"📄 Ваш контракт:\n\n🔗 Канал: {link}\n👤 Владелец: {user_id}\n📈 Подписчиков: {subs}\n🏷 Уровень: {level}\n⏱️ Последняя выплата: {last_dt}",
            reply_markup=kb
        )
        return

    # Проверка, чтобы пользователь не создал более одного pending
    pending = load_pending()
    if str(user_id) in pending:
        await msg.answer("У вас уже есть незавершённая заявка на партнёрку. Ожидайте подтверждения админом.")
        return

    # Показываем кнопку "Создать контракт"
    kb_inline = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Создать контракт", callback_data=f"partners_create:{user_id}")]])
    await msg.answer(
        f"🤝 {await gsname(name, user_id)}, вы можете создать партнёрский контракт.\n\nНажмите кнопку 'Создать контракт' и следуйте инструкциям.",
        reply_markup=kb_inline
    )


# ====== Пользователь нажал 'Создать контракт' (начало процесса) ======
@dp.callback_query(F.data.startswith("partners_create"))
async def partners_create_cb(query: CallbackQuery, state: FSMContext):
    requester_id = int(query.data.split(":")[1])
    if query.from_user.id != requester_id:
        return await query.answer("Эта кнопка не для вас.", show_alert=True)

    pending = load_pending()
    if str(requester_id) in pending:
        return await query.answer("У вас уже есть незавершённая заявка.", show_alert=True)

    # создаём заготовку pending со статусом "awaiting_channel_link"
    pending[str(requester_id)] = {
        "owner_id": requester_id,
        "status": "awaiting_channel_link",
        "requested_at": int(time.time()),
        # channel_link, verify_code появятся позже
    }
    save_pending(pending)

    await state.set_state(PartnerCreateStates.waiting_for_channel_link)
    await state.update_data(requester_id=requester_id)
    await query.message.answer(
        "Отлично! Введите, пожалуйста, ссылку на канал (например: https://t.me/yourchannel или @yourchannel).")


# ====== Пользователь отправил ссылку на канал (в state waiting_for_channel_link) ======
@dp.message(PartnerCreateStates.waiting_for_channel_link)
async def user_sent_channel_link(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    data = await state.get_data()
    requester_id = data.get("requester_id")
    if requester_id is None or requester_id != user_id:
        await msg.reply("Ошибка: сессия не найдена. Нажмите 'Создать контракт' и попробуйте заново.")
        await state.clear()
        return

    text = (msg.text or "").strip()
    if not text:
        await msg.reply("Пожалуйста, отправьте ссылку на канал.")
        return

    # Простая нормализация ссылки / валидатор
    link = text
    if link.startswith("@"):
        link = link[1:]
        link = f"https://t.me/{link}"
    elif link.startswith("t.me/"):
        link = f"https://t.me/{link.split('t.me/')[-1]}"
    elif link.startswith("http://") or link.startswith("https://"):
        # принимаем как есть, но нормализуем до https://t.me/...
        if "t.me/" not in link:
            await msg.reply(
                "Похоже это не ссылка на Telegram-канал. Убедитесь, что ссылка вида https://t.me/yourchannel или @yourchannel.")
            return
    else:
        # возможно просто username без @
        if "/" not in link and "." not in link:
            link = f"https://t.me/{link}"
        else:
            await msg.reply(
                "Похоже это не ссылка на Telegram-канал. Убедитесь, что ссылка вида https://t.me/yourchannel или @yourchannel.")
            return

    # Создаём код и сохраняем pending
    code = gen_verify_code()
    pending = load_pending()
    p = pending.get(str(requester_id), {})
    p["channel_link"] = link
    p["verify_code"] = code
    p["status"] = "awaiting_user_confirm"  # пользователь должен вставить код в описание и нажать Подтвердить
    p["requested_at"] = int(time.time())
    pending[str(requester_id)] = p
    save_pending(pending)

    # Пишем пользователю инструкции
    kb_confirm = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтвердить размещение кода",
                              callback_data=f"partners_user_confirm:{requester_id}")],
        [InlineKeyboardButton(text="Отменить заявку", callback_data=f"partners_user_cancel:{requester_id}")]
    ])
    await msg.reply(
        f"Готово — ваш уникальный код: `{code}`.\n\nВставьте этот код в описание вашего канала (Описание/Info). После того как вставите — нажмите кнопку \"Подтвердить размещение кода\".\n\nЕсли вы передумали, нажмите \"Отменить заявку\".",
        reply_markup=kb_confirm
    )

    await state.set_state(PartnerCreateStates.waiting_for_user_confirm)
    # не удаляем state данных — они нужны для подтверждения


# ====== Пользователь подтвердил размещение кода (отправляем админу ссылку и код) ======
@dp.callback_query(F.data.startswith("partners_user_confirm"))
async def partners_user_confirm_cb(query: CallbackQuery):
    parts = query.data.split(":")
    requester_id = int(parts[1])
    caller_id = query.from_user.id
    if caller_id != requester_id:
        return await query.answer("Эта кнопка не для вас.", show_alert=True)

    pending = load_pending()
    p = pending.get(str(requester_id))
    if not p or p.get("status") != "awaiting_user_confirm":
        return await query.answer("Заявка не найдена или уже обработана.", show_alert=True)

    # Переводим в статус ожидания проверки админом
    p["status"] = "awaiting_admin"
    pending[str(requester_id)] = p
    save_pending(pending)

    channel_link = p.get("channel_link", "<не указана>")
    code = p.get("verify_code", "<отсутствует>")

    # Отправляем админу ссылку и код (админ сам проверяет наличие кода в описании)
    kb_admin = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести количество подписчиков",
                              callback_data=f"partners_admin_enter_subs:{requester_id}:0")],
        [InlineKeyboardButton(text="Отклонить", callback_data=f"partners_admin_reject:{requester_id}:0")]
    ])
    try:
        await bot.send_message(
            ADMIN_PARTNER_ID,
            f"🆕 Новая заявка на партнёрку от {query.from_user.full_name} (id: {requester_id}).\nКанал: {channel_link}\nКод проверки: `{code}`\n\nПроверьте, что код действительно вставлен в описание канала, затем выберите действие:",
            reply_markup=kb_admin
        )
    except Exception:
        # если не удалось отправить админу — сообщим пользователю
        await query.message.answer("Не удалось уведомить администратора. Попробуйте позже.")
        return await query.answer("Ошибка при отправке админу.", show_alert=True)

    await query.message.edit_text("Код отправлен администратору для проверки. Ожидайте решения.")
    return await query.answer("Заявка отправлена администратору.", show_alert=True)


# ====== Пользователь отменил заявку ======
@dp.callback_query(F.data.startswith("partners_user_cancel"))
async def partners_user_cancel_cb(query: CallbackQuery):
    parts = query.data.split(":")
    requester_id = int(parts[1])
    caller_id = query.from_user.id
    if caller_id != requester_id:
        return await query.answer("Эта кнопка не для вас.", show_alert=True)

    pending = load_pending()
    if str(requester_id) in pending:
        pending.pop(str(requester_id), None)
        save_pending(pending)
    await query.message.edit_text("Заявка отменена.")
    return await query.answer("Заявка отменена.", show_alert=True)


# ====== Админ: нажал 'Ввести количество подписчиков' ======
@dp.callback_query(F.data.startswith("partners_admin_enter_subs"))
async def admin_enter_subs_cb(query: CallbackQuery, state: FSMContext):
    if query.from_user.id != ADMIN_PARTNER_ID:
        return await query.answer("Эта кнопка только для администратора.", show_alert=True)

    parts = query.data.split(":")
    requester_id = int(parts[1])

    pending = load_pending()
    if str(requester_id) not in pending:
        return await query.answer("Заявка не найдена или уже обработана.", show_alert=True)

    p = pending.get(str(requester_id), {})
    channel_link = p.get("channel_link")

    # Сообщаем админу ссылку (текст) и просим ввести число подписчиков
    await query.message.edit_text(
        f"Введите, пожалуйста, количество подписчиков для заявителя (id: {requester_id}).\nКанал: {channel_link}")
    await state.set_state(PartnerApproveStates.waiting_for_subs)
    await state.update_data(requester_id=requester_id)


# ====== Админ: вводит число подписчиков ======
@dp.message(PartnerApproveStates.waiting_for_subs)
async def admin_received_subs(msg: Message, state: FSMContext):
    if msg.from_user.id != ADMIN_PARTNER_ID:
        return

    data = await state.get_data()
    requester_id = data.get("requester_id")
    if not requester_id:
        await msg.reply("Ошибка. Повторите процесс.")
        await state.clear()
        return

    text = (msg.text or "").strip()
    try:
        subs = int(text)
    except Exception:
        await msg.reply("Введите, пожалуйста, число (например: 450).")
        return

    pending = load_pending()
    p = pending.get(str(requester_id), {})
    p["subs"] = subs
    p["status"] = "subs_set"
    pending[str(requester_id)] = p
    save_pending(pending)

    channel_info = p.get("channel_link", "не указан")
    kb_finish = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Завершить", callback_data=f"partners_admin_finish:{requester_id}")],
        [InlineKeyboardButton(text="Отклонить", callback_data=f"partners_admin_reject:{requester_id}")]
    ])

    await msg.reply(
        f"Количество подписчиков для {requester_id} установлено: {subs}.\nКанал: {channel_info}\n\nНажмите 'Завершить' чтобы создать контракт или 'Отклонить' чтобы отклонить.",
        reply_markup=kb_finish
    )
    await state.clear()


# ====== Админ: отклоняет заявку ======
@dp.callback_query(F.data.startswith("partners_admin_reject"))
async def admin_reject_cb(query: CallbackQuery):
    if query.from_user.id != ADMIN_PARTNER_ID:
        return await query.answer("Эта кнопка только для администратора.", show_alert=True)

    parts = query.data.split(":")
    requester_id = int(parts[1])
    pending = load_pending()
    if str(requester_id) in pending:
        pending.pop(str(requester_id), None)
        save_pending(pending)

    try:
        await bot.send_message(requester_id, "Ваша заявка на партнёрку была отклонена администратором.")
    except Exception:
        pass

    await query.message.edit_text("Заявка отклонена.")


# ====== Админ: завершает заявку -> создаём контракт ======
@dp.callback_query(F.data.startswith("partners_admin_finish"))
async def admin_finish_cb(query: CallbackQuery):
    if query.from_user.id != ADMIN_PARTNER_ID:
        return await query.answer("Эта кнопка только для администратора.", show_alert=True)

    requester_id = int(query.data.split(":")[1])
    pending = load_pending()
    p = pending.get(str(requester_id))
    if not p or p.get("status") not in ("subs_set", "ready_to_finalize", "awaiting_admin"):
        return await query.answer("Нет готовой заявки для завершения.", show_alert=True)

    subs = int(p.get("subs", 0))
    channel_link = p.get("channel_link")
    link = channel_link or "<не указан>"
    level = subs_to_level(subs)

    partners = load_partners()
    existing = partners.get("channels", {})
    max_id = 0
    for v in existing.values():
        try:
            if int(v.get("id", 0)) > max_id:
                max_id = int(v.get("id"))
        except Exception:
            pass
    new_id = max_id + 1

    key = f"{link}_{new_id}"
    partners.setdefault("channels", {})[key] = {
        "id": new_id,
        "link": link,
        "subs": subs,
        "level": level,
        "last_payment": 0,
        "owner_id": requester_id,
        # channel_chat_id оставляем пустым, т.к. у нас нет chat_id
        "channel_chat_id": None
    }
    save_partners(partners)

    # удаляем pending
    pending.pop(str(requester_id), None)
    save_pending(pending)

    # лог
    append_log({
        "time": int(time.time()),
        "action": "create_contract",
        "owner_id": requester_id,
        "contract_id": new_id,
        "subs": subs,
        "level": level,
        "link": link
    })

    try:
        await bot.send_message(requester_id,
                               f"✅ Ваша партнёрская заявка подтверждена!\nКанал: {link}\nПодписчиков: {subs}\nУровень: {level}\nВы можете получать выплаты раз в 24 часа командой /partners -> Получить выплату.")
    except Exception:
        pass

    await query.message.edit_text(
        f"Контракт создан для {requester_id}.\nКанал: {link}\nПодписчиков: {subs}\nУровень: {level}")


# ====== Владелец: получить выплату ======
@dp.callback_query(F.data.startswith("partners_claim"))
async def partners_claim_cb(query: CallbackQuery):
    parts = query.data.split(":")
    contract_id = int(parts[1])
    requester_id = int(parts[2])
    caller_id = query.from_user.id
    if caller_id != requester_id:
        return await query.answer("Эта кнопка не для вас.", show_alert=True)

    key, found = find_contract_by_id(contract_id)
    if not found:
        return await query.answer("Контракт не найден.", show_alert=True)

    last_ts = int(found.get("last_payment", 0) or 0)
    now_ts = int(time.time())
    if now_ts - last_ts < 24 * 3600:
        left = 24 * 3600 - (now_ts - last_ts)
        hrs = left // 3600
        mins = (left % 3600) // 60
        return await query.answer(f"Выплата доступна через {hrs}h {mins}m.", show_alert=True)

    level = int(found.get("level", 1))
    payout = LEVEL_PAYOUT.get(level, 0)

    # начисляем пользователю coins (mDrops)
    user_data = await load_data(requester_id)
    if not user_data:
        await create_user_data(requester_id)
        user_data = await load_data(requester_id)

    user_data["coins"] = user_data.get("coins", 0) + payout
    await save_data(requester_id, user_data)

    # обновляем last_payment
    partners = load_partners()
    if key and key in partners.get("channels", {}):
        partners["channels"][key]["last_payment"] = now_ts
        save_partners(partners)

    append_log({
        "time": now_ts,
        "action": "payout",
        "owner_id": requester_id,
        "contract_id": contract_id,
        "level": level,
        "amount": payout
    })

    await query.answer(f"Выплата выполнена: {payout} mDrops.", show_alert=True)
    try:
        await bot.send_message(requester_id,
                               f"✅ Вы получили {payout} mDrops за партнёрскую программу (уровень {level}).")
    except Exception:
        pass


# ====== Удаление контракта владельцем ======
@dp.callback_query(F.data.startswith("partners_delete"))
async def partners_delete_cb(query: CallbackQuery):
    parts = query.data.split(":")
    contract_id = int(parts[1])
    user_id = int(parts[2])
    caller_id = query.from_user.id
    if caller_id != user_id:
        return await query.answer("Эта кнопка не для вас.", show_alert=True)

    partners = load_partners()
    found_key = None
    for k, v in partners.get("channels", {}).items():
        try:
            if int(v.get("id", -1)) == contract_id and int(v.get("owner_id", -1)) == user_id:
                found_key = k
                break
        except Exception:
            continue

    if not found_key:
        for k, v in partners.get("channels", {}).items():
            try:
                if int(v.get("id", -1)) == contract_id:
                    found_key = k
                    break
            except Exception:
                continue

    if found_key:
        partners["channels"].pop(found_key, None)
        save_partners(partners)
        append_log({
            "time": int(time.time()),
            "action": "delete_contract",
            "owner_id": user_id,
            "contract_id": contract_id
        })
        await query.message.edit_text("Контракт удалён.")
    else:
        await query.answer("Контракт не найден.", show_alert=True)


_CARDS_TABLE = "cards"
_TX_TABLE = "card_transactions"


# ----------------- Вспомогательные функции -----------------

async def try_pay_card(user_id: str, amount: float, note: Optional[str] = None) -> Dict[str, Any]:
    """
    Попытаться оплатить сумму amount картой пользователя user_id.
    Логика:
      1) Если в user['bank'] есть 'default_card' — пробуем оплатить с неё.
      2) Если дефолтная карта отсутствует или недостаточно средств — перебираем все карты пользователя
         и выбираем первую подходящую (не заблокированную и с достаточным балансом).
      3) Если удалось списать — возвращаем {"success": True, "comment": "Успешно", "card": card_number}
         Иначе — {"success": False, "comment": "Причина ошибки"}.
    Комментарии и сообщения на русском.
    """
    user_id = str(user_id)
    # валидация суммы
    try:
        amt = float(amount)
    except Exception:
        return {"success": False, "comment": "Неверная сумма"}
    if amt <= 0:
        return {"success": False, "comment": "Сумма должна быть положительной"}

    # загрузим данные пользователя (чтобы проверить дефолтную карту)
    try:
        user = await load_data(user_id) or {}
    except Exception:
        user = {}

    bank = user.get("bank", {}) if isinstance(user, dict) else {}
    default_card = bank.get("default_card") if isinstance(bank, dict) else None

    # вспомогательная внутренняя функция: попытаться списать с конкретной карты
    async def _try_card(card_number: str) -> Tuple[bool, str]:
        """
        Пытается списать amt с указанной карты. Возвращает (True, msg) или (False, msg).
        Использует pay_with_card (уже реализованную) для унификации логики.
        """
        # получим карту и проверим
        card = await get_card(card_number)
        if not card:
            return False, "Карта не найдена"
        if card.get("blocked"):
            return False, "Карта заблокирована"
        # проверка баланса
        try:
            bal = float(card.get("balance", 0))
        except Exception:
            bal = 0.0
        if bal < amt:
            return False, "Недостаточно средств на карте"
        # выполняем оплату через существующую функцию (не требуем PIN, т.к. это оплата владельцем)
        success, msg = await pay_with_card(user_id, card_number, amt, pin=None, require_pin=False, note=note)
        if success:
            return True, msg
        else:
            return False, msg

    # 1) пробуем дефолтную карту (если указана)
    if default_card:
        ok, msg = await _try_card(default_card)
        if ok:
            return {"success": True, "comment": msg, "card": default_card}
        # если не удалось — продолжаем пробовать другие карты

    # 2) перебираем все карты пользователя (list_cards_by_owner)
    try:
        cards = await list_cards_by_owner(user_id)
    except Exception:
        cards = {}

    # cards — dict card_number -> data
    # отсортируем по созданию (если есть поле created_at) для детерминизма
    items = []
    for cn, cd in (cards or {}).items():
        created = cd.get("created_at") if isinstance(cd, dict) else None
        try:
            # если created_at есть и это ISO-строка — преобразуем для сортировки
            if created:
                ts = datetime.fromisoformat(created)
            else:
                ts = datetime.utcfromtimestamp(0)
        except Exception:
            ts = datetime.utcfromtimestamp(0)
        items.append((cn, cd, ts))
    items.sort(key=lambda x: x[2])  # старые первые

    for cn, cd, _ in items:
        ok, msg = await _try_card(cn)
        if ok:
            return {"success": True, "comment": msg, "card": cn}

    # 3) не нашли подходящую карту
    return {"success": False, "comment": "Не найдена карта с достаточным балансом или все карты заблокированы"}


def _ensure_db_schema():
    """Создаёт таблицы cards и card_transactions при необходимости (синхронно)."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {_CARDS_TABLE} (
                card_number TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                balance REAL NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0,
                pin_hash TEXT DEFAULT NULL,
                created_at TEXT NOT NULL,
                metadata TEXT DEFAULT '{{}}'
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {_TX_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_number TEXT,
                ts INTEGER,
                type TEXT,
                amount REAL,
                other TEXT,
                initiator_id TEXT,
                note TEXT
            )
        """)
        conn.commit()


def _gen_card_number() -> str:
    """Сгенерировать уникальный 16-значный номер карты (без Luhn)."""
    # Prefix 4000... to look like Visa; гарантируем уникальность при сохранении.
    return "4000" + "".join(str(random.randint(0, 9)) for _ in range(12))


def _hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode("utf-8")).hexdigest()


def _record_tx_sync(card_number: str, typ: str, amount: float, other: Optional[str], initiator_id: Optional[str],
                    note: Optional[str]):
    ts = int(time.time())
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {_TX_TABLE} (card_number, ts, type, amount, other, initiator_id, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (card_number, ts, typ, float(amount), other, initiator_id, note))
        conn.commit()


# ----------------- Синхронные операции (sqlite) -----------------

def sync_create_card(owner_id: str, name: str = "", initial_balance: float = 0.0, pin: Optional[str] = None,
                     metadata: Optional[dict] = None) -> str:
    """Создать карту синхронно и вернуть card_number."""
    _ensure_db_schema()
    card_number = _gen_card_number()
    # Убедиться в уникальности (в редком случае коллизии — регенерим)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        tries = 0
        while True:
            cur.execute(f"SELECT 1 FROM {_CARDS_TABLE} WHERE card_number = ?", (card_number,))
            if cur.fetchone() is None:
                break
            card_number = _gen_card_number()
            tries += 1
            if tries > 10:
                raise RuntimeError("Не удалось сгенерировать уникальный номер карты")

        pin_hash = _hash_pin(pin) if pin else None
        created_at = datetime.utcnow().isoformat()
        md = json.dumps(metadata or {}, ensure_ascii=False)
        cur.execute(
            f"INSERT INTO {_CARDS_TABLE} (card_number, owner_id, name, balance, blocked, pin_hash, created_at, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (card_number, str(owner_id), name or "", float(initial_balance), 0, pin_hash, created_at, md))
        conn.commit()
    # записываем транзакцию "создание/пополнение"
    if initial_balance:
        _record_tx_sync(card_number, "create_balance", initial_balance, None, owner_id, "initial balance on create")
    else:
        _record_tx_sync(card_number, "create", 0.0, None, owner_id, "card created")
    return card_number


def sync_get_card(card_number: str) -> Dict[str, Any]:
    """Вернуть запись карты или {}"""
    _ensure_db_schema()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_CARDS_TABLE} WHERE card_number = ?", (str(card_number),))
        row = cur.fetchone()
        if not row:
            return {}
        d = dict(row)
        if d.get("metadata"):
            try:
                d["metadata"] = json.loads(d["metadata"])
            except Exception:
                d["metadata"] = {}
        # привести blocked к bool
        d["blocked"] = bool(int(d.get("blocked", 0)))
        return d


def sync_update_card_balance(card_number: str, new_balance: float):
    _ensure_db_schema()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE {_CARDS_TABLE} SET balance = ? WHERE card_number = ?",
                    (float(new_balance), str(card_number)))
        conn.commit()


def sync_set_card_block(card_number: str, blocked: bool):
    _ensure_db_schema()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE {_CARDS_TABLE} SET blocked = ? WHERE card_number = ?",
                    (1 if blocked else 0, str(card_number)))
        conn.commit()


def sync_set_pin(card_number: str, pin: Optional[str]):
    ph = _hash_pin(pin) if pin else None
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE {_CARDS_TABLE} SET pin_hash = ? WHERE card_number = ?", (ph, str(card_number)))
        conn.commit()


def sync_list_cards_by_owner(owner_id: str) -> Dict[str, Dict[str, Any]]:
    _ensure_db_schema()
    out: Dict[str, Dict[str, Any]] = {}
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_CARDS_TABLE} WHERE owner_id = ?", (str(owner_id),))
        rows = cur.fetchall()
        for r in rows:
            d = dict(r)
            try:
                d["metadata"] = json.loads(d.get("metadata", "{}"))
            except Exception:
                d["metadata"] = {}
            d["blocked"] = bool(int(d.get("blocked", 0)))
            out[str(d["card_number"])] = d
    return out


def sync_delete_card(card_number: str) -> bool:
    _ensure_db_schema()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM {_CARDS_TABLE} WHERE card_number = ?", (str(card_number),))
        if cur.fetchone() is None:
            return False
        cur.execute(f"DELETE FROM {_CARDS_TABLE} WHERE card_number = ?", (str(card_number),))
        conn.commit()
        # добавим запись в журнал
        _record_tx_sync(card_number, "delete", 0.0, None, None, "card deleted")
        return True


def sync_record_tx(card_number: str, typ: str, amount: float, other: Optional[str], initiator_id: Optional[str],
                   note: Optional[str]):
    _ensure_db_schema()
    _record_tx_sync(card_number, typ, amount, other, initiator_id, note)


def sync_get_tx_history(card_number: str, limit: int = 50) -> List[Dict[str, Any]]:
    _ensure_db_schema()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {_TX_TABLE} WHERE card_number = ? ORDER BY id DESC LIMIT ?",
                    (str(card_number), limit))
        rows = cur.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            out.append(d)
        return out


# ----------------- Асинхронные обёртки -----------------

async def create_card(owner_id: str, name: str = "", initial_balance: float = 0.0, pin: Optional[str] = None,
                      metadata: Optional[dict] = None) -> str:
    """
    Создать карту для пользователя owner_id.
    Ограничение: у пользователя может быть не более 1 карты.
    Возвращает номер карты (строка) в случае успеха.
    В случае, если у пользователя уже есть карта — выбрасывает ValueError с сообщением на русском.
    """
    owner_id = str(owner_id)
    # проверяем, есть ли уже карты у владельца
    existing = await list_cards_by_owner(owner_id)
    if existing and len(existing) > 0:
        # пользователь уже имеет карту — не позволяем создать новую
        raise ValueError("Нельзя создать более одной карты — у пользователя уже есть карта.")
    # иначе создаём карту синхронно в отдельном потоке
    # sync_create_card возвращает номер карты
    card_number = await asyncio.to_thread(sync_create_card, owner_id, name, float(initial_balance), pin, metadata)
    return str(card_number)


async def get_card(card_number: str) -> Dict[str, Any]:
    return await asyncio.to_thread(sync_get_card, str(card_number))


async def update_card_balance(card_number: str, new_balance: float):
    return await asyncio.to_thread(sync_update_card_balance, str(card_number), float(new_balance))


async def set_card_block(card_number: str, blocked: bool):
    return await asyncio.to_thread(sync_set_card_block, str(card_number), bool(blocked))


async def set_card_pin(card_number: str, pin: Optional[str]):
    return await asyncio.to_thread(sync_set_pin, str(card_number), pin)


async def list_cards_by_owner(owner_id: str) -> Dict[str, Dict[str, Any]]:
    return await asyncio.to_thread(sync_list_cards_by_owner, str(owner_id))


async def delete_card(card_number: str) -> bool:
    return await asyncio.to_thread(sync_delete_card, str(card_number))


async def record_tx(card_number: str, typ: str, amount: float, other: Optional[str], initiator_id: Optional[str],
                    note: Optional[str]):
    return await asyncio.to_thread(sync_record_tx, str(card_number), typ, float(amount), other, initiator_id, note)


async def get_tx_history(card_number: str, limit: int = 50):
    return await asyncio.to_thread(sync_get_tx_history, str(card_number), int(limit))


# ----------------- Высокоуровневые банковские операции -----------------

async def topup_card_from_balance(user_id: str, card_number: str, amount: float) -> Tuple[bool, str]:
    """
    Пополнение карты с основного баланса пользователя.
    Возвращает (успех, сообщение).
    """
    user_id = str(user_id)
    try:
        amt = float(amount)
    except Exception:
        return False, "Неверная сумма"
    if amt <= 0:
        return False, "Сумма должна быть положительной"

    user = await load_data(user_id)
    if not user:
        return False, "Пользователь не найден"

    balance = float(user.get("coins", 0))
    if balance < amt:
        return False, "Недостаточно средств на основном балансе"

    card = await get_card(card_number)
    if not card:
        return False, "Карта не найдена"

    if card.get("blocked"):
        return False, "Карта заблокирована"

    # Списываем с баланса и кладём на карту
    user["coins"] = balance - amt
    await save_data(user_id, user)

    new_balance = float(card.get("balance", 0)) + amt
    await update_card_balance(card_number, new_balance)
    await record_tx(card_number, "topup_from_balance", amt, None, user_id, f"popolnenie s osnovnogo balansa")

    return True, f"Карта {card_number} пополнена на {amt} mDrops"


async def withdraw_card_to_balance(user_id: str, card_number: str, amount: float) -> Tuple[bool, str]:
    """
    Снятие с карты на основной баланс (владелец карты).
    """
    user_id = str(user_id)
    try:
        amt = float(amount)
    except Exception:
        return False, "Неверная сумма"
    if amt <= 0:
        return False, "Сумма должна быть положительной"

    card = await get_card(card_number)
    if not card:
        return False, "Карта не найдена"

    if str(card.get("owner_id")) != str(user_id):
        return False, "Вы не являетесь владельцем карты"

    if card.get("blocked"):
        return False, "Карта заблокирована"

    balance = float(card.get("balance", 0))
    if balance < amt:
        return False, "Недостаточно средств на карте"

    # удериваем баланс карты
    new_card_balance = balance - amt
    await update_card_balance(card_number, new_card_balance)
    # начисляем основному балансу
    user = await load_data(user_id)
    if not user:
        user = {}
    user["coins"] = float(user.get("coins", 0)) + amt
    await save_data(user_id, user)

    await record_tx(card_number, "withdraw_to_balance", -amt, None, user_id, "withdraw to main balance")
    return True, f"С карты {card_number} снято {amt} mDrops и зачислено на основной баланс"


from typing import Optional, Tuple


async def pay_with_card(owner_id: str, amount: float, pin: Optional[str] = None, require_pin: bool = False,
                        note: Optional[str] = None) -> Tuple[bool, str]:
    """
    Оплата картой владельца (owner_id).
    Ограничение: у пользователя максимум одна карта.
    Логика:
      - Находит карту пользователя (дефолтная или первая).
      - Проверяет блокировку, PIN (если require_pin=True) и баланс.
      - Списывает сумму, записывает транзакцию и логирует все шаги.
    Возвращает (True, сообщение) при успехе или (False, причина).
    """
    # валидируем сумму
    try:
        amt = float(amount)
    except Exception:
        append_log({"event": "pay_with_card_invalid_amount", "owner_id": str(owner_id), "amount": amount},
                   add_timestamp=True)
        return False, "Неверная сумма"
    if amt <= 0:
        append_log({"event": "pay_with_card_nonpositive_amount", "owner_id": str(owner_id), "amount": amt},
                   add_timestamp=True)
        return False, "Сумма должна быть положительной"

    owner_id = str(owner_id)

    # Попытка загрузить карты пользователя
    try:
        cards = await list_cards_by_owner(owner_id)  # dict card_number -> data
    except Exception as exc:
        append_log({
            "event": "pay_with_card_list_cards_error",
            "owner_id": owner_id,
            "amount": amt,
            "error": repr(exc)
        }, add_timestamp=True)
        return False, "Ошибка при доступе к картам"

    if not cards:
        append_log({"event": "pay_with_card_no_cards", "owner_id": owner_id, "amount": amt}, add_timestamp=True)
        return False, "У вас нет карты"

    # Выбираем карту: сначала дефолтная из user['bank']['default_card'], иначе первая
    card_number = None
    try:
        user = await load_data(owner_id) or {}
        bank = user.get("bank", {}) if isinstance(user, dict) else {}
        default_card = bank.get("default_card")
        if default_card and default_card in cards:
            card_number = default_card
    except Exception as exc:
        append_log({
            "event": "pay_with_card_load_user_error",
            "owner_id": owner_id,
            "amount": amt,
            "error": repr(exc)
        }, add_timestamp=True)
        card_number = None

    if not card_number:
        # берём первую карту (детерминированно)
        try:
            card_number = sorted(list(cards.keys()))[0]
        except Exception as exc:
            append_log({
                "event": "pay_with_card_choose_card_error",
                "owner_id": owner_id,
                "amount": amt,
                "error": repr(exc)
            }, add_timestamp=True)
            return False, "Не удалось определить карту пользователя"

    # Получаем данные карты
    try:
        card = await get_card(card_number)
    except Exception as exc:
        append_log({
            "event": "pay_with_card_get_card_error",
            "owner_id": owner_id,
            "card_number": card_number,
            "amount": amt,
            "error": repr(exc)
        }, add_timestamp=True)
        return False, "Ошибка при получении данных карты"

    if not card:
        append_log(
            {"event": "pay_with_card_card_not_found", "owner_id": owner_id, "card_number": card_number, "amount": amt},
            add_timestamp=True)
        return False, "Карта не найдена"

    # Подготовим лог-объект базовый
    masked = f"{str(card_number)}" if card_number else None
    base_log = {
        "event": "pay_with_card_attempt",
        "owner_id": owner_id,
        "card_number": card_number,
        "masked_card": masked,
        "amount": amt,
        "note": note
    }

    # проверка блокировки
    if card.get("blocked"):
        append_log({**base_log, "result": "fail", "reason": "карта заблокирована"}, add_timestamp=True)
        return False, "Карта заблокирована"

    # проверка PIN, если требуется
    if require_pin:
        phash = card.get("pin_hash")
        if not phash:
            append_log({**base_log, "result": "fail", "reason": "PIN не установлен"}, add_timestamp=True)
            return False, "На карте не установлен PIN"
        if not pin:
            append_log({**base_log, "result": "fail", "reason": "PIN не передан"}, add_timestamp=True)
            return False, "Требуется PIN"
        try:
            if phash != _hash_pin(pin):
                append_log({**base_log, "result": "fail", "reason": "неверный PIN"}, add_timestamp=True)
                return False, "Неверный PIN"
        except Exception as exc:
            append_log({**base_log, "result": "error", "reason": "ошибка проверки PIN", "error": repr(exc)},
                       add_timestamp=True)
            return False, "Ошибка проверки PIN"

    # проверка баланса
    try:
        bal = float(card.get("balance", 0))
    except Exception:
        bal = 0.0
    base_log["balance_before"] = bal

    if bal < amt:
        append_log({**base_log, "result": "fail", "reason": "недостаточно средств", "balance": bal}, add_timestamp=True)
        return False, "Недостаточно средств на карте"

    # Выполняем списание и запись транзакции, оборачиваем в try чтобы залогировать исключения
    new_balance = bal - amt
    try:
        await update_card_balance(card_number, new_balance)
        await record_tx(card_number, "payment", -amt, None, owner_id, note or "payment")
        # Лог успеха
        append_log({
            **base_log,
            "result": "success",
            "balance_after": new_balance
        }, add_timestamp=True)
    except Exception as exc:
        # Попытка отката (не гарантирую, но попытаемся вернуть старый баланс при ошибке записи)
        try:
            # Попробуем восстановить баланс синхронно (жёсткий откат)
            await update_card_balance(card_number, bal)
        except Exception:
            # если откат не удался — логируем это
            append_log({
                **base_log,
                "result": "error",
                "reason": "ошибка при записи транзакции, откат не выполнен",
                "error": repr(exc)
            }, add_timestamp=True)
            return False, "Ошибка при попытке списания с карты"

        # если откат прошёл (или хотя бы попытка), логируем исходную ошибку
        append_log({
            **base_log,
            "result": "error",
            "reason": "ошибка при записи транзакции, выполнен откат",
            "error": repr(exc)
        }, add_timestamp=True)
        return False, "Ошибка при попытке списания с карты"

    # форматируем сумму для сообщения
    try:
        disp = format_balance(amt)
    except Exception:
        disp = str(amt)

    success_msg = f"Оплата картой {masked} на сумму {disp} mDrops выполнена"
    return True, success_msg


async def transfer_card_to_card(from_user_id: str, from_card_number: str, to_card_number: str, amount: float,
                                pin: Optional[str] = None) -> Tuple[bool, str]:
    """
    Перевод с карты на карту по номеру. Если переводит не владелец карты — требуется PIN.
    """
    try:
        amt = float(amount)
    except Exception:
        return False, "Неверная сумма"
    if amt <= 0:
        return False, "Сумма должна быть положительной"

    from_card = await get_card(from_card_number)
    to_card = await get_card(to_card_number)
    if not from_card:
        return False, "Исходная карта не найдена"
    if not to_card:
        return False, "Карта получателя не найдена"

    if from_card.get("blocked"):
        return False, "Исходная карта заблокирована"
    if to_card.get("blocked"):
        return False, "Карта получателя заблокирована"

    owner = str(from_card.get("owner_id"))
    if str(from_user_id) != owner:
        # не владелец — необходим PIN
        if not pin:
            return False, "Требуется PIN для перевода чужой карты"
        phash = from_card.get("pin_hash")
        if not phash or phash != _hash_pin(pin):
            return False, "Неверный PIN"

    if float(from_card.get("balance", 0)) < amt:
        return False, "Недостаточно средств на исходной карте"

    # выполняем транзакцию: списание и начисление
    new_from_balance = float(from_card.get("balance", 0)) - amt
    new_to_balance = float(to_card.get("balance", 0)) + amt
    await update_card_balance(from_card_number, new_from_balance)
    await update_card_balance(to_card_number, new_to_balance)

    await record_tx(from_card_number, "transfer_out", -amt, to_card_number, str(from_user_id),
                    f"transfer to {to_card_number}")
    await record_tx(to_card_number, "transfer_in", amt, from_card_number, str(from_user_id),
                    f"received from {from_card_number}")
    return True, f"Перевод {amt} mDrops с {from_card_number} на {to_card_number} выполнен"


# ----------------- Утилиты для интеграции -----------------

async def try_pay_with_preferred_card(user_id: str, amount: float, note: Optional[str] = None) -> Tuple[bool, str]:
    """
    Пробуем списать средствами с дефолтной карты пользователя (если есть поле user['bank']['default_card']),
    иначе False.
    Возвращает (успех, сообщение). Если не получилось, можно списывать с основного баланса.
    """
    user = await load_data(str(user_id)) or {}
    bank = user.get("bank", {})
    default_card = bank.get("default_card")
    if not default_card:
        return False, "Нет дефолтной карты"
    return await pay_with_card(user_id, default_card, amount, require_pin=False, note=note)


# ----------------- Хендлеры (примерные, можно адаптировать) -----------------
# Ниже — несколько готовых обработчиков команд/кнопок: /cards, создание, пополнение, снятие, перевод, просмотр карты, блокировка.

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State


# Простые FSM состояния для операций (локально)
class CardStates(StatesGroup):
    creating_card_name = State()
    creating_card_initial = State()
    creating_card_pin = State()
    topup_amount = State()
    withdraw_amount = State()
    transfer_card_target = State()
    transfer_amount = State()
    set_pin = State()


# /mycards - список карт пользователя
@dp.message(F.text.lower().in_(["карта", "банк", "/card", "/bank"]))
async def cmd_my_cards(message: Message):
    if message.chat.type != "private":
        return await message.reply(
            f"🍓 {await gsname(message.from_user.first_name, message.from_user.id)}, карта дотупка только в ЛС с ботом!",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="👉 Перейти в лс", url="t.me/gmegadbot")]]))

    uid = str(message.from_user.id)
    cards = await list_cards_by_owner(uid)
    if not cards:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="➕ Создать карту", callback_data="card_create")]])
        return await message.answer("У вас нет карт.", reply_markup=kb)
    lines = []
    kb_rows = []
    for cn, card in cards.items():
        bal = int(card.get("balance", 0))
        bl = "🔒" if card.get("blocked") else ""
        lines.append(f"• <b>{card.get('name') or 'Без имени'}</b> — <code>{cn}</code> — {format_balance(bal)} {bl}")
        kb_rows.append([InlineKeyboardButton(text=f"🔍 Просмотр {cn[-4:]}", callback_data=f"card_view:{cn}"),
                        InlineKeyboardButton(text="📥 Пополнить", callback_data=f"card_topup:{cn}")])
    if not cards.items():
        kb_rows.append([InlineKeyboardButton(text="➕ Создать карту", callback_data="card_create")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await message.answer("<b>💳 Ваши карты:</b>\n\n" + "\n".join(lines), parse_mode="HTML", reply_markup=kb)


# create card flow (простая версия)
@dp.callback_query(F.data.lower().startswith("card_create"))
async def cb_card_create_start(query: CallbackQuery, state: FSMContext):
    await state.set_state(CardStates.creating_card_name)
    await query.message.edit_text("🔰 Введите название карты (пример: Основная, Для донатов) или 'отмена'")


@dp.message(CardStates.creating_card_name)
async def cb_card_create_name(msg: Message, state: FSMContext):
    if (msg.text or "").strip().lower() == "отмена":
        await state.clear()
        return await msg.reply("Отмена")
    name = (msg.text or "").strip()[:50]
    await state.update_data(card_name=name)
    await state.set_state(CardStates.creating_card_initial)
    await msg.reply("💰 Укажите начальный баланс карты (деньги пополнятся с вашего баланса) или \"0\"")


@dp.message(CardStates.creating_card_initial)
async def cb_card_create_initial(msg: Message, state: FSMContext):
    data = await state.get_data()
    name = data.get("card_name", "")
    try:
        amount = float(parse_bet_input(msg.text.strip()))
    except Exception:
        await state.clear()
        return await msg.reply("Неверная сумма. Отмена.")
    uid = str(msg.from_user.id)
    # проверим баланс пользователя
    user = await load_data(uid)
    if float(user.get("coins", 0)) < amount:
        await state.clear()
        return await msg.reply("Недостаточно средств на основном балансе.")
    # попросим PIN (опционально)
    await state.update_data(card_initial=amount)
    await state.set_state(CardStates.creating_card_pin)
    await msg.reply("⚙️ Введите PIN для карты (4-6 цифр)\n\n‼️ Сохраните свой PIN и не давайте его никому!")


@dp.message(CardStates.creating_card_pin)
async def cb_card_create_pin(msg: Message, state: FSMContext):
    data = await state.get_data()
    name = data.get("card_name", "Карта")
    amount = float(data.get("card_initial", 0))
    pin = (msg.text or "").strip()
    if pin.lower() == "отмена":
        await state.clear()
        return await msg.reply("Отмена.")
    if pin:
        if not pin.isdigit() or not (4 <= len(pin) <= 6):
            await state.clear()
            return await msg.reply("Неверный PIN. Должно быть 4-6 цифр. Отмена.")
    uid = str(msg.from_user.id)
    # списываем с основного баланса
    user = await load_data(uid)
    user["coins"] = float(user.get("coins", 0)) - amount
    await save_data(uid, user)
    # создаём карту
    card_number = await create_card(uid, name=name, initial_balance=amount, pin=pin if pin else None)
    await state.clear()
    await msg.reply(f"✅ Карта создана: <code>{card_number}</code>\n🔰 Название: {name}\n💰 Баланс: {amount:.2f}",
                    parse_mode="HTML")


# view card
@dp.callback_query(F.data.lower().startswith("card_view:"))
async def cb_card_view(query: CallbackQuery):
    await query.answer()
    cn = query.data.split(":", 1)[1]
    card = await get_card(cn)
    if not card:
        return await query.message.edit_text("Карта не найдена.")
    owner_id = str(card.get("owner_id"))
    lines = []
    lines.append(f"💳 Карта: <b>{card.get('name') or 'Без имени'}</b>")
    lines.append(f"🔗 Номер: <code>{cn}</code>")
    lines.append(f"💰 Баланс: <b>{int(card.get('balance', 0))}</b> mDrops")
    lines.append(f"🛡 Статус: <b>{'ЗАБЛОКИРОВАНА' if card.get('blocked') else 'Активна'}</b>")
    lines.append(f"🔰 Владелец: <code>{owner_id}</code>")
    # имя владельца, если доступно
    try:
        ch = await bot.get_chat(int(owner_id))
        name = getattr(ch, "first_name", None) or getattr(ch, "username", None) or owner_id
        lines[-1] = f"Владелец: <b>{name}</b> (<code>{owner_id}</code>)"
    except Exception:
        pass
    # tx last 10
    txs = await get_tx_history(cn, limit=10)
    tx_lines = []
    for t in txs:
        ts = datetime.utcfromtimestamp(int(t["ts"])).strftime("%Y-%m-%d %H:%M")
        tx_lines.append(f"{ts} | {t['type']} | {t['amount']} | {t.get('note') or ''}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Пополнить", callback_data=f"card_topup:{cn}"),
         InlineKeyboardButton(text="📤 Снять на баланс", callback_data=f"card_withdraw:{cn}")],
        [InlineKeyboardButton(text="📤 Перевод по номеру", callback_data=f"card_transfer:{cn}")],
        [InlineKeyboardButton(text="🔒 Заблокировать" if not card.get("blocked") else "Разблокировать",
                              callback_data=f"card_block_toggle:{cn}")],
        [InlineKeyboardButton(text="❌ Удалить карту", callback_data=f"card_delete:{cn}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_cards")]
    ])
    text = "\n".join(lines) + "\n\nИстория:\n" + ("\n".join(tx_lines) if tx_lines else "Транзакций нет")
    try:
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        await query.message.answer(text, parse_mode="HTML", reply_markup=kb)


# topup flow
@dp.callback_query(F.data.lower().startswith("card_topup:"))
async def cb_card_topup(query: CallbackQuery, state: FSMContext):
    cn = query.data.split(":", 1)[1]
    await state.update_data(card_target=cn)
    await state.set_state(CardStates.topup_amount)
    await query.message.edit_text(
        "💰 Введите сумму для пополнения с основного баланса (например 1000) или \"отмена\" для отмены")


@dp.message(CardStates.topup_amount)
async def cb_card_topup_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    cn = data.get("card_target")
    if not cn:
        await state.clear()
        return await msg.reply("❌ Цель не найдена. Действие отменено.")
    text = (msg.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        return await msg.reply("Отмена.")
    try:
        amt = float(parse_bet_input(text))
    except Exception:
        await state.clear()
        return await msg.reply("Неверная сумма. Отмена.")
    res, msgt = await topup_card_from_balance(str(msg.from_user.id), cn, amt)
    await state.clear()
    return await msg.reply(msgt)


# withdraw flow
@dp.callback_query(F.data.lower().startswith("card_withdraw:"))
async def cb_card_withdraw_start(query: CallbackQuery, state: FSMContext):
    cn = query.data.split(":", 1)[1]
    await state.update_data(card_target=cn)
    await state.set_state(CardStates.withdraw_amount)
    await query.message.edit_text("📥 Введите сумму для снятия на основной баланс или \"отмена\"")


@dp.message(CardStates.withdraw_amount)
async def cb_card_withdraw_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    cn = data.get("card_target")
    if not cn:
        await state.clear()
        return await msg.reply("Ошибка. Отмена.")
    t = (msg.text or "").strip().lower()
    if t == "отмена":
        await state.clear()
        return await msg.reply("Отмена.")
    try:
        amt = float(parse_bet_input(t))
    except Exception:
        await state.clear()
        return await msg.reply("Неверная сумма.")
    res, ms = await withdraw_card_to_balance(str(msg.from_user.id), cn, amt)
    await state.clear()
    await msg.reply(ms)


# transfer flow: запрос цели (номер карты)
@dp.callback_query(F.data.lower().startswith("card_transfer:"))
async def cb_card_transfer_start(query: CallbackQuery, state: FSMContext):
    cn = query.data.split(":", 1)[1]
    await state.update_data(card_source=cn)
    await state.set_state(CardStates.transfer_card_target)
    await query.message.edit_text("Введите номер карты получателя (16 цифр) или \"отмена\"")


@dp.message(CardStates.transfer_card_target)
async def cb_card_transfer_target(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    if text.lower() == "отмена":
        await state.clear()
        return await msg.reply("Отмена.")
    target = text
    await state.update_data(card_target=target)
    await state.set_state(CardStates.transfer_amount)
    await msg.reply("💸 Введите сумму перевода:")


@dp.message(CardStates.transfer_amount)
async def cb_card_transfer_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    src = data.get("card_source")
    tgt = data.get("card_target")
    if not src or not tgt:
        await state.clear()
        return await msg.reply("Данные потеряны. Отмена.")
    try:
        amt = float(parse_bet_input(msg.text.strip()))
    except Exception:
        await state.clear()
        return await msg.reply("Неверная сумма.")
    # если переводит владелец — не требует PIN. Если кто-то другой — потребует PIN (не реализовано тут)
    res, ms = await transfer_card_to_card(str(msg.from_user.id), src, tgt, amt, pin=None)
    await state.clear()
    await msg.reply(ms)


# block toggle
@dp.callback_query(F.data.lower().startswith("card_block_toggle:"))
async def cb_card_block_toggle(query: CallbackQuery):
    cn = query.data.split(":", 1)[1]
    card = await get_card(cn)
    if not card:
        return await query.message.edit_text("Карта не найдена.")
    owner = str(card.get("owner_id"))
    uid = str(query.from_user.id)
    if uid != owner and uid != str(ADMIN_ID):
        return await query.answer("Нет прав", show_alert=True)
    new_state = not bool(card.get("blocked"))
    await set_card_block(cn, new_state)
    await query.message.edit_text("Карта заблокирована." if new_state else "Карта разблокирована.")


# delete
@dp.callback_query(F.data.lower().startswith("card_delete:"))
async def cb_card_delete(query: CallbackQuery):
    cn = query.data.split(":", 1)[1]
    card = await get_card(cn)
    if not card:
        return await query.message.edit_text("Карта не найдена.")
    owner = str(card.get("owner_id"))
    uid = str(query.from_user.id)
    if uid != owner and uid != str(ADMIN_ID):
        return await query.answer("Нет прав", show_alert=True)
    # можно вернуть баланс владельцу перед удалением (опционально)
    bal = float(card.get("balance", 0))
    if bal > 0:
        # вернуть на основной баланс
        u = await load_data(owner)
        u["coins"] = float(u.get("coins", 0)) + bal
        await save_data(owner, u)
    deleted = await delete_card(cn)
    if deleted:
        await query.message.edit_text("❌ Карта удалена. Баланс возвращён владельцу (если был).")
    else:
        await query.message.edit_text("❌ Не удалось удалить карту.")


@dp.callback_query(F.data == "back_cards")
async def cmd_my_cards(cb: CallbackQuery):
    uid = str(cb.from_user.id)
    cards = await list_cards_by_owner(uid)
    if not cards:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="➕ Создать карту", callback_data="card_create")]])
        return await cb.answer("У вас нет карт.", reply_markup=kb)
    lines = []
    kb_rows = []
    for cn, card in cards.items():
        bal = int(card.get("balance", 0))
        bl = "🔒" if card.get("blocked") else ""
        lines.append(f"• <b>{card.get('name') or 'Без имени'}</b> — <code>{cn}</code> — {format_balance(bal)} {bl}")
        kb_rows.append([InlineKeyboardButton(text=f"🔍 Просмотр {cn[-4:]}", callback_data=f"card_view:{cn}"),
                        InlineKeyboardButton(text="📥 Пополнить", callback_data=f"card_topup:{cn}")])
    if not cards.items():
        kb_rows.append([InlineKeyboardButton(text="➕ Создать карту", callback_data="card_create")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await cb.message.edit_text("<b>💳 Ваши карты:</b>\n\n" + "\n".join(lines), parse_mode="HTML", reply_markup=kb)


# -------------- MEGADROP COLLABORATION -------------- #

@dp.message(F.text.lower().startswith("мд"))
async def handle_megadrop_asks(msg: Message):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    question_text = msg.text
    hru = ["мд ты как", "мд как ты", "мд как дела?", "мд как дела"]
    choose_the_game = ["мд дай случайную игру", "мд дай рандомную игру", "мд какую игру сыграть",
                       "мд какую игру играть", "мд случайная игра"]
    all_games = ["башня", "золото", "краш", "рулетка", "кости", "мины", "кнб", "боул", "рр", "сундуки", "баскетбол",
                 "дуэль"]
    words = question_text.split(" ")
    if words[1] == "выбери":
        if " или " in question_text:
            parts = words[1:]
            variants = " ".join(parts[1:]).split(" или ")
            if not variants:
                return await msg.reply(f"{await gsname(name, user_id)}, введи варианты!")

            phrases = ["я думаю что", "наверное", "я скажу", "ктобы мог подумать, но это", "и это"]
            return await msg.reply(f"{await gsname(name, user_id)}, {random.choice(phrases)} {random.choice(variants)}")
    elif question_text.lower() in hru:
        phrases = ["жесть нагрузка", "кайфуюю", "скажи спс соламону", "топ донатор - @MB_team_1 (1500 звезд)",
                   "ктобы мог подумать, но ЭТО ЖЕСТЬ НАГРУЗКА", "я скажу ...", "нормис",
                   "напиши окак в комменты, это смешно п...", "норм, го в башню", "забыл про бонус?))",
                   "забыл да? /help", "та ниче так, бизнес свой веду", "девачки тишее"]
        return await msg.reply(f"{await gsname(name, user_id)}, {random.choice(phrases)}")
    elif question_text.lower() in choose_the_game:
        phrases = ["я думаю что", "наверное", "я скажу", "ктобы мог подумать, но это", "и это", "пожалуй это будет"]
        return await msg.reply(f"{await gsname(name, user_id)} {random.choice(phrases)} {random.choice(all_games)}")
    elif words[1] == "рандом" and words[2] == "от":
        if " до " in question_text:
            parts = words[1:]
            variants = [int(parts[2]), int(parts[4])]

            if not variants:
                return await msg.reply(f"{await gsname(name, user_id)}, введи варианты!")

            phrases = ["я думаю что", "наверное", "я скажу", "ктобы мог подумать, но это", "и это", "пожалуй"]
            return await msg.reply(
                f"{await gsname(name, user_id)}, {random.choice(phrases)} {random.randint(variants[0], variants[1])}")
    else:
        await msg.reply(
            f"{await gsname(name, user_id)}, я не знаю как ответить!\n\n{html.blockquote(f"Вопросы для МегаДропа:\n{html.code("мд выбери (текст 1) или (текст 2)")}\n{html.code("мд как дела")}\n{html.code("мд дай рандомную игру")}\n{html.code("мд рандом от (число 1) до (число 2)")}")}")


# -------------- ADMIN COMMANDS -------------- #

LOGS_FOR_PAGE = 10


def make_logs_keyboard(page: int, total_pages: int, user_id: int) -> types.InlineKeyboardMarkup:
    prev_page = max(page - 1, 0)
    next_page = min(page + 1, total_pages - 1)

    prev_btn = types.InlineKeyboardButton(
        text="⬅️ Новее", callback_data=f"logs:{user_id}:{prev_page}"
    )
    next_btn = types.InlineKeyboardButton(
        text="Старее ➡️", callback_data=f"logs:{user_id}:{next_page}"
    )
    page_btn = types.InlineKeyboardButton(
        text=f"Страница {page + 1}/{total_pages}", callback_data=f"logs:{user_id}:noop"
    )

    inline_keyboard = [
        [prev_btn, next_btn],
        [page_btn],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=inline_keyboard)


def format_logs_page(logs: list[str], page: int) -> str:
    start = page * LOGS_FOR_PAGE
    page_logs = logs[start:start + LOGS_FOR_PAGE]
    if not page_logs:
        return "Немає логів на цій сторінці."
    lines = []
    for i, item in enumerate(page_logs, start=start + 1):
        lines.append(f"{i}. {item}")
    header = f"Логи (показано {start + 1}-{start + len(page_logs)} з {len(logs)})\n\n"
    return header + "\n\n".join(lines)


@dp.callback_query(F.data.startswith("send_logs_callback"))
async def handle_send_callback_log(query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Ты не имеешь доступа к данной кнопке!")

    data = load_log()
    logs = data.get("events", []) or []
    if not logs:
        return await query.answer("Логи пустые")

    logs = list(reversed(logs))

    total_pages = max(1, ceil(len(logs) / LOGS_FOR_PAGE))
    page = 0
    text = format_logs_page(logs, page)
    kb = make_logs_keyboard(page, total_pages, user_id)
    await query.message.edit_text(text, reply_markup=kb)


@dp.message(Command("send_log"))
async def handle_send_log(message: types.Message):
    user_id = message.from_user.id
    if user_id not in gadmins():
        return

    if message.chat.type != "private":
        return await message.reply("Логи доступны только в лс")

    data = load_log()
    logs = data.get("events", []) or []
    if not logs:
        return await message.reply("Логи пустые")

    logs = list(reversed(logs))

    total_pages = max(1, ceil(len(logs) / LOGS_FOR_PAGE))
    page = 0
    text = format_logs_page(logs, page)
    kb = make_logs_keyboard(page, total_pages, user_id)
    await message.answer(text, reply_markup=kb)


@dp.callback_query(lambda c: c.data and c.data.startswith("logs:"))
async def process_logs_page(callback: CallbackQuery):
    await callback.answer()
    try:
        _, owner_id, page_str = callback.data.split(":", 2)
        if int(owner_id) != callback.from_user.id:
            return await callback.answer("Не твоя кнопка")
        if page_str == "noop":
            return
        page = int(page_str)
    except Exception:
        return

    if callback.message.chat.type != "private":
        return await callback.answer("Логи доступны только в лс")

    data = load_log()
    logs = data.get("events", []) or []
    if not logs:
        try:
            await callback.message.edit_text("Логи пустые", reply_markup=None)
        except Exception:
            pass
        return

    logs = list(reversed(logs))
    total_pages = max(1, ceil(len(logs) / LOGS_FOR_PAGE))

    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1

    text = format_logs_page(logs, page)
    kb = make_logs_keyboard(page, total_pages, callback.from_user.id)

    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)


@dp.message(Command("clear_log"))
async def handle_clear_log(message: Message):
    user_id = message.from_user.id

    if user_id in gadmins():
        if int(user_id) not in SPECIAL_ADMINS:
            return await message.answer("Ты не имеешь доступа к данному действию!")
        save_log({"events": []})

        await message.answer(f"Логи успешно очищены!")


@dp.callback_query(F.data.startswith("clear_logs_callback"))
async def handle_clear_log(query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Ты не имеешь доступа к данной кнопке!")

    if int(caller_id) not in SPECIAL_ADMINS:
        return await query.answer("Ты не имеешь доступа к данному действию!")

    save_log({"events": []})

    await query.answer(f"Логи успешно очищены!")


@dp.callback_query(F.data.startswith("admin_give_status"))
async def handle_clear_log(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    data = query.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Ты не имеешь доступа к данной кнопке!")
    if int(caller_id) not in SPECIAL_ADMINS:
        return await query.answer("Ты не имеешь доступа к данному действию!")

    statuses_str = " • "
    for i in range(len(STATUSES)):
        statuses_str = statuses_str + f"{STATUSES[i]} ({i}) \n • "

    statuses_str = statuses_str[:len(statuses_str) - 2]

    await query.message.edit_text(
        f"{await gsname(query.from_user.first_name, user_id)}, введи номер статуса, который хочешь выдать (номер статуса указан возле названия статуса)\n\n{statuses_str}",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_status_give_status)


class AdminPanelStates(StatesGroup):
    waiting_for_mdrops_amount = State()
    waiting_for_id_mdrops_add = State()
    waiting_for_ggs_amount = State()
    waiting_for_id_ggs_add = State()
    waiting_for_id_for_ban = State()
    waiting_for_id_for_unban = State()
    waiting_for_status_give_status = State()
    waiting_for_id_give_status = State()
    waiting_for_id_clear_data = State()
    waiting_for_id_clear_mdrops = State()
    waiting_for_promo_data = State()
    waiting_for_text_send_message = State()
    waiting_for_kb_choose_send_message = State()
    waiting_for_send_kb_send_message = State()


@dp.message(Command("admin_send_message"))
async def handle_msg_admin_send(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    if user_id not in gadmins():
        return

    parts = msg.text.split(" ")
    if len(parts) < 2:
        return await msg.answer(f"Использваоние:\n<code>/admin_send_message (ID)</code>")
    elif not parts[1].isdigit():
        return await msg.answer(f"Использваоние:\n<code>/admin_send_message (ID)</code>")

    if not await bot.get_chat(parts[1]):
        return await msg.answer("Данный пользователь еще не зарегистирован")

    await state.update_data(id=parts[1])

    await state.set_state(AdminPanelStates.waiting_for_text_send_message)
    await msg.answer("Введите текст сообщения", reply_markup=ckb(user_id))


@dp.message(AdminPanelStates.waiting_for_text_send_message)
async def handle_waiting_for_text_send_message(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    text = msg.text

    await state.update_data(text=text)

    await msg.answer(f"Добаваить кноку с URL-ссылкой?", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Да", callback_data="add_kb_admin_send")],
                         [InlineKeyboardButton(text="Нет, отправить уже", callback_data="admin_send_msg_now")]]))
    await state.set_state(AdminPanelStates.waiting_for_kb_choose_send_message)


@dp.message(AdminPanelStates.waiting_for_kb_choose_send_message)
async def handle_waiting_for_promo_data(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    await msg.answer(f"Добаваить кноку с URL-ссылкой?", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Да (дальше можно отменить)", callback_data="add_kb_admin_send")],
                         [InlineKeyboardButton(text="Нет, отправить уже", callback_data="admin_send_msg_now")]]))


@dp.callback_query(F.data == "add_kb_admin_send")
async def handle_add_kb_admin_send(query: CallbackQuery, state: FSMContext):
    uid = query.from_user.id
    await state.set_state(AdminPanelStates.waiting_for_send_kb_send_message)

    await query.message.edit_text("Введите кнопку в формате:\n(текст кнопки)<code>|</code>(ссылка)",
                                  reply_markup=ckb(uid))


@dp.message(AdminPanelStates.waiting_for_send_kb_send_message)
async def handle_waiting_for_send_kb_send_message(msg: Message, state: FSMContext):
    uid = msg.from_user.id

    text, url = msg.text.split("|")
    sd = await state.get_data()
    tuid = sd["id"]
    mtext = sd["text"]
    try:
        await bot.send_message(tuid, mtext, reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=text, url=url)]]))
        await msg.answer("Отправка прошла успешно")
        return await state.clear()
    except Exception as e:
        await msg.answer(f"Произошла ошибка при отправке: {e}")
        return await state.clear()


@dp.callback_query(F.data == "admin_send_msg_now")
async def handle_admin_send_msg_now(query: CallbackQuery, state: FSMContext):
    uid = query.from_user.id

    sd = await state.get_data()
    tuid = sd["id"]
    mtext = sd["text"]
    try:
        await bot.send_message(tuid, mtext)
        await query.message.edit_text("Отправка прошла успешно")
        return await state.clear()
    except Exception as e:
        await query.message.edit_text(f"Произошла ошибка при отправке: {e}")
        return await state.clear()


@dp.message(AdminPanelStates.waiting_for_promo_data)
async def handle_waiting_for_promo_data(message: Message, state: FSMContext):
    if message.from_user.id not in SPECIAL_ADMINS:
        return

    # розбиваємо текст
    parts = message.text.strip().split()

    if len(parts) != 2:
        return await message.reply("<название> <reward>")

    name = parts[0]
    try:
        reward = int(parts[1])  # можна змінити на int, якщо завжди ціле
    except ValueError:
        return await message.reply("❌ Reward должен быть числом.")

    # збереження в базу
    import sqlite3
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO promos (name, reward) VALUES (?, ?)",
            (name, reward)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return await message.reply(f"⚠️ Промо '{name}' уже существует.")
    finally:
        conn.close()
    await send_log(
        f"Админ @{message.from_user.username} ({message.from_user.id}) создал промокод \"{name}\" с призом {reward} mDrops")
    await message.reply(f"✅ Промокод <b>{name}</b> создан!\n🎁 Приз: {reward}")
    await state.clear()


@dp.message(AdminPanelStates.waiting_for_status_give_status)
async def handle_id_for_status_giver(msg: Message, state: FSMContext):
    user_id = msg.from_user.id

    if msg.text.isdigit():
        status_id = int(msg.text)
    elif len(STATUSES) < int(msg.text):
        return await msg.answer("Статуса с таким номером не существует!", kb=ckb(user_id))
    elif int(msg.text) < 0:
        return await msg.answer("Статуса с таким номером не существует!", kb=ckb(user_id))
    else:
        return await msg.answer("Введи правильный номер статуса!", kb=ckb(user_id))

    await state.update_data(status_id=status_id)
    await state.set_state(AdminPanelStates.waiting_for_id_give_status)
    await msg.answer(f"{await gsname(msg.from_user.first_name, user_id)}, введи ID получателя статуса",
                     reply_markup=ckb(user_id))


@dp.message(AdminPanelStates.waiting_for_id_give_status)
async def handle_id_for_status_giver(msg: Message, state: FSMContext):
    user_id = msg.from_user.id

    if msg.text.isdigit():
        id = int(msg.text)
    else:
        return await msg.answer("Введи нормальный ID!", kb=ckb(user_id))

    data_state = await state.get_data()
    status = data_state.get("status_id")

    data = await load_data(id)
    data["status"] = int(status)
    await save_data(id, data)

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await state.clear()
    await msg.answer(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно выдал статус \"{get_status(status)}\" пользователю {target_name} ({id})")
    append_log(
        f"{await gsname(msg.from_user.first_name, user_id)} успешно выдал статус \"{get_status(status)}\" пользователю {target_name} ({id})",
        add_timestamp=True)


@dp.callback_query(F.data.startswith("admin_clear_player"))
async def handle_clear_log(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    data = query.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Ты не имеешь доступа к данной кнопке!")
    if int(caller_id) not in SPECIAL_ADMINS:
        return await query.answer("Ты не имеешь доступа к данному действию!")

    await query.message.edit_text(
        f"{await gsname(query.from_user.first_name, user_id)}, введи ID игрока, которому надо сбросить БД",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_id_clear_data)


@dp.message(AdminPanelStates.waiting_for_id_clear_data)
async def handle_waiting_for_id_clear_data(msg: Message, state: FSMContext):
    user_id = msg.from_user.id

    if msg.text.isdigit():
        id = int(msg.text)
    else:
        return await msg.answer("Введи нормальный ID!", kb=ckb(user_id))

    target_data = await load_data(id)
    if not target_data:
        return await msg.answer("Данный пользователь еще не зарегистрирован!")
    target_data = {
        "coins": 0,
        "GGs": 0,
        "lost_coins": 0,
        "won_coins": 0,
        "status": 0
    }

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await save_data(id, target_data)
    append_log(f"{await gsname(msg.from_user.first_name, user_id)} успешно очистил БД игрока {target_name} ({id})",
               add_timestamp=True)
    await msg.reply(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно сбросил бд игрока {target_name} ({id})")
    await state.clear()


@dp.callback_query(F.data.startswith("admin_clear_mDrops"))
async def handle_clear_log(query: CallbackQuery, state: FSMContext):
    user_id = query.from_user.id
    data = query.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await query.answer("Ты не имеешь доступа к данной кнопке!")

    await query.message.edit_text(
        f"{await gsname(query.from_user.first_name, user_id)}, введи ID игрока, которому надо сбросить mDrops",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_id_clear_mdrops)


import html as pyhtml


def safe_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, (int,)):
            return v
        if isinstance(v, float):
            return int(v)
        s = str(v).strip()
        if s == "":
            return default
        return int(s)
    except Exception:
        return default


def format_contract_row(idx: int, c: dict) -> str:
    """
    Формирует одну запись контракта для вывода (HTML-формат).
    Безопасно обрабатывает last_payment, даже если там лежит некорректная строка.
    """
    contract_id = pyhtml.escape(str(c.get("id", "-")))
    link_raw = str(c.get("link", "—"))
    link_display = pyhtml.escape(link_raw)
    link_href = link_raw
    if not link_href.startswith("http"):
        if link_href.startswith("@"):
            link_href = f"https://t.me/{link_href[1:]}"
        else:
            if "/" not in link_href and "." not in link_href:
                link_href = f"https://t.me/{link_href}"
            else:
                link_href = link_href

    owner = pyhtml.escape(str(c.get("owner_id", "—")))
    subs = pyhtml.escape(str(c.get("subs", "—")))
    level = pyhtml.escape(str(c.get("level", "—")))

    last_ts = safe_int(c.get("last_payment", 0) or 0)
    if last_ts:
        try:
            last_dt = datetime.utcfromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            last_dt = "неизвестно"
    else:
        last_dt = "никогда"
    last_dt = pyhtml.escape(last_dt)

    return (
        f"<b>{idx}.</b> Контракт <code>#{contract_id}</code>\n"
        f"🔗 Канал: <a href=\"{pyhtml.escape(link_href)}\">{link_display}</a>\n"
        f"👤 Владелец: <code>{owner}</code>  •  📈 Подписчиков: <b>{subs}</b>\n"
        f"🏷 Уровень: <b>{level}</b>  •  ⏱ Последняя выплата: <code>{last_dt}</code>\n"
    )


def build_partners_page_text(channels_list: list, page: int, page_size: int) -> str:
    total = len(channels_list)
    if total == 0:
        return "📄 <b>Список контрактов пуст.</b>"

    total_pages = math.ceil(total / page_size)
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1

    start = page * page_size
    end = start + page_size
    slice_items = channels_list[start:end]

    header = f"📄 <b>Контракты (страница {page + 1}/{total_pages})</b>\n\n"
    body_lines = []
    for i, c in enumerate(slice_items, start=1 + start):
        body_lines.append(format_contract_row(i, c))
    footer = f"\nВсего контрактов: <b>{total}</b>"

    return header + "\n".join(body_lines) + footer


def build_partners_page_kb(channels_slice: list, page: int, total_items: int, page_size: int) -> InlineKeyboardMarkup:
    """
    Создаёт InlineKeyboard с:
    - для каждого контракта на странице — кнопку 'Завершить #id' (callback: partners_finish:<id>)
    - навигацией Назад/Дальше/Закрыть
    """
    kb = []
    # Кнопки "Завершить" для каждой записи на странице
    for c in channels_slice:
        cid = safe_int(c.get("id", 0))
        # добавляем кнопку с понятным текстом, callback содержит id
        kb.append([InlineKeyboardButton(text=f"✅ Завершить #{cid}", callback_data=f"partners_finish:{cid}")])

    # Навигация
    total_pages = max(1, math.ceil(total_items / page_size))
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"partners_list:{page - 1}"))
    else:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"partners_list:noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"partners_list:{page + 1}"))
    else:
        nav_row.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"partners_list:noop"))

    kb.append(nav_row)
    kb.append([InlineKeyboardButton(text="Закрыть", callback_data="partners_list:close")])

    return InlineKeyboardMarkup(inline_keyboard=kb)


# ====== Команда для админа /partners_list ======
@dp.message(F.text == "/partners_list")
async def partners_list_cmd(msg: Message):
    # проверяем: доступ имеют админы (ADMINS) — оставляем вашу логику
    if not msg.from_user.id in gadmins():
        return

    partners = load_partners()
    channels = list(partners.get("channels", {}).values())

    # Сортируем по id безопасно
    try:
        channels = sorted(channels, key=lambda x: safe_int(x.get("id", 0)))
    except Exception:
        pass

    page = 0
    text = build_partners_page_text(channels, page, PAGE_SIZE)
    # передаём именно срез элементов текущей страницы в клавиатуру
    start = page * PAGE_SIZE
    slice_items = channels[start:start + PAGE_SIZE]
    kb = build_partners_page_kb(slice_items, page, len(channels), PAGE_SIZE)

    await msg.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)


# ====== Callback для пагинации ======
@dp.callback_query(F.data.startswith("partners_list:"))
async def partners_list_page_cb(query: CallbackQuery):
    # Только админ может пользоваться
    if not query.from_user.id in gadmins():
        return

    parts = query.data.split(":")
    action = parts[1] if len(parts) > 1 else None

    if action == "close":
        try:
            await query.message.delete()
        except Exception:
            pass
        return await query.answer()

    if action == "noop":
        return await query.answer()

    try:
        page = int(action)
    except Exception:
        return await query.answer("Неизвестное действие.", show_alert=True)

    partners = load_partners()
    channels = list(partners.get("channels", {}).values())
    try:
        channels = sorted(channels, key=lambda x: safe_int(x.get("id", 0)))
    except Exception:
        pass

    text = build_partners_page_text(channels, page, PAGE_SIZE)
    start = page * PAGE_SIZE
    slice_items = channels[start:start + PAGE_SIZE]
    kb = build_partners_page_kb(slice_items, page, len(channels), PAGE_SIZE)

    try:
        await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await query.message.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)

    await query.answer()


@dp.callback_query(F.data.startswith("partners_finish:"))
async def partners_finish_cb(query: CallbackQuery):
    parts = query.data.split(":")
    if len(parts) < 2:
        return await query.answer("Неверные данные.", show_alert=True)

    try:
        contract_id = int(parts[1])
    except Exception:
        return await query.answer("Неверный id контракта.", show_alert=True)

    caller = query.from_user
    caller_id = caller.id

    # Проверка прав: используем SPECIAL_ADMINS если есть, иначе fallback на ADMINS
    allowed = False
    if "SPECIAL_ADMINS" in globals() and isinstance(SPECIAL_ADMINS, (list, set, tuple)):
        if caller_id in SPECIAL_ADMINS:
            allowed = True
    else:
        # fallback: дать право всем из ADMINS (если SPECIAL_ADMINS не задан)
        if caller_id in gadmins():
            allowed = True

    if not allowed:
        return await query.answer("У вас нет прав на завершение контракта.", show_alert=True)

    # Ищем контракт
    key, contract = find_contract_by_id(contract_id)
    if not contract or not key:
        return await query.answer("Контракт не найден.", show_alert=True)

    owner_id = contract.get("owner_id")
    # удаляем контракт
    partners = load_partners()
    try:
        partners.get("channels", {}).pop(key, None)
        save_partners(partners)
    except Exception:
        return await query.answer("Ошибка при удалении контракта.", show_alert=True)

    # логируем
    append_log({
        "time": int(time.time()),
        "action": "force_finish",
        "contract_id": contract_id,
        "owner_id": owner_id,
        "by_admin_id": caller_id,
        "by_admin_name": caller.full_name
    })

    # уведомляем владельца (если возможно)
    try:
        if owner_id:
            await bot.send_message(owner_id,
                                   f"❗️ Ваш контракт <code>#{contract_id}</code> был завершён администратором {caller.full_name} (id: {caller_id}).",
                                   parse_mode="HTML")
    except Exception:
        # если отправка не удалась — не критично, просто логируем
        append_log({
            "time": int(time.time()),
            "action": "notify_owner_failed",
            "contract_id": contract_id,
            "owner_id": owner_id
        })

    # Ответ админу и обновление UI: редактируем текущее сообщение списка — удалим кнопку завершения для этого контракта
    try:
        # Попробуем отредактировать сообщение: пересоберём страницу, на которой находился контракт
        partners = load_partners()
        channels = list(partners.get("channels", {}).values())
        try:
            channels = sorted(channels, key=lambda x: safe_int(x.get("id", 0)))
        except Exception:
            pass

        # Если текущ message содержит текст — попробуем найти текущую страницу по текстовой нумерации.
        # Для простоты — покажем первую страницу заново.
        page = 0
        text = build_partners_page_text(channels, page, PAGE_SIZE)
        start = page * PAGE_SIZE
        slice_items = channels[start:start + PAGE_SIZE]
        kb = build_partners_page_kb(slice_items, page, len(channels), PAGE_SIZE)

        await query.message.edit_text(f"✅ Контракт #{contract_id} завершён.\n\n{text}", parse_mode="HTML",
                                      reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        # Если не получилось редактировать — просто отправим ответ
        await query.message.answer(f"✅ Контракт #{contract_id} завершён.")
    return await query.answer("Контракт завершён.", show_alert=True)


@dp.message(AdminPanelStates.waiting_for_id_clear_mdrops)
async def handle_waiting_for_id_clear_data(msg: Message, state: FSMContext):
    user_id = msg.from_user.id

    if msg.text.isdigit():
        id = int(msg.text)
    else:
        return await msg.answer("Введи нормальный ID!", kb=ckb(user_id))

    target_data = await load_data(id)
    if not target_data:
        return await msg.answer("Данный пользователь еще не зарегистрирован!")

    target_data["coins"] = 0

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await save_data(id, target_data)
    append_log(f"{await gsname(msg.from_user.first_name, user_id)} успешно очистил mDrops игрока {target_name} ({id})",
               add_timestamp=True)
    await msg.reply(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно сбросил mDrops игрока {target_name} ({id})")
    await state.clear()


@dp.message(Command("admin"))
async def handle_admin_panel(message: Message):
    user_id = message.from_user.id

    if not user_id in gadmins():
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Начиcлить mDrops", callback_data=f"add_admins_mdrops:{user_id}")],
                         [InlineKeyboardButton(text="Начиcлить GGs", callback_data=f"add_admins_ggs:{user_id}")],
                         [InlineKeyboardButton(text="Забанить", callback_data=f"admin_ban:{user_id}"),
                          InlineKeyboardButton(text="Разбанить", callback_data=f"admins_unban:{user_id}")],
                         [InlineKeyboardButton(text="Логи", callback_data=f"send_logs_callback:{user_id}"),
                          InlineKeyboardButton(text="Очистить Логи", callback_data=f"clear_logs_callback:{user_id}")],
                         [InlineKeyboardButton(text="Дать Статус", callback_data=f"admin_give_status:{user_id}")],
                         [InlineKeyboardButton(text="Очистить Игрока", callback_data=f"admin_clear_player:{user_id}"),
                          InlineKeyboardButton(text="Очистить mDrops Игрока",
                                               callback_data=f"admin_clear_mDrops:{user_id}")],
                         [InlineKeyboardButton(text="Новый Промо", callback_data=f"admin_new_promo:{user_id}")]])

    await message.reply(f"{await gsname(message.from_user.first_name, user_id)}, ты в админ-панели, выбери действие:",
                        reply_markup=kb)


@dp.callback_query(F.data.startswith("admin_new_promo:"))
async def handle_callback_admin_ban(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await callback.answer("Ты не имеешь доступа к данной кнопке!")
    if int(caller_id) not in SPECIAL_ADMINS:
        return await callback.answer("Ты не имеешь доступа к данному действию!")

    await callback.message.edit_text(
        f"{await gsname(callback.from_user.first_name, user_id)}, введи данные нового промокода в формате \"(название) (приз)\"",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_promo_data)


@dp.callback_query(F.data.startswith("admins_unban:"))
async def handle_callback_admin_ban(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await callback.answer("Ты не имеешь доступа к данной кнопке!")

    await callback.message.edit_text(
        f"{await gsname(callback.from_user.first_name, user_id)}, введи ID пользователя которого хочешь разбанить",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_id_for_unban)


@dp.message(AdminPanelStates.waiting_for_id_for_unban)
async def handle_waiting_for_id_for_unban(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    id = msg.text

    data = await load_data(id)
    if not data:
        await state.clear()
        return await msg.reply("Данный пользователь еще не зарегистрирован!")

    data = load_banned()
    if int(id) not in data["banned"]:
        return await msg.reply("⚠️ Пользователь не забанен.")

    data["banned"].remove(int(id))
    save_banned(data)

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await msg.answer(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно разбанил пользователя {target_name} ({id})")
    append_log(f"{await gsname(msg.from_user.first_name, user_id)} успешно разбанил пользователя {target_name} ({id})",
               add_timestamp=True)
    await state.clear()


@dp.callback_query(F.data.startswith("admin_ban:"))
async def handle_callback_admin_ban(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await callback.answer("Ты не имеешь доступа к данной кнопке!")

    await callback.message.edit_text(
        f"{await gsname(callback.from_user.first_name, user_id)}, введи ID пользователя которого хочешь забанить",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_id_for_ban)


@dp.message(AdminPanelStates.waiting_for_id_for_ban)
async def handle_waiting_for_id_for_ban(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    id = msg.text

    data = await load_data(id)
    if not data:
        await state.clear()
        return await msg.reply("Данный пользователь еще не зарегистрирован!")

    if int(id) in SPECIAL_ADMINS:
        return await msg.answer("Ты не можешь забанить главного админа!")

    data = load_banned()
    if id in data["banned"]:
        return await msg.reply("⚠️ Пользователь уже забанен.")

    data["banned"].append(int(id))
    save_banned(data)

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await msg.answer(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно забанил пользователя {target_name} ({id})")
    append_log(f"{await gsname(msg.from_user.first_name, user_id)} успешно забанил пользователя {target_name} ({id})",
               add_timestamp=True)
    await state.clear()


@dp.callback_query(F.data.startswith("add_admins_mdrops:"))
async def handle_callback_add_admins_mdrops(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await callback.answer("Ты не имеешь доступа к данной кнопке!")
    if int(caller_id) not in SPECIAL_ADMINS:
        return await callback.answer("Ты не имеешь доступа к данному действию!")

    await callback.message.edit_text(
        f"{await gsname(callback.from_user.first_name, user_id)}, введи количество mDrops которое желаешь начислить",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_mdrops_amount)


@dp.message(AdminPanelStates.waiting_for_mdrops_amount)
async def handle_waiting_for_mdrops_amount(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    amount_text = msg.text
    amount = 0

    if "к" in amount_text:
        amount = parse_bet_input(amount_text)
    elif amount_text.isdigit():
        try:
            amount = int(amount_text)
        except Exception:
            return await msg.reply("Неверное количество", reply_markup=ckb(user_id))
    else:
        return await msg.reply("Неверное количество", reply_markup=ckb(user_id))

    await state.update_data(amount=amount)

    await msg.reply(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты начисляешь {amount} mDrops, введи ID получателя")
    await state.set_state(AdminPanelStates.waiting_for_id_mdrops_add)


@dp.message(AdminPanelStates.waiting_for_id_mdrops_add)
async def handle_waiting_for_mdrops_amount(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    id = msg.text

    data = await load_data(id)
    if not data:
        await state.clear()
        return await msg.reply("Данный пользователь еще не зарегистрирован!")

    data_state = await state.get_data()
    amount = data_state.get("amount")

    data["coins"] += amount
    await save_data(id, data)

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await msg.answer(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно начислил {amount} mDrops пользователю {target_name} ({id})")
    append_log(
        f"{await gsname(msg.from_user.first_name, user_id)} успешно начислил {amount} mDrops пользователю {target_name} ({id})",
        add_timestamp=True)
    await state.clear()


@dp.callback_query(F.data.startswith("add_admins_ggs:"))
async def handle_callback_add_admins_mdrops(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data
    caller_id = data.split(":")[1]

    if int(caller_id) != int(user_id):
        return await callback.answer("Ты не имеешь доступа к данной кнопке!")
    if int(caller_id) not in SPECIAL_ADMINS:
        return await callback.answer("Ты не имеешь доступа к данному действию!")

    await callback.message.edit_text(
        f"{await gsname(callback.from_user.first_name, user_id)}, введи количество GGs которое желаешь начислить",
        reply_markup=ckb(user_id))
    await state.set_state(AdminPanelStates.waiting_for_ggs_amount)


@dp.message(AdminPanelStates.waiting_for_ggs_amount)
async def handle_waiting_for_ggs_amount(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    amount_text = msg.text
    amount = 0

    if "к" in amount_text:
        amount = parse_bet_input(amount_text)
    elif amount_text.isdigit():
        try:
            amount = int(amount_text)
        except Exception:
            return await msg.reply("Неверное количество", reply_markup=ckb(user_id))
    else:
        return await msg.reply("Неверное количество", reply_markup=ckb(user_id))

    await state.update_data(amount=amount)

    await msg.reply(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты начисляешь {amount} GGs, введи ID получателя")
    await state.set_state(AdminPanelStates.waiting_for_id_ggs_add)


@dp.message(AdminPanelStates.waiting_for_id_ggs_add)
async def handle_waiting_for_ggs_amount(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    id = msg.text

    data = await load_data(id)
    if not data:
        await state.clear()
        return await msg.reply("Данный пользователь еще не зарегистрирован!")

    data_state = await state.get_data()
    amount = data_state.get("amount")

    data["GGs"] += amount
    await save_data(id, data)

    chat = await bot.get_chat(id)
    target_name = chat.first_name

    await msg.answer(
        f"{await gsname(msg.from_user.first_name, user_id)}, ты успешно начислил {amount} GGs пользователю {target_name} ({id})")
    append_log(
        f"{await gsname(msg.from_user.first_name, user_id)} успешно начислил {amount} GGs пользователю {target_name} ({id})",
        add_timestamp=True)
    await state.clear()


@dp.message(Command("data"))
async def handle_admin_data(message: Message):
    user_id = message.from_user.id

    parts = message.text.strip().lower().split()

    if user_id in gadmins():
        if len(parts) < 2:
            return await message.answer("❌ Использование:\n/data (ID пользователя)")

        target_id = parts[1]

        data = await load_data(str(target_id))
        await message.answer(str(data))


@dp.message(F.text.startswith("/get"))
async def handle_admin_get(message: Message):
    """
    Команда для админов: /get <user_id> или ответ на сообщение пользователя.
    Показывает общую информацию о пользователе + банковскую информацию (депозит, кредит, карта).
    """
    # проверка прав
    try:
        uid = message.from_user.id
    except Exception:
        return
    if uid not in gadmins():
        return

    parts = (message.text or "").strip().split()
    target_id = None

    # получаем target_id из аргументов или из reply
    if len(parts) > 1 and parts[1].strip():
        try:
            target_id = int(parts[1].strip())
        except ValueError:
            await message.answer("⚠️ Неверный ID пользователя. Использование: /get <ID>")
            return
    elif message.reply_to_message and getattr(message.reply_to_message, "from_user", None):
        try:
            target_id = int(message.reply_to_message.from_user.id)
        except Exception:
            target_id = None
    else:
        await message.answer("⚠️ Использование: /get <ID пользователя> (или ответ на сообщение пользователя)")
        return

    if target_id is None:
        await message.answer("⚠️ Не удалось определить ID пользователя.")
        return

    # блокированные айди (защита от оскорблений)
    if str(target_id) in {"8375492513", "8257726098"}:
        return await message.reply("та пошел ты")

    # проверяем бан (banned.json может отсутствовать)
    ban_status = "незабанен"
    try:
        import pathlib, json
        bpath = pathlib.Path("banned.json")
        if bpath.exists():
            with bpath.open(encoding="utf-8") as bf:
                bj = json.load(bf)
            banned_list = bj.get("banned", []) if isinstance(bj, dict) else []
            if int(target_id) in [int(x) for x in banned_list]:
                ban_status = "забанен"
    except Exception:
        # молча игнорируем ошибки чтения файла
        pass

    # получаем базовую информацию о чате (username/first_name)
    username = None
    first_name = None
    try:
        chat = await bot.get_chat(target_id)
        # Chat или ChatFullInfo — стараемся получить имя/username аккуратно
        username = getattr(chat, "username", None)
        first_name = getattr(chat, "first_name", None) or getattr(chat, "title", None)
    except Exception:
        # если не удалось — оставим None
        pass

    # загружаем данные пользователя из хранилища
    data = await load_data(str(target_id))
    if not data:
        return await message.reply("Пользователь еще не зарегистрирован!")

    # Баланс и базовые метрики (без KeyError)
    coins = data.get("coins", 0)
    ggs = int(data.get("GGs", 0)) if data.get("GGs") is not None else 0
    won = data.get("won_coins", 0)
    lost = data.get("lost_coins", 0)
    status = get_status(data.get("status"))
    clan = data.get("clan") if data.get("clan") else "нету"
    total_donated = data.get("total_donated", "не донатил")

    # Банк: депозит
    deposit_text = "🔒 Нет активного депозита"
    percent_deposit = "—"
    end_deposit = "—"
    try:
        bank = data.get("bank", {}) or {}
        deposit = bank.get("deposit")
        if deposit:
            amt = float(deposit.get("amount", 0))
            percent = float(deposit.get("percent", 0))
            # попытка разобрать время окончания
            end_iso = deposit.get("end")
            end_deposit = end_iso or "—"
            # расчёт примерный итоговой суммы (как раньше)
            profit = amt * (percent / 100.0)
            deposit_text = f"{format_balance(amt)} mDrops"
            percent_deposit = f"{percent}%"
    except Exception:
        deposit_text = "🔒 Нет активного депозита"
        percent_deposit = "—"
        end_deposit = "—"

    # Банк: кредит
    credit_text = "✅ Нет активного кредита"
    credit_start = "—"
    try:
        credit = bank.get("credit") if bank else None
        if credit:
            c_amt = float(credit.get("amount", 0))
            credit_text = f"{format_balance(c_amt)} mDrops"
            credit_start = credit.get("start", "—")
    except Exception:
        credit_text = "✅ Нет активного кредита"
        credit_start = "—"

    # Карта: у нас 1 карта максимум — получим её (если есть) через list_cards_by_owner
    card_info_lines = []
    try:
        cards = await list_cards_by_owner(str(target_id))  # dict card_number->data
        if cards:
            # берем первую карту (у пользователя ровно одна по бизнес-логике)
            card_number = list(cards.keys())[0]
            card = await get_card(card_number)
            if card:
                cname = card.get("name") or "Без имени"
                cbalance = float(card.get("balance", 0))
                cblocked = bool(card.get("blocked"))
                created_at = card.get("created_at") or "—"
                # маскируем номер карты: показываем только последние 4 цифры
                try:
                    masked = f"<tg-spoiler>{str(card_number)[-16:-12]} {str(card_number)[-12:-8]} {str(card_number)[-8:-4]}</tg-spoiler> {str(card_number)[-4:]}"
                except Exception:
                    masked = str(card_number)
                card_info_lines.append(f"Номер: {masked}")
                card_info_lines.append(f"Название: {cname}")
                card_info_lines.append(f"Баланс карты: <b>{format_balance(cbalance)}</b> mDrops")
                card_info_lines.append(f"Статус: {'ЗАБЛОКИРОВАНА' if cblocked else 'Активна'}")
                card_info_lines.append(f"Создана: {created_at}")
                # дефолтная карта?
                default_card = bank.get("default_card") if bank else None
                if default_card and default_card == card_number:
                    card_info_lines.append("Это дефолтная карта пользователя")
            else:
                card_info_lines.append("Карта не найдена (ошибка чтения БД)")
        else:
            card_info_lines.append("Карт нет")
    except Exception:
        card_info_lines.append("Ошибка при получении карт")

    # Формируем итоговый текст аккуратно (не используем вложенные f-строки с кавычками)
    header = f"🆔 {html.code(str(target_id))} / {('@' + username) if username else (first_name or str(target_id))}\n\n"
    body_lines = [
        f"💰 Баланс: {format_balance(coins)} mDrops",
        f"💎 GGs: {ggs}",
        f"📈 Выиграно: {format_balance(won)} mDrops",
        f"🗿 Проиграно: {format_balance(lost)} mDrops",
        f"⚡️ Статус: {status}",
        f"🛡 Клан: {clan}",
        "",
        f"⛔️ Бан: {ban_status}",
        "",
        "🏦 Банк (старая версия):",
        f" • Депозит: {deposit_text}",
        f"   Процент: {percent_deposit}",
        f"   Конец: {end_deposit}",
        f" • Кредит: {credit_text}",
        f"   Начало: {credit_start}",
        "",
        f"{html.bold('Карта пользователя:')}"
    ]

    # добавляем строки про карту
    body_lines.extend(card_info_lines)
    body_lines.append("")
    body_lines.append(f"{html.bold('Общая сумма доната:')} {total_donated} ⭐️")

    final_text = header + "\n".join(body_lines)

    # отправляем ответ (HTML)
    try:
        await message.reply(final_text, parse_mode="HTML")
    except Exception:
        # если редактирование/парсинг HTML упал — отправим без parse_mode
        await message.reply(final_text)


@dp.message(Command("clear_b"))
async def admin_clear_b(message: Message):
    if message.from_user.id not in gadmins():
        return

    parts = (message.text or "").strip().split(maxsplit=1)
    target_id = None

    if len(parts) > 1 and parts[1].strip():
        try:
            target_id = int(parts[1].strip())
        except ValueError:
            await message.answer("⚠️ Неверный ID пользователя. Использование: /clear_b <ID>")
            return
    elif message.reply_to_message and message.reply_to_message.from_user:
        target_id = int(message.reply_to_message.from_user.id)
    else:
        await message.answer("⚠️ Использование: /clear_b <ID пользователя> (или ответ на сообщение пользователя)")
        return

    uid_str = str(target_id)
    data = await load_data(uid_str)
    if data is None:
        await message.answer("❌ Пользователь не найден.")
        return

    data["coins"] = 0
    await save_data(uid_str, data)

    first_name = None
    try:
        chat = await message.bot.get_chat(target_id)
        first_name = getattr(chat, "first_name", None) or getattr(chat, "username", None)
    except Exception:
        first_name = None

    actor = f"@{message.from_user.username}" if getattr(message.from_user, "username", None) else str(
        message.from_user.id)
    target_display = f"{first_name} ({target_id})" if first_name else str(target_id)

    await send_log(f"Админ {actor} ({message.from_user.id}) обнулил все mDrops игрока {target_display}")
    await message.answer(f"✅ Все mDrops игрока {target_display} успешно обнулены.")


@dp.message(Command("clear"))
async def admin_clear(message: Message):
    user_id = str(message.from_user.id)

    if int(user_id) not in gadmins():
        return
    if int(user_id) not in SPECIAL_ADMINS:
        return await message.answer("Ты не имеешь доступа к данному действию!")

    args = message.text.split()
    if len(args) != 2:
        await message.answer("⚠️ Использование: /clear {ID пользователя}")
        return

    target_id = args[1]
    data = await load_data(target_id)
    if data is None:
        await message.answer("❌ Пользователь не найден.")
        return

    # Очищення основних полів
    data = {
        "coins": 0,
        "GGs": 0,
        "lost_coins": 0,
        "won_coins": 0,
        "status": 0
    }

    await save_data(target_id, data)

    chat = await bot.get_chat(target_id)
    first_name = chat.first_name

    await send_log(
        f"Админ @{message.from_user.username} ({message.from_user.id}) очистисл всю БД игрока {first_name} ({target_id})")
    await message.answer(f"✅ Все данные пользователя {target_id} успешно очищены.")


@dp.message(Command("statuses_info"))
async def handle_statuses_info(message: Message):
    user_id = message.from_user.id

    if not user_id in gadmins():
        return

    statuses_str = " • "
    for i in range(len(STATUSES)):
        statuses_str = statuses_str + f"{STATUSES[i]} ({i}) \n • "

    statuses_str = statuses_str[:len(statuses_str) - 2]

    special_statuses_str = "\n⭐️ Уникальные статусы:\n • "
    for i in range(len(get_unique_statuses())):
        special_statuses_str = special_statuses_str + f"{get_unique_statuses()[i]} (999{i}) \n • "

    special_statuses_str = special_statuses_str[:len(special_statuses_str) - 2]

    await message.answer(f"💡 Существующие статусы:\n{statuses_str} {special_statuses_str}")


@dp.message(F.text.lower().startswith("/istatus"))
async def handle_new_status(message: Message):
    user_id = message.from_user.id

    if not user_id in gadmins():
        return
    recipient_id = str(message.reply_to_message.from_user.id)
    recipient_data = await load_data(recipient_id)
    if not recipient_data:
        await create_user_data(recipient_id)
        recipient_data = await load_data(recipient_id)

    text = message.text.strip().lower()
    numb = text.split()[1]

    recipient_data["status"] = int(numb)
    await save_data(recipient_id, recipient_data)
    await send_log(
        f"Админ {html.link(await gsname(message.from_user.first_name, message.from_user.id), f"t.me/{message.from_user.username}")} выдал статус под номером {int(numb)} ({STATUSES[int(numb)]})")

    await message.answer(f"Статус номер {numb} ({STATUSES[int(numb)]}) выдано пользователю ID {recipient_id}")


@dp.message(F.text.lower().startswith("/new_promo"))
async def handle_new_promo(message: Message):
    if message.from_user.id not in gadmins():
        return

    # розбиваємо текст
    parts = message.text.strip().split()

    if len(parts) != 3:
        return await message.reply("❌ Использование: /new_promo <название> <reward>")

    name = parts[1]
    try:
        reward = float(parts[2])  # можна змінити на int, якщо завжди ціле
    except ValueError:
        return await message.reply("❌ Reward должен быть числом.")

    # збереження в базу
    import sqlite3
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO promos (name, reward) VALUES (?, ?)",
            (name, reward)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return await message.reply(f"⚠️ Промо '{name}' уже существует.")
    finally:
        conn.close()
    await send_log(
        f"Админ @{message.from_user.username} ({message.from_user.id}) создал промокод \"{name}\" с призом {reward} mDrops")
    await message.reply(f"✅ Промокод <b>{name}</b> создан!\n🎁 Приз: {reward}")


@dp.message(F.text.lower() == "/manual_save_db")
async def handle_manual_save_db(msg: Message):
    user_id = msg.from_user.id
    name = msg.from_user.first_name

    if not user_id in SPECIAL_ADMINS:
        return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("PRAGMA wal_checkpoint(FULL);")
            conn.commit()

        append_log(f"{await gsname(name, user_id)} вручную сохраненил БД!", add_timestamp=True)
    except Exception as e:
        return await handle_error(msg.from_user.username, e, user_id, 888)

    await msg.reply(f"{await gsname(name, user_id)}, ручное сохранение БД прошло успешно!")


@dp.message(F.text.lower().startswith("/hhh"))
async def hhh_command(message: Message):
    if int(bot.id) == 8257726098:
        if not message.from_user.id in SPECIAL_ADMINS:
            return

    if not message.reply_to_message:
        await message.answer("❗ Ответе пользователю, которому хотите начислить mDrops.")
        return

    recipient_id = str(message.reply_to_message.from_user.id)
    recipient_data = await load_data(recipient_id)
    if not recipient_data:
        await create_user_data(recipient_id)
        recipient_data = await load_data(recipient_id)

    text = message.text.strip().lower()
    amount_text = text.split()[1]

    amount = 0

    if amount_text in ["все", "вб"]:
        amount = 10000  # or another value representing "all money"
    else:
        try:
            amount = parse_bet_input(amount_text)
            amount = int(amount)

            if amount < 0:
                raise ValueError
        except ValueError:
            await message.answer("❗ Неверная сумма")
            return

    recipient_data["coins"] += amount
    await save_data(recipient_id, recipient_data)

    chat = await bot.get_chat(recipient_id)
    recipient_name = chat.first_name

    await send_log(
        f"Админ @{message.from_user.username} ({message.from_user.id}) начислил {format_balance(amount)} mDrops {recipient_name} ({recipient_id})")
    await message.answer(
        f"💸 {format_balance(amount)} mDrops было начислено пользователю {await gsname(recipient_name, message.from_user.id)}.")


@dp.message(F.text.lower().startswith("/ggg"))
async def ggg_command(message: Message):
    if not message.from_user.id in SPECIAL_ADMINS:
        return

    if not message.reply_to_message:
        await message.answer("❗ Ответе пользователю, которому хотите начислить GGs.")
        return

    recipient_id = str(message.reply_to_message.from_user.id)
    recipient_data = await load_data(recipient_id)
    if not recipient_data:
        await create_user_data(recipient_id)
        recipient_data = await load_data(recipient_id)

    text = message.text.strip().lower()
    amount_text = text.split()[1]

    amount = 0

    if amount_text in ["все", "вб"]:
        amount = 10  # or another value representing "all money"
    else:
        try:
            amount = parse_bet_input(amount_text)
            amount = int(amount)

            if amount < 0:
                raise ValueError
        except ValueError:
            await message.answer("❗ Неверная сумма")
            return

    recipient_data["GGs"] += amount
    await save_data(recipient_id, recipient_data)

    recipient_name = message.reply_to_message.from_user.first_name
    await send_log(
        f"Админ @{message.from_user.username} ({message.from_user.id}) начислил {format_balance(amount)} GGs {recipient_name} ({recipient_id})")
    await message.answer(f"💸 {format_balance(amount)} GGs было начислено пользователю {await gsname(recipient_name)}.")


BANNED_FILE = "banned.json"


def load_banned():
    try:
        with open(BANNED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "banned" not in data:
                data["banned"] = []
            return data
    except FileNotFoundError:
        return {"banned": []}


def save_banned(data):
    with open(BANNED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


@dp.message(Command("ban"))
async def ban_command(message: Message):
    if message.from_user.id not in gadmins():
        return

    parts = message.text.split()
    if len(parts) < 2 and not message.reply_to_message:
        return await message.reply("❌ Использование: /ban <user_id> или в ответ на сообщение.")

    # Якщо є реплай — банимо того, на кого відповідь
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        try:
            target_id = int(parts[1])
        except ValueError:
            return await message.reply("❌ Неверный user_id.")

    if target_id in SPECIAL_ADMINS:
        return await message.answer("Нельзя забанить главного админа")

    data = load_banned()
    if target_id in data["banned"]:
        return await message.reply("⚠️ Пользователь уже забанен.")

    data["banned"].append(target_id)
    save_banned(data)

    await message.reply(f"✅ Пользователь {target_id} добавлен в бан.")


@dp.message(Command("unban"))
async def unban_command(message: Message):
    if message.from_user.id not in gadmins():
        return

    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("❌ Использование: /unban <user_id>")

    try:
        target_id = int(parts[1])
    except ValueError:
        return await message.reply("❌ Неверный user_id.")

    data = load_banned()
    if target_id not in data["banned"]:
        return await message.reply("⚠️ Пользователь не забанен.")

    data["banned"].remove(target_id)
    save_banned(data)

    await message.reply(f"✅ Пользователь {target_id} разбанен.")


def _clear_all_deposits_sync(db_path: str):
    """
    Синхронна функція, що виконує всі блокуючі операції з sqlite.
    Повертає (modified_count, modified_uids_list).
    """
    modified = []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        # переконаємося, що таблиця існує
        cur.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("SELECT key, value FROM json_data")
        rows = cur.fetchall()

        for row in rows:
            key = row["key"]
            try:
                data = json.loads(row["value"]) if row["value"] else {}
            except Exception:
                # якщо не вдається розпарсити — пропускаємо
                continue

            bank = data.get("bank")
            if isinstance(bank, dict) and "deposit" in bank:
                # видаляємо депозит
                del bank["deposit"]
                # якщо bank став пустим, можна його лишити або видалити; тут лишаємо порожній dict
                data["bank"] = bank

                # зберігаємо назад JSON
                new_json = json.dumps(data, ensure_ascii=False)
                cur.execute("UPDATE json_data SET value = ? WHERE key = ?", (new_json, key))
                modified.append(key)

        conn.commit()
        return len(modified), modified
    finally:
        conn.close()


# Файл с администраторами и блокировка
ADMIN_FILE = "admins_data.json"
ADMIN_FILE_LOCK = __import__("asyncio").Lock()


async def _load_admins():
    """Возвращает список admin ID (int). Если файла нет — возвращает []."""
    try:
        with open(ADMIN_FILE, "r", encoding="utf-8") as f:
            data = __import__("json").load(f)
            admins = data.get("admins", [])
            return [int(x) for x in admins]
    except FileNotFoundError:
        return []
    except Exception:
        # При ошибке безопаснее вернуть пустой список
        return []


async def _save_admins(admins):
    """Сохраняет список admin ID в файл в формате {"admins":[...]} (атомарно)."""
    data = {"admins": [int(x) for x in admins]}
    tmp = ADMIN_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        __import__("json").dump(data, f, ensure_ascii=False, indent=2)
    __import__("os").replace(tmp, ADMIN_FILE)


# Хендлер /add_admin
@dp.message(F.text.startswith("/add_admin"))
async def add_admin_handler(message: Message):
    """
    Использование: /add_admin <ID>
    Добавляет указанный числовой ID в admins_data.json.
    Если файл пустой — разрешено первое добавление (чтобы установить первого админа).
    Если в файле уже есть админы — добавлять может только существующий админ.
    """
    try:
        parts = (message.text or "").strip().split()
        if len(parts) != 2:
            return await message.reply("Использование: /add_admin <ID>\nПример: /add_admin 5143424934")

        try:
            new_id = int(parts[1])
        except ValueError:
            return await message.reply("Неверный ID. Укажите числовой Telegram ID (например 5143424934).")

        caller_id = int(message.from_user.id)

        async with ADMIN_FILE_LOCK:
            admins = await _load_admins()

            # если уже есть админы — проверяем, что вызывающий является админом
            if admins and caller_id not in admins:
                return await message.reply("Доступ запрещён: вы не являетесь администратором.")

            if new_id in admins:
                return await message.reply(f"ID {new_id} уже присутствует в списке админов.")

            admins.append(new_id)
            await _save_admins(admins)

        return await message.reply(f"✅ Добавлен администратор: {new_id}")

    except Exception as e:
        try:
            await message.reply("❗ Произошла ошибка при добавлении администратора.")
        except Exception:
            pass
        raise


# Хендлер /remove_admin
@dp.message(F.text.startswith("/remove_admin"))
async def remove_admin_handler(message: Message):
    """
    Использование: /remove_admin <ID>
    Удаляет указанный ID из admins_data.json.
    Удалять может только существующий админ. Нельзя удалить последнего админа.
    """
    try:
        parts = (message.text or "").strip().split()
        if len(parts) != 2:
            return await message.reply("Использование: /remove_admin <ID>\nПример: /remove_admin 5143424934")

        try:
            rem_id = int(parts[1])
        except ValueError:
            return await message.reply("Неверный ID. Укажите числовой Telegram ID (например 5143424934).")

        caller_id = int(message.from_user.id)

        async with ADMIN_FILE_LOCK:
            admins = await _load_admins()

            if not admins:
                return await message.reply("Список администраторов пуст — операция невозможна.")

            if caller_id not in admins:
                return await message.reply("Доступ запрещён: вы не являетесь администратором.")

            if rem_id not in admins:
                return await message.reply(f"ID {rem_id} отсутствует в списке администраторов.")

            # Запрет удалять последнего администратора
            if len(admins) == 1 and admins[0] == rem_id:
                return await message.reply("Невозможно удалить последнего администратора.")

            admins = [a for a in admins if a != rem_id]
            await _save_admins(admins)

        return await message.reply(f"✅ Удалён администратор: {rem_id}")

    except Exception as e:
        try:
            await message.reply("❗ Произошла ошибка при удалении администратора.")
        except Exception:
            pass
        raise


@dp.message(Command("clear_deposits"))
async def cmd_clear_deposits(message: types.Message):
    """
    /clear_deposits confirm  - видалить усі депозити у користувачів
    Якщо викликано без 'confirm' — бот відповість інструкцією.
    """
    uid = message.from_user.id
    if uid not in SPECIAL_ADMINS:
        return

    args = message.text.split(" ")

    if len(args) < 2:
        return await message.reply(
            "⚠️ Внимание! Эта команда удалит все депозиты у всех игроков.\n"
            "Чтобы подтвердить, выполни команду с параметром `confirm`:\n\n"
            "/clear_deposits confirm"
        )

    # повідомлення про старт
    msg = await message.reply("⏳ Начинаю очистку депозитов...")

    # запускаємо блокуючу роботу в окремому потоці
    loop = asyncio.get_event_loop()
    try:
        modified_count, modified_uids = await loop.run_in_executor(None, _clear_all_deposits_sync, DB_PATH)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка при очистке: {e}")
        return

    # лог і відповідь
    try:
        append_log(f"ADMIN {message.from_user.full_name} ({uid}) cleared deposits for {modified_count} users.",
                   add_timestamp=True)
    except Exception:
        pass

    await msg.edit_text(f"✅ Очищены депозиты у {modified_count} пользователей.")


def _reset_coffres_rating_in_clans_sync(db_path: str):
    """
    Проходит по всем записям json_data, ищет ключ 'clans' (dict или list)
    и внутри каждого клана устанавливает coffres = 0 и rating = 0 (если есть).
    Возвращает (количество изменённых, список примеров ключей).
    """

    def process_clan_obj(clan_obj):
        """Обнуляет coffres и rating в словаре или в элементах списка."""
        changed = False
        if isinstance(clan_obj, dict):
            if "coffres" in clan_obj:
                clan_obj["coffres"] = 0
                changed = True
            if "rating" in clan_obj:
                clan_obj["rating"] = 0
                changed = True
            # рекурсивно проверяем вложенные объекты
            for k, v in list(clan_obj.items()):
                if isinstance(v, (dict, list)):
                    sub_changed = process_clan_obj(v)
                    changed = changed or sub_changed
        elif isinstance(clan_obj, list):
            for item in clan_obj:
                if isinstance(item, (dict, list)):
                    sub_changed = process_clan_obj(item)
                    changed = changed or sub_changed
        return changed

    modified_keys = []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("SELECT key, value FROM json_data")
        rows = cur.fetchall()

        for row in rows:
            key = row["key"]
            try:
                data = json.loads(row["value"]) if row["value"] else {}
            except Exception:
                continue

            modified = False

            # если есть верхнеуровневый ключ "clans"
            if isinstance(data, dict) and "clans" in data:
                clans_obj = data["clans"]
                if isinstance(clans_obj, (dict, list)):
                    if process_clan_obj(clans_obj):
                        modified = True

            # рекурсивно ищем все "clans" на любом уровне
            def recurse_find_and_process(obj):
                nonlocal modified
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k == "clans" and isinstance(v, (dict, list)):
                            if process_clan_obj(v):
                                modified = True
                        elif isinstance(v, (dict, list)):
                            recurse_find_and_process(v)
                elif isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, (dict, list)):
                            recurse_find_and_process(item)

            recurse_find_and_process(data)

            if modified:
                new_json = json.dumps(data, ensure_ascii=False)
                cur.execute("UPDATE json_data SET value = ? WHERE key = ?", (new_json, key))
                modified_keys.append(key)

        conn.commit()
        return len(modified_keys), modified_keys[:50]
    finally:
        conn.close()


@dp.message(Command("reset_clans_items"))
async def cmd_reset_clans_items(message: types.Message):
    """
    /reset_clans_items confirm  - обнуляет coffres и rating в кланах.
    Если без confirm — выдаст инструкцию.
    """
    uid = message.from_user.id
    if uid not in SPECIAL_ADMINS:
        return await message.reply("❌ У тебя нет прав для этой команды.")

    args = message.text.split(" ")
    if len(args) < 2:
        return await message.reply(
            "⚠️ Эта команда обнулит поля `coffres` и `rating` во всех объектах `clans`.\n"
            "Чтобы подтвердить, введи:\n\n"
            "/reset_clans_items confirm"
        )

    msg = await message.reply("⏳ Идёт обнуление coffres и rating в clans...")

    loop = asyncio.get_event_loop()
    try:
        modified_count, sample_keys = await loop.run_in_executor(None, _reset_coffres_rating_in_clans_sync, DB_PATH)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка во время операции: {e}")
        return

    try:
        append_log(
            f"ADMIN {message.from_user.full_name} ({uid}) reset coffres/rating в clans у {modified_count} записей.",
            add_timestamp=True)
    except Exception:
        pass

    reply_text = f"✅ Обработано записей: {modified_count}."
    if sample_keys:
        reply_text += f"\nПримеры изменённых ключей (до 50):\n" + ", ".join(sample_keys)
    await msg.edit_text(reply_text)


@dp.message(F.text.lower().startswith("/clear_orders"))
async def clear_exchange_orders_request(message: Message):
    try:
        user_id = int(message.from_user.id)
        if user_id not in SPECIAL_ADMINS:
            return await message.reply("❌ У вас нет прав для выполнения этой команды.")

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить очистку", callback_data=f"confirm_clear_orders:{user_id}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_clear_orders")
            ]
        ])
        await message.reply(
            "⚠️ Вы собираетесь полностью очистить таблицу `exchange_orders` в базе данных.\n"
            "Это действие НЕОБРАТИМО.\n\n"
            "Нажмите «Подтвердить очистку», чтобы продолжить, или «Отмена», чтобы отменить.",
            reply_markup=kb
        )
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 201)


# Callback: отмена
@dp.callback_query(F.data == "cancel_clear_orders")
async def cancel_clear_orders(callback: CallbackQuery):
    try:
        await callback.answer("Очистка отменена.", show_alert=False)
        try:
            await callback.message.edit_text("❎ Очистка отменена администратором.")
        except:
            pass
    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 202)


# Callback: подтверждение и выполнение очистки
@dp.callback_query(F.data.startswith("confirm_clear_orders"))
async def confirm_clear_orders(callback: CallbackQuery):
    try:
        parts = callback.data.split(":")
        if len(parts) != 2:
            return await callback.answer("Неверные данные.", show_alert=True)

        requester_id = int(parts[1])
        user_id = int(callback.from_user.id)

        # Проверка прав
        if user_id not in SPECIAL_ADMINS or user_id != requester_id:
            return await callback.answer("❌ У вас нет прав подтверждать эту операцию.", show_alert=True)

        await callback.answer("Выполняю очистку базы...", show_alert=False)

        deleted_count = 0
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()

            # Считаем количество записей
            cur.execute("SELECT COUNT(*) FROM exchange_orders")
            row = cur.fetchone()
            total_before = int(row[0]) if row else 0

            # Удаляем все записи
            cur.execute("DELETE FROM exchange_orders")
            conn.commit()

            # Опционально: освобождаем место
            cur.execute("VACUUM")
            conn.commit()

            deleted_count = total_before
        except sqlite3.Error as db_err:
            await handle_error(callback.from_user.username, db_err, callback.from_user.id, 203)
            try:
                await callback.message.edit_text(f"❗ Ошибка при работе с БД: {db_err}")
            except:
                pass
            return
        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass

        msg = f"✅ Таблица `exchange_orders` очищена. Удалено записей: {deleted_count}."
        try:
            await callback.message.edit_text(msg)
        except:
            await callback.answer(msg, show_alert=True)

        # Лог админам
        try:
            await send_log(
                f"[ADMIN] @{callback.from_user.username} ({user_id}) очистил таблицу exchange_orders. Удалено: {deleted_count}.")
        except:
            pass

    except Exception as e:
        await handle_error(callback.from_user.username, e, callback.from_user.id, 204)
        await callback.answer("⚠ Произошла ошибка при очистке.", show_alert=True)


DELAY_BETWEEN = 0.06  # пауза между отправками в секундах (чтобы снизить риск rate limit)


@dp.message(F.text.startswith("/rass"))
async def rass_handler(message: types.Message):
    sender_id = message.from_user.id
    # Проверка прав
    if sender_id not in SPECIAL_ADMINS:
        return await message.reply("❌ У вас нет прав для этой команды.")

    # Разбираем текст: /rass ТЕКСТ_РАССЫЛКИ
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.reply("ℹ️ Использование: /rass ТЕКСТ_РАССЫЛКИ")

    broadcast_text = parts[1].strip()

    # Собираем уникальные id пользователей из users и json_data
    recipients = set()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            # Таблица users (если есть)
            try:
                cur.execute("SELECT id FROM users")
                for row in cur.fetchall():
                    if row and row[0] is not None:
                        recipients.add(str(row[0]))
            except Exception:
                # Таблицы users может не быть — игнорируем
                pass

            # json_data keys (основное хранилище)
            try:
                cur.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
                cur.execute("SELECT key FROM json_data")
                for row in cur.fetchall():
                    if row and row[0] is not None:
                        recipients.add(str(row[0]))
            except Exception:
                pass
    except Exception as e:
        return await message.reply(f"⚠️ Ошибка при чтении БД: {e}")

    if not recipients:
        return await message.reply("⚠️ Не найдено пользователей для рассылки.")

    await message.reply(f"🚀 Рассылка запускается: {len(recipients)} получателей. Ждите отчёта...")

    sent = 0
    failed = []

    for uid_str in recipients:
        try:
            uid = int(uid_str)
        except Exception:
            failed.append((uid_str, "invalid id"))
            continue

        # опционально: не отправлять самому себе
        # if uid == sender_id:
        #     continue

        try:
            await bot.send_message(uid, broadcast_text)
            sent += 1
        except Exception as e:
            failed.append((uid_str, str(e)))
        await asyncio.sleep(DELAY_BETWEEN)

    # Формируем отчёт (первые 10 ошибок)
    report = f"✅ Рассылка завершена.\nОтправлено: {sent}\nНе удалось отправить: {len(failed)}"
    if failed:
        sample = failed[:10]
        report += "\n\nПервые ошибки:\n"
        for uid_err, err in sample:
            report += f"- {uid_err}: {err}\n"
        if len(failed) > 10:
            report += f"... ещё {len(failed) - 10} ошибок."

    await message.reply(report)


# @dp.message(lambda message: message.photo)
async def handle_photo(message: Message):
    """
    Користувач відправив фото.
    Telegram завжди надсилає кілька варіантів фото з різною роздільністю,
    тому беремо останнє (найбільше).
    """
    if message.from_user.id not in SPECIAL_ADMINS:
        return
    # Отримуємо список PhotoSize
    photos = message.photo

    # Беремо останнє (найкраща якість)
    largest_photo = photos[-1]

    # file_id потрібен саме від PhotoSize
    file_id = largest_photo.file_id

    # Відправляємо користувачу назад
    await message.answer(
        f"Спасибо за фото!\n"
        f"Его <b>file_id</b>:\n<code>{file_id}</code>",
        parse_mode="HTML"
    )


# @dp.message(F.entities)
async def get_custom_emoji_id(message: Message):
    # Перевірка, чи користувач — адмін
    if message.from_user.id not in SPECIAL_ADMINS:
        return  # ігноруємо неадмінів

    found = False
    for entity in message.entities:
        if entity.type == "custom_emoji":
            emoji_id = entity.custom_emoji_id
            await message.reply(
                f"🆔 Custom Emoji ID:\n<code>{emoji_id}</code>\n\n"
                f"Приклад використання:\n<code>{'{html.custom_emoji(\"' + emoji_id + '\")}'}</code>"
            )
            found = True
    if not found:
        await message.reply("❌ У цьому повідомленні немає кастомних емодзі.")


async def periodic_checkpoint(skip=False):
    while True:
        if skip:
            await asyncio.sleep(120)  # чекати 2 хв
        else:
            await asyncio.sleep(1)  # чекати с сек
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("PRAGMA wal_checkpoint(FULL);")
                conn.commit()
        except Exception as e:
            await send_log(f"❌ Error checkpoint: {e}")


def ensure_qs_file():
    """
    Переконаємось, що qs.json існує — якщо ні, створюємо з DEFAULT_UNIQUE_STATUSES.
    Файл має структуру: {"UNIQUE_STATUSES": [ ... ]}
    """
    if not QS_PATH.exists():
        data = {"UNIQUE_STATUSES": DEFAULT_UNIQUE_STATUSES}
        QS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_qs() -> dict:
    """
    Повертає вміст qs.json як dict. Якщо файл відсутній — створює дефолт і повертає його.
    """
    ensure_qs_file()
    try:
        text = QS_PATH.read_text(encoding="utf-8")
        return json.loads(text)
    except Exception:
        # Випадок пошкодженого файлу — перезаписуємо дефолтом
        data = {"UNIQUE_STATUSES": DEFAULT_UNIQUE_STATUSES}
        QS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return data


def save_qs(data: dict):
    """
    Зберігає dict в qs.json (перезапис).
    """
    QS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_unique_statuses() -> list:
    """
    Повертає список унікальних статусів (список рядків).
    Гарантує, що повернеться list навіть якщо в json немає ключа.
    """
    data = load_qs()
    vals = data.get("UNIQUE_STATUSES")
    if isinstance(vals, list):
        # Очищуємо від нестрокових елементів і приводимо до рядків
        return [str(x) for x in vals]
    # якщо ключа немає або неправильного типу — повертаємо дефолт
    return list(DEFAULT_UNIQUE_STATUSES)


# --- Адмін-команда для додавання нового статусу ---
@dp.message(F.text.startswith("/add_unique_status"))
async def cmd_add_status(message: types.Message):
    """
    Використання:
    /add_status Текст статуса

    Доступ тільки для SPECIAL_ADMINS.
    """
    sender = message.from_user.id
    if sender not in SPECIAL_ADMINS:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.reply("ℹ️ Использование: /add_status ТЕКСТ_СТАТУСУ")
    new_status = parts[1].strip()

    # Завантажуємо поточні
    data = load_qs()
    current = data.get("UNIQUE_STATUSES")
    if not isinstance(current, list):
        current = []

    # Перевірка на дубль (чутлива до точного рядка). Якщо треба — можна normalise.
    if new_status in current:
        return await message.reply(f"ℹ️ Статус уже существует: {new_status}")

    current.append(new_status)
    data["UNIQUE_STATUSES"] = current
    try:
        save_qs(data)
    except Exception as e:
        return await message.reply(f"⚠️ Ошибка: {e}")

    await message.reply(f"✅ Добавлен новый статус:\n{new_status}\nВсего: {len(current)}")


# --- Опціонально: команда для перегляду всіх статусів ---
@dp.message(F.text.startswith("/list_statuses"))
async def cmd_list_statuses(message: types.Message):
    statuses = get_unique_statuses()
    if not statuses:
        return await message.reply("ℹ️ Нету уникальных статусов.")
    lines = "\n".join(f"{i + 1}. {s}" for i, s in enumerate(statuses))
    await message.reply(f"📜 Уникальные статусы ({len(statuses)}):\n{lines}")


@dp.message(F.text.lower().in_(["/gtop", "идтоп", "/gtop@gmegadbot"]))
async def top_players(message: Message, bot: Bot):
    try:
        import time
        now = time.time()

        # якщо кеш ще актуальний -> віддаємо його
        if now - TOP_CACHE["time"] < CACHE_TTL and TOP_CACHE["text"]:
            await message.answer(TOP_CACHE["text"])
            return

        # перерахунок топу
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS json_data (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """)
            cursor.execute("SELECT key, value FROM json_data")
            rows = cursor.fetchall()

        players = []
        for key, value in rows:
            try:
                data = json.loads(value)
                coins = float(data.get("coins", 0))
            except Exception:
                coins = 0.0
            players.append((key, coins))

        players.sort(key=lambda x: x[1], reverse=True)
        top = players[:10]
        total_players = len(players)

        if total_players == 0:
            text = "<b>🏆 Топ игроков:</b>\nПока нет игроков."
        else:
            lines = ["<b>🏆 Топ игроков:</b>"]
            for i, (uid, coins) in enumerate(top, 1):
                try:
                    chat = await bot.get_chat(int(uid))
                    name = f"{await gsname(chat.first_name)} ({uid})" or await gsname(chat.username) or f"ID {uid}"
                except Exception:
                    name = f"ID {uid}"

                if i == 1:
                    i = "🥇"
                elif i == 2:
                    i = "🥈"
                elif i == 3:
                    i = "🥉"
                else:
                    i = "🏅"
                lines.append(f"{i} | {name} | {format_balance(coins)} mDrops")

            lines.append(f"\nВсего игроков: {total_players}")
            text = "\n".join(lines)

        # зберігаємо в кеш
        TOP_CACHE["time"] = now
        TOP_CACHE["text"] = text

        await message.answer(text)
    except Exception as e:
        await handle_error(message.from_user.username, e, message.from_user.id, 109)

@dp.message(Command("gdata"))
async def send_data_db(message: types.Message):
    # перевірка чи це адмін
    if message.from_user.id != 8493326566:
        return

    try:
        db_file = FSInputFile("data.db")
        await message.answer_document(db_file, caption="📦 Ось твій файл data.db")
    except FileNotFoundError:
        await message.answer("⚠️ Файл data.db не знайдено!")
    except Exception as e:
        await message.answer(f"❗ Помилка: {e}")

# -------------- LAUNCH -------------- #

async def main():
    # threading.Thread(target=webapp.run_flask, daemon=True).start()
    await send_log("Бот запущен")
    asyncio.create_task(periodic_checkpoint())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
