import copy
from math import e
import concurrent
from operator import ge, mul
from pyrogram import Client, filters, types, enums, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, InlineQuery,InlineQueryResultArticle,InputTextMessageContent
from pyrogram.errors import PeerIdInvalid, FloodWait, RPCError
import re
import random
import time
import asyncio
from datetime import datetime, timedelta
import sqlite3
from random import randint, uniform, choice, sample
import os, json
from requests import get
import threading
from functools import wraps
from asyncio import Semaphore
from decimal import Decimal, getcontext
from typing import Optional, Union

getcontext().prec = 50

# Конфигурация основного бота член
BOT_TOKEN = "8400887112:AAEoO0gCAc7V5uLExcXURsn7UChXN6itDRU"
API_ID = 17711477
API_HASH = "bcf7bc9e630e4699a4d1db1f474df0c9"
ADMINS = [5775987690, 8493326566]
API_OWNER = [5775987690, 8493326566]

USER_API_ID = "17711477"  
USER_API_HASH = "bcf7bc9e630e4699a4d1db1f474df0c9"

app = Client("osohso", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, parse_mode=enums.ParseMode.HTML, workers=1)

# покажем username бота
print(app.name)
active_mines_games = {}
DBB = "NewDbb (1).db"

db = sqlite3.connect(DBB)
cursor = db.cursor()

# Создание таблицы safe_games, если ещё 

db.commit()
db.close()

# 🎰 Лотерея со ставкой
LOTTERY_ICONS = ["🍒", "🍋", "🍉", "⭐", "💎", "🍀"]

farm_devices = {
    "cheap":  {"name": "💻 Дешевая ферма", "base_price": 1_000_000, "income": 50_000},
    "medium": {"name": "🎮 Средняя ферма", "base_price": 50_000_000, "income": 400_000},
    "high":   {"name": "⚡ Высокая ферма", "base_price": 300_000_000, "income": 3_000_000},
    "top":    {"name": "🏭 Топовая ферма", "base_price": 2_000_000_000, "income": 25_000_000},
}

#@app.on_message(~filters.user(list(ADMINS_NEW)))
async def handle_non_admin(client: Client, message: Message):
    text = (
        "В тестовом боте могут работать только админы!\n\n"
        "Разработчик: @ferzister (по вопросам чтобы стать админом писать "
        '<a href="https://t.me/ferzister">сюда [тык]</a>)'
    )
    await message.reply_text(text,
                             disable_web_page_preview=True,
                             parse_mode=ParseMode.HTML)

async def is_banned_user(user_id: int) -> bool:
    with open('banned_users.json', 'r') as f:
        banned_users = json.load(f)
    return int(user_id) in banned_users

async def banned_filter(_, __, message: Message) -> bool:
    return await is_banned_user(message.from_user.id)


@app.on_message(filters.group & filters.create(banned_filter))
async def handle_banned(client, message: Message):
    pass

@app.on_message(filters.create(banned_filter))
async def handle_banned(client, message: Message):
    text = (
        "🚫 Вы забанены!\n"
        "Разбан: 200 ⭐️"
    )
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Купить разбан", url="https://t.me/ferzister")]]
    )
    await message.reply(text, reply_markup=kb)



@app.on_message(filters.command("test"), group=16)
async def test(client, message):
    args = message.text.split()
    if len(args) == 1:
        await message.reply("test")
    else:
        txt = " ".join(args[1:])
        await message.reply(txt)

emojis = ["", "👍", "😀", "🤯", "😎", "👽", "👾", "🤖", "👻", "👑", "🎩", "🎰", "🎀", "🐍", "🦈"]
emoji_prices = [0, 1000, 25000, 100000, 500000, 2000000, 7500000, 25000000, 100000000, 1000000000, 1000000000000,
                10000000000000, 1e3984, 1e3984, 1e3984]

# ============ QUEST SYSTEM ============
import json
import os

QUESTS_FILE = 'quests.json'

default_quests = [
    {
        "quest_id": 1,
        "quest_name": "Игроман",
        "quest_description": "Выйграйте в игре 'рулетка' 10 раз ставкой больше 50.000 💰",
        "completed": False,
        "count": 0,
        "max_count": 10
    },
    {
        "quest_id": 2,
        "quest_name": "Лузер",
        "quest_description": "Проиграйте в игре 'Минер' 10 раз ставкой больше 10.000 💰",
        "completed": False,
        "count": 0,
        "max_count": 10
    },
    {
        "quest_id": 3,
        "quest_name": "Кладмен",
        "quest_description": "Создайте 5 депозитов с любой суммой",
        "completed": False,
        "count": 0,
        "max_count": 5
    },
    {
        "quest_id": 4,
        "quest_name": "Зеро",
        "quest_description": "Словите 'Зеро' в игре 'рулетка' 2 раза",
        "completed": False,
        "count": 0,
        "max_count": 2
    },
    {
        "quest_id": 5,
        "quest_name": "Ребус",
        "quest_description": "Ищите спряченные буквы в разных сообщениях от бота, и отгадайте слово!\nОтветы присылайте через: /answer «слово»",
        "completed": False,
        "count": 0,
        "max_count": 1
    }
]


def load_quests():
    try:
        with open(QUESTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_quests(data):
    with open(QUESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def init_user_quests(user_id: int):
    data = load_quests()
    str_id = str(user_id)
    if str_id not in data:
        data[str_id] = copy.deepcopy(default_quests)
        save_quests(data)

def get_user_quests(user_id: int):
    data = load_quests()
    return data.get(str(user_id), [])

def update_quest(user_id: int, quest_id: int):
    try:
        with open(QUESTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}

    user_id_str = str(user_id)
    if user_id_str not in data:
        return False

    quests = data[user_id_str]  # список квестов пользователя

    for quest in quests:
        if quest["quest_id"] == quest_id:
            if not quest.get("completed", False):
                quest["count"] = quest.get("count", 0) + 1
                if quest["count"] >= quest.get("max_count", 1):
                    quest["completed"] = True
            break
    else:
        return False

    with open(QUESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    return True


def all_quests_completed(user_id: int):
    quests = get_user_quests(user_id)
    return all(q.get("completed", False) for q in quests)

def format_quests(quests: list):
    text = "<b>📜 Ваши квесты:</b>\n\n"
    for q in quests:
        status = "✅" if q.get("completed", False) else "❌"
        text += f"• <b>{q.get('quest_name', '???')}</b> {status}\n"
        text += f"  └ {q.get('quest_description', '')}\n"
        text += f"  └ Прогресс: <code>{q.get('count', 0)} / {q.get('max_count', 0)}</code>\n\n"
    return text

# Асинхронный обработчик для Pyrogram
async def show_quests(client: Client, message: Message):
    user_id = message.from_user.id
    quests = get_user_quests(user_id)

    if not quests:
        init_user_quests(user_id)
        await message.reply("Квесты созданы, попробуйте снова.", quote=True)
        return

    if all_quests_completed(user_id):
        await message.reply(f"🔥 Поздравляем! Вы завершили все квесты!\n\nСсылка на 2 часть ивента: {encrypt_user_id(user_id)}", quote=True)
        return

    text = format_quests(quests)
    await message.reply(text, quote=True)

verifed_quests = {}  # {user_id: set(quest_ids)}

def check_verifed_quests(user_id: int, quest_id: int) -> bool:
    """Проверяет, было ли уже уведомление о выполнении квеста для пользователя."""
    return user_id in verifed_quests and quest_id in verifed_quests[user_id]

def verifed_quests_completed(user_id: int, quest_id: int) -> None:
    """Помечает квест как уведомленный о выполнении для пользователя."""
    if user_id not in verifed_quests:
        verifed_quests[user_id] = set()
    verifed_quests[user_id].add(quest_id)


if not os.path.exists('banned_users.json'):
    with open('banned_users.json', 'w') as f:
        json.dump([], f)

if not os.path.exists('quests.json'):
    with open('quests.json', 'w') as f:
        json.dump(default_quests, f)

api_semaphore = Semaphore(30)

def rate_limit(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with api_semaphore:
            return await func(*args, **kwargs)
    return wrapper

craft_recipes = {
    "stone_sediments": {"items": {"stone": 2}, "result": 1}, # осколки камня
    "stone_plate": {"items": {"stone": 5, "stone_sediments": 10}, "result": 2}, # каменная плита
    "iron_plate": {"items": {"iron_ingots": 5}, "result": 2}, # железный плита
    "iron_ingots": {"items": {"iron": 3}, "result": 1}, # железный слитки
    "gold_ingots": {"items": {"gold": 4}, "result": 1}, # золотые слитки
    "diamond_cores": {"items": {"diamond": 5}, "result": 1}, # алмазные ядры
    "emerald_dust": {"items": {"emerald": 3}, "result": 1}, # изумрудный пыль
    "iron_ingots_dense": {"items": {"iron": 10}, "result": 1}, # плотный железный слиток
    "gold_nuggets": {"items": {"gold": 1}, "result": 10}, # золотой сомородок
}

drill_motor_levels_price = [
    {"level": 1, "price": 1000, "items": {"stone_sediments": 5}},
    {"level": 2, "price": 2000, "items": {"stone_sediments": 10, "iron_ingots": 5}},
    {"level": 3, "price": 5000, "items": {"stone_sediments": 15, "iron_ingots": 10, "gold_bars": 2}},
]

drill_head_levels_price = [
    {"level": 1, "price": 1000, "items": {"stone_sediments": 5}},
    {"level": 2, "price": 2000, "items": {"stone_sediments": 10, "iron_ingots": 5}},
]

resource_tiers = {
    0: ["stone"],
    10: ["stone", "iron"],
    20: ["stone", "iron", "gold"],
    30: ["stone", "iron", "gold", "diamond"],
    40: ["stone", "iron", "gold", "diamond", "emerald"]
}

import sqlite3

def wipe_except_crash_ids(db_path):
    db = sqlite3.connect(db_path)
    cursor = db.cursor()

    tables_to_clear = [
        'promos', 'user_promos', 'inv', 'drill',
        'shop', 'inv_user', 'mines_games', 'bank_deposits',
        'MARKETPLACE', 'ref_system', 'treasure_games'
    ]
    for table in tables_to_clear:
        cursor.execute(f"DELETE FROM {table}")

    cursor.execute("""
        UPDATE crash SET 
            username = NULL,
            first_name = NULL,
            money = 10000,
            stone = 0,
            iron = 0,
            gold = 0,
            diamond = 0,
            emerald = 0,
            last_bonus = NULL,
            boor = 1,
            lose_count = 0,
            win_count = 0,
            total_games = 0,
            status = 0,
            registered_at = CURRENT_TIMESTAMP
    """)

    db.commit()
    db.close()



@app.on_message(filters.command('wipe'))
async def wipe(client, message):
    if message.from_user.id not in API_OWNER:
        return

    btn = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🧨 УВЕРЕН???", callback_data="wipe:1")]]
    )

    await message.reply("<b>УВЕРЕН???</b>", reply_markup=btn)
from random import randint
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

@app.on_callback_query(filters.regex("wipe:1"))
async def wipe_callback(client, callback_query):
    if callback_query.from_user.id not in API_OWNER:
        return

    buttons = [InlineKeyboardButton("❌", callback_data="wipe:stop") for _ in range(8)]

    yes_index = randint(0, 8)
    buttons.insert(yes_index, InlineKeyboardButton("✅", callback_data="wipe:yes"))

    rows = [buttons[i:i+3] for i in range(0, 9, 3)]

    await callback_query.message.edit_text(
        "ТОЧНО??", 
        reply_markup=InlineKeyboardMarkup(rows)
    )


@app.on_callback_query(filters.regex("wipe:yes"))
async def confirm_wipe(client, callback_query):
    if callback_query.from_user.id not in API_OWNER:
        return await callback_query.answer("❌ Это не твоя кнопка", show_alert=True)

    await callback_query.answer("✅ Wipe подтверждён", show_alert=True)

    wipe_except_crash_ids(DBB)

    await callback_query.message.edit_text("готово")

@app.on_callback_query(filters.regex("wipe:stop"))
async def cancel_wipe(client, callback_query):
    await callback_query.message.edit_text("отменено")


def create_farm_table():
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS farm (
        user_id INTEGER,
        device TEXT,
        count INTEGER DEFAULT 0,
        last_claim TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, device)
    )
    """)
    db.commit()
    db.close()



def create_database():
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crash (
            id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            money REAL,
            stone INTEGER DEFAULT 0,
            iron INTEGER DEFAULT 0,
            gold INTEGER DEFAULT 0,
            diamond INTEGER DEFAULT 0,
            emerald INTEGER DEFAULT 0,
            last_bonus DATETIME,
            boor INTEGER DEFAULT 1,
            lose_count INTEGER DEFAULT 0,
            win_count INTEGER DEFAULT 0,
            total_games INTEGER DEFAULT 0,
            status INTEGER DEFAULT 0,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            hidden INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promos (
            name TEXT PRIMARY KEY,
            money REAL,
            is_active INTEGER DEFAULT 1,
            activations INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_promos (
            user_id INTEGER,
            promo_name TEXT,
            FOREIGN KEY (user_id) REFERENCES crash(id),
            FOREIGN KEY (promo_name) REFERENCES promos(name),
            PRIMARY KEY (user_id, promo_name)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inv (
            id INTEGER PRIMARY KEY,
            details INTEGER DEFAULT 0,
            stone_sediments INTEGER DEFAULT 0,
            iron_ingots INTEGER DEFAULT 0,
            gold_bars INTEGER DEFAULT 0,
            diamond_cores INTEGER DEFAULT 0,
            emerald_dust INTEGER DEFAULT 0,
            iron_ingots_dense INTEGER DEFAULT 0,
            gold_nuggets INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS drill (
            user_id INTEGER PRIMARY KEY,
            motor_lvl INTEGER DEFAULT 1,
            drill_head_lvl INTEGER DEFAULT 1,
            frame INTEGER DEFAULT 1,
            power_source INTEGER DEFAULT 1,
            handle INTEGER DEFAULT 0,
            cooling INTEGER DEFAULT 0,
            gearbox INTEGER DEFAULT 0,
            oil INTEGER DEFAULT 0,
            energy INTEGER DEFAULT 10,
            max_energy INTEGER DEFAULT 10,
            max_oil_engine INTEGER DEFAULT 10,
            max_cooling_engine INTEGER DEFAULT 10,
            heal_drill_engine INTEGER DEFAULT 100
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shop (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price INTEGER,
            quantity INTEGER,  -- quantity = -1: бесконечное, 0: закончился, >0: ограниченное
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inv_user (
            user_id INTEGER,
            item_name TEXT,
            quantity INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES crash(id),
            FOREIGN KEY (item_name) REFERENCES shop(name),
            PRIMARY KEY (user_id, item_name)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mines_games (
            game_id TEXT PRIMARY KEY,
            user_id INTEGER,
            game_data TEXT,
            FOREIGN KEY (user_id) REFERENCES crash(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bank_deposits (
            deposit_id TEXT PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            current_amount INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES crash(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MARKETPLACE (
                   seller_id INTEGER,
                    item_name TEXT,
                    price INTEGER,
                    quantity INTEGER DEFAULT 1)""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ref_system (
        user_id INTEGER PRIMARY KEY,
        invited_by INTEGER,
        ref_count INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS treasure_games (
        id TEXT PRIMARY KEY,              -- Уникальный ID игры
        user_id INTEGER,
        bet INTEGER,
        treasures INTEGER,
        found INTEGER DEFAULT 0,
        attempts_left INTEGER,
        board TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    db.commit()
    db.close()

create_database()
create_farm_table()

async def set_lose_monet(user_id, amount):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET lose_count = lose_count + ? WHERE id = ?", (float(amount), user_id))
    db.commit()
    db.close()

async def set_win_monet(user_id, amount):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET win_count = win_count + ? WHERE id = ?", (float(amount), user_id))
    db.commit()
    db.close()

async def get_inventory_data(user_id):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT * FROM inv WHERE id = ?", (user_id,))
    result = cursor.fetchone()
    db.close()
    if result:
        return {
            "id": result[0],
            "details": result[1],
            "stone_sediments": result[2],
            "iron_ingots": result[3],
            "gold_bars": result[4],
            "diamond_cores": result[5],
            "emerald_dust": result[6],
            "iron_ingots_dense": result[7],
            "gold_nuggets": result[8]
        }
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("INSERT INTO inv (id) VALUES (?)", (user_id,))
    db.commit()
    db.close()
    return await get_inventory_data(user_id)

async def get_user_drill_data(user_id):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT * FROM drill WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    db.close()
    if result:
        return {
            "user_id": result[0],
            "motor_lvl": result[1],
            "drill_head_lvl": result[2],
            "frame": result[3],
            "power_source": result[4],
            "handle": result[5],
            "cooling": result[6],
            "gearbox": result[7],
            "oil": result[8],
            "energy": result[9],
            "max_energy": result[10],
            "max_oil_engine": result[11],
            "max_cooling_engine": result[12],
            "heal_drill_engine": result[13]
        }
    return None  # Если дрели нет, возвращаем None

def can_craft(user_id, item_name):
    user_data = get_user_data(user_id)
    print(f"can_craft: user_id={user_id}, item_name={item_name}")
    print(f"user_data={user_data}")
    
    if not user_data or item_name not in craft_recipes:
        print(f"Failed: user_data is None or item_name not in craft_recipes")
        return False
    
    recipe = craft_recipes[item_name]
    print(f"recipe={recipe}")
    
    for resource, required_amount in recipe["items"].items():
        available = user_data.get(resource, 0)
        print(f"Checking {resource}: available={available}, required={required_amount}")
        if available < required_amount:
            print(f"Not enough {resource}")
            return False
    return True

async def craft_item(user_id, item_name):
    recipe = craft_recipes[item_name]
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    
    # Уменьшаем ресурсы
    for resource, amount in recipe["items"].items():
        cursor.execute(f"UPDATE crash SET {resource} = {resource} - ? WHERE id = ?", (amount, user_id))
    
    # Добавляем результат
    cursor.execute(f"UPDATE inv SET {item_name} = {item_name} + ? WHERE id = ?", (recipe["result"], user_id))
    
    db.commit()
    db.close()

async def upgrade_drill_component(user_id, component, levels_price):
    drill_data = await get_user_drill_data(user_id)
    inv = await get_inventory_data(user_id)
    user_data = get_user_data(user_id)
    current_level = drill_data[component]
    next_level_data = next((lvl for lvl in levels_price if lvl["level"] == current_level + 1), None)
    
    if not next_level_data:
        return "Максимальный уровень достигнут."
    if user_data["money"] < next_level_data["price"]:
        return "Недостаточно монет."
    for item, amount in next_level_data["items"].items():
        if inv.get(item, 0) < amount:
            return f"Недостаточно {item}."
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute(f"UPDATE drill SET {component} = ? WHERE user_id = ?", (current_level + 1, user_id))
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(next_level_data["price"]), user_id))
    for item, amount in next_level_data["items"].items():
        cursor.execute(f"UPDATE inv SET {item} = {item} - ? WHERE id = ?", (amount, user_id))
    db.commit()
    db.close()
    return f"Компонент {component} улучшен до уровня {current_level + 1}!"

async def refill_oil(user_id):
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    cost = 1000
    if user_data["money"] < cost:
        return "Недостаточно монет."
    if drill_data["oil"] >= drill_data["max_oil_engine"]:
        return "Масло уже на максимуме."
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE drill SET oil = ? WHERE user_id = ?", (drill_data["max_oil_engine"], user_id))
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(cost), user_id))
    db.commit()
    db.close()
    return "Масло пополнено!"

async def refill_cooling(user_id):
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    cost = 1000
    if user_data["money"] < cost:
        return "Недостаточно монет."
    if drill_data["cooling"] >= drill_data["max_cooling_engine"]:
        return "Охлаждение уже на максимуме."
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE drill SET cooling = ? WHERE user_id = ?", (drill_data["max_cooling_engine"], user_id))
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(cost), user_id))
    db.commit()
    db.close()
    return "Охлаждение восстановлено!"

async def repair_drill(user_id):
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    inv = await get_inventory_data(user_id)
    cost = 2000
    required_items = {"stone_sediments": 5}
    
    if user_data["money"] < cost:
        return "Недостаточно монет."
    if drill_data["heal_drill_engine"] >= 100:
        return "Бур уже в отличном состоянии."
    for item, amount in required_items.items():
        if inv.get(item, 0) < amount:
            return f"Недостаточно {item}."
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE drill SET heal_drill_engine = 100 WHERE user_id = ?", (user_id,))
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(cost), user_id))
    for item, amount in required_items.items():
        cursor.execute(f"UPDATE inv SET {item} = {item} - ? WHERE id = ?", (amount, user_id))
    db.commit()
    db.close()
    return "Бур починен!"

async def refill_energy(message, user_id):
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    drill_lvl = drill_data["drill_head_lvl"]
    cost = 10000 * (drill_lvl + 1)
    text = "Пополнение энергий стоит: " + str(cost) + " монет."
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Пополнить", callback_data=f"on_refill_energy_{user_id}")]
    ])
    await message.edit(text, reply_markup=markup)

@app.on_message(filters.command("savedb"))
async def save_db_command(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        await message.reply_text("❌ У вас нет доступа к этой команде.")
        return

    try:
        # Просто открываем соединение с базой и вызываем commit(), чтобы гарантировать сохранение
        db = sqlite3.connect(DBB)
        db.commit()
        db.close()
        await message.reply_text("✅ База данных сохранена успешно.")
    except Exception as e:
        await message.reply_text(f"❌ Ошибка при сохранении базы данных: {e}")

@app.on_callback_query(filters.regex(r"on_refill_energy_"))
@rate_limit
async def on_refill_energy_callback(client, callback_query):
    user_id = int(callback_query.data.split("_")[-1])
    resultt = await on_refill_energy(user_id)
    await callback_query.message.edit_text(resultt)

async def on_refill_energy(user_id):
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    drill_lvl = drill_data["drill_head_lvl"]
    cost = 10000 * (drill_lvl + 1)
    
    if user_data["money"] < cost:
        return "Недостаточно монет."
    if drill_data["energy"] >= drill_data["max_energy"]:
        return "Энергия уже на максимуме."
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE drill SET energy = ? WHERE user_id = ?", (drill_data["max_energy"], user_id))
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(cost), user_id))
    db.commit()
    db.close()
    return "Энергия пополнена!"
    

import requests
# @app.on_message(filters.command("бот", prefixes=""), group=8)
@rate_limit
async def generate_text(client, message):
    user_input = message.text.split(maxsplit=1)
    if len(user_input) < 2:
        return await message.reply("Укажите текст для генерации.")
    prompt = user_input[1]
    msg = await message.reply("Я думаю...")
    try:
        promptt =f"ответь не здароваясь, {prompt}"
        response = requests.get(f"https://text.pollinations.ai/{promptt}")
        response.raise_for_status()
        generated_text = response.text
    except Exception as e:
        generated_text = f"Ошибка: {e}"

    await msg.edit(
        f"{generated_text}\n\n**За авторсвом: @{message.from_user.username}**\n**Запрос:** \n__{prompt}__",
        parse_mode=enums.ParseMode.MARKDOWN
    )



# @app.on_message(filters.command("drill"))
@rate_limit
async def drill_command_handler(client, message):
    args = message.text.split()
    if len(args) == 1:
        pass
    else:
        return
    await drill_command(client, message)

@rate_limit
async def drill_command(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    if await is_banned_user(user_id):
        await message.reply_text("Вы забанены.")
        return
    
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    if not drill_data:
        await message.reply("У вас нет дрели. Купите ее с помощью /buy_drill за 5000 монет!")
        return
    
    calc_drill_exp = sum(drill_data[k] * 1.5 for k in ["motor_lvl", "drill_head_lvl", "frame", "power_source", "handle", "cooling", "gearbox", "oil"])
    calc_drill_lvl = int(calc_drill_exp // 10)
    
    text = f"🎲 Ваша дрель:\n💰 Баланс: {format_balance(user_data['money'])}\nУровень: {calc_drill_lvl}\n"
    text += f"🔋 Мотор: {drill_data['motor_lvl']}\n🔧 Сверло: {drill_data['drill_head_lvl']}\n🎚 Каркас: {drill_data['frame']}\n"
    text += f"🔋 Питание: {drill_data['power_source']}\n🔧 Держатель: {drill_data['handle']}\n"
    text += f"❄️ Охлаждение: {drill_data['cooling']}/{drill_data['max_cooling_engine']}\n"
    text += f"🛢 Масло: {drill_data['oil']}/{drill_data['max_oil_engine']}\n"
    text += f"🔧 Состояние: {drill_data['heal_drill_engine']}/100"
    
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Улучшить", callback_data=f"drill_upgrade_{user_id}"),
         InlineKeyboardButton("Добавить запчасть", callback_data=f"drill_parts_{user_id}")],
        [InlineKeyboardButton("Управление", callback_data=f"drill_manage_{user_id}")]
    ])
    await message.reply(text, reply_markup=markup)

async def inventory_command_handler(client, message):
    args = message.text.split()
    if len(args) == 1:
        pass
    else:
        return
    await inventory_command(client, message)

@rate_limit
async def inventory_command(client, message):
    user_id = message.from_user.id
    
    # Проверка регистрации и бана
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы. Используйте /ss или /рег.")
        return
    if await is_banned_user(user_id):
        await message.reply("Вы забанены.")
        return

    # Получение данных
    user_data = get_user_data(user_id)
    inv = await get_inventory_data(user_id)
    
    if not user_data or not inv:
        await message.reply("Ошибка загрузки инвентаря. Попробуйте позже.")
        return

    # Формирование текста инвентаря
    text = "🎒 Ваш инвентарь:\n"
    resources = [
        ("🪨 Камень", user_data['stone']),
        ("🔩 Железо", user_data['iron']),
        ("🥇 Золото", user_data['gold']),
        ("💎 Алмаз", user_data['diamond']),
        ("💚 Изумруд", user_data['emerald']),
        ("✨ Осколки камня", inv['stone_sediments']),
        ("🔧 Железные слитки", inv['iron_ingots']),
        ("🥇 Золотые слитки", inv['gold_bars']),
        ("💎 Алмазные ядра", inv['diamond_cores']),
        ("💚 Изумрудная пыль", inv['emerald_dust']),
        ("🔩 Плотные слитки", inv['iron_ingots_dense']),
        ("🥇 Золотые кусочки", inv['gold_nuggets']),
    ]
    for name, amount in resources:
        text += f"{name}: {amount}\n"

    # Кнопка крафта
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Крафт", callback_data=f"craft_menu_{user_id}")]
    ])
    await message.reply(text, reply_markup=markup)

@app.on_callback_query(filters.regex(r"craft_menu_"))
@rate_limit
async def craft_menu(client, callback_query):
    parts = callback_query.data.split("_")
    if len(parts) != 3 or parts[1] != "menu" or not parts[2].isdigit():
        await callback_query.message.edit_text("Ошибка: неверный запрос меню крафта.")
        return
    
    user_id = int(parts[2])
    bot_info = await client.get_me()
    if callback_query.message.chat.id != user_id:
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("В ЛС бота", url=f"t.me/{bot_info.username}")]
        ])
        await callback_query.message.edit_text("Крафт доступен только в ЛС бота.", reply_markup=markup)
        return

    text = "📜 Меню крафта:\n"
    buttons = []
    
    for item_name, recipe in craft_recipes.items():
        if can_craft(user_id, item_name):
            callback_data = f"craft_item_{item_name}_{user_id}"
            print(f"Generated callback_data: {callback_data}")  # Отладка
            buttons.append([InlineKeyboardButton(f"Скрафтить {item_name}", callback_data=callback_data)])
        else:
            required = ", ".join(f"{k}: {v}" for k, v in recipe["items"].items())
            text += f"{item_name} (Требуется: {required}) - Недоступно\n"
    
    if not buttons:
        text += "Нет доступных предметов для крафта.\nДобывайте ресурсы с помощью /mine!"
    
    markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(text, reply_markup=markup)

@app.on_callback_query(filters.regex(r"craft_item_"))
@rate_limit
async def craft_item_callback(client, callback_query):
    """Обработка крафта предмета через callback."""
    parts = callback_query.data.split("_")
    if len(parts) < 4 or parts[1] != "item" or not parts[-1].isdigit():
        await callback_query.message.edit_text(f"Ошибка: неверный запрос крафта ({callback_query.data}).")
        return
    
    item_name = "_".join(parts[2:-1])
    user_id = int(parts[-1])
    
    if item_name not in craft_recipes:
        await callback_query.message.edit_text(f"Ошибка: неизвестный предмет ({item_name}).")
        return
    
    if can_craft(user_id, item_name):
        await craft_item(user_id, item_name)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=f"craft_menu_{user_id}")]])
        await callback_query.message.edit_text(f"🎉 Вы скрафтили {item_name}!", reply_markup=markup)
    else:
        recipe = craft_recipes[item_name]
        required = ", ".join(f"{k}: {v}" for k, v in recipe["items"].items())
        await callback_query.message.edit_text(f"Недостаточно ресурсов для {item_name}. Требуется: {required}")

@app.on_callback_query(filters.regex(r"craft_item_"))
@rate_limit
async def craft_item_callback(client, callback_query):
    print(f"Received callback_data: {callback_query.data}")  # Отладка
    parts = callback_query.data.split("_")
    
    # Проверка формата: craft_item_itemname_userid
    if len(parts) < 4 or parts[1] != "item" or not parts[-1].isdigit():
        await callback_query.message.edit_text(f"Ошибка: неверный запрос крафта ({callback_query.data}).")
        return
    
    # Извлечение item_name и user_id
    item_name = "_".join(parts[2:-1])  # Собираем имя предмета, если оно содержит "_"
    user_id = int(parts[-1])
    
    # Проверка, что предмет существует
    if item_name not in craft_recipes:
        await callback_query.message.edit_text(f"Ошибка: неизвестный предмет ({item_name}).")
        return
    
    # Проверка возможности крафта
    if can_craft(user_id, item_name):
        await craft_item(user_id, item_name)
        await callback_query.message.edit_text(f"🎉 Вы скрафтили {item_name}!")
    else:
        recipe = craft_recipes[item_name]
        required = ", ".join(f"{k}: {v}" for k, v in recipe["items"].items())
        await callback_query.message.edit_text(f"Недостаточно ресурсов для {item_name}. Требуется: {required}")

@app.on_callback_query(filters.regex(r"drill_upgrade_"))
@rate_limit
async def drill_upgrade_menu(client, callback_query):
    user_id = int(callback_query.data.split("_")[-1])
    if user_id != callback_query.from_user.id:  # Исправлено на from_user.id
        await callback_query.answer("Это не твоя кнопка!", show_alert=True)
        return
    
    text = "Выберите компонент для улучшения:\n"
    buttons = [
        [InlineKeyboardButton("Мотор", callback_data=f"upgrade_motor_{user_id}")],
        [InlineKeyboardButton("Сверло", callback_data=f"upgrade_drill_head_{user_id}")],
        [InlineKeyboardButton("Каркас", callback_data=f"upgrade_frame_{user_id}")],
        [InlineKeyboardButton("Питание", callback_data=f"upgrade_power_source_{user_id}")],
        [InlineKeyboardButton("Держатель", callback_data=f"upgrade_handle_{user_id}")]
    ]
    markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(text, reply_markup=markup)

@app.on_callback_query(filters.regex(r"upgrade_"))
@rate_limit
async def upgrade_component(client, callback_query):
    parts = callback_query.data.split("_")
    component = {"motor": "motor_lvl", "drill_head": "drill_head_lvl", "frame": "frame", "power_source": "power_source", "handle": "handle"}[parts[1]]
    user_id = int(parts[2])
    
    levels_price = {
        "motor_lvl": drill_motor_levels_price,
        "drill_head_lvl": drill_head_levels_price,
    }.get(component, drill_motor_levels_price)
    
    result = await upgrade_drill_component(user_id, component, levels_price)
    await callback_query.message.edit_text(result)

@app.on_callback_query(filters.regex(r"drill_parts_"))
async def drill_parts_menu(client, callback_query):
    """Меню добавления запчастей к дрели (заглушка)."""
    user_id = int(callback_query.data.split("_")[-1])
    if user_id != callback_query.from_user.id:
        await callback_query.answer("Это не твоя кнопка!", show_alert=True)
        return
    # TODO: Реализовать добавление запчастей
    await callback_query.message.edit_text("Добавление запчастей пока не реализовано.")
    
@app.on_callback_query(filters.regex(r"drill_manage_"))
@rate_limit
async def drill_manage_menu(client, callback_query):
    # Парсинг callback_data
    parts = callback_query.data.split("_")
    if len(parts) < 3 or not parts[-1].isdigit():
        await callback_query.answer("Ошибка: неверный формат запроса.", show_alert=True)
        return
    
    user_id = int(parts[-1])
    
    # Проверка, что кнопку нажимает владелец
    if user_id != callback_query.from_user.id:
        await callback_query.answer("Это не твоя кнопка!", show_alert=True)
        return
    
    # Формирование меню
    text = "⚙️ Управление буром:\n"
    buttons = [
        [InlineKeyboardButton("Пополнить масло", callback_data=f"manage_oil_{user_id}")],
        [InlineKeyboardButton("Охладить", callback_data=f"manage_cooling_{user_id}")],
        [InlineKeyboardButton("Пополнит Энергию", callback_data=f"manage_energy_{user_id}")],
        [InlineKeyboardButton("Починить бур", callback_data=f"manage_repair_{user_id}")]]
    markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(text, reply_markup=markup)

@app.on_callback_query(filters.regex(r"manage_"))
@rate_limit
async def manage_drill(client, callback_query):
    parts = callback_query.data.split("_")
    action = parts[1]
    user_id = int(parts[2])
    if user_id != callback_query.from_user.id:  # Исправлено на from_user.id
        await callback_query.answer("Это не твоя кнопка!", show_alert=True) 
        return
    
    if action == "oil":
        result = await refill_oil(user_id)
    elif action == "cooling":
        result = await refill_cooling(user_id)
    elif action == "repair":
        result = await repair_drill(user_id)
    elif action == "energy":
        result = await refill_energy(callback_query.message, user_id)
    else:
        result = "Неизвестное действие."
    
    await callback_query.message.edit_text(result)

@rate_limit
async def mine_command_1(client, message):
    args = message.text.split()
    if len(args) == 1:
        pass
    else:
        return
    await mine_command(client, message)

@rate_limit
async def mine_command(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    
    if message.chat.type != enums.ChatType.PRIVATE:
        data_bot = await client.get_me()
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("В ЛС бота", url=f"t.me/{data_bot.username}")]
        ])
        await message.reply("Команда доступна только в ЛС бота.", reply_markup=markup)
        return
    
    if await is_banned_user(user_id):
        await message.reply_text("Вы забанены.")
        return
    
    
    drill_data = await get_user_drill_data(user_id)
    if not drill_data:
        await message.reply("У вас нет дрели. Купите ее с помощью /buy_drill за 5000 монет!")
        return
    
    if drill_data["energy"] < 1:
        await message.reply("Недостаточно энергии для добычи.")
        return
    if drill_data["oil"] < 1:
        await message.reply("Недостаточно масла для работы бура.")
        return
    if drill_data["heal_drill_engine"] < 5:
        await message.reply("Бур сломан, почините его перед добычей.")
        return
    
    calc_drill_exp = sum(drill_data[k] * 1.5 for k in ["motor_lvl", "drill_head_lvl", "frame", "power_source", "handle", "cooling", "gearbox", "oil"])
    calc_drill_lvl = int(calc_drill_exp // 10)
    bonus_percent = calc_drill_lvl * 0.01
    
    available_resources = next(tier for lvl, tier in sorted(resource_tiers.items(), reverse=True) if calc_drill_lvl >= lvl)
    resource = choice(available_resources)
    base_amount = randint(1, 6)
    amount = int(base_amount * (1.2 * bonus_percent + 1))
    
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("""
        UPDATE drill 
        SET energy = energy - 1, 
            oil = oil - 1, 
            heal_drill_engine = heal_drill_engine - 5 
        WHERE user_id = ?
    """, (user_id,))
    cursor.execute(f"UPDATE crash SET {resource} = {resource} + ? WHERE id = ?", (amount, user_id))
    db.commit()
    db.close()
    
    await message.reply(f"⛏ Вы добыли {amount} {resource}!\nЭнергия: {drill_data['energy']-1}/{drill_data['max_energy']}\nМасло: {drill_data['oil']-1}/{drill_data['max_oil_engine']}\nСостояние: {drill_data['heal_drill_engine']-5}/100")

async def check_user(user_id):
    user_data = get_user_data(user_id)
    return user_data is not None and not await is_banned_user(user_id)

def register_user(user_id, username, first_name):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO crash (id, username, money, first_name) VALUES (?, ?, ?, ?)", (user_id, username, 10000, first_name))
        cursor.execute("INSERT INTO drill (user_id) VALUES (?)", (user_id,))
        db.commit()
        db.close()
        return True
    except sqlite3.IntegrityError:
        db.close()
        return False

# ================== ФАРМИНГ ТЕХНИКОЙ ==================


def get_user_farm(user_id):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT device, count, last_claim FROM farm WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()
    db.close()
    farm = {k: {"count": 0, "last_claim": datetime.now()} for k in farm_devices.keys()}
    for device, count, last_claim in rows:
        farm[device] = {"count": count, "last_claim": datetime.fromisoformat(last_claim)}
    return farm

def update_user_farm(user_id, device, new_count):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("""
    INSERT INTO farm (user_id, device, count) 
    VALUES (?, ?, ?)
    ON CONFLICT(user_id, device) DO UPDATE SET count=?
    """, (user_id, device, new_count, new_count))
    db.commit()
    db.close()

def claim_income(user_id):
    farm = get_user_farm(user_id)
    now = datetime.now()
    total_income = 0
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    for device, data in farm.items():
        count = data["count"]
        if count > 0:
            last_claim = data["last_claim"]
            hours = (now - last_claim).total_seconds() / 3600
            income = int(hours * farm_devices[device]["income"] * count)
            total_income += income
            cursor.execute("UPDATE farm SET last_claim = ? WHERE user_id = ? AND device = ?",
                           (now.isoformat(), user_id, device))
    if total_income > 0:
        cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(total_income), user_id))
    db.commit()
    db.close()
    return total_income

def get_device_price(user_id, device):
    farm = get_user_farm(user_id)
    count = farm[device]["count"]
    return int(farm_devices[device]["base_price"] * (1.03 ** count))

import math


async def set_user_lose_count(user_id, count):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET lose_count = ? WHERE id = ?", (count, user_id))
    db.commit()
    db.close()

async def set_user_win_count(user_id, count):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET win_count = ? WHERE id = ?", (count, user_id))
    db.commit()
    db.close()

def get_user_data(user_id):
    """Получение данных пользователя из базы."""
    try:
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            cursor.execute(
                "SELECT username, id, money, lose_count, win_count, stone, iron, gold, diamond, emerald, last_bonus, status FROM crash WHERE id = ?",
                (user_id,))
            result = cursor.fetchone()
            if result:
                return {
                    "username": result[0],
                    "id": result[1],
                    "money": result[2],
                    "lose_count": result[3],
                    "win_count": result[4],
                    "stone": result[5],
                    "iron": result[6],
                    "gold": result[7],
                    "diamond": result[8],
                    "emerald": result[9],
                    "last_bonus": result[10],
                    "status": result[11],
                }
        return None
    except sqlite3.Error as e:
        print(f"Database error in get_user_data: {e}")
        return None

async def get_user_inventory(user_id):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT item_name, quantity FROM inv_user WHERE user_id = ?", (user_id,))
    result = cursor.fetchall()
    db.close()
    return result

@app.on_message(filters.command("uinv"), group=1)
async def user_inventory(client, message):
    user_id = message.from_user.id
    print(f"Пытается использовать команду /uinv: {user_id}")
    if user_id not in API_OWNER:
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Используйте: /uinv (id)")
        return

    user_id = args[1]
    inventory = await get_user_inventory(user_id)
    if not inventory:
        await message.reply("Инвентарь пользователя пуст.")
        return
    
    inventory_str = "\n".join([f"{item[0]}: {item[1]}" for item in inventory])
    await message.reply(f"Инвентарь пользователя {user_id}\n{inventory_str}")    

@app.on_message(filters.command("user"))
async def show_user(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Используйте: /user (id)")
        return
    
    user_id = args[1]
    user_data = get_user_data(user_id)
    await message.reply(f"Юзер: {user_data['username']}\nID: {user_id}\nБаланс: {user_data['money']}\nВыигрышныш: {user_data['win_count']}\nПроигрышныш: {user_data['lose_count']}\nПоследний бонус: {user_data['last_bonus']}\nСтатус: {user_data['status']}")

top1, game1, ti, spammers = ["MrFezix", 3], 0, time.time(), []


import re

def _to_decimal_safe(x: Union[None, int, float, str, Decimal]) -> Optional[Decimal]:
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return None

def parse_bet_amount(arg: str, user_money: Optional[Union[int, float, str, Decimal]] = None) -> int:
    if arg is None:
        return -1

    s = str(arg).strip().lower()
    s = s.replace(" ", "").replace("_", "")

    # перевірка на "все" варіанти
    if s in ("все", "всё", "all"):
        um = _to_decimal_safe(user_money)
        if um is None:
            return -1
        try:
            return int(um)
        except Exception:
            return -1

    m = re.fullmatch(r'([0-9]+(?:[.,][0-9]{1,2})?)([kк]*)', s)
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
        return int(result)  # усікання дробової частини
    except Exception:
        return -1
    
def spin_roulette():
    return randint(0, 36)


def get_color(number):
    if number == 0:
        return "зеленый"
    red_numbers = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]
    return "красный" if number in red_numbers else "черный"


def is_even(number):
    return number % 2 == 0 and number != 0


@rate_limit
async def requires_registration(func):
    async def wrapper(client, message):
        print(f"[DEBUG] Декоратор requires_registration для {func.__name__}, команда: {message.text}")
        if not check_user(message.from_user.id):
            await message.reply_text("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
            return
        return await func(client, message)

    return wrapper


async def get_all_users_data():
    conn = sqlite3.connect(DBB)
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, first_name, money, status FROM crash")
    users = cursor.fetchall()
    conn.close()
    return users

async def set_user_drill_data(user_id, type, value):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE drill SET " + type + " =? WHERE user_id = ?", (value, user_id))
    db.commit()
    db.close()

async def get_all_users_ids():
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT id FROM crash")
    result = cursor.fetchall()
    db.close()
    return [user_id[0] for user_id in result]

async def update_user_data(user_id, username, first_name):
    try:
        with sqlite3.connect(DBB) as db:  # Используем DBB вместо "users.db"
            cursor = db.cursor()
            cursor.execute("SELECT id FROM crash WHERE id = ?", (user_id,))
            exists = cursor.fetchone()

            username = username or f"User_{user_id}"  # Обработка None
            first_name = first_name or "NoName"

            if exists:
                cursor.execute("UPDATE crash SET username = ?, first_name = ? WHERE id = ?", 
                              (username, first_name, user_id))
            else:
                cursor.execute("INSERT INTO crash (id, username, money, first_name) VALUES (?, ?, ?, ?)", 
                              (user_id, username, 10000, first_name))
                cursor.execute("INSERT INTO drill (user_id) VALUES (?)", (user_id,))
            
            db.commit()
    except sqlite3.Error as e:
        print(f"Ошибка в update_user_data: {e}")
        raise  # Поднимаем исключение для отладки

async def start_command(client, message):
    if message.chat.type == "private":
        if not check_user(message.from_user.id):
            register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
            await message.reply_text(
                f"Привет! Добро пожаловать в бот!\nТы успешно зарегистрирован, твой баланс 1000 монет.\nИспользуй /hb или список чтобы узнать список команд.")
        else:
            await message.reply_text(f"Привет! Рад снова тебя видеть!\nИспользуй /hb или список чтобы узнать список команд.")
    else:
        await message.reply_text("Привет! Чтобы узнать список команд, используйте /hb или список.")


async def register_command(client, message):
    user_id = message.from_user.id
    username = message.from_user.username
    if await check_user(user_id):
        return
    else:
        register_user(user_id, username, message.from_user.first_name)
        await message.reply_text("<b>Вы успешно зарегистрированы!</b>")
        
async def profile_command_short(client, message):
    args = message.text.split()
    if len(args) == 1:
        pass
    else:
        return
    await profile_command(client, message)


@app.on_message(filters.command("cpas"))
async def admin_commands(client, message: Message):
    if message.from_user.id not in API_OWNER:  # Проверка, является ли пользователь администратором
        await message.reply("❌ У вас нет прав для использования этой команды.")
        return
    
    admin_commands = """
    🛠 <b>Список команд для админа:</b>
    
    /cpas - Показать список команд администратора
    /wipe - Очистить базу данных (удалить данные всех пользователей)
    /savedb - Сохранить базу данных
    /set_status - Установить статус пользователю
    /hhh - Пример команды для админа
    /uhhh - Пример команды для пользователя
    /wipe_db - Полностью очистить базу данных
    /user - Показать информацию о пользователе
    /uinv - Показать инвентарь пользователя
    """

    await message.reply(admin_commands)


async def profile_command(client, message):
    user_id = message.from_user.id
    user_data = get_user_data(user_id)
    if not user_data:
        await message.reply_text("<b>Ошибка получения данных профиля, пропишите /ss</b>")
        return
    BOT1 = await app.get_me()
    BOT_USERNAME = BOT1.username
    if user_id in ADMINS:

    	await message.reply_text(
        f"🔥<b>Профиль:\n📝Имя: @{user_data['username']}</b>\n🪪<b>Айди: </b><code>{user_data['id']}</code>\n💰<b>Деньги:</b><code> {format_balance(user_data['money'])}</code>\n📉<b>Проиграно:</b><code> {format_balance(user_data['lose_count'])}</code>\n📈<b>Выигрыш:</b><code> {format_balance(user_data['win_count'])}</code>\n"
        f"📊<b>Статус:</b> {emojis[user_data['status']]}\n🔗<b>Ваша ссылка:</b> <code>https://t.me/{BOT_USERNAME}?start=ref_{user_id}</code>\n\n"
        f"[<b><i>Верифицированный аккаунт ✅</i></b>]")
    else:
    	await message.reply_text(
        f"🔥<b>Профиль:\n📝Имя: @{user_data['username']}</b>\n🪪<b>Айди: </b><code>{user_data['id']}</code>\n💰<b>Деньги:</b><code> {format_balance(user_data['money'])}</code>\n📉<b>Проиграно:</b><code> {format_balance(user_data['lose_count'])}</code>\n📈<b>Выигрыш:</b><code> {format_balance(user_data['win_count'])}</code>\n"
        f"📊<b>Статус:</b> {emojis[user_data['status']]}\n🔗<b>Ваша ссылка:</b> <code>https://t.me/{BOT_USERNAME}?start=ref_{user_id}</code>")

# @app.on_message(filters.command("new_promo"))
async def create_promo_command(client, message):
    # Проверяем, что client — это объект Client
    if not hasattr(client, 'get_me'):
        await message.reply_text("Внутренняя ошибка бота: неверный объект client.")
        return
    
    result = message.text.split()
    if len(result) < 4:
        await message.reply_text("<i>Используйте: /new_promo (название промо) (количество монет) (количество активаций)</i>")
        return

    promo_name = result[1]
    money = parse_bet_amount(result[2], get_user_data(message.from_user.id)['money'])
    activations = int(result[3])    

    args = message.text.split()
    result, error = await validate_promo_args(promo_name, money, activations)
    if error:
        await message.reply_text(error)
        return

    db = sqlite3.connect(DBB)
    cursor = db.cursor()

    cursor.execute("SELECT is_active FROM promos WHERE name = ?", (promo_name,))
    existing = cursor.fetchone()
    if existing:
        if existing[0] == 0:
            await message.reply_text("<i>Этот промокод уже использовался и больше не активен. Вы не можете создать новый с таким же именем.</i>")
        else:
            await message.reply_text("<i>Промокод с таким названием уже существует.</i>")
        return

    try:
        # Проверка на существование промокода
        cursor.execute("SELECT 1 FROM promos WHERE name = ?", (promo_name,))
        if cursor.fetchone():
            await message.reply_text("<i>Промокод с таким названием уже существует.</i>")
            return

        if message.from_user.id in API_OWNER:
            # Для владельцев API — просто создаем
            cursor.execute("INSERT INTO promos (name, money, activations) VALUES (?, ?, ?)",
                          (promo_name, money, activations))
            db.commit()
            bot_data = await client.get_me()
            await message.reply_text(
                f"<b>Пpомокод</b> {promo_name} <b>создан! <a href='t.me/{bot_data.username}?start=promo_{promo_name}'>ТЫК ДЛЯ АКТИВАЦИИ</a></b>\n"
                f"Начисление: {format_balance(money)} монет\nАктиваций: {activations}\n\n"
                f"<b>Чтобы активировать:</b> <code>/pr {promo_name}</code>"
            )
        else:
            # Для обычных игроков — проверяем и списываем деньги
            await message.reply_text("<i>Вы создаете промокод от имени обычного игрока...</i>")
            value = money * activations
            user_data = get_user_data(message.from_user.id)
            if user_data['money'] < value:
                await message.reply_text(f"<i>У вас недостаточно монет. Нужно еще {value - user_data['money']} монет.</i>")
                return

            cursor.execute("INSERT INTO promos (name, money, activations) VALUES (?, ?, ?)",
                          (promo_name, money, activations))
            cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(value), message.from_user.id))
            db.commit()
            bot_data = await client.get_me()
            await message.reply_text(
                f"<b>Пpомокод</b> {promo_name} <b>создан! <a href='t.me/{bot_data.username}?start=promo_{promo_name}'>ТЫК ДЛЯ АКТИВАЦИИ</a></b>\n"
                f"Начисление: {format_balance(money)} монет\nАктиваций: {activations}\n\n"
                f"<b>Чтобы активировать:</b> <code>/pr {promo_name}</code>\n"
                f"Или ссылка: <code>t.me/{bot_data.username}?start=promo_{promo_name}</code>"
            )
    except Exception as e:
        await message.reply_text(f"Произошла ошибка в базе данных: {e}")
    finally:
        db.close()
    
    await app.send_message("-1004869586301", f"""
<b>Момент: Создание промокода</b>
<b>Создатель:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Название:</b> [ЗАСЕКРЕЧЕНО]
<b>Сумма:</b> [ЗАСЕКРЕЧЕНО]
<b>Активаций:</b> [ЗАСЕКРЕЧЕНО]
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")

import re

async def validate_promo_args(promo_name, money, activations):
    if len(promo_name) > 15:
        return None, "<i>Используйте: /new_promo (название промо) (количество монет) (количество активаций)</i>"
    
    promo_name = promo_name

    if len(promo_name) > 15:
        return None, "<i>Название промокода должно содержать не более 15 символов.</i>"
    if not re.fullmatch(r'[a-zA-Z0-9]+', promo_name):
        return None, "<i>Название промокода может содержать только латинские буквы и цифры.</i>"
    
    if promo_name[0].isdigit():
        return None, "<i>Название промокода не может начинаться с цифры.</i>"

    if not money or not activations:
        return None, "<i>Сумма и активации должны быть целыми числами.</i>"
    
    try:
        if money <= 0 or activations <= 0:
            return None, "<i>Сумма и количество активаций должны быть больше 0.</i>"
        return (promo_name, money, activations), None
    except ValueError:
        return None, "<i>Сумма и активации должны быть целыми числами.</i>" 
    



@app.on_message(filters.command("set_status"))
async def set_status_command(client, message):
    if message.from_user.id not in API_OWNER:
        return
    
    args = message.text.split()
    if len(args) != 3:
        await message.reply_text("<i>Используйте: /set_status status_id user_id</i>")
        return
    
    try:
        # Извлекаем аргументы из команды
        status_id = int(args[1])  # Первый аргумент — идентификатор статуса
        user_id = int(args[2])    # Второй аргумент — идентификатор пользователя
        
        # Проверяем, что status_id в допустимом диапазоне
        if status_id < 0 or status_id >= len(emojis):
            await message.reply_text(f"<i>Неверный идентификатор статуса. Допустимый диапазон: 0–{len(emojis)-1}</i>")
            return
        
        # Проверяем, существует ли пользователь
        user_data = get_user_data(user_id)
        if not user_data:
            await message.reply_text("<i>Пользователь не зарегистрирован.</i>")
            return
        
        # Обновляем статус в базе данных
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        cursor.execute("UPDATE crash SET status = ? WHERE id = ?", (status_id, user_id))
        db.commit()
        db.close()
        
        # Получаем информацию о пользователе
        get_user = await app.get_users(user_id)
        await message.reply_text(
            f"<b>Статус <a href='t.me/{get_user.username}'>{get_user.first_name}</a> обновлен!</b>\n"
            f"Новый статус: {emojis[status_id]}"
        )
    except ValueError:
        await message.reply_text("<i>Оба аргумента должны быть целыми числами.</i>")
    except Exception as e:
        await message.reply_text(f"Произошла ошибка: {e}")

CHAT_ID = -1002596797604

async def activate_promo_command(client, message):
    user_id = message.from_user.id
    try:
        if not await check_user(user_id):
            await message.reply_text("<i>Вы не зарегистрированы.</i>")
            return

        try:
            member = await client.get_chat_member(CHAT_ID, user_id)
            if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
                btn = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("Зайти в чат", url=f"https://t.me/chatFerzister")
                        ]
                    ])
                await message.reply_text("<i>Вы должны быть участником чата, чтобы активировать промокод.</i>", reply_markup=btn)
                return
        except Exception:
            btn = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Зайти в чат", url=f"https://t.me/chatFerzister")
                    ]
                ])
            await message.reply_text("<i>Вы должны быть участником чата, чтобы активировать промокод.</i>", reply_markup=btn)
            return

        promo_name = message.text.split()[1]
        db = sqlite3.connect(DBB)
        cursor = db.cursor()

        cursor.execute("SELECT money, activations, is_active FROM promos WHERE name = ?", (promo_name,))
        promo = cursor.fetchone()
        if not promo:
            await message.reply_text("<i>Промокод не найден.</i>")
            db.close()
            return

        money, activations, is_active = promo

        if not is_active:
            await message.reply_text("<i>Этот промокод больше не активен.</i>")
            db.close()
            return

        cursor.execute("SELECT 1 FROM user_promos WHERE user_id = ? AND promo_name = ?", (user_id, promo_name))
        if cursor.fetchone():
            await message.reply_text("<i>Вы уже использовали этот промокод.</i>")
            db.close()
            return

        if activations <= 0:
            cursor.execute("UPDATE promos SET is_active = 0 WHERE name = ?", (promo_name,))
            db.commit()
            await message.reply_text("<i>У этого промокода закончились активации.</i>")
            db.close()
            return

        cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(money), user_id))
        cursor.execute("INSERT INTO user_promos (user_id, promo_name) VALUES (?, ?)", (user_id, promo_name))
        cursor.execute("UPDATE promos SET activations = activations - 1 WHERE name = ?", (promo_name,))
        db.commit()

        await message.reply_text(f"<b>Промокод активирован!</b>\nНачислено {money} монет.")

        # Проверка на 0 активаций после использования
        cursor.execute("SELECT activations FROM promos WHERE name = ?", (promo_name,))
        remaining = cursor.fetchone()[0]
        if remaining <= 0:
            cursor.execute("UPDATE promos SET is_active = 0 WHERE name = ?", (promo_name,))
            db.commit()
            await message.reply_text("<i>У этого промокода закончились активации и он был деактивирован.</i>")

        db.close()

    except IndexError:
        await message.reply_text("<i>Укажите название промокода.</i>")
    except Exception as e:
        await message.reply_text(f"Произошла ошибка: {e}")

@app.on_message(filters.command("cpr"))
async def check_promo_command(client, message):
    args = message.text.split()
    if len(args) != 2:
        await message.reply_text("<i>Используйте: /cpr (название промо)</i>")
        return

    promo_name = args[1]
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT money, activations, is_active FROM promos WHERE name = ?", (promo_name,))
    promo = cursor.fetchone()
    db.close()

    if not promo:
        await message.reply_text("<i>Промокод не найден.</i>")
        return

    money, activations, is_active = promo
    status = "Активен" if is_active else "Неактивен"
    await message.reply_text(
        f"<b>Информация о промокоде:</b>\n"
        f"📛 <b>Название:</b> <code>{promo_name}</code>\n"
        f"💰 <b>Сумма:</b> <code>{money}</code>\n"
        f"🔄 <b>Осталось активаций:</b> <code>{activations}</code>\n"
        f"⚙️ <b>Статус:</b> <code>{status}</code>"
    )

async def bonus_command(client, message):
    user_id = message.from_user.id
    user_data = get_user_data(user_id)
    now = datetime.now()

    # Проверка — получал ли бонус в последний час
    if user_data and user_data["last_bonus"]:
        last_bonus = datetime.fromisoformat(user_data["last_bonus"])
        time_since_last_bonus = now - last_bonus
        if time_since_last_bonus < timedelta(hours=1):
            remaining_time = timedelta(hours=1) - time_since_last_bonus
            hours, remainder = divmod(remaining_time.total_seconds(), 3600)
            minutes = divmod(remainder, 60)[0]
            await message.reply_text(
                f"<i>Вы сможете получить бонус через {int(hours)}ч {int(minutes)}м.</i>"
            )
            return

    # Бонусы по статусам
    status_bonus_map = {
        0:  [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000],
        1:  [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000],
        2:  [20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000, 180000, 200000],
        3:  [40000, 80000, 120000, 160000, 200000, 240000, 280000, 320000, 360000, 400000],
        4:  [80000, 160000, 240000, 320000, 400000, 480000, 560000, 640000, 720000, 800000],
        5:  [100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000],
        6:  [300000, 600000, 900000, 1200000, 1500000, 1800000, 2100000, 2400000, 2700000, 3000000],
        7:  [700000, 1400000, 2100000, 2800000, 3500000, 4200000, 4900000, 5600000, 6300000, 7000000],
        8:  [1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000],
        9:  [5000000, 10000000, 15000000, 20000000, 25000000, 30000000, 35000000, 40000000, 45000000, 50000000],
        10: [10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000, 80000000, 90000000, 100000000],
        11: [30000000, 60000000, 90000000, 120000000, 150000000, 180000000, 210000000, 240000000, 270000000, 300000000],
        12: [50000000, 100000000, 150000000, 200000000, 250000000, 300000000, 350000000, 400000000, 450000000, 500000000],
        13: [70000000, 140000000, 210000000, 280000000, 350000000, 420000000, 490000000, 560000000, 630000000, 700000000],
    }

    user_status = user_data.get("status", 0)
    possible_bonuses = status_bonus_map.get(user_status, status_bonus_map[0])

    bonus = choice(possible_bonuses)

    # Обновляем баланс и время последнего бонуса
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute(
        "UPDATE crash SET money = money + ?, last_bonus = ? WHERE id = ?",
        (bonus, now.isoformat(), user_id)
    )
    db.commit()
    db.close()

    await message.reply_text(
        f"<b>🎁 Вы получили бонус</b> <code>{format_balance(bonus)}</code> <b>монет!</b>\n"
        f"<i>Ваш статус: {emojis[user_status]}</i>"
    )


def parse_bet_input(arg: str, user_money: Optional[Union[int, float, str, Decimal]] = None) -> int:
    if arg is None:
        return -1

    s = str(arg).strip().lower()
    s = s.replace(" ", "").replace("_", "")

    if s in ("все", "всё", "all"):
        um = _to_decimal_safe(user_money)
        if um is None:
            return -1
        try:
            return int(um)
        except Exception:
            return -1

    m = re.fullmatch(r'([0-9]+(?:[.,][0-9]{1,2})?)([kк]*)', s)
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

# предполагаются эти функции у тебя в коде:
# spin_roulette(), get_color(number), is_even(number), format_balance(n)
# а также get_user_data(user_id) -> {'money': ...}, set_win_monet, set_lose_monet

async def roulette_command(client, message: Message):
    try:
        text = message.text or ""
        parts = text.split()

        if len(parts) < 3:
            await message.reply_text("<i>Используйте: рул (сумма ставки) (ставка)</i>\nПример: рул 1k чет")
            return

        bet_amount_str = parts[1]
        prediction_raw = parts[2].lower()

        # получить сумму ставки
        user_id = message.from_user.id
        user_data = get_user_data(user_id)  # должен вернуть словарь с 'money'
        bet_amount = parse_bet_amount(bet_amount_str, user_data['money'])

        if bet_amount is None:
            await message.reply_text("<i>Неправильный формат суммы ставки.</i>")
            return
        if bet_amount < 10:
            await message.reply_text("<i>Минимальная ставка 10 монет.</i>")
            return
        if user_data['money'] < bet_amount:
            await message.reply_text("<i>Недостаточно монет на балансе.</i>")
            return

        # Определим, является ли прогноз числом или строкой
        pred_num = None
        pred_str = None
        if re.match(r"^\d+$", prediction_raw):
            pred_num = int(prediction_raw)
            if pred_num < 0 or pred_num > 36:
                await message.reply_text("<i>Некорректный номер. Допустимые значения: 0-36.</i>")
                return
        else:
            pred_str = prediction_raw

        # Нормализованный набор допустимых ставок (строки)
        valid_predictions = {
            "красное", "кра", "red",
            "черное", "чер", "black",
            "четное", "чет", "even", "чёт",
            "нечетное", "нечет", "odd", "нечёт",
            "1-12", "13-24", "25-36",
            "бол", "больше", "big",
            "мал", "меньше", "small",
            "зеро", "zero", "зеленый", "зеленое"
        }

        if pred_num is None and pred_str not in valid_predictions:
            await message.reply_text(
                "<i>Некорректный тип ставки.</i>\nДопустимые ставки: <code>кра</code>, <code>чер</code>, <code>чет</code>, <code>нечет</code>, "
                "<code>1-12</code>, <code>13-24</code>, <code>25-36</code>, <code>бол</code>, <code>мал</code>, <code>зеро</code> или число от <i>0 до 36</i>"
            )
            return

        # Крутилка
        winning_number = spin_roulette()  # int 0..36
        winning_color = get_color(winning_number)  # ожидается 'красный'/'черный'/'зеленый'
        winning_even = is_even(winning_number)  # bool, обычно False для 0

        payout = 0.0

        # проверяем выигрыши
        if pred_num is not None:
            if winning_number == pred_num:
                payout = bet_amount * 35
        else:
            # строковые варианты
            if pred_str in ("кра", "красное", "red") and winning_color == "красный":
                payout = bet_amount * 1.9
            elif pred_str in ("чер", "черное", "black") and winning_color == "черный":
                payout = bet_amount * 1.9
            elif pred_str in ("чет", "четное", "even", "чёт") and winning_even and winning_number != 0:
                payout = bet_amount * 1.9
            elif pred_str in ("нечет", "нечетное", "odd", "нечёт") and (not winning_even) and winning_number != 0:
                payout = bet_amount * 1.9
            elif pred_str == "1-12" and 1 <= winning_number <= 12:
                payout = bet_amount * 2.7
            elif pred_str == "13-24" and 13 <= winning_number <= 24:
                payout = bet_amount * 2.7
            elif pred_str == "25-36" and 25 <= winning_number <= 36:
                payout = bet_amount * 2.7
            elif pred_str in ("бол", "больше", "big") and 19 <= winning_number <= 36:
                payout = bet_amount * 1.9
            elif pred_str in ("мал", "меньше", "small") and 1 <= winning_number <= 18:
                payout = bet_amount * 1.9
            elif pred_str in ("зеро", "zero", "зеленый", "зеленое") and winning_number == 0:
                payout = bet_amount * 36

        # Обновление баланса в БД: делаем net-приписывание (выигрыш - ставка) если выиграл, иначе вычитаем ставку
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        if payout > 0:
            net = float(payout - bet_amount)  # чистая прибавка к балансу
            cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (net, user_id))
            db.commit()
            new_balance = user_data['money'] + net
            await set_win_monet(user_id, payout)
            await message.reply_text(
                f"💸 <b>Ставка:</b> <code>{format_balance(bet_amount)}</code>\n"
                f"🎉 <b>Выигрыш:</b> <code>{format_balance(payout)}</code>\n"
                f"📈 <b>Выпало:</b> <code>{winning_number}</code> ({winning_color}, "
                f"{'четное' if winning_even and winning_number != 0 else 'нечетное' if not winning_even and winning_number != 0 else 'зеленый'})\n"
                f"💰 <b>Баланс:</b> <code>{format_balance(new_balance)}</code>"
            )
        else:
            # проигрыш: списываем ставку
            cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet_amount), user_id))
            db.commit()
            new_balance = user_data['money'] - bet_amount
            await set_lose_monet(user_id, bet_amount)
            await message.reply_text(
                f"💸 <b>Ставка:</b> <code>{format_balance(bet_amount)}</code>\n"
                f"🎟 <b>Проигрыш:</b> <code>{format_balance(bet_amount)}</code>\n"
                f"📈 <b>Выпало:</b> <code>{winning_number}</code> ({winning_color}, "
                f"{'четное' if winning_even and winning_number != 0 else 'нечетное' if not winning_even and winning_number != 0 else 'зеленый'})\n"
                f"💰 <b>Баланс:</b> <code>{format_balance(new_balance)}</code>"
            )
        db.close()

    except Exception as e:
        # выводим реальную ошибку для отладки
        await message.reply_text(f"Произошла ошибка: {e}")

@app.on_message(filters.command("new_channel_promo"))
async def create_channel_promo_command(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        await message.reply_text("❌ У вас нет доступа к этой команде.")
        return

    args = message.text.split()
    if len(args) != 4:
        await message.reply_text("Используйте: /new_channel_promo (название) (сумма) (активации)")
        return

    promo_name = args[1]
    try:
        money = float(args[2])
        activations = int(args[3])
    except ValueError:
        await message.reply_text("❌ Сумма и активации должны быть числами.")
        return

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO promos (name, money, activations) VALUES (?, ?, ?)", (promo_name, money, activations))
        db.commit()
        await message.reply_text(f"✅ Промокод '{promo_name}' создан с наградой {money} монет и {activations} активациями.")
        
        channel_text = (
            "🎉 <b>Новый промокод!</b> 🎉\n\n"
            f"🔑 <b>Промокод:</b> <code>{promo_name}</code>\n"
            f"💰 <b>Награда:</b> {format_balance(money)} монет\n"
            f"📊 <b>Активаций:</b> {activations}\n\n"
            "🚀 Активируйте в боте прямо сейчас!"
        )
        
        channel_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Активировать", url=f"http://t.me/Bfjsjdjndndbot?start=promo_{promo_name}")]
        ])
        
        await app.send_message("GG_dangerizardhe", channel_text, reply_markup=channel_markup)
        
    except sqlite3.IntegrityError:
        await message.reply_text("❌ Промокод с таким именем уже существует.")
    finally:
        db.close()


# Список игроков, которые сейчас крутят колесо
active_wheel_players = set()

@app.on_message(filters.command("wheel") | filters.regex(r"колесо", flags=re.IGNORECASE))
async def wheel_command(client, message: Message):
    user_id = message.from_user.id
    user_data = get_user_data(user_id)

    if not user_data:
        await message.reply_text("<i>Вы не зарегистрированы.</i>")
        return

    if user_id in active_wheel_players:
        await message.reply_text("<i>Вы уже крутите колесо! Дождитесь окончания игры.</i>")
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply_text("<i>Используйте: /wheel сумма_ставки</i>")
        return

    bet_amount = parse_bet_amount(parts[1], user_data['money'])
    if bet_amount is None or bet_amount < 10:
        await message.reply_text("<i>Минимальная ставка 10 монет.</i>")
        return
    if user_data['money'] < bet_amount:
        await message.reply_text("<i>Недостаточно монет.</i>")
        return

    # Добавляем игрока в активные
    active_wheel_players.add(user_id)

    # Запускаем игру
    asyncio.create_task(run_wheel_game(client, message, bet_amount, user_id))

async def run_wheel_game(client, message: Message, bet_amount: int, user_id: int):
    import random as rnd
    import asyncio
    import sqlite3

    try:
        # Сообщение "Колесо крутится..."
        status_msg = await message.reply_text("🎡 Колесо крутится...\n\n<b>Шансы выпадения:</b>\n❌ Проигрыш: 16%\nx0.2: 18%\nx0.5: 17%\nx1: 16%\nx1.5: 13%\nx2: 11%\nx5: 9%\n")

        # Ждём 1.5 секунды
        await asyncio.sleep(1.5)

        # Сектора
        sectors_common = ["❌ Проигрыш", "x0.2", "x0.5"]
        sectors_rare = [ "x1", "x1.5", "x2", "x5"]

        # Определяем финальный сектор (с твоей логикой)
        if bet_amount < 1000000000:
            final_sector = rnd.choice(sectors_common if rnd.random() < (2/3) else sectors_rare)
        else:
            final_sector = rnd.choice(sectors_common if rnd.random() < (3/4) else sectors_rare)

        # Рассчитываем выигрыш
        multiplier = 0.0
        if final_sector != "❌ Проигрыш" and final_sector.startswith("x"):
            multiplier = float(final_sector[1:])
        win_amount = int(bet_amount * multiplier)

        # Обновляем баланс в базе
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet_amount), user_id))
        if win_amount > 0:
            cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(win_amount), user_id))
        db.commit()
        db.close()

        # Обновляем статистику
        if win_amount > bet_amount:
            await set_win_monet(user_id, win_amount - bet_amount)
        else:
            await set_lose_monet(user_id, bet_amount - win_amount)

        # Итоговое сообщение
        final_text = (
            f"🎡 <b>Колесо фортуны</b>\n"
            f"Сектор: <b>{final_sector}</b>\n"
            f"Ставка: <code>{format_balance(bet_amount)}</code>\n"
            f"Выигрыш: <code>{format_balance(win_amount)}</code>"
        )
        await status_msg.edit_text(final_text)

    finally:
        # Убираем игрока из активных даже если была ошибка
        active_wheel_players.discard(user_id)



@app.on_message(filters.command("lottery") | filters.regex(r"лотерея", flags=re.IGNORECASE))
async def lottery_command(client, message):
    user_id = message.from_user.id
    user_data = get_user_data(user_id)

    if not user_data:
        await message.reply("❌ Вы не зарегистрированы.")
        return

    # Проверка ставки
    args = message.text.split()
    if len(args) < 2:
        await message.reply("💰 Укажите ставку. Пример: /lottery 100000")
        return

    bet = parse_bet_amount(args[1], user_data['money'])
    if not bet or bet <= 0:
        await message.reply("❌ Неверная сумма ставки.")
        return
    if user_data['money'] < bet:
        await message.reply("❌ У вас недостаточно монет.")
        return

    # Списание ставки
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet), user_id))
    db.commit()
    db.close()

    # Генерация значков
    slots = [choice(LOTTERY_ICONS) for _ in range(5)]
    result_text = " ".join(slots)

    # Подсчёт совпадений
    max_count = max(slots.count(icon) for icon in LOTTERY_ICONS)
    progress = f"{max_count}/3"

    # Проверка победы
    if max_count >= 3:
        prize = bet * 3
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(prize), user_id))
        db.commit()
        db.close()
        await message.reply(
            f" {result_text}\n\n"
            f"📊 Совпадений: {progress}\n"
            f"🍀 Вы выиграли! Ваша награда: {format_balance(prize)} 💰"
        )
    else:
        await message.reply(
            f" {result_text}\n\n"
            f"📊 Совпадений: {progress}\n"
            f"😔 Увы, вы проиграли {format_balance(bet)} 💰"
        )

async def buy_status_command(client, message):
    user_id = message.from_user.id
    user_data = get_user_data(user_id)
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply_text("<i>Укажите номер статуса для покупки (1-11).</i>")
            return
        status_id = int(parts[1])
    except (IndexError, ValueError):
        await message.reply_text("<i>Укажите номер статуса для покупки (1-11).</i>")
        return
    if not 1 <= status_id <= 11:
        await message.reply_text("<i>Укажите номер статуса от 1 до 11</i>")
        return
    if user_data['status'] >= status_id:
        await message.reply_text("<i>Вы не можете купить статус хуже текущего.</i>")
        return
    price = emoji_prices[status_id]
    if user_data['money'] < price:
        await message.reply_text("<i>Недостаточно монет для покупки этого статуса.</i>")
        return
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET money = money - ?, status = ? WHERE id = ?", (float(price), status_id, user_id))
    db.commit()
    db.close()
    await message.reply_text(f"<b>Вы успешно купили статус {emojis[status_id]} за</b> <code>{price}</code> <b>монет!</b>")


# async def top_balance_command(client, message):
#     db = sqlite3.connect(DBB)
#     cursor = db.cursor()
#     cursor.execute("SELECT username, money, status FROM crash ORDER BY money DESC LIMIT 10")
#     top_users = cursor.fetchall()
#     db.close()
#     if not top_users:
#         await message.reply_text("<i>В топе пока нет пользователей.</i>")
#         return
#     top_message = "<b>🏆 Топ 10 игроков по балансу:</b>\n"
#     for i, (username, money, status) in enumerate(top_users):
#         top_message += f"<b>{i + 1})</b> {username} {emojis[status]} - <i>{format_balance(money)} монет</i>\n"
#     await message.reply_text(top_message)

async def get_top_users(order_by="money", order="DESC", limit=10):
    try:
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            query = f"""
                SELECT id, username, first_name, money, status, win_count, lose_count
                FROM crash
                WHERE hidden = 0
                ORDER BY {order_by} {order}
                LIMIT ?
            """
            cursor.execute(query, (limit,))
            return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Ошибка при получении топа пользователей: {e}")
        return []

async def get_top_users_raw(order_by="money", order="DESC", limit=10):
    """Топ по любому полю, без фильтра hidden."""
    try:
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            query = f"""
                SELECT id, username, first_name, money, status, win_count, lose_count
                FROM crash
                ORDER BY {order_by} {order}
                LIMIT ?
            """
            cursor.execute(query, (limit,))
            return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"[DB ERROR] get_top_users_raw: {e}")
        return []

@app.on_callback_query(filters.regex(r"^top_(balance|wins|losses)$"))
async def top_balance_callback(client, callback):
    category = callback.data.split("_")[1]

    if category == "balance":
        title = "🏆 Топ 10 игроков по балансу:"
        top_users = await get_top_users()
        order_by = "money"
    elif category == "wins":
        title = "🏅 Топ 10 по победам:"
        top_users = await get_top_users_raw(order_by="win_count")
        order_by = "win_count"
    else:
        title = "💀 Топ 10 по поражениям:"
        top_users = await get_top_users_raw(order_by="lose_count")
        order_by = "lose_count"

    if not top_users:
        return await callback.answer("В топе пока нет пользователей.")

    text = await format_top_message(top_users, f"<b>{title}</b>", order_by=order_by)

    
    all_buttons = {
        "balance": InlineKeyboardButton("🏆 По балансу", callback_data="top_balance"),
        "wins": InlineKeyboardButton("🏅 По победам", callback_data="top_wins"),
        "losses": InlineKeyboardButton("💀 По сливам", callback_data="top_losses"),
    }
    keyboard = InlineKeyboardMarkup([[btn] for key, btn in all_buttons.items() if key != category])

    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer()
    except Exception as e:
        print(f"[TOP ERROR] {e}")
        await callback.answer("❌ Ошибка при обновлении топа.")

async def format_top_message(users, title, order_by="money"):
    if not users:
        return "<i>В топе пока нет пользователей.</i>"

    msg = f"<b>{title}</b>\n\n"
    for i, (user_id, username, first_name, money, status, win_count, lose_count) in enumerate(users, 1):
        name = username or first_name or f"ID {user_id}"
        emoji = emojis[status] if 0 <= status < len(emojis) else ""
        
        value = {
            "money": f"{format_balance(money)}",
            "win_count": f"{format_balance(win_count)}",
            "lose_count": f"{format_balance(lose_count)}"
        }.get(order_by, f"{format_balance(money)}")

        msg += f"<b>{i})</b> {name} {emoji} — <i>{value}</i>\n"
    return msg

async def top_balance_command(client, message):
    top_users = await get_top_users(order_by="money", order="DESC")
    if not top_users:
        return await message.reply("<i>В топе пока нет пользователей.</i>")

    text = await format_top_message(top_users, "<b>🏆 Топ 10 игроков по балансу:</b>", order_by="money")
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏅 По победам", callback_data="top_wins")],
        [InlineKeyboardButton("💀 По сливам", callback_data="top_losses")]
    ])
    await message.reply(text, reply_markup=buttons)

async def toggle_top_status(client, message):
    user_id = message.from_user.id

    with sqlite3.connect(DBB) as db:
        cursor = db.cursor()
        cursor.execute("SELECT hidden FROM crash WHERE id = ?", (user_id,))
        result = cursor.fetchone()

        if not result:
            return await message.reply("❌ Тебя нет в базе. Сначала зайди в игру.")

        hidden = result[0]

        if hidden == 1:
            
            cursor.execute("UPDATE crash SET hidden = 0 WHERE id = ?", (user_id,))
            db.commit()
            return await message.reply("🔓 Ты снова открыт в топе!")
        else:
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙈 Скрыть из топа", callback_data="hide_me")]
            ])
            return await message.reply(
                "🔓 Твой профиль сейчас <b>открыт</b> в топе.",
                reply_markup=keyboard
            )

@app.on_callback_query(filters.regex("^hide_me$"))
async def hide_me_callback(client, callback):
    user_id = callback.from_user.id

    try:
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            cursor.execute("UPDATE crash SET hidden = 1 WHERE id = ?", (user_id,))
            db.commit()

        await callback.answer("🔒 Ты скрыт из топа.", show_alert=True)
        await callback.message.edit_text(
            "🔒 Ты успешно скрыл свой профиль из топа.\n"
            "Чтобы снова стать видимым, введи /st."
        )
    except Exception as e:
        print(f"[HIDE ERROR] {e}")
        await callback.answer("❌ Ошибка при скрытии.", show_alert=True)

@app.on_message(filters.regex(r"игры", flags=re.IGNORECASE))
async def help_commahnd(client, message):
    mainn_text = """
<b>🎮 Игры</b>
────────────────
<code>Рул</code> или <code>/rul &lt;сумма&gt; &lt;ставка&gt;</code> — Рулетка
<code>Кости &lt;сумма&gt; | &lt;тип&gt;</code> — Игра в кости
<code>Минер &lt;сумма&gt;</code> — Игра в мины
<code>Краш &lt;сумма&gt; &lt;(икс)&gt;</code> — Игра краш
<code>Дуэль &lt;сумма&gt;</code> — Дуэль между игроками
<code>Башня &lt;сумма&gt;</code> — Игра в башню 🆕
<code>Сейф &lt;сумма&gt;</code> — Игра в сейф 🆕 (угадай 1 ключ из 4)
<code>Колесо &lt;сумма&gt;</code> — Колесо фортуны (на тесте)
<code>Лотерея &lt;сумма&gt;</code> — Игра в лотерею (выбей 3 одинаковых эмодзи) (на тесте)
"""
    await message.reply_text(mainn_text)

@app.on_message(filters.regex(r"статус", flags=re.IGNORECASE) or filters.regex(r"статусы", flags=re.IGNORECASE) )
async def help_commfahnd(client, message):
    mainnn_text = """
<b>🏅 Статусы</b>
────────────────
" " — начальный статус (0 💰) (бонус от 5к до 50к)
"👍" — 1к 💰 (бонус от 10к до 100к)
"😀" — 25к 💰 (бонус от 20к до 200к)
"🤯" — 100к 💰 (бонус от 40к до 400к)
"😎" — 500к 💰 (бонус от 80к до 800к)
"👽" — 2кк 💰 (бонус от 100к до 1кк)
"👾" — 7.5кк 💰 (бонус от 300к до 3кк)
"🤖" — 25кк 💰 (бонус от 700к до 7кк)
"👻" — 100кк 💰 (бонус от 1кк до 10кк)
"👑" — 1ккк 💰 (бонус от 5кк до 50кк)
"🎩" — 1кккк 💰 (бонус от 10кк до 100кк)
"🎰" — 10кккк 💰 (бонус от 30кк до 300кк)
"🎀" — Платный и эксклюзивный (бонус от 50кк до 500кк)
"🐍" — Платный и эксклюзивный (бонус от 70кк до 700кк)

<i>/bb (1-11) - для приобретения статуса!</i>

"""
    await message.reply_text(mainnn_text)




# /hb — список команд
@app.on_message(filters.command(["hb", "список", "помощь"]))
async def help_command(client, message):
    main_text = """
<b>📖 Меню команд</b>
───────────────────────
<b>⚙️ Основное</b>
<code>/ss</code> — Регистрация
<code>/meb</code> или <code>Я</code> — Профиль
<code>Бонус</code> — Бонус (каждые 1 час)
<code>/pr &lt;название&gt;</code> — Активировать промокод
<code>/перевести &lt;сумма&gt; &lt;айди пользователя&gt;</code> — Перевод
<code>/bb (1-9)</code> или <code>/купить (1-9)</code> — Купить статус
<code>/топ</code> или <code>топ</code> — Топ 10 игроков
<code>/hb</code> или <code>список</code> — Список команд
<code>/new_promo [название] [сумма] [активаций]</code> — Создать промокод
<code>/inv</code> — Ваш инвентарь
<code>/market</code> — Маркет
<code>/shop</code> — Купить предмет
<code>реф</code> — Ваша реф
<code>/st</code> — Скрыть/открыть профиль в топе
<code>/farm</code> — Фарминг
"""
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 Игры", callback_data="help:games"),
         InlineKeyboardButton("💰 Экономика", callback_data="help:econ")],
        [InlineKeyboardButton("🛒 Маркет", callback_data="help:market"),
         InlineKeyboardButton("🏅 Статусы", callback_data="help:statuses")],
        [InlineKeyboardButton("📞 Поддержка", url="https://t.me/ferzister")]
    ])
    await message.reply_text(main_text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)


# Обработка нажатий на кнопки
@app.on_callback_query(filters.regex(r"^help:(games|econ|market|statuses|main)$"))
async def help_section_cb(client, callback_query):
    section = callback_query.data.split(":")[1]

    if section == "games":
        text = """
<b>🎮 Игры</b>
────────────────
<code>Рул</code> или <code>/rul &lt;сумма&gt; &lt;ставка&gt;</code> — Рулетка
<code>Кости &lt;сумма&gt; | &lt;тип&gt;</code> — Игра в кости
<code>Минер &lt;сумма&gt;</code> — Игра в мины
<code>Краш &lt;сумма&gt; &lt;(икс)&gt;</code> — Игра краш
<code>Дуэль &lt;сумма&gt;</code> — Дуэль между игроками
<code>Башня &lt;сумма&gt;</code> — Игра в башню 🆕
<code>Сейф &lt;сумма&gt;</code> — Игра в сейф 🆕 (угадай 1 ключ из 4)
<code>Колесо &lt;сумма&gt;</code> — Колесо фортуны (на тесте)
<code>Лотерея &lt;сумма&gt;</code> — Игра в лотерею (выбей 3 одинаковых эмодзи) (на тесте)
"""
        nav = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="help:main")]])
        await callback_query.message.edit_text(text, reply_markup=nav, parse_mode=enums.ParseMode.HTML)

    elif section == "econ":
        text = """
<b>💰 Экономика / Сервисы</b>
──────────────────────────
<code>/pr &lt;название&gt;</code> — Активировать промокод
<code>/перевести &lt;сумма&gt; &lt;айди&gt;</code> — Перевод
<code>/bb (1-9)</code> / <code>/купить (1-9)</code> — Купить статус
<code>/new_promo ...</code> — Создать промокод (спишется с баланса)
<code>/inv</code> — Инвентарь
<code>/market</code>, <code>/shop</code> — Маркет/Покупка
<code>реф</code> — Ваша реф ссылка
<code>/топ</code> — Топ игроков
<code>/st</code> — Скрыть/открыть профиль в топе
<code>/farm</code> — Фарминг
"""
        nav = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="help:main")]])
        await callback_query.message.edit_text(text, reply_markup=nav, parse_mode=enums.ParseMode.HTML)

    elif section == "market":
        text = """
<b>🛒 Маркет</b>
────────────────
Используйте <code>/market</code> чтобы просмотреть магазин 
и <code>/shop</code> чтобы купить предмет.
"""
        nav = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="help:main")]])
        await callback_query.message.edit_text(text, reply_markup=nav, parse_mode=enums.ParseMode.HTML)

    elif section == "statuses":
        text = """
<b>🏅 Статусы</b>
────────────────
" " — начальный статус (0 💰) (бонус от 5к до 50к)
"👍" — 1к 💰 (бонус от 10к до 100к)
"😀" — 25к 💰 (бонус от 20к до 200к)
"🤯" — 100к 💰 (бонус от 40к до 400к)
"😎" — 500к 💰 (бонус от 80к до 800к)
"👽" — 2кк 💰 (бонус от 100к до 1кк)
"👾" — 7.5кк 💰 (бонус от 300к до 3кк)
"🤖" — 25кк 💰 (бонус от 700к до 7кк)
"👻" — 100кк 💰 (бонус от 1кк до 10кк)
"👑" — 1ккк 💰 (бонус от 5кк до 50кк)
"🎩" — 1кккк 💰 (бонус от 10кк до 100кк)
"🎰" — 10кккк 💰 (бонус от 30кк до 300кк)
"🎀" — Платный и эксклюзивный (бонус от 50кк до 500кк)
"🐍" — Платный и эксклюзивный (бонус от 70кк до 700кк)

<i>/bb (1-11) - для приобретения статуса!</i>
"""
        nav = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="help:main")]])
        await callback_query.message.edit_text(text, reply_markup=nav, parse_mode=enums.ParseMode.HTML)

    elif section == "main":
        # Возврат на основной список
        main_text = """
<b>📖 Меню команд</b>
────────────────────────
<b>⚙️ Основное</b>
<code>/ss</code> — Регистрация
<code>/meb</code> или <code>Я</code> — Профиль
<code>Бонус</code> — Бонус (каждые 1 час)
<code>/pr &lt;название&gt;</code> — Активировать промокод
<code>/перевести &lt;сумма&gt; &lt;айди пользователя&gt;</code> — Перевод
<code>/bb (1-11)</code> или <code>/купить (1-11)</code> — Купить статус
<code>/топ</code> или <code>топ</code> — Топ 10 игроков
<code>/hb</code> или <code>список</code> — Список команд
<code>/new_promo [название] [сумма] [активаций]</code> — Создать промокод
<code>/inv</code> — Ваш инвентарь
<code>/market</code> — Маркет
<code>/shop</code> — Купить предмет
<code>реф</code> — Ваша реф
<code>/st</code> — Скрыть/открыть профиль в топе
<code>/farm</code> — Фарминг
"""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎲 Игры", callback_data="help:games"),
             InlineKeyboardButton("💰 Экономика", callback_data="help:econ")],
            [InlineKeyboardButton("🛒 Маркет", callback_data="help:market"),
             InlineKeyboardButton("🏅 Статусы", callback_data="help:statuses")],
            [InlineKeyboardButton("📞 Поддержка", url="https://t.me/ferzister")]
        ])
        await callback_query.message.edit_text(main_text, reply_markup=kb, parse_mode=enums.ParseMode.HTML)


async def save_conf_command(client, message):
    if message.from_user.id not in API_OWNER:
        return
    
    await save_json_to_db()
    await message.reply_text("Конфигурация сохранена!")

async def save_json_to_db(json_file="hgvvv_data.json", db_file="db.db"):
    if not os.path.exists(json_file):
        return f"Файл {json_file} не найден!"

    # Подключаемся к базе данных
    db = sqlite3.connect(db_file)
    cursor = db.cursor()

    # Вставляем данные в таблицу crash
    for user in data["crash"]:
        cursor.execute("""
            INSERT OR REPLACE INTO crash (id, username, money, stone, iron, gold, diamond, emerald, last_bonus, boor, lose_count, win_count, total_games, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user.get("id"), user.get("username"), user.get("money"), user.get("stone", 0), user.get("iron", 0),
            user.get("gold", 0), user.get("diamond", 0), user.get("emerald", 0), user.get("last_bonus"),
            user.get("boor", 1), user.get("lose_count", 0), user.get("win_count", 0), user.get("total_games", 0),
            user.get("status", 0)
        ))

    # Вставляем данные в таблицу promos
    for promo in data["promos"]:
        cursor.execute("""
            INSERT OR REPLACE INTO promos (name, money, activations)
            VALUES (?, ?, ?)
        """, (promo.get("name"), promo.get("money"), promo.get("activations")))

    # Вставляем данные в таблицу user_promos
    for user_promo in data["user_promos"]:
        cursor.execute("""
            INSERT OR REPLACE INTO user_promos (user_id, promo_name)
            VALUES (?, ?)
        """, (user_promo.get("user_id"), user_promo.get("promo_name")))

    # Вставляем данные в таблицу inv (если есть в JSON)
    if "inv" in data:
        for inv in data["inv"]:
            cursor.execute("""
                INSERT OR REPLACE INTO inv (id, details, stone_sediments, iron_ingots, gold_bars, diamond_cores, emerald_dust, iron_ingots_dense, gold_nuggets)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                inv.get("id"), inv.get("details", 0), inv.get("stone_sediments", 0), inv.get("iron_ingots", 0),
                inv.get("gold_bars", 0), inv.get("diamond_cores", 0), inv.get("emerald_dust", 0),
                inv.get("iron_ingots_dense", 0), inv.get("gold_nuggets", 0)
            ))

    # Вставляем данные в таблицу drill (если есть в JSON)
    if "drill" in data:
        for drill in data["drill"]:
            cursor.execute("""
                INSERT OR REPLACE INTO drill (user_id, motor_lvl, drill_head_lvl, frame, power_source, handle, cooling, gearbox, oil, energy, max_energy, max_oil_engine, max_cooling_engine, heal_drill_engine)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                drill.get("user_id"), drill.get("motor_lvl", 1), drill.get("drill_head_lvl", 1), drill.get("frame", 1),
                drill.get("power_source", 1), drill.get("handle", 0), drill.get("cooling", 0), drill.get("gearbox", 0),
                drill.get("oil", 0), drill.get("energy", 10), drill.get("max_energy", 10), drill.get("max_oil_engine", 10),
                drill.get("max_cooling_engine", 10), drill.get("heal_drill_engine", 100)
            ))

    # Добавляем записи в inv и drill для всех пользователей из crash
    cursor.execute("SELECT id FROM crash")
    user_ids = [row[0] for row in cursor.fetchall()]

    for user_id in user_ids:
        # Проверяем, есть ли запись в inv, если нет — добавляем с дефолтными значениями
        cursor.execute("SELECT id FROM inv WHERE id = ?", (user_id,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO inv (id, details, stone_sediments, iron_ingots, gold_bars, diamond_cores, emerald_dust, iron_ingots_dense, gold_nuggets)
                VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0)
            """, (user_id,))

        # Проверяем, есть ли запись в drill, если нет — добавляем с дефолтными значениями
        cursor.execute("SELECT user_id FROM drill WHERE user_id = ?", (user_id,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO drill (user_id, motor_lvl, drill_head_lvl, frame, power_source, handle, cooling, gearbox, oil, energy, max_energy, max_oil_engine, max_cooling_engine, heal_drill_engine)
                VALUES (?, 1, 1, 1, 1, 0, 0, 0, 0, 10, 10, 10, 10, 100)
            """, (user_id,))

    # Фиксируем изменения и закрываем соединение
    db.commit()
    db.close()
    return "Данные успешно сохранены в базу данных, включая новые записи в inv и drill!"

async def transfer_command(client, message):
    try:
        parts = message.text.split()
        if len(parts) != 3 and not message.reply_to_message:
            await message.reply_text("<i>Используйте: /перевести (количество монет) (ID/@username пользователя)</i>\nИли ответьте на сообщение пользователя.")
            return

        amount_str = parts[1] if len(parts) > 1 else None
        recipient_id = None

        # Если указан ID или username
        if len(parts) == 3:
            recipient_id = parts[2]
            if recipient_id.startswith("@"):
                recipient_username = recipient_id[1:]
                db = sqlite3.connect(DBB)
                cursor = db.cursor()
                cursor.execute("SELECT id FROM crash WHERE username = ?", (recipient_username,))
                recipient_data = cursor.fetchone()
                db.close()
                if recipient_data:
                    recipient_id = recipient_data[0]
                else:
                    await message.reply_text("<i>Получатель с таким юзернеймом не найден.</i>")
                    return
            else:
                try:
                    recipient_id = int(recipient_id)
                except ValueError:
                    await message.reply_text("<i>Неверный формат ID пользователя.</i>")
                    return

        # Если ответ на сообщение
        elif message.reply_to_message and message.reply_to_message.from_user:
            recipient_id = message.reply_to_message.from_user.id

        if not recipient_id:
            await message.reply_text("<i>Не удалось определить получателя.</i>")
            return

        user_id = message.from_user.id
        user_data = get_user_data(user_id)
        recipient_data = get_user_data(recipient_id)

        if not recipient_data:
            await message.reply_text("<i>Получатель не зарегистрирован в системе.</i>")
            return

        if recipient_id == user_id:
            await message.reply_text("<i>Вы не можете перевести монеты самому себе.</i>")
            return

        amount = parse_bet_amount(amount_str, user_data['money'])
        if amount is None:
            await message.reply_text("<i>Неправильный формат суммы для перевода.</i>")
            return
        if amount < 10:
            await message.reply_text("<i>Минимальная сумма перевода — 10 монет.</i>")
            return
        if user_data['money'] < amount:
            await message.reply_text("<i>Недостаточно монет на балансе для перевода.</i>")
            return

        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(amount), user_id))
        cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(amount), recipient_id))
        db.commit()
        db.close()

        await message.reply_text(
            f"<b>✅ Успешно переведено</b> <code>{format_balance(amount)}</code> <b>монет пользователю</b> <code>{recipient_data['username'] or recipient_id}</code>."
        )
    except ValueError:
        await message.reply_text("<i>Неправильный формат команды. Используйте: /перевести (количество монет) (ID/@username пользователя)</i>")
    except Exception as e:
        await message.reply_text(f"Произошла ошибка: {e}")

# пример 

def encrypt_user_id(user_id: int) -> str:
    cipher_map = {
        '0': '3',
        '1': '4',
        '2': '9',
        '3': '1',
        '4': '7',
        '5': '2',
        '6': '8',
        '7': '5',
        '8': '0',
        '9': '6'
    }

    encrypted = ''.join(cipher_map.get(ch, ch) for ch in str(user_id))
    return f"https://t.me/gbursebot?start=et_{encrypted}"

async def get_link(client, message):
    try:
        user_id = message.from_user.id
        encrypted_id = encrypt_user_id(user_id)
        await message.reply(f"Ваш зашифрованный ссылка: {encrypted_id}")
    except Exception as e:
        await message.reply(f"Произошла ошибка: {e}")

async def get_hash(client, message):
    try:
        # Разбиваем сообщение на части и проверяем наличие аргумента
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply("Используйте: /hash <ID>")
            return
            
        tg_id = parts[1]
        # Используем встроенную функцию hash
        hash_result = hash(tg_id)
        await message.reply(f"ID: {tg_id}\nHash: {hash_result}")
    except Exception as e:
        await message.reply(f"Произошла ошибка: {e}")
    

async def dice_command(client, message):
    try:
        parts = message.text.split()
        if len(parts) != 3:
            await message.reply_text(
                "<i>Используйте: /кости (сумма ставки) (тип ставки)</i>\nПример: /кости 100 больше\nТипы ставок: больше, меньше, равно, [2-12]",
            )
            return
        _, bet_amount_str, prediction = parts
        prediction = prediction.lower()
        user_id = message.from_user.id
        user_data = get_user_data(user_id)
        bet_amount = parse_bet_amount(bet_amount_str, user_data['money'])
        if bet_amount is None:
            await message.reply_text("<i>Неправильный формат суммы ставки.</i>")
            return
        if bet_amount < 10:
            await message.reply_text("<i>Минимальная ставка 10 монет.</i>")
            return
        if user_data['money'] < bet_amount:
            await message.reply_text("<i>Недостаточно монет на балансе.</i>")
            return
        dice_result = randint(2, 12)
        payout = 0
        valid_predictions = ["бол", "больше", "мал", "меньше", "равно"]
        if prediction not in valid_predictions and (
                not prediction.isdigit() or not (2 <= int(prediction) <= 12)):
            await message.reply_text(
                "<i>Некорректный тип ставки.</i>\nДопустимые ставки: больше, меньше, равно, или число от 2 до 12.",
            )
            return
        if prediction in ["бол", "больше"] and dice_result > 7:
            payout = bet_amount * 1.9
        elif prediction in ["мал", "меньше"] and dice_result < 7:
            payout = bet_amount * 1.9
        elif prediction == "равно" and dice_result == 7:
            payout = bet_amount * 5.5
        elif prediction.isdigit() and 2 <= int(prediction) <= 12 and dice_result == int(prediction):
            payout = bet_amount * 5.5
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        if payout > 0:
            cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(payout - bet_amount), user_id))
            db.commit()
            bbb = user_data['money'] + int(payout - bet_amount)
            await set_win_monet(user_id, int(payout))
            await message.reply_text(
                f"<b>🎲 Выпало:</b> {dice_result}\n<b>🎉 Вы выиграли:</b> {format_balance(int(payout))} монет!\n<b>💰 Баланс:</b> {format_balance(bbb)}",
            )
        else:
            cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet_amount), user_id))
            db.commit()
            bbb =  user_data['money'] - bet_amount
            await set_lose_monet(user_id, bet_amount)
            await message.reply_text(
                f"<b>🎲 Выпало:</b> {dice_result}\n<b>😔 Вы проиграли:</b> {format_balance(bet_amount)} монет\n<b>💰 Баланс:</b> {format_balance(bbb)}",
            )
        db.close()
    except ValueError:
        await message.reply_text(
            "<i>Неправильный формат команды. Используйте: /кости (сумма ставки) (тип ставки)</i>\nПример: кости 100 больше",
        )
    except Exception as e:
        await message.reply_text(f"Произошла ошибка: {e}")

lottery_lock = asyncio.Lock()
LOTTERY_FILE = "lottery_data.json"
LOTTERY_REWARDS = ["Losse", "1к", "10к", "5к", "2к", "500", "7.5к", "2.5к"]
LOTTERY_GIFS_DIR = "gifs"

if not os.path.exists(LOTTERY_FILE):
    with open(LOTTERY_FILE, "w") as f:
        json.dump({}, f)

def load_lottery_cooldowns() -> dict:
    with open(LOTTERY_FILE, "r") as f:
        return json.load(f)

def save_lottery_cooldowns(data: dict):
    with open(LOTTERY_FILE, "w") as f:
        json.dump(data, f)

async def show_lottery_menu(client, message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎰 Крутить", callback_data="lottery:spin")]
    ])
    await message.reply("🎁 Добро пожаловать в лотерею! Выберите действие:", reply_markup=kb)





import random
import sqlite3
import json
from random import randint
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from asyncio import Lock

active_mines_games = {}          # game_id -> game_data dict
user_locks = {}

def load_game_db(game_id):
    try:
        with sqlite3.connect(DBB) as db:
            r = db.execute("SELECT game_data FROM mines_games WHERE game_id = ?", (game_id,)).fetchone()
            return json.loads(r[0]) if r else None
    except Exception as e:
        logger.exception(f"load_game_db error: {e}")
        return None

def save_game_db(game_id, game):
    try:
        with sqlite3.connect(DBB) as db:
            db.execute(
                "INSERT OR REPLACE INTO mines_games (game_id,user_id,game_data) VALUES (?,?,?)",
                (game_id, game['user_id'], json.dumps(game))
            )
            db.commit()
    except Exception as e:
        logger.exception(f"save_game_db error: {e}")

def delete_game_db(game_id):
    try:
        with sqlite3.connect(DBB) as db:
            db.execute("DELETE FROM mines_games WHERE game_id = ?", (game_id,))
            db.commit()
    except Exception as e:
        logger.exception(f"delete_game_db error: {e}")

def cleanup(game_id):
    active_mines_games.pop(game_id, None)
    delete_game_db(game_id)

# --- Надійне редагування / фолбек (редагує, або шле нове) ---
async def edit_or_send(client, chat_id, msg_id, text, reply_markup=None):
    """Спроба редагування; при невдачі — відправка нового повідомлення."""
    seconds = None
    # Фолбек: якщо немає id — просто шлемо
    if not msg_id:
        try:
            m = await client.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            return getattr(m, "id", None)
        except Exception as e:
            logger.exception(f"send_message fallback failed: {e}")
            return None

    # Спроба редагувати
    try:
        await client.edit_message_text(chat_id, msg_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        return msg_id
    except FloodWait as fw:
        seconds = getattr(fw, "value", getattr(fw, "x", 1))
        logger.warning(f"FloodWait during edit: {seconds}s")
        await asyncio.sleep(seconds)
        try:
            await client.edit_message_text(chat_id, msg_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            return msg_id
        except Exception as e:
            logger.exception(f"edit retry failed: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error during edit: {e}")

    # Якщо редагування не пройшло — шлемо нове
    try:
        m = await client.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        return getattr(m, "id", None)
    except Exception as e:
        logger.exception(f"Fallback send_message also failed: {e}")
        return None

# --- Побудова клавіатури (використовуємо mines_positions явно) ---
async def create_mines_keyboard(game_id, opened_cells, mines_positions, exploded, last_index, all_opened):
    btns = []
    mines_set = set(mines_positions or [])
    for i in range(25):
        if i == last_index and exploded:
            face = '💥'
        elif i in opened_cells:
            face = '🌀'
        elif (exploded or all_opened) and i in mines_set:
            face = '💣'
        else:
            face = '❔'
        btns.append(InlineKeyboardButton(face, callback_data=f"mines_cell_{game_id}_{i}"))
    kb = [btns[i:i+5] for i in range(0, 25, 5)]
    markup = InlineKeyboardMarkup(kb)
    if not exploded and not all_opened:
        if opened_cells:
            markup.inline_keyboard.append([InlineKeyboardButton("💰 Забрать приз", callback_data=f"mines_take_{game_id}")])
        else:
            markup.inline_keyboard.append([InlineKeyboardButton("🚫 Отменить", callback_data=f"mines_cancel_{game_id}")])
    return markup

# --- Константи ---
MULTIPLIERS = [
    1.0,1.05,1.18,1.32,1.51,1.79,2.01,2.3,2.9,3.5,4.3,5.2,
    6.98,9.11,12.94,15.79,23.41,34.11,53.97,89.69,187.38,356.45,2433.8
]

# --- Формування та відправка повідомлення гри ---
async def send_game_message(client, game, status="continue", win_amount=None):
    gid = game['game_id']
    uid = game['user_id']
    board = game.get('board', ['❔']*25)
    opened = game.get('opened_cells', [])
    bet = game.get('bet_amount', 0)
    mult = game.get('current_multiplier', 1.0)
    exploded = game.get('exploded', False)
    all_opened = game.get('all_cells_opened', False)

    try:
        balance = format_balance(get_user_data(uid)['money'])
    except Exception:
        balance = "—"

    # Підготуємо клавіатуру з mines_positions
    mines_positions = game.get('mines_positions', [])

    if status == "lose":
        markup = await create_mines_keyboard(gid, opened, mines_positions, True, game.get('last_cell_index'), False)
        text = (f"<b>💥 Бум! Вы попали на мину.</b>\n"
                f"Ставка: <code>{format_balance(bet)}</code>\n"
                f"Баланс: <code>{balance}</code>")
    elif status == "win":
        markup = await create_mines_keyboard(gid, opened, mines_positions, False, None, True)
        text = (f"<b>🎉 Все открыто! Вы выиграли <code>{format_balance(win_amount)}</code></b>\n"
                f"Баланс: <code>{balance}</code>")
    else:  # continue
        markup = await create_mines_keyboard(gid, opened, mines_positions, exploded, None, all_opened)
        possible = format_balance(int(bet * mult))
        text = (f"<b>🟢 Продолжай играть</b>\n"
                f"Открыто: <b>{len(opened)}</b>\n"
                f"Коэффициент: <code>{mult:.2f}x</code>\n"
                f"Возможный выигрыш: <code>{possible}</code>")

    new_id = await edit_or_send(client, game['chat_id'], game.get('message_id'), text, reply_markup=markup)
    if new_id and new_id != game.get('message_id'):
        game['message_id'] = new_id
        active_mines_games[gid] = game
        save_game_db(gid, game)

# --- Команда створення гри ---
@rate_limit
async def mines_command(client, message):
    uid = message.from_user.id
    if not await check_user(uid):
        return await message.reply("Вы не зарегистрированы.")
    if await is_banned_user(uid):
        return await message.reply("Вы забанены.")

    loader = await message.reply_text("Генерация игры...")
    try:
        parts = (message.text or "").split()
        if len(parts) != 2:
            return await loader.edit_text("<i>Используйте: минер (сумма)</i>")

        bet = parse_bet_amount(parts[1], get_user_data(uid)['money'])
        if not bet or bet < 10:
            return await loader.edit_text("<i>Неверная сумма / мин ставка 10</i>")
        if get_user_data(uid)['money'] < bet:
            return await loader.edit_text("<i>Недостаточно монет.</i>")

        # Списываем ставку
        with sqlite3.connect(DBB) as db:
            db.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet), uid))
            db.commit()

        game_id = str(randint(1000000, 9999999))
        board = ['❔'] * 25
        mines = sample(range(25), 2)

        game = {
            'user_id': uid,
            'bet_amount': bet,
            'board': board,
            'mines_positions': mines,
            'opened_cells': [],
            'current_multiplier': 1.0,
            'message_id': None,
            'chat_id': message.chat.id,
            'exploded': False,
            'all_cells_opened': False,
            'game_id': game_id
        }

        active_mines_games[game_id] = game
        save_game_db(game_id, game)

        markup = await create_mines_keyboard(game_id, [], mines, False, None, False)
        msg = await message.reply("<b>💣 Игра началась</b>", reply_markup=markup)
        game['message_id'] = msg.id
        save_game_db(game_id, game)

        await client.delete_messages(message.chat.id, loader.id)
    except FloodWait as fw:
        await loader.edit_text(f"Ошибка: {fw}")
    except Exception as e:
        logger.exception(f"mines_command error: {e}")
        await loader.edit_text(f"Ошибка запуска: {e}")

# --- Клік по клітинці ---
@rate_limit
@app.on_callback_query(filters.regex(r'^mines_cell_'))
async def mines_cell_callback(client, cq):
    uid = cq.from_user.id
    data = cq.data.split('_')
    if len(data) < 4:
        return await cq.answer("Неверные данные.")
    gid, idx = data[2], int(data[3])

    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    if user_locks[uid].locked():
        return await cq.answer("⏳ Подождите...")

    async with user_locks[uid]:
        try:
            game = active_mines_games.get(gid) or load_game_db(gid)
            if not game or game.get('user_id') != uid:
                return await cq.answer("Игра не найдена.")
            if game.get('exploded') or game.get('all_cells_opened'):
                return await cq.answer("Игра завершена.")
            if idx in game.get('opened_cells', []):
                return await cq.answer("Эта ячейка уже открыта.")

            board = game.get('board', ['❔']*25)
            mines = game.get('mines_positions', [])
            opened = game.get('opened_cells', [])
            bet = game.get('bet_amount', 0)
            chat_id = game.get('chat_id')
            msg_id = game.get('message_id')

            # --- Попадання в міну (реальна міна) ---
            if idx in mines:
                game['exploded'] = True
                game['last_cell_index'] = idx

                unopened = [i for i in range(25) if i not in opened and i not in mines]
                if unopened:
                    third = choice(unopened)
                    mines.append(third)
                    board[third] = '💣'

                # Показуємо всі міни і виділяємо вибрану
                for m in mines:
                    board[m] = '💣'
                board[idx] = '💥'  # вибухнута клітинка

                game['mines_positions'] = mines
                game['board'] = board
                active_mines_games[gid] = game
                save_game_db(gid, game)

                # Віднімаємо ставку та оновлюємо БД/статус
                await set_lose_monet(uid, bet)
                await send_game_message(client, game, status="lose")

                cleanup(gid)
                return await cq.answer()

            # --- Скриптовий шанс програшу (додаємо міну в натиснуту клітинку) ---
            if randint(1, 11) == 1:
                mines.append(idx)
                board[idx] = '💥'
                game['last_cell_index'] = idx
                game['exploded'] = True

                for m in mines:
                    board[m] = '💣'
                board[idx] = '💥'

                game['mines_positions'] = mines
                game['board'] = board
                active_mines_games[gid] = game
                save_game_db(gid, game)

                await set_lose_monet(uid, bet)
                await send_game_message(client, game, status="lose")

                cleanup(gid)
                return await cq.answer()

            # --- Безпечна клітинка ---
            opened.append(idx)
            board[idx] = '🌀'
            num_open = len(opened)

            game['opened_cells'] = opened
            game['board'] = board
            game['current_multiplier'] = MULTIPLIERS[min(num_open, len(MULTIPLIERS)-1)]
            active_mines_games[gid] = game
            save_game_db(gid, game)

            await send_game_message(client, game, status="continue")

            # --- Перевірка на виграш (22 відкриті) ---
            if num_open >= 22:
                win = int(bet * game['current_multiplier'])
                try:
                    with sqlite3.connect(DBB) as db:
                        db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(win), uid))
                        db.commit()
                except Exception as e_db:
                    logger.exception(f"DB awarding win failed: {e_db}")

                await set_win_monet(uid, win)
                game['all_cells_opened'] = True
                for m in game.get('mines_positions', []):
                    board[m] = '💣'
                game['board'] = board
                save_game_db(gid, game)

                await send_game_message(client, game, status="win", win_amount=win)
                cleanup(gid)

            await cq.answer()
        except FloodWait as fw:
            wait_seconds = getattr(fw, "value", getattr(fw, "x", 1))
            logger.warning(f"FloodWait for user {uid}: sleeping {wait_seconds}s")
            await asyncio.sleep(wait_seconds)
            game = active_mines_games.get(gid) or load_game_db(gid)
            if game and not (game.get('exploded') or game.get('all_cells_opened')):
                try:
                    await send_game_message(client, game, status="continue")
                except Exception as e:
                    logger.exception(f"Error after FloodWait updating game {gid}: {e}")
            return await cq.answer("⏳ Подождите и попробуйте снова.")
        except Exception as e:
            logger.exception(f"Error in mines_cell_callback for user {uid}: {e}")
            try:
                game = active_mines_games.get(gid) or load_game_db(gid)
                if game:
                    await send_game_message(client, game, status="continue")
            except Exception:
                pass
            return await cq.answer("Произошла ошибка. Попробуйте снова.")

# --- Забрать приз ---
@rate_limit
@app.on_callback_query(filters.regex(r'^mines_take_'))
async def mines_take_prize_callback(client, cq):
    uid = cq.from_user.id
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    if user_locks[uid].locked():
        return await cq.answer("⏳ Подожди...")
    async with user_locks[uid]:
        gid = cq.data.split('_')[2]
        game = active_mines_games.get(gid) or load_game_db(gid)
        if not game or game.get('user_id') != uid:
            return await cq.answer("Игра не найдена.")
        if game.get('exploded') or game.get('all_cells_opened'):
            return await cq.answer("Игра завершена.")

        bet = game.get('bet_amount', 0)
        win = int(bet * game.get('current_multiplier', 1.0))

        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(win), uid))
                db.execute("DELETE FROM mines_games WHERE game_id = ?", (gid,))
                db.commit()
        except Exception as e:
            logger.exception(f"DB error in take prize: {e}")

        await set_win_monet(uid, win)
        user_data = get_user_data(uid)
        balance = format_balance(user_data['money'])
        await edit_or_send(client, game['chat_id'], game.get('message_id'),
                           f"<b>🎉 Вы забрали приз: <code>{format_balance(win)}</code>\nБаланс: <code>{balance}</code></b>",
                           reply_markup=None)
        cleanup(gid)
        await cq.answer()

# --- Відмінити гру ---
@rate_limit
@app.on_callback_query(filters.regex(r'^mines_cancel_'))
async def mines_cancel_callback(client, cq):
    uid = cq.from_user.id
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    if user_locks[uid].locked():
        return await cq.answer("⏳ Подожди...")
    async with user_locks[uid]:
        gid = cq.data.split('_')[2]
        game = active_mines_games.get(gid) or load_game_db(gid)
        if not game or game.get('user_id') != uid:
            return await cq.answer("Игра не найдена.")
        if game.get('exploded') or game.get('all_cells_opened'):
            return await cq.answer("Игра завершена.")

        bet = game.get('bet_amount', 0)
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(bet), uid))
                db.execute("DELETE FROM mines_games WHERE game_id = ?", (gid,))
                db.commit()
        except Exception as e:
            logger.exception(f"DB error in cancel: {e}")

        user_data = get_user_data(uid)
        balance = format_balance(user_data['money'])
        await edit_or_send(client, game['chat_id'], game.get('message_id'),
                           f"<b>🚫 Игра отменена. Ставка возвращена. Баланс: <code>{balance}</code></b>",
                           reply_markup=None)
        cleanup(gid)
        await cq.answer()

# --- Адмінська команда поповнення (залишив як було) ---

@app.on_message(filters.command('uhhh'))
async def hhh(client, message):
    tg_id = message.from_user.id
    if tg_id not in API_OWNER:
        return

    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("❌ Использование: /uhhh <сумма> <user_id>")
        return

    bet_str = parts[1]
    send_id = parts[2]

    user_data = get_user_data(send_id)
    if not user_data:
        await message.reply("Пользователь не найден")
        return

    log_msgs = {
        "chat_id": message.chat.id,
        "message_id": message.id,
        "user_id": message.from_user.id,
        "username": message.from_user.username,
        "first_name": message.from_user.first_name,
        "text": message.text,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    await save_log(log_msgs)

    user_money = float(user_data['money'])
    money = parse_bet_amount(str(bet_str), user_money)
    money = float(money)

    try:
        with sqlite3.connect(DBB) as db:
            db.execute("UPDATE crash SET money = money - ? WHERE id = ?", (money, send_id))
            db.commit()
    except Exception as e:
        logger.exception(f"DB error in hhh: {e}")

    user = await client.get_users(send_id)
    await message.reply(f"✅ Вы успешно сняли с баланса пользователя {user.first_name} {format_balance(money)}💰")
    await app.send_message("https://t.me/qpdnsnnfd", f"""
<b>Момент: Снятия</b>
<b>Администратор:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Пользователь:</b> {user.first_name} (@{user.username} #{user.id})
<b>Сумма:</b> -{format_balance(money)} (-{money})
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>Фулл текст:</b> <pre lang='txt'>{message.text}</pre>
""")

@app.on_message(filters.command('hhh'))
async def hhh(client, message):
    tg_id = message.from_user.id
    if tg_id not in API_OWNER:
        return

    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("❌ Использование: /hhh <сумма> <user_id>")
        return

    bet_str = parts[1]
    send_id = parts[2]

    user_data = get_user_data(send_id)
    if not user_data:
        await message.reply("Пользователь не найден")
        return

    log_msgs = {
        "chat_id": message.chat.id,
        "message_id": message.id,
        "user_id": message.from_user.id,
        "username": message.from_user.username,
        "first_name": message.from_user.first_name,
        "text": message.text,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    await save_log(log_msgs)

    user_money = float(user_data['money'])
    money = parse_bet_amount(str(bet_str), user_money)
    money = float(money)

    try:
        with sqlite3.connect(DBB) as db:
            db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (money, send_id))
            db.commit()
    except Exception as e:
        logger.exception(f"DB error in hhh: {e}")

    user = await client.get_users(send_id)
    await message.reply(f"✅ Вы успешно пополнили баланс пользователя {user.first_name} на {format_balance(money)}💰")
    await app.send_message("https://t.me/qpdnsnnfd", f"""
<b>Момент: Выдача</b>
<b>Администратор:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Пользователь:</b> {user.first_name} (@{user.username} #{user.id})
<b>Сумма:</b> {format_balance(money)} ({money})
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>Фулл текст:</b> <pre lang='txt'>{message.text}</pre>
""")

# ============ BAN SYSTEM ============

is_banned_users = 'banned_users.json'


if not os.path.exists(is_banned_users):
    with open(is_banned_users, 'w') as f:
        json.dump([], f)

async def ban_user(user_id):
    with open(is_banned_users, 'r') as f:
        banned_users = json.load(f)

    user_id = int(user_id)  
    if user_id not in banned_users:
        banned_users.append(user_id)

        
        with open(is_banned_users, 'w') as f:
            json.dump(banned_users, f)

async def get_banned_users():
    with open(is_banned_users, 'r') as f:
        return json.load(f)

async def is_banned_user(user_id):
    banned_users = await get_banned_users()
    return int(user_id) in banned_users  

async def unban_user(user_id):
    with open(is_banned_users, 'r') as f:
        banned_users = json.load(f)

    user_id = int(user_id)  
    if user_id in banned_users:
        banned_users.remove(user_id)
        
        
        with open(is_banned_users, 'w') as f:
            json.dump(banned_users, f)

PAGE_SIZE = 50  
USER_PAGES = {}  

# async def get_top_users(order_by="money", order="DESC", limit=10):
#     """Берёт топ из базы по нужному полю."""
#     try:
#         with sqlite3.connect(DBB) as db:
#             cursor = db.cursor()
#             query = f"""
#                 SELECT id, username, first_name, money, status, win_count, lose_count
#                 FROM crash
#                 ORDER BY {order_by} {order}
#                 LIMIT ?
#             """
#             cursor.execute(query, (limit,))
#             return cursor.fetchall()
#     except sqlite3.Error as e:
#         print(f"Ошибка при получении топа пользователей: {e}")
#         return []

# async def format_top_message(users, title, order_by="money"):
#     if not users:
#         return "<i>В топе пока нет пользователей.</i>"

#     msg = f"<b>{title}</b>\n\n"
#     for i, (user_id, username, first_name, money, status, win_count, lose_count) in enumerate(users, 1):
#         name = username or first_name or f"ID {user_id}"
#         emoji = emojis[status] if 0 <= status < len(emojis) else ""
        
#         value = {
#             "money": f"{format_balance(money)}",
#             "win_count": f"{format_balance(win_count)}",
#             "lose_count": f"{format_balance(lose_count)}"
#         }.get(order_by, f"{format_balance(money)}")

#         msg += f"<b>{i})</b> {name} {emoji} — <i>{value}</i>\n"
#     return msg

# async def top_balance_command(client, message):
#     top_users = await get_top_users(order_by="money", order="DESC")
#     if not top_users:
#         await message.reply("<i>В топе пока нет пользователей.</i>")
#         return

#     text = await format_top_message(top_users, "<b>🏆 Топ 10 игроков по балансу:</b>", order_by="money")
#     buttons = InlineKeyboardMarkup([
#         [InlineKeyboardButton("Топ по выигрышам", callback_data="top_wins")],
#         [InlineKeyboardButton("Топ по проигрышам", callback_data="top_losses")]
#     ])
#     await message.reply(text, reply_markup=buttons)

# # Обработка кнопок топа
# @app.on_callback_query(filters.regex(r"^top_(balance|wins|losses)$"))
# async def top_balance_callback(client, callback):
#     category = callback.data.split("_")[1]

#     if category == "balance":
#         order_by = "money"
#         title = "<b>🏆 Топ 10 игроков по балансу:</b>"
#     elif category == "wins":
#         order_by = "win_count"
#         title = "<b>🏆 Топ 10 игроков по выигрышам:</b>"
#     else:
#         order_by = "lose_count"
#         title = "<b>🏆 Топ 10 игроков по проигрышам:</b>"

#     top_users = await get_top_users(order_by=order_by, order="DESC")
#     if not top_users:
#         await callback.answer("В топе пока нет пользователей.")
#         return

#     text = await format_top_message(top_users, title, order_by=order_by)

#     # Кнопки, исключая текущую активную
#     all_buttons = {
#         "balance": InlineKeyboardButton("Топ по балансу", callback_data="top_balance"),
#         "wins": InlineKeyboardButton("Топ по выигрышам", callback_data="top_wins"),
#         "losses": InlineKeyboardButton("Топ по проигрышам", callback_data="top_losses"),
#     }

#     buttons = [btn for key, btn in all_buttons.items() if key != category]
#     keyboard = InlineKeyboardMarkup([[btn] for btn in buttons])

#     try:
#         await callback.message.edit_text(text, reply_markup=keyboard)
#         await callback.answer()
#     except Exception as e:
#         print(f"Ошибка при обновлении сообщения: {e}")
#         await callback.answer("Ошибка обновления топа.")

@app.on_inline_query()
async def inline_ref_handler(client, inline_query: InlineQuery):
    query = inline_query.query.lower()

    if query in ["реф", "ref", "Реф", "Ref"]:
        user_id = inline_query.from_user.id
        bot_info = await client.get_me()
        BOT_USERNAME = bot_info.username

        ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
        text = "Приглашаю тебя играть вместе со мной!"

        # Создаём кнопку
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Присоединиться", url=ref_link)]
        ])

        await inline_query.answer(
            results=[
                InlineQueryResultArticle(
                    title="Реферальная ссылка",
                    description="Отправить приглашение другу",
                    input_message_content=InputTextMessageContent(text),
                    reply_markup=keyboard
                )
            ],
            cache_time=0  # 0 чтобы не кэшировался, можешь изменить на 3600
        )
    elif query in ["топ", "top", "tb"]:
        top_users = await get_top_users(order_by="money", order="DESC")
        if not top_users:
            text = "<i>В топе пока нет пользователей.</i>"
        else:
            text = await format_top_message(top_users, "🏆 Топ 10 игроков по балансу:", order_by="money")

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Топ по выигрышам", callback_data="top_wins")],
            [InlineKeyboardButton("Топ по проигрышам", callback_data="top_losses")]
        ])

        await inline_query.answer(
            results=[
                InlineQueryResultArticle(
                    title="🏆 Топ игроков",
                    description="Посмотреть топ 10 по балансу",
                    input_message_content=InputTextMessageContent(text),
                    reply_markup=keyboard
                )
            ],
            cache_time=0
        )

    else:
        text = """
Помощь по Инлайн Запросам"""
        await inline_query.answer(
            results=[
                InlineQueryResultArticle(
                    title="Помощь",
                    description="Помощь по Инлайн Запросам",
                    input_message_content=InputTextMessageContent(text),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("Помощь", callback_data="help")]
                        ]
                    )
                )
            ],
            cache_time=0
        )




async def my_refs_handler(client, message):
    user_id = message.from_user.id

    db = sqlite3.connect(DBB)
    cursor = db.cursor()

    cursor.execute("SELECT ref_count FROM ref_system WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    db.close()

    BOT1 = await app.get_me()
    BOT_USERNAME = BOT1.username
    await message.reply(f"<b>🔗 Ваша ссылка:</b><code>https://t.me/{BOT_USERNAME}?start=ref_{user_id}</code>\n💰 <b>За одного приглашенного человека вы получите 3.000.000 монет!</b>")

async def start_handler(client, message):
    user_id = message.from_user.id
    args = message.text.split()  # Разбиваем команду на части
    print(f"Received /start with args: {args}")

    # Базовый случай: просто /start
    if len(args) == 1:
        await message.reply("Привет! Я бот, который поможет тебе играть в мины и добывать ресурсы.\n"
                           "Используй /hb или 'список' для просмотра всех команд.")
        return

    # Обработка случая с рефералом: /start ref_{id}
    if len(args) == 2 and args[1].startswith("ref_"):
        ref_id = args[1].replace("ref_", "")  # Извлекаем ID реферала
        print(ref_id)
        # Защита от ошибок
        try:
            ref_id = int(ref_id)
        except ValueError:
            ref_id = None

        # Проверяем, зарегистрирован ли пользователь
        if not await check_user(user_id):
            register_user(user_id, message.from_user.username, message.from_user.first_name)

            # Проверяем, что пользователь не вводит свою же ссылку
            if ref_id and ref_id != user_id and await check_user(ref_id):
                # Добавляем запись в ref_system
                db = sqlite3.connect(DBB)
                cursor = db.cursor()
                cursor.execute("SELECT * FROM ref_system WHERE user_id = ?", (user_id,))
                if cursor.fetchone() is None:
                    cursor.execute("INSERT INTO ref_system (user_id, invited_by) VALUES (?, ?)", (user_id, ref_id))
                    cursor.execute("UPDATE ref_system SET ref_count = ref_count + 1 WHERE user_id = ?", (ref_id,))
                    cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (3000000, user_id))
                    db.commit()
                    await app.send_message(ref_id, f"Пользователь {message.from_user.first_name} ({user_id}) перешел по вашему рефералу.\nВы получаете 3.000.000 монет за реферальную ссылку.")
                    await message.reply("Привет! Я бот, который поможет тебе играть в мины и добывать ресурсы.\n"
                           "Используй /hb или 'список' для просмотра всех команд.")

                else:
                    await message.reply("Вы уже зарегистрированы. Реферальная ссылка не будет учтена.")
                db.close()
            else:
                await message.reply("Вы не можете использовать свою ссылку или пользователь не найден.")
        else:
            await message.reply("Вы уже зарегистрированы.")

    # Обработка случая с промокодом: /start promo_{promo_name}
    if len(args) == 2 and args[1].startswith("promo_"):
        promo_name = args[1].replace("promo_", "")  # Извлекаем имя промокода
        
        # Проверяем, зарегистрирован ли пользователь
        if not await check_user(user_id):
            register_user(user_id, message.from_user.username, message.from_user.first_name)
            await message.reply("Вы успешно зарегистрированы!\nТеперь активируем ваш промокод...")

        # Проверяем бан
        if await is_banned_user(user_id):
            await message.reply("Вы забанены и не можете активировать промокоды.")
            return

        # Проверяем промокод в базе
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        cursor.execute("SELECT money, activations FROM promos WHERE name = ?", (promo_name,))
        promo = cursor.fetchone()

        if not promo:
            await message.reply("Промокод не найден.")
            db.close()
            return

        money, activations = promo

        # Проверяем, использовал ли пользователь этот промокод
        cursor.execute("SELECT 1 FROM user_promos WHERE user_id = ? AND promo_name = ?", (user_id, promo_name))
        if cursor.fetchone():
            await message.reply("Вы уже использовали этот промокод.")
            db.close()
            return

        # Проверяем количество активаций
        if activations <= 0:
            await message.reply("У этого промокода закончились активации.")
            cursor.execute("DELETE FROM promos WHERE name = ?", (promo_name,))
            db.commit()
            db.close()
            return

        # Активируем промокод
        cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(money), user_id))
        cursor.execute("INSERT INTO user_promos (user_id, promo_name) VALUES (?, ?)", (user_id, promo_name))
        cursor.execute("UPDATE promos SET activations = activations - 1 WHERE name = ?", (promo_name,))
        db.commit()

        # Проверяем, нужно ли удалить промокод
        cursor.execute("SELECT activations FROM promos WHERE name = ?", (promo_name,))
        remaining_activations = cursor.fetchone()[0]
        if remaining_activations <= 0:
            cursor.execute("DELETE FROM promos WHERE name = ?", (promo_name,))
            db.commit()

        db.close()

        await message.reply(f"Промокод '{promo_name}' успешно активирован!\n"
                          f"Начислено {money} монет.\n"
                          f"Ваш баланс: {format_balance(get_user_data(user_id)['money'])}")

from random import sample, choices
from string import ascii_letters, digits

treasure_locks = {}

# @app.on_message(filters.command("клад"))
@rate_limit
async def treasure_game_command(client, message):
    args = message.text.split()
    if len(args) != 3:
        await message.reply_text("Пример: <code>клад 10000 2</code> (ставка, кол-во кладов от 1 до 4)")
        return

    user_id = message.from_user.id
    user_data = get_user_data(user_id)
    if not user_data:
        await message.reply_text("Вы не зарегистрированы. /ss")
        return

    bet = parse_bet_amount(args[1], user_data['money'])
    treasures = int(args[2])
    if not bet or bet <= 0 or treasures < 1 or treasures > 4:
        await message.reply_text("Ставка некорректна или кол-во кладов не от 1 до 4.")
        return

    if user_data['money'] < bet:
        await message.reply_text("Недостаточно монет.")
        return

    game_id = ''.join(choices(ascii_letters + digits, k=8))

    positions = [(i, j) for i in range(4) for j in range(4)]
    treasure_positions = sample(positions, treasures)
    board = {
        "treasures": treasure_positions,
        "opened": []
    }

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet), user_id))
    cursor.execute("INSERT INTO treasure_games (id, user_id, bet, treasures, attempts_left, found, board) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (game_id, user_id, bet, treasures, 3, 0, json.dumps(board)))
    db.commit()
    db.close()

    await send_treasure_board(client, message.chat.id, game_id)


# ================== КОМАНДЫ ==================
@app.on_message(filters.command("farm") | filters.regex(r"Фарма", flags=re.IGNORECASE) | filters.regex(r"Ферма", flags=re.IGNORECASE))
async def farm_menu(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("❌ Вы не зарегистрированы. Используйте /ss")
        return
    
    # Выводим ферму с балансом и устройствами
    farm = get_user_farm(user_id)
    text = "🏦 <b>Ваша ферма</b>\n\n"
    total_income = 0
    for dev_id, dev in farm_devices.items():
        count = farm[dev_id]["count"]
        income = dev["income"] * count
        total_income += income
        text += f"{dev['name']}: <b>{count}</b> шт → {income:,} 💰/час\n"
    text += f"\n⏳ Общий доход: <b>{total_income:,} 💰/час</b>"
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Забрать доход", callback_data=f"claim_{user_id}")],
        [InlineKeyboardButton("💳 Купить устройства", callback_data=f"buuy_menu_{user_id}")]
    ])
    await message.reply(text, reply_markup=kb)

@app.on_callback_query(filters.regex(r"claim_"))
async def claim_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if f"claim_{user_id}" != callback_query.data:
        await callback_query.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    income = claim_income(user_id)
    if income > 0:
        await callback_query.message.edit_text(f"📥 Вы забрали {income:,} 💰",
                                               reply_markup=InlineKeyboardMarkup(
                                                   [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_farm_{user_id}")]]
                                               ))
    else:
        await callback_query.message.edit_text("⌛ Пока нечего забирать.",
                                               reply_markup=InlineKeyboardMarkup(
                                                   [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_farm_{user_id}")]]
                                               ))

@app.on_callback_query(filters.regex(r"buuy_menu_"))
async def buy_menu(client, callback_query):
    user_id = callback_query.from_user.id
    if f"buuy_menu_{user_id}" != callback_query.data:
        await callback_query.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    
    # Покупка устройств с фермерскими устройствами
    kb = []
    for dev_id, dev in farm_devices.items():
        price = get_device_price(user_id, dev_id)
        kb.append([InlineKeyboardButton(f"{dev['name']} — {price:,} 💰", callback_data=f"buuy_{dev_id}_{user_id}")])
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_farm_{user_id}")])
    
    await callback_query.message.edit_text("💳 <b>Выберите устройство для покупки</b>:", reply_markup=InlineKeyboardMarkup(kb))

@app.on_callback_query(filters.regex(r"buuy_"))
async def buy_device(client, callback_query):
    # Разбираем данные callback
    parts = callback_query.data.split("_")
    
    # Проверка на корректный формат данных
    if len(parts) != 3:
        await callback_query.answer("❌ Неверный формат данных.", show_alert=True)
        return
    
    dev_id, user_id = parts[1], int(parts[2])

    # Проверка на совпадение user_id с отправителем
    if user_id != callback_query.from_user.id:
        await callback_query.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    
    # Получаем данные пользователя
    user_data = get_user_data(user_id)
    if user_data is None:
        await callback_query.answer("❌ Не удалось получить данные пользователя.", show_alert=True)
        return
    
    # Получаем цену устройства
    price = get_device_price(user_id, dev_id)
    if user_data["money"] < price:
        await callback_query.answer("Недостаточно монет 💰", show_alert=True)
        return
    
    # Сохраняем покупку
    farm = get_user_farm(user_id)
    new_count = farm[dev_id]["count"] + 1
    update_user_farm(user_id, dev_id, new_count)
    
    # Обновляем баланс пользователя
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(price), user_id))
    db.commit()
    db.close()
    
    # Перерисовываем клавиатуру с обновленными ценами
    kb = []
    for d_id, dev in farm_devices.items():
        new_price = get_device_price(user_id, d_id)
        kb.append([InlineKeyboardButton(f"{dev['name']} — {new_price:,} 💰", callback_data=f"buuy_{d_id}_{user_id}")])
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_farm_{user_id}")])
    
    await callback_query.message.edit_text(
        f"🎉 Вы купили {farm_devices[dev_id]['name']}!\nТеперь у вас {new_count} шт.\n\n"
        f"Выберите следующее устройство для покупки:",
        reply_markup=InlineKeyboardMarkup(kb)
    )

@app.on_callback_query(filters.regex(r"back_farm_"))
async def back_farm(client, callback_query):
    user_id = callback_query.from_user.id
    if f"back_farm_{user_id}" != callback_query.data:
        await callback_query.answer("❌ Не твоя кнопка!", show_alert=True)
        return
    
    # Показываем снова меню фермы
    farm = get_user_farm(user_id)
    text = "🏦 <b>Ваша ферма</b>\n\n"
    total_income = 0
    for dev_id, dev in farm_devices.items():
        count = farm[dev_id]["count"]
        income = dev["income"] * count
        total_income += income
        text += f"{dev['name']}: <b>{count}</b> шт → {income:,} 💰/час\n"
    text += f"\n⏳ Общий доход: <b>{total_income:,} 💰/час</b>"
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Забрать доход", callback_data=f"claim_{user_id}")],
        [InlineKeyboardButton("💳 Купить устройства", callback_data=f"buuy_menu_{user_id}")]
    ])
    await callback_query.message.edit_text(text, reply_markup=kb)



async def send_treasure_board(client, chat_id, game_id, edit_message=None):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT board, attempts_left, found, treasures, user_id FROM treasure_games WHERE id = ?", (game_id,))
    data = cursor.fetchone()
    db.close()

    if not data:
        return

    board, attempts_left, found, treasures, user_id = json.loads(data[0]), data[1], data[2], data[3], data[4]
    treasures_set = [tuple(x) for x in board["treasures"]]
    opened = [tuple(x) for x in board["opened"]]

    buttons = []
    for i in range(4):
        row = []
        for j in range(4):
            pos = (i, j)
            if pos in opened:
                label = "🎁" if pos in treasures_set else "❌"
            else:
                label = "⬜"
            row.append(InlineKeyboardButton(label, callback_data=f"treasure_{game_id}_{i}_{j}"))
        buttons.append(row)

    buttons.append([InlineKeyboardButton("💰 Забрать", callback_data=f"treasure_take_{game_id}")])
    text = f"🎯 <b>Клад</b>\nПопыток: <b>{attempts_left}</b>\nНайдено: <b>{found}</b> из {treasures}"
    markup = InlineKeyboardMarkup(buttons)

    if edit_message:
        await edit_message.edit_text(text, reply_markup=markup)
    else:
        await client.send_message(chat_id, text, reply_markup=markup)


@app.on_callback_query(filters.regex(r"^treasure_([a-zA-Z0-9]+)_(\d)_(\d)$"))
@rate_limit
async def handle_treasure_click(client, callback):
    game_id, i, j = callback.data.split("_")[1:]
    i, j = int(i), int(j)

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT user_id, bet, treasures, attempts_left, found, board FROM treasure_games WHERE id = ?", (game_id,))
    data = cursor.fetchone()
    if not data:
        await callback.message.edit_text("Игра не найдена.")
        return

    user_id, bet, treasures, attempts_left, found, board_raw = data
    if callback.from_user.id != user_id:
        await callback.answer("Не твоя игра.", show_alert=True)
        return

    if game_id not in treasure_locks:
        treasure_locks[game_id] = asyncio.Lock()

    async with treasure_locks[game_id]:
        board = json.loads(board_raw)
        board["treasures"] = [tuple(x) for x in board["treasures"]]
        board["opened"] = [tuple(x) for x in board["opened"]]
        pos = (i, j)

        if pos in board["opened"]:
            await callback.answer("Уже открыто.")
            return

        board["opened"].append(pos)
        msg = "🎁 Клад найден!" if pos in board["treasures"] else "❌ Мимо."
        if pos in board["treasures"]:
            found += 1
        else:
            attempts_left -= 1

        if found == treasures:
            win = int(bet * treasure_multiplier(treasures))
            cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(win), user_id))
            cursor.execute("DELETE FROM treasure_games WHERE id = ?", (game_id,))
            db.commit()
            db.close()
            await callback.message.edit_text(f"🎉 Все клады найдены! Вы выиграли <b>{format_balance(win)}</b> монет!")
            return

        if attempts_left <= 0:
            cursor.execute("DELETE FROM treasure_games WHERE id = ?", (game_id,))
            db.commit()
            db.close()
            reveal = "🪙 Клады были тут:\n" + "\n".join([f"({x},{y})" for x, y in board["treasures"]])
            await callback.message.edit_text("😞 Попытки закончились. Вы проиграли.\n\n" + reveal)
            return

        cursor.execute("UPDATE treasure_games SET board = ?, attempts_left = ?, found = ? WHERE id = ?",
                       (json.dumps(board), attempts_left, found, game_id))
        db.commit()
        db.close()

        await callback.answer(msg)
        await send_treasure_board(client, callback.message.chat.id, game_id, edit_message=callback.message)


@app.on_callback_query(filters.regex(r"^treasure_take_([a-zA-Z0-9]+)$"))
@rate_limit
async def handle_treasure_take(client, callback):
    game_id = callback.data.split("_")[2]

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT user_id, bet, treasures, found FROM treasure_games WHERE id = ?", (game_id,))
    data = cursor.fetchone()
    if not data:
        await callback.answer("Игра не найдена.")
        return

    user_id, bet, treasures, found = data
    if callback.from_user.id != user_id:
        await callback.answer("Не твоя кнопка.")
        return

    if game_id not in treasure_locks:
        treasure_locks[game_id] = asyncio.Lock()

    async with treasure_locks[game_id]:
        k = treasure_multiplier(treasures)
        win = int(bet * (found / treasures) * k)

        cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(win), user_id))
        cursor.execute("DELETE FROM treasure_games WHERE id = ?", (game_id,))
        db.commit()
        db.close()

        await callback.message.edit_text(f"💰 Забрано! Вы получили <b>{format_balance(win)}</b> монет (за {found}/{treasures} кладов)")


@app.on_message(filters.command("клад_отладка"))
@rate_limit
async def debug_treasure_game(client, message):
    user_id = message.from_user.id
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT id, board FROM treasure_games WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
    data = cursor.fetchone()
    db.close()

    if not data:
        await message.reply_text("Нет активной игры.")
        return

    game_id, board_raw = data
    board = json.loads(board_raw)
    treasures = [tuple(x) for x in board["treasures"]]
    await message.reply_text(f"🧭 Игра ID: <code>{game_id}</code>\nКлады:\n" + "\n".join([f"({i}, {j})" for i, j in treasures]))


def treasure_multiplier(treasures):
    return {
        1: 3.5,
        2: 2.2,
        3: 1.7,
        4: 1.3
    }.get(treasures, 1.0)

@app.on_message(filters.command("клад_отладка"))
@rate_limit
async def debug_treasure_game(client, message):
    user_id = message.from_user.id
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT board FROM treasure_games WHERE user_id = ?", (user_id,))
    data = cursor.fetchone()
    db.close()

    if not data:
        await message.reply_text("Нет активной игры.")
        return

    board = json.loads(data[0])
    treasures = [tuple(x) for x in board["treasures"]]
    await message.reply_text("🧭 Клады находятся тут:\n" + "\n".join([f"({i}, {j})" for i, j in treasures]))


def treasure_multiplier(treasures):
    return {
        1: 3.5,
        2: 2.2,
        3: 1.7,
        4: 1.3
    }.get(treasures, 1.0)


PAGE_SIZE = 50  # Количество пользователей на странице
USER_PAGES = {}  # Хранение текущей страницы для каждого пользователя
USER_SORT = {}   # Хранение текущей сортировки для каждого пользователя
USER_FILTER = {} # Хранение текущего фильтра для каждого пользователя

@app.on_message(filters.command('users'))
async def get_all_users(client, message):
    tg_id = message.from_user.id
    if tg_id not in API_OWNER:
        await message.reply("У вас нет доступа к этой команде.")
        return

    # Инициализируем страницу, сортировку и фильтр
    USER_PAGES[tg_id] = 0
    USER_SORT[tg_id] = "default"  # По умолчанию сортировка "по умолчанию"
    USER_FILTER[tg_id] = "all"    # По умолчанию фильтр "все"

    users = await fetch_users_data(tg_id)
    if not users:
        await message.reply("Пользователей нет в базе.")
        return

    await send_users_page(client, message, tg_id, users, 0)

# Функция для получения пользователей с учетом сортировки и фильтра
async def fetch_users_data(tg_id):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()

    # Базовый запрос
    base_query = "SELECT id, username, first_name, money, status FROM crash"

    # Применяем фильтр
    filter_type = USER_FILTER.get(tg_id, "all")
    if filter_type == "banned":
        where_clause = " WHERE status = 1"
    elif filter_type == "rich":
        where_clause = " WHERE money > 1000"
    else:
        where_clause = ""

    # Применяем сортировку
    sort_type = USER_SORT.get(tg_id, "default")
    if sort_type == "money_high":
        order_by = " ORDER BY money DESC"
    elif sort_type == "money_low":
        order_by = " ORDER BY money ASC"
    elif sort_type == "active":
        order_by = " ORDER BY status ASC"
    elif sort_type == "inactive":
        order_by = " ORDER BY status DESC"
    else:
        order_by = ""

    query = f"{base_query}{where_clause}{order_by}"
    cursor.execute(query)
    users = cursor.fetchall()
    db.close()
    return users

# Функция отправки списка пользователей постранично
async def send_users_page(client, message, tg_id, users, page):
    start_idx = page * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    users_page = users[start_idx:end_idx]

    # Определяем текущий фильтр и сортировку для отображения
    current_filter = USER_FILTER.get(tg_id, "all")
    filter_display = {
        "all": "Все",
        "banned": "Заблокированные",
        "rich": "С деньгами > 1000"
    }
    current_sort = USER_SORT.get(tg_id, "default")
    sort_display = {
        "default": "По умолчанию",
        "money_high": "Деньги (много)",
        "money_low": "Деньги (мало)",
        "active": "Активные",
        "inactive": "Неактивные",
    }

    # Формируем текст сообщения
    total_pages = ((len(users) - 1) // PAGE_SIZE) + 1
    text = (f"👥 <b>кол-во игроков:</b> {len(users)}\n"
            f"<b>Страница:</b> {page + 1}/{total_pages}\n"
            f"Фильтр: {filter_display[current_filter]}\n"
            f"Сортировка: {sort_display[current_sort]}\n\n")

    # Добавляем пользователей с нумерацией
    emojis = ["", "👍", "😀", "🤯", "😎", "👽", "👾", "🤖", "👻", "👑", "🎩", "🎰", "🎀", "🐍", "🦈"]

    # Добавляем пользователей с нумерацией
    for idx, (user_id, username, first_name, money, status) in enumerate(users_page, start=start_idx + 1):
        display_name = f"@{username}" if username else f"ID {user_id}"
        # Определяем эмодзи статуса: если status == 0, то 🔴, иначе берем из списка emojis
        status_emoji = "🔴" if status == 0 else emojis[status] if 0 < status < len(emojis) else "❓"
        text += f"{idx}) {display_name} [{user_id}] - {money}💰 - {status_emoji}\n"

    # Создаем кнопки навигации
    buttons = []
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅ Прошлая страница", callback_data=f"users_page:{page - 1}"))
    if end_idx < len(users):
        nav_buttons.append(InlineKeyboardButton("След. страница ➡", callback_data=f"users_page:{page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    # Добавляем кнопки фильтрации
    filter_buttons = [
        [InlineKeyboardButton(f"Все{' ✅' if current_filter == 'all' else ''}", callback_data="users_filter:all"),
         InlineKeyboardButton(f"Заблокированные{' ✅' if current_filter == 'banned' else ''}", callback_data="users_filter:banned")],
        [InlineKeyboardButton(f"С деньгами > 1000{' ✅' if current_filter == 'rich' else ''}", callback_data="users_filter:rich")]
    ]
    buttons.extend(filter_buttons)

    # Добавляем кнопки сортировки
    sort_buttons = [
        [InlineKeyboardButton(f"Деньги (много){' ✅' if current_sort == 'money_high' else ''}", callback_data="users_sort:money_high"),
         InlineKeyboardButton(f"Деньги (мало){' ✅' if current_sort == 'money_low' else ''}", callback_data="users_sort:money_low")],
        [InlineKeyboardButton(f"Активные{' ✅' if current_sort == 'inactive' else ''}", callback_data="users_sort:inactive"),
         InlineKeyboardButton(f"Неактивные{' ✅' if current_sort == 'active' else ''}", callback_data="users_sort:active")],
        [InlineKeyboardButton(f"По умолчанию{' ✅' if current_sort == 'default' else ''}", callback_data="users_sort:default")]
    ]
    buttons.extend(sort_buttons)

    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
    # Если это ответ на команду, отправляем новое сообщение, иначе редактируем существующее
    if message.text and message.text.startswith("/users"):
        await message.reply_text(text, reply_markup=reply_markup, disable_web_page_preview=True)
    else:
        await message.edit_text(text, reply_markup=reply_markup, disable_web_page_preview=True)

# Обработчик кнопок для смены страниц
@app.on_callback_query(filters.regex(r"users_page:(\d+)"))
async def change_users_page(client, callback_query):
    page = int(callback_query.matches[0].group(1))
    tg_id = callback_query.from_user.id

    if tg_id not in API_OWNER:
        await callback_query.answer("У вас нет доступа к этой команде!", show_alert=True)
        return

    users = await fetch_users_data(tg_id)
    if not users:
        await callback_query.answer("Пользователей нет в базе.", show_alert=True)
        return

    USER_PAGES[tg_id] = page
    await send_users_page(client, callback_query.message, tg_id, users, page)
    await callback_query.answer()

# Обработчик кнопок фильтрации
@app.on_callback_query(filters.regex(r"users_filter:(\w+)"))
async def change_users_filter(client, callback_query):
    filter_type = callback_query.matches[0].group(1)
    tg_id = callback_query.from_user.id

    if tg_id not in API_OWNER:
        await callback_query.answer("У вас нет доступа к этой команде!", show_alert=True)
        return

    # Проверяем, не выбран ли уже этот фильтр
    current_filter = USER_FILTER.get(tg_id, "all")
    if current_filter == filter_type:
        await callback_query.answer("Этот фильтр уже выбран!", show_alert=True)
        return

    # Обновляем фильтр
    USER_FILTER[tg_id] = filter_type
    USER_PAGES[tg_id] = 0  # Сбрасываем страницу на первую

    users = await fetch_users_data(tg_id)
    if not users:
        await callback_query.answer("Пользователей по этому фильтру нет.", show_alert=True)
        return

    await send_users_page(client, callback_query.message, tg_id, users, 0)
    await callback_query.answer(f"Фильтр изменен на: {filter_type}")

# Обработчик кнопок сортировки
@app.on_callback_query(filters.regex(r"users_sort:(\w+)"))
async def change_users_sort(client, callback_query):
    sort_type = callback_query.matches[0].group(1)
    tg_id = callback_query.from_user.id

    if tg_id not in API_OWNER:
        await callback_query.answer("У вас нет доступа к этой команде!", show_alert=True)
        return

    # Проверяем, не выбрана ли уже эта сортировка
    current_sort = USER_SORT.get(tg_id, "default")
    if current_sort == sort_type:
        await callback_query.answer("Эта сортировка уже выбрана!", show_alert=True)
        return

    # Обновляем сортировку
    USER_SORT[tg_id] = sort_type
    USER_PAGES[tg_id] = 0  # Сбрасываем страницу на первую

    users = await fetch_users_data(tg_id)
    if not users:
        await callback_query.answer("Пользователей нет в базе.", show_alert=True)
        return

    await send_users_page(client, callback_query.message, tg_id, users, 0)
    await callback_query.answer(f"Сортировка изменена на: {sort_type}")

    # (""" CREATE TABLE IF NOT EXISTS drill (
    #                user_id INTEGER PRIMARY KEY, 0
    #                motor_lvl INTEGER DEFAULT 1 - уровень мотора, 1
    #                drill_head_lvl INTEGER DEFAULT 1 - уровень дрель, 2
    #                frame INTEGER DEFAULT 1 - каркас, 3
    #                power_source INTEGER DEFAULT 1 - питание, 4
    #                handle INTEGER DEFAULT 0 - держатель, 5
    #                cooling INTEGER DEFAULT 0 - охлаждение, 6
    #                gearbox INTEGER DEFAULT 0 - редуктор, 7
    #                oil INTEGER DEFAULT 0 - масло, 8
    #                energy INTEGER DEFAULT 10 - энергия, 9
    #                max_energy INTEGER DEFAULT 10 - максимальная энергия, 10
    #                max_oil_engine INTEGER DEFAULT 10 - максимальное масло, 11
    #                max_cooling_engine INTEGER DEFAULT 10 - максимальное охлаждение, 12
    #                heal_drill_engine INTEGER DEFAULT 10 - лечение дрель 13,
    #                drill_state INTEGER DEFAULT 0 - состояние дрель, 14 [0 - ]
    #                ) """)
    
@app.on_message(filters.command("buy_drill"))
async def buy_drill_command(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    if await is_banned_user(user_id):
        await message.reply_text("Вы забанены.")
        return
    
    drill_data = await get_user_drill_data(user_id)
    user_data = get_user_data(user_id)
    
    # Проверяем, есть ли дрель
    if drill_data:  # Если запись есть, значит дрель уже куплена
        await message.reply("У вас уже есть дрель!")
        return
    
    # Стоимость дрели
    cost = 5000
    if user_data["money"] < cost:
        await message.reply(f"Недостаточно монет! Нужно {cost} монет.")
        return
    
    # Покупка дрели
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(cost), user_id))
    if drill_data:
        # Если запись уже есть (но дрель "не куплена"), обновляем
        cursor.execute("""
            UPDATE drill 
            SET motor_lvl = 1, drill_head_lvl = 1, frame = 1, power_source = 1, 
                handle = 0, cooling = 0, gearbox = 0, oil = 0, 
                energy = 10, max_energy = 10, max_oil_engine = 10, 
                max_cooling_engine = 10, heal_drill_engine = 10
            WHERE user_id = ?
        """, (user_id,))
    else:
        # Если записи нет, создаем новую
        cursor.execute("""
            INSERT INTO drill (user_id, motor_lvl, drill_head_lvl, frame, power_source, 
                              handle, cooling, gearbox, oil, energy, max_energy, 
                              max_oil_engine, max_cooling_engine, heal_drill_engine)
            VALUES (?, 1, 1, 1, 1, 0, 0, 0, 0, 10, 10, 10, 10, 10)
        """, (user_id,))
    db.commit()
    db.close()
    
    await message.reply(f"🎉 Вы купили дрель за {cost} монет! Теперь можно добывать ресурсы с помощью /mine.")

SEND_BATCH = 30       # сколько сообщений отправить перед паузой
SLEEP_TIME = 2        # сколько секунд спать после SEND_BATCH

@app.on_message(filters.command("rass"))
async def rass_command(client, message):
    if message.from_user.id not in API_OWNER:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Используй: /rass <текст>")
        return

    text = parts[1]
    await message.reply("✅ Рассылка началась в фоне.")
    asyncio.create_task(start_rassilka(client, text))
    await app.send_message("-1004869586301", f"""
<b>Момент: Рассылка</b>
<b>Администратор:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
==================
<b>Текст:</b> {text}
==================
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>Фулл текст:</b> <pre lang='txt'>{message.text}</pre>
""")

async def start_rassilka(client, text):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT id FROM crash")
    users = [row[0] for row in cursor.fetchall()]
    db.close()

    total = len(users)
    success = 0
    failed = 0

    print(f"=== Рассылка: {total} пользователей ===")

    for i, user_id in enumerate(users, start=1):
        try:
            await client.send_message(user_id, text)
            success += 1
            print(f"[✅] {user_id}")
        except Exception as e:
            failed += 1
            print(f"[❌] {user_id} — {str(e)}")

    print(f"Готово. ✅: {success} ❌: {failed}")

@app.on_message(filters.command("gt"))
async def gt(client, message):
    tg_id = message.from_user.id
    if tg_id not in API_OWNER:
        return
    await message.reply(message)  
    await message.reply(f"<emoji id=5431376038628171216>📊</emoji>")  

duel_data = {}  # Хранение данных дуэли
duel_locks = {}  # Блокировки для асинхронной обработки

from asyncio import Lock
import random
import asyncio

async def duel_command(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    if await is_banned_user(user_id):
        await message.reply("Вы забанены.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Используйте: дуэль <ставка>")
        return

    bet_amount_str = parts[1]
    user_data = get_user_data(user_id)
    bet_amount = parse_bet_amount(bet_amount_str, user_data['money'])

    if bet_amount is None:
        await message.reply("Неправильный формат ставки.")
        return
    if bet_amount < 10:
        await message.reply("Минимальная ставка — 10 монет.")
        return
    if user_data['money'] < bet_amount:
        await message.reply("Недостаточно монет на балансе.")
        return

    for game in duel_data.values():
        if game['player1_id'] == user_id or game.get('player2_id') == user_id:
            # отменить дуэль
            btn = InlineKeyboardButton("❌ Отменить дуэль", callback_data=f"duel_cancel_{game['game_id']}")
            await message.reply("У вас уже есть активная дуэль!", reply_markup=InlineKeyboardMarkup([[btn]]))
            return

    game_id = str(randint(100000, 99999999))
    duel_data[game_id] = {
        "game_id": game_id,
        "player1_id": user_id,
        "player2_id": None,
        "player1_num": None,
        "player2_num": None,
        "bet": bet_amount,
        "chat_id": message.chat.id,
        "message_id": None,
        "state": "select_number",
        "round": 1,
        "timeout": asyncio.get_event_loop().time() + 180,
        "used_dice": []
    }
    duel_locks[game_id] = Lock()

    try:
        with sqlite3.connect(DBB) as db:
            db.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet_amount), user_id))
            db.commit()
    except sqlite3.Error:
        await message.reply("Ошибка базы данных. Попробуйте позже.")
        return

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(str(i), callback_data=f"duel_select_{game_id}_{i}") for i in range(1, 4)],
        [InlineKeyboardButton(str(i), callback_data=f"duel_select_{game_id}_{i}") for i in range(4, 7)]
    ])
    msg = await message.reply("Вы создаёте игру. Выберите число от 1 до 6:", reply_markup=markup)
    duel_data[game_id]["message_id"] = msg.id

# Обработка выбора числа создателем
@app.on_callback_query(filters.regex(r"^duel_select_"))
async def duel_select_callback(client, cq):
    user_id = cq.from_user.id
    if not await check_user(user_id) or await is_banned_user(user_id):
        await cq.answer("Вы не можете участвовать в дуэли.")
        return

    parts = cq.data.split("_")
    if len(parts) != 4:
        await cq.answer("Неверный формат команды.")
        return
    game_id, number = parts[2], int(parts[3])
    game = duel_data.get(game_id)

    if not game:
        await cq.answer("Дуэль не найдена.")
        return
    if game["player1_id"] != user_id:
        await cq.answer("Это не ваша дуэль!")
        return
    if game["state"] != "select_number":
        await cq.answer("Вы уже выбрали число!")
        return
    if not 1 <= number <= 6:
        await cq.answer("Неверное число.")
        return

    async with duel_locks[game_id]:
        if game_id not in duel_data:
            await cq.answer("Дуэль завершена.")
            return
        game["player1_num"] = number
        game["state"] = "awaiting_opponent"

        user = await client.get_users(user_id)
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Принять вызов", callback_data=f"duel_accept_{game_id}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"duel_cancel_{game_id}")]
        ])
        await client.edit_message_text(
            chat_id=game["chat_id"],
            message_id=game["message_id"],
            text=f"👑 Игрок {user.first_name} вызывает на дуэль!\n"
                 f"💰 Ставка: {format_balance(game['bet'])} монет\n"
                 f"🎲 Число игрока: {number}",
            reply_markup=markup
        )
    await cq.answer()

# Отмена дуэли создателем
@app.on_callback_query(filters.regex(r"^duel_cancel_"))
async def duel_cancel_callback(client, cq):
    user_id = cq.from_user.id
    if not await check_user(user_id) or await is_banned_user(user_id):
        await cq.answer("Вы не можете участвовать в дуэли.")
        return

    parts = cq.data.split("_")
    if len(parts) != 3:
        await cq.answer("Неверный формат команды.")
        return
    game_id = parts[2]
    game = duel_data.get(game_id)

    if not game:
        await cq.answer("Дуэль не найдена.")
        return
    if game["player1_id"] != user_id:
        await cq.answer("Это не ваша дуэль!")
        return
    if game["state"] not in ["select_number", "awaiting_opponent"]:
        await cq.answer("Нельзя отменить на этом этапе!")
        return

    async with duel_locks[game_id]:
        if game_id not in duel_data:
            await cq.answer("Дуэль завершена.")
            return
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(game["bet"]), game["player1_id"]))
                db.commit()
        except sqlite3.Error:
            await cq.answer("Ошибка базы данных.")
            return

        await client.edit_message_text(
            chat_id=game["chat_id"],
            message_id=game["message_id"],
            text="❌ Дуэль отменена создателем!",
            reply_markup=None
        )
        del duel_data[game_id]
        del duel_locks[game_id]
    await cq.answer()
    try:
        await cq.message.edit("❌ Дуэль отменена создателем!")
    except Exception:
        pass

# Принятие вызова
@app.on_callback_query(filters.regex(r"^duel_accept_"))
async def duel_accept_callback(client, cq):
    user_id = cq.from_user.id
    if not await check_user(user_id) or await is_banned_user(user_id):
        await cq.answer("Вы не можете участвовать в дуэли.")
        return

    parts = cq.data.split("_")
    if len(parts) != 3:
        await cq.answer("Неверный формат команды.")
        return
    game_id = parts[2]
    game = duel_data.get(game_id)

    if not game:
        await cq.answer("Дуэль не найдена.")
        return
    if game["player1_id"] == user_id:
        await cq.answer("Вы не можете принять свою дуэль!")
        return
    if game["state"] != "awaiting_opponent":
        await cq.answer("Дуэль уже принята или завершена!")
        return
    if any(g["player1_id"] == user_id or g.get("player2_id") == user_id for g in duel_data.values()):
        await cq.answer("У вас уже есть активная дуэль!")
        return

    user_data = get_user_data(user_id)
    if user_data["money"] < game["bet"]:
        await cq.answer("Недостаточно монет!")
        return

    async with duel_locks[game_id]:
        if game_id not in duel_data:
            await cq.answer("Дуэль завершена.")
            return
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(game["bet"]), user_id))
                db.commit()
        except sqlite3.Error:
            await cq.answer("Ошибка базы данных.")
            return

        game["player2_id"] = user_id
        game["state"] = "opponent_select"

        player1 = await client.get_users(game["player1_id"])
        player2 = await client.get_users(user_id)
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(str(i), callback_data=f"duel_opponent_{game_id}_{i}") for i in range(1, 4)],
            [InlineKeyboardButton(str(i), callback_data=f"duel_opponent_{game_id}_{i}") for i in range(4, 7)]
        ])
        await client.edit_message_text(
            chat_id=game["chat_id"],
            message_id=game["message_id"],
            text=f"⚔️ Игрок {player1.first_name} вызывает на дуэль игрока {player2.first_name}!\n"
                 f"💰 Ставка: {format_balance(game['bet'])} монет\n"
                 f"🎲 Число игрока 1: {game['player1_num']}\n\n"
                 f"Игрок 2, выберите число:",
            reply_markup=markup
        )
    await cq.answer()

# Выбор числа вторым игроком
@app.on_callback_query(filters.regex(r"^duel_opponent_"))
async def duel_opponent_callback(client, cq):
    user_id = cq.from_user.id
    if not await check_user(user_id) or await is_banned_user(user_id):
        await cq.answer("Вы не можете участвовать в дуэли.")
        return

    parts = cq.data.split("_")
    if len(parts) != 4:
        await cq.answer("Неверный формат команды.")
        return
    game_id, number = parts[2], int(parts[3])
    game = duel_data.get(game_id)

    if not game:
        await cq.answer("Дуэль не найдена.")
        return
    if game["player2_id"] != user_id:
        await cq.answer("Это не ваша дуэль!")
        return
    if game["state"] != "opponent_select":
        await cq.answer("Вы уже выбрали число!")
        return
    if number == game["player1_num"]:
        await cq.answer("Нельзя выбрать то же число, что у игрока 1!")
        return
    if not 1 <= number <= 6:
        await cq.answer("Неверное число.")
        return

    async with duel_locks[game_id]:
        if game_id not in duel_data:
            await cq.answer("Дуэль завершена.")
            return
        game["player2_num"] = number
        game["state"] = "throwing_dice"
        await play_duel_round(client, game_id)
    await cq.answer()

# Логика раундов
async def play_duel_round(client, game_id):
    game = duel_data.get(game_id)
    if not game:
        return

    player1 = await client.get_users(game["player1_id"])
    player2 = await client.get_users(game["player2_id"])
    chat_id = game["chat_id"]
    message_id = game["message_id"]

    await client.edit_message_text(
        chat_id, message_id,
        text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
             f"💰 Ставка: {format_balance(game['bet'])} монет\n"
             f"🎲 Число игрока 1: {game['player1_num']}\n"
             f"🎲 Число игрока 2: {game['player2_num']}\n\n"
             f"🎲 Кидаем кости..."
    )
    await asyncio.sleep(3)

    all_numbers = list(range(1, 7))
    available = [n for n in all_numbers if n not in game["used_dice"]]
    if not available:
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(game["bet"]), game["player1_id"]))
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(game["bet"]), game["player2_id"]))
                db.commit()
        except sqlite3.Error:
            pass
        await client.edit_message_text(
            chat_id, message_id,
            text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
                 f"💰 Ставка: {format_balance(game['bet'])} монет\n\n"
                 f"🎲 Все числа уже выпадали!\n"
                 f"🏳️ Игра завершена без победителя.",
            reply_markup=None
        )
        del duel_data[game_id]
        del duel_locks[game_id]
        return

    dice_result = choice(available)
    game["used_dice"].append(dice_result)
    game["round"] += 1
    current_bet = game["bet"]

    if dice_result == game["player1_num"] and dice_result == game["player2_num"]:
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(current_bet // 2), game["player1_id"]))
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(current_bet // 2), game["player2_id"]))
                db.commit()
        except sqlite3.Error:
            pass
        await client.edit_message_text(
            chat_id, message_id,
            text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
                 f"💰 Ставка: {format_balance(current_bet)} монет\n"
                 f"🎲 Число игрока 1: {game['player1_num']}\n"
                 f"🎲 Число игрока 2: {game['player2_num']}\n\n"
                 f"🎲 Выпало: {dice_result}\n"
                 f"🏳️ Ничья! Ставка поделена пополам.",
            reply_markup=None
        )
        del duel_data[game_id]
        del duel_locks[game_id]
        return

    elif dice_result == game["player1_num"]:
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", ((float(current_bet * 2), game["player1_id"])))
                db.commit()
        except sqlite3.Error:
            pass
        await client.edit_message_text(
            chat_id, message_id,
            text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
                 f"💰 Ставка: {format_balance(current_bet)} монет\n"
                 f"🎲 Число игрока 1: {game['player1_num']}\n"
                 f"🎲 Число игрока 2: {game['player2_num']}\n\n"
                 f"🎲 Выпало: {dice_result}\n"
                 f"👑 Победил {player1.first_name}! (+{format_balance(current_bet * 2)} монет)",
            reply_markup=None
        )
        await set_win_monet(player1.id, current_bet)
        await set_lose_monet(player2.id, current_bet)
        del duel_data[game_id]
        del duel_locks[game_id]
        return

    elif dice_result == game["player2_num"]:
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(current_bet * 2), game["player2_id"]))
                db.commit()
        except sqlite3.Error:
            pass
        await client.edit_message_text(
            chat_id, message_id,
            text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
                 f"💰 Ставка: {format_balance(current_bet)} монет\n"
                 f"🎲 Число игрока 1: {game['player1_num']}\n"
                 f"🎲 Число игрока 2: {game['player2_num']}\n\n"
                 f"🎲 Выпало: {dice_result}\n"
                 f"👑 Победил {player2.first_name}! (+{format_balance(current_bet * 2)} монет)",
            reply_markup=None
        )
        del duel_data[game_id]
        del duel_locks[game_id]
        return

    game["bet"] = int(current_bet * 0.95)
    if game["bet"] < 1:
        await client.edit_message_text(
            chat_id, message_id,
            text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
                 f"💰 Ставка исчерпана\n"
                 f"🎲 Число игрока 1: {game['player1_num']}\n"
                 f"🎲 Число игрока 2: {game['player2_num']}\n\n"
                 f"🎲 Выпало: {dice_result}\n"
                 f"🏳️ Ставка исчерпана, игра завершена.",
            reply_markup=None
        )
        del duel_data[game_id]
        del duel_locks[game_id]
        return

    await client.edit_message_text(
        chat_id, message_id,
        text=f"⚔️ {player1.first_name} vs {player2.first_name}\n"
             f"💰 Ставка: {format_balance(game['bet'])} монет\n"
             f"🎲 Число игрока 1: {game['player1_num']}\n"
             f"🎲 Число игрока 2: {game['player2_num']}\n\n"
             f"🎲 Раунд {game['round']}:\n"
             f"Выпало: {dice_result}\n"
             f"Никто не угадал, ставка уменьшена на 5%.",
        reply_markup=None
    )
    await asyncio.sleep(2)
    await play_duel_round(client, game_id)



@app.on_message(filters.command('ggm'))
async def global_get_money(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        return

    if len(message.text.split()) < 2:
        await message.reply("❌ Укажите сколько монет выдать игрокам: /ggm [количество]")
        return
    
    amount = int(message.text.split()[1])
    if amount <= 0:
        await message.reply("❌ Сколько монет выдать должно быть больше нуля.")
        return

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT id, money FROM crash")
    rows = cursor.fetchall()
    db.close()

    for row in rows:
        user_id = row[0]
        try:
            with sqlite3.connect(DBB) as db:
                db.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(amount), user_id))
                db.commit()
        except sqlite3.Error:
            pass
        print(f"✅ {user_id} получил {amount} монет.")

@app.on_message(filters.command('p_ban'))
async def handle_ban_user(client, message):
    tg_id = message.from_user.id
    if tg_id not in API_OWNER:  
        return

    if len(message.text.split()) < 2:
        await message.reply("❌ Укажите ID пользователя: /p_ban <id>")
        return

    user_id = message.text.split()[1]
    if await is_banned_user(user_id):
        await message.reply("✅ Игрок уже забанен.")
        return
    
    await ban_user(user_id)
    await message.reply(f"🚫 Пользователь {user_id} забанен")
    await app.send_message("-1004869586301", f"""
<b>Момент: Бан</b>
<b>Администратор:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Пользователь:</b> {user.first_name} (@{user.username} #{user.id})
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>Фулл текст:</b> <pre lang='txt'>{message.text}</pre>
""")

@app.on_message(filters.command('p_unban'))
async def handle_unban_user(client, message):
    tg_id = message.from_user.id
    if tg_id not in API_OWNER:
        return

    if len(message.text.split()) < 2:
        await message.reply("❌ Укажите ID пользователя: /p_unban <id>")
        return

    user_id = message.text.split()[1]

    if not await is_banned_user(user_id):
        await message.reply("✅ Пользователь не забанен.")
        return

    await unban_user(user_id)
    await message.reply(f"✅ Пользователь {user_id} разбанен")
    await app.send_message("-1004869586301", f"""
<b>Момент: Разбан</b>
<b>Администратор:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Пользователь:</b> {user.first_name} (@{user.username} #{user.id})
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>Фулл текст:</b> <pre lang='txt'>{message.text}</pre>
""")

# ----------------------- БАШНЯ ----------------------- #

TOWER_MULTIPLIERS = [1.1, 1.3, 1.6, 2.0, 2.4, 2.9, 3.5, 5.0, 7.1]

# In-memory game state and cooldowns
# теперь: game_id -> state
active_tower_games = {}
# user_id -> list of game_ids
user_tower_games = {}
tower_cooldowns = {}
button_cooldowns = {}

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

def parse_bet_input(arg: str, user_money: Optional[Union[int, float, str, Decimal]] = None) -> int:
    if arg is None:
        return -1

    s = str(arg).strip().lower()
    s = s.replace(" ", "").replace("_", "")

    if s in ("все", "всё", "all"):
        um = _to_decimal_safe(user_money)
        if um is None:
            return -1
        try:
            return int(um)
        except Exception:
            return -1

    m = re.fullmatch(r'([0-9]+(?:[.,][0-9]{1,2})?)([kк]*)', s)
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


async def get_balance(user_id: int) -> int:
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT money FROM crash WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    db.close()
    return row[0] if row else 0

async def update_balance(user_id: int, delta: int):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (delta, user_id))
    db.commit()
    db.close()

def create_tower_game(game_id: str, user_id: int, bet: int):
    bombs = []
    for _ in range(9):
        row = [0] * 5
        row[rnd.randint(0, 4)] = 1
        bombs.append(row)
    state = {
        "user_id": user_id,
        "bet": bet,
        "level": 0,
        "bombs": bombs,
        "selected": [],
        "lost": False,
        "game_id": game_id
    }
    active_tower_games[game_id] = state
    user_tower_games.setdefault(user_id, []).append(game_id)
    return state

def remove_tower_game(game_id: str):
    state = active_tower_games.pop(game_id, None)
    if not state:
        return
    uid = state.get("user_id")
    if uid and uid in user_tower_games:
        if game_id in user_tower_games[uid]:
            user_tower_games[uid].remove(game_id)
        if not user_tower_games[uid]:
            del user_tower_games[uid]

def build_tower_keyboard(game_id: str, state: dict) -> InlineKeyboardMarkup:
    level = state["level"]
    bombs = state["bombs"]
    selected = state["selected"]
    kb = []
    # Current row buttons include game_id and index
    buttons = [
        InlineKeyboardButton("❔", callback_data=f"tower_choose_{game_id}_{i}") for i in range(5)
    ]
    kb.append(buttons)
    # Completed rows (bottom-up)
    for i in range(level - 1, -1, -1):
        row = bombs[i]
        choice = selected[i]
        row_btns = []
        for j in range(5):
            emoji = "🌀" if j == choice else "❔"
            row_btns.append(InlineKeyboardButton(emoji, callback_data="noop"))
        kb.append(row_btns)
    # Control buttons
    if level == 0:
        kb.append([InlineKeyboardButton("Отмена", callback_data=f"tower_cancel_{game_id}")])
    else:
        kb.append([InlineKeyboardButton("Забрать приз", callback_data=f"tower_collect_{game_id}")])
    return InlineKeyboardMarkup(kb)

def build_final_tower_keyboard(game_id: str, state: dict) -> InlineKeyboardMarkup:
    bombs = state["bombs"]
    selected = state["selected"]
    lost = state["lost"]
    last = len(selected) - 1 if lost else min(len(selected) - 1, 8)
    kb = []
    for i in range(last, -1, -1):
        row = bombs[i]
        choice = selected[i]
        row_btns = []
        for j in range(5):
            if row[j] == 1 and j == choice:
                emoji = "💥"
            elif row[j] == 1:
                emoji = "💣"
            elif j == choice:
                emoji = "🌀"
            else:
                emoji = "❔"
            row_btns.append(InlineKeyboardButton(emoji, callback_data="noop"))
        kb.append(row_btns)
    # no control buttons on final keyboard
    return InlineKeyboardMarkup(kb)

# Start tower - allows up to 2 concurrent games per user
@app.on_message(filters.text & filters.regex(r"^башня", flags=re.IGNORECASE))
async def start_tower(client: Client, message: Message):
    user_id = message.from_user.id
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply_text("Используйте: Башня <ставка>, например: Башня 300 или Башня 1к")
    arg = parts[1]
    balance = await get_balance(user_id)
    bet = parse_bet_input(arg, balance)
    if bet == -1:
        return await message.reply_text("Неверная ставка. Укажите число или 'все'.")
    if bet < 10 or bet > balance:
        return await message.reply_text("Ставка должна быть от 10 до вашего баланса.")
    now = asyncio.get_event_loop().time()
    if user_id in tower_cooldowns and now - tower_cooldowns[user_id] < 5:
        return await message.reply_text("Подождите перед новой игрой.")
    tower_cooldowns[user_id] = now

    # limit to 2 active games per user
    active_list = user_tower_games.get(user_id, [])
    if len(active_list) >= 2:
        return await message.reply_text("У вас уже 2 активные игры. Закончите одну, прежде чем начинать новую.")

    # Subtract bet
    await update_balance(user_id, -bet)

    # create game with unique id
    game_id = uuid.uuid4().hex
    state = create_tower_game(game_id, user_id, bet)

    await message.reply_text(
        f"🟢 Ты начал игру \"Башня\"!\n====================\nУровень: 1/10\nСтавка: {format_balance(bet)}\nМножитель: x1.1",
        reply_markup=build_tower_keyboard(game_id, state)
    )

# Choose button handler (data: tower_choose_{game_id}_{idx})
@app.on_callback_query(filters.regex(r"^tower_choose_"))
async def handle_choose(client: Client, callback: CallbackQuery):
    data = callback.data  # e.g. tower_choose_abcd1234_2
    parts = data.split("_", 3)
    # parts = ["tower","choose","{game_id}","{idx}"]
    if len(parts) < 4:
        return await callback.answer("Неверные данные.", show_alert=True)
    game_id = parts[2]
    try:
        idx = int(parts[3])
    except ValueError:
        return await callback.answer("Неверные данные.", show_alert=True)

    state = active_tower_games.get(game_id)
    if not state:
        return await callback.answer("Игра не найдена или уже завершена.", show_alert=True)
    user_id = callback.from_user.id
    if state["user_id"] != user_id:
        return await callback.answer("Это не ваша игра!", show_alert=True)

    level = state["level"]
    # append selected
    state["selected"].append(idx)
    if state["bombs"][level][idx] == 1:
        state["lost"] = True
        balance = await get_balance(user_id)
        await callback.message.edit_text(
            f"Вы проиграли!\nВаш баланс: {format_balance(balance)}",
            reply_markup=build_final_tower_keyboard(game_id, state)
        )
        remove_tower_game(game_id)
        return

    state["level"] += 1
    # won full tower
    if state["level"] >= 9:
        win = round(state["bet"] * TOWER_MULTIPLIERS[8])
        await update_balance(user_id, win)
        balance = await get_balance(user_id)
        await callback.message.edit_text(
            f"Поздравляем! Вы забрали {format_balance(win)}!\nВаш баланс: {format_balance(balance)}",
            reply_markup=build_final_tower_keyboard(game_id, state)
        )
        remove_tower_game(game_id)
        return

    mult = TOWER_MULTIPLIERS[state["level"] - 1]
    await callback.message.edit_text(
        f"🟢 Продолжай играть!\n====================\nУровень: {state['level']+1}/10\nСтавка: {format_balance(state['bet'])}\nВозможный выигрыш: {format_balance(state['bet'] * mult)}\nМножитель: x{mult:.1f}",
        reply_markup=build_tower_keyboard(game_id, state)
    )

# Cancel specific game (data: tower_cancel_{game_id})
@app.on_callback_query(filters.regex(r"^tower_cancel_"))
async def handle_cancel(client: Client, callback: CallbackQuery):
    data = callback.data
    parts = data.split("_", 2)
    if len(parts) < 3:
        return await callback.answer("Неверные данные.", show_alert=True)
    game_id = parts[2]
    state = active_tower_games.get(game_id)
    if not state:
        return await callback.answer("Игра не найдена.", show_alert=True)
    user_id = callback.from_user.id
    if state["user_id"] != user_id:
        return await callback.answer("Это не ваша игра!", show_alert=True)
    if state['level'] != 0:
        return await callback.answer("Нельзя отменить.", show_alert=True)
    bet = state['bet']
    await update_balance(user_id, bet)
    await callback.message.edit_text(f"Игра отменена, {format_balance(bet)} возвращены.")
    remove_tower_game(game_id)

# Collect specific game (data: tower_collect_{game_id})
@app.on_callback_query(filters.regex(r"^tower_collect_"))
async def handle_collect(client: Client, callback: CallbackQuery):
    data = callback.data
    parts = data.split("_", 2)
    if len(parts) < 3:
        return await callback.answer("Неверные данные.", show_alert=True)
    game_id = parts[2]
    state = active_tower_games.get(game_id)
    if not state:
        return await callback.answer("Игра не найдена.", show_alert=True)
    user_id = callback.from_user.id
    if state["user_id"] != user_id:
        return await callback.answer("Это не ваша игра!", show_alert=True)
    if state['level'] == 0:
        return await callback.answer("Слишком рано.", show_alert=True)
    lvl = state['level'] - 1
    win = round(state['bet'] * TOWER_MULTIPLIERS[lvl])
    await update_balance(user_id, win)
    balance = await get_balance(user_id)
    await callback.message.edit_text(
        f"Вы забрали {format_balance(win)}!\nВаш баланс: {format_balance(balance)}",
        reply_markup=None
    )
    remove_tower_game(game_id)


from datetime import datetime
import uuid
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import pytz

@app.on_message(filters.command("ssss"))
async def test_bank_interest(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        return

    moscow_tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(moscow_tz)

    with sqlite3.connect(DBB) as db:
        cursor = db.cursor()
        cursor.execute("SELECT deposit_id, current_amount FROM bank_deposits WHERE is_active = 1")
        deposits = cursor.fetchall()
        for deposit_id, current_amount in deposits:
            new_amount = int(current_amount * 1.05)  # +5%
            cursor.execute("UPDATE bank_deposits SET current_amount = ? WHERE deposit_id = ?",
                          (float(new_amount), deposit_id))
        db.commit()

    await message.reply(f"[BANK] Начислены проценты для {len(deposits)} депозитов в {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"[BANK] Начислены проценты для {len(deposits)} депозитов в {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

async def bank_command(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    if await is_banned_user(user_id):
        await message.reply("Вы забанены.")
        return

    parts = message.text.split()
    user_data = get_user_data(user_id)
    moscow_tz = pytz.timezone('Europe/Moscow')

    if len(parts) == 1:
        # Показать список депозитов
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            cursor.execute("SELECT deposit_id, amount, current_amount, created_at FROM bank_deposits WHERE user_id = ? AND is_active = 1", (user_id,))
            deposits = cursor.fetchall()

        if not deposits:
            await message.reply("У вас нет активных депозитов.\nСоздайте депозит: /bank <сумма> (например, 100к, 1.5кк) или /bank все")
            return

        response = "<b>🏦 Ваши депозиты:</b>\n\n"
        for deposit in deposits:
            deposit_id, amount, current_amount, created_at = deposit
            # Преобразуем created_at в МСК
            created_at_dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            created_at_msk = pytz.utc.localize(created_at_dt).astimezone(moscow_tz)
            response += (f"📌 <b>Депозит ID:</b> <code>{deposit_id[:8]}</code>\n"
                        f"💰 Начальная сумма: <code>{format_balance(amount)}</code>\n"
                        f"📈 Текущая сумма: <code>{format_balance(current_amount)}</code>\n"
                        f"📅 Создан: <code>{created_at_msk.strftime('%Y-%m-%d %H:%M:%S')}</code>\n\n")

        # Создаем клавиатуру для закрытия депозитов
        inline_keyboard = [[InlineKeyboardButton(f"Закрыть депозит {deposit[0][:8]}", callback_data=f"bank_close_{deposit[0]}")] for deposit in deposits]
        markup = InlineKeyboardMarkup(inline_keyboard)
        
        response += f"<b>💰 Баланс:</b> <code>{format_balance(user_data['money'])}</code>\n"
        response += "<i>Начисление 5% каждую неделю в 00:00 понедельника по МСК.</i>"
        await message.reply(response, reply_markup=markup)
        return

    if len(parts) == 2:
        # Создать новый депозит
        input_amount = parts[1].lower()
        if input_amount in ("все", "всё"):
            amount = user_data['money']
        else:
            amount = parse_bet_amount(input_amount, user_data['money'])
            if amount is None:
                await message.reply("<i>Укажите сумму депозита числом (например, 1000, 100к, 1.5кк) или 'все'</i>")
                return

        if amount < 10:
            await message.reply("<i>Минимальная сумма депозита — 10 монет.</i>")
            return
        if amount > user_data['money']:
            await message.reply("<i>Недостаточно монет на балансе.</i>")
            return
        if amount == 0:
            await message.reply("<i>Ваш баланс равен 0. Пополните баланс, чтобы создать депозит.</i>")
            return

        # Проверяем количество активных депозитов
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(*) FROM bank_deposits WHERE user_id = ? AND is_active = 1", (user_id,))
            deposit_count = cursor.fetchone()[0]

        if deposit_count >= 10:
            await message.reply("<i>У вас уже 10 активных депозитов. Закройте один, чтобы создать новый.</i>")
            return

        # Создаем 
        deposit_id = str(uuid.uuid4())
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(amount), user_id))
            cursor.execute("INSERT INTO bank_deposits (deposit_id, user_id, amount, current_amount) VALUES (?, ?, ?, ?)",
                          (deposit_id, user_id, float(amount), float(amount)))
            db.commit()

        balance = format_balance(user_data['money'] - amount)
        await message.reply(f"<b>🏦 Депозит создан!</b>\nID: <code>{deposit_id[:8]}</code>\nСумма: <code>{format_balance(amount)}</code>\n💰 Баланс: <code>{balance}</code>")
        await app.send_message("-1004869586301", f"""
<b>Момент: Создание депозита</b>
<b>Создатель:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Сумма:</b> {format_balance(amount)}
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """)
        update_quest(user_id, 3)
        if is_quest_completed(user_id, 3):
            if not check_verifed_quests(user_id, 3):
                await app.send_message(user_id, "<b>Вы выполнили квест</b> 'Кладмен'!")
                verifed_quests_completed(user_id, 3)


        return

    await message.reply("<i>Используйте: /bank — для просмотра депозитов\n/bank <сумма> (например, 100к, 1.5кк) или /bank все — для создания депозита</i>")

@app.on_callback_query(filters.regex(r'^bank_close_'))
async def bank_close_callback(client, callback_query):
    user_id = callback_query.from_user.id
    deposit_id = callback_query.data.split('_')[2]

    if user_id not in user_locks:
        user_locks[user_id] = Lock()

    if user_locks[user_id].locked():
        await callback_query.answer("⏳ Подождите, идет обработка!")
        return

    async with user_locks[user_id]:
        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            cursor.execute("SELECT current_amount FROM bank_deposits WHERE deposit_id = ? AND user_id = ? AND is_active = 1",
                          (deposit_id, user_id))
            deposit = cursor.fetchone()

            if not deposit:
                await callback_query.answer("Депозит не найден или уже закрыт.")
                return

            current_amount = deposit[0]
            cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(current_amount), user_id))
            cursor.execute("UPDATE bank_deposits SET is_active = 0 WHERE deposit_id = ?", (deposit_id,))
            db.commit()

        user_data = get_user_data(user_id)
        balance = format_balance(user_data['money'])
        await callback_query.message.edit_text(
            f"<b>🏦 Депозит закрыт!</b>\nID: <code>{deposit_id[:8]}</code>\nВы получено: <code>{format_balance(current_amount)}</code>\n💰 Баланс: <code>{balance}</code>"
        )
        await callback_query.answer()
    
    await app.send_message("-1004869586301", f"""
<b>Момент: Закрытие депозита</b>
<b>Закрыватель:</b> {callback_query.from_user.first_name} (@{callback_query.from_user.username} #{callback_query.from_user.id})
<b>Депозит:</b> {deposit_id}
<b>Сумма:</b> {format_balance(current_amount)}
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """)

def get_user_quests(user_id: int):
    data = load_quests()
    str_id = str(user_id)
    return data.get(str_id, [])

def format_quests(quests: list):
    text = "<b>📜 Ваши квесты:</b>\n\n"
    for q in quests:
        status = "✅" if q["completed"] else "❌"
        text += f"• <b>{q['quest_name']}</b> {status}\n"
        text += f"  └ {q['quest_description']}\n"
        text += f"  └ Прогресс: <code>{q['count']} / {q['max_count']}</code>\n\n"
    return text


def completed_all_quests(user_id):
    quests = get_user_quests(user_id)
    for quest in quests:
        if not is_quest_completed(quest):
            return False
    return True

def all_quests_completed(quests: list):
    return all(q.get("completed", False) for q in quests)

def is_quest_completed(user_id: str, quest_id: int) -> bool:
    with open(QUESTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    user_quests = data.get(str(user_id), [])
    for quest in user_quests:
        if quest["quest_id"] == quest_id:
            return quest.get("completed", False)
    return False

def create_default_quests(user_id):
    global default_quests
    with open(QUESTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    data[str(user_id)] = default_quests

    with open(QUESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# @app.on_message(filters.command(["quests", "квесты", "qu"]))
async def show_quests(client: Client, message: Message):
    user_id = message.from_user.id
    quests = get_user_quests(user_id)

    if not quests:
        create_default_quests(user_id)
        await message.reply("ПОПРОБУЙТЕ ЕЩЕ РАЗ")
        return

    if all_quests_completed(quests):
        await message.reply(f"🔥 Поздравляем! Вы завершили все квесты!\n\nСсылка на 2 часть ивента: {encrypt_user_id(user_id)}", quote=True)
        return

    text = format_quests(quests)
    await message.reply(text, quote=True)



@app.on_message(filters.command("answer"))
async def show_quests_answer(client: Client, message: Message):
    user_id = str(message.from_user.id)
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Используйте: /answer (ответ)")
        return
    answer = args[1]
    if answer == "минор":
        await message.reply("Правильный ответ!")
        update_quest(user_id, 5)
    else:
        await message.reply("Неправильный ответ!")

@app.on_message(filters.command("fff"))
async def add_shop_item(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        await message.reply("У вас нет доступа к этой команде.")
        return

    parts = message.text.split()
    if len(parts) < 4:
        await message.reply("Используйте: /fff (название) (цена) (количество)\nПример: /fff меч 1000 5")
        return

    # Все части сообщения, кроме команды и последнего числа, составляют название предмета
    name = " ".join(parts[1:-2])
    price_str, quantity_str = parts[-2], parts[-1]

    # Проверка формата цены и количества
    try:
        price = int(price_str)
        quantity = int(quantity_str)
        if price < 0 or quantity < 0:
            await message.reply("Цена и количество должны быть неотрицательными.")
            return
    except ValueError:
        await message.reply("Цена и количество должны быть числами.")
        return

    # Добавление предмета в магазин
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO shop (name, price, quantity) VALUES (?, ?, ?)", (name, price, quantity))
        db.commit()
        await message.reply(f"Предмет '{name}' добавлен в магазин!\nЦена: {price} монет\nКоличество: {quantity}")
    except sqlite3.IntegrityError:
        await message.reply(f"Предмет с названием '{name}' уже существует в магазине.")
    finally:
        db.close()
    await app.send_message("-1004869586301", f"""
<b>Момент: Добавление предмета в магазин</b>
<b>Администратор:</b> {message.from_user.first_name} (@{message.from_user.username} #{message.from_user.id})
<b>Предмет:</b> {name}
<b>Цена:</b> {price} монет
<b>Количество:</b> {quantity}
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>Фулл текст:</b> <pre lang='txt'>{message.text}</pre>
""")

SHOP_PAGE_SIZE = 10  # Количество предметов на странице
SHOP_PAGES = {}      # Хранение текущей страницы для каждого пользователя
SHOP_CALLER = {}     # Хранение ID пользователя, вызвавшего /shop для каждого сообщения
SHOP_SORT = {}       # Хранение текущего критерия сортировки для каждого пользователя

from pyrogram.enums import ChatType, ParseMode


async def show_shop(client, message):

    if message.chat.type == ChatType.PRIVATE:
        pass
    else:
        await message.reply("Магазин доступен только в личных сообщениях.")
        return

    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    
    if await is_banned_user(user_id):
        await message.reply("Вы забанены.")
        return

    # Инициализируем страницу и сортировку для пользователя
    SHOP_PAGES[user_id] = 0
    SHOP_SORT[user_id] = "default"  # По умолчанию сортировка "по умолчанию"

    # Получаем все предметы из магазина
    items = fetch_shop_items(SHOP_SORT[user_id])

    if not items:
        await message.reply("Магазин пуст.")
        return

    # Отправляем первую страницу
    msg = await message.reply("🛒 Загружаю магазин...")
    SHOP_CALLER[msg.id] = user_id  # Сохраняем, кто вызвал /shop
    await update_shop_message(client, msg, user_id, items, 0)

# Функция для получения предметов с учетом сортировки
def fetch_shop_items(sort_type):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()

    # Определяем ORDER BY в зависимости от типа сортировки
    if sort_type == "cheap":
        order_by = "ORDER BY price ASC"
    elif sort_type == "expensive":
        order_by = "ORDER BY price DESC"
    elif sort_type == "new":
        order_by = "ORDER BY created_at DESC"
    elif sort_type == "old":
        order_by = "ORDER BY created_at ASC"
    elif sort_type == "quantity_high":
        # При сортировке по количеству: -1 (бесконечное) должно быть вверху
        order_by = "ORDER BY CASE WHEN quantity = -1 THEN 0 ELSE 1 END, quantity DESC"
    elif sort_type == "quantity_low":
        # При сортировке по количеству: -1 (бесконечное) должно быть внизу
        order_by = "ORDER BY CASE WHEN quantity = -1 THEN 1 ELSE 0 END, quantity ASC"
    else:
        order_by = ""  # По умолчанию без сортировки

    query = f"SELECT name, price, quantity FROM shop {order_by}"
    cursor.execute(query)
    items = cursor.fetchall()
    db.close()
    return items

async def update_shop_message(client, message, user_id, items, page):
    start_idx = page * SHOP_PAGE_SIZE
    end_idx = start_idx + SHOP_PAGE_SIZE
    items_page = items[start_idx:end_idx]

    # Определяем текущий критерий сортировки
    current_sort = SHOP_SORT.get(user_id, "default")
    sort_display = {
        "default": "По умолчанию",
        "cheap": "Дешевые",
        "expensive": "Дорогие",
        "new": "Новые",
        "old": "Старые",
        "quantity_high": "Кол-во (много)",
        "quantity_low": "Кол-во (мало)"
    }
    text = (f"🛒 <b>Магазин (страница {page + 1}/{((len(items) - 1) // SHOP_PAGE_SIZE) + 1}):</b>\n"
            f"Всего предметов: {len(items)}\n"
            f"Сортировка: {sort_display[current_sort]}\n\n"
            f"Выберите предмет:")
    
    # Создаем кнопки для предметов
    buttons = []
    for name, price, quantity in items_page:
        display_qty = "❌" if quantity == 0 else f"{quantity if quantity > 0 else '❌'}"
        button_text = f"{name} — {format_balance(price)} ({display_qty})"
        buttons.append([InlineKeyboardButton(button_text, callback_data=f"shop_info:{name}:{page}")])
    
    buttons.append([InlineKeyboardButton(f"Назад", callback_data=f"market_view_edit")])

    # Добавляем кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅ Назад", callback_data=f"shop_page:{page - 1}"))
    if end_idx < len(items):
        nav_buttons.append(InlineKeyboardButton("Вперед ➡", callback_data=f"shop_page:{page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    # Добавляем кнопки сортировки с выделением текущей
    sort_buttons = [
        [InlineKeyboardButton(f"Дешевые{' ✅' if current_sort == 'cheap' else ''}", callback_data="shop_sort:cheap"),
         InlineKeyboardButton(f"Дорогие{' ✅' if current_sort == 'expensive' else ''}", callback_data="shop_sort:expensive")],
        [InlineKeyboardButton(f"Новые{' ✅' if current_sort == 'new' else ''}", callback_data="shop_sort:new"),
         InlineKeyboardButton(f"Старые{' ✅' if current_sort == 'old' else ''}", callback_data="shop_sort:old")],
        [InlineKeyboardButton(f"Кол-во (много){' ✅' if current_sort == 'quantity_high' else ''}", callback_data="shop_sort:quantity_high"),
         InlineKeyboardButton(f"Кол-во (мало){' ✅' if current_sort == 'quantity_low' else ''}", callback_data="shop_sort:quantity_low")],
        [InlineKeyboardButton(f"По умолчанию{' ✅' if current_sort == 'default' else ''}", callback_data="shop_sort:default")]
    ]
    buttons.extend(sort_buttons)

    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
    await client.edit_message_text(
        chat_id=message.chat.id,
        message_id=message.id,
        text=text,
        reply_markup=reply_markup
    )

# Обработчик кнопок для смены страниц
@app.on_callback_query(filters.regex(r"shop_page:(\d+)"))
async def change_shop_page(client, callback_query):
    page = int(callback_query.matches[0].group(1))
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /shop
    if SHOP_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш магазин!", show_alert=True)
        return

    # Получаем предметы с учетом текущей сортировки
    items = fetch_shop_items(SHOP_SORT.get(user_id, "default"))

    if not items:
        await callback_query.answer("Магазин пуст.", show_alert=True)
        return

    # Обновляем текущую страницу пользователя
    SHOP_PAGES[user_id] = page

    # Редактируем сообщение
    await update_shop_message(client, callback_query.message, user_id, items, page)
    await callback_query.answer()

# Обработчик кнопок сортировки
@app.on_callback_query(filters.regex(r"shop_sort:(\w+)"))
async def change_shop_sort(client, callback_query):
    sort_type = callback_query.matches[0].group(1)
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /shop
    if SHOP_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш магазин!", show_alert=True)
        return

    # Проверяем, не выбрана ли уже эта сортировка
    current_sort = SHOP_SORT.get(user_id, "default")
    if current_sort == sort_type:
        await callback_query.answer("Эта сортировка уже выбрана!", show_alert=True)
        return

    # Обновляем критерий сортировки
    SHOP_SORT[user_id] = sort_type
    SHOP_PAGES[user_id] = 0  # Сбрасываем страницу на первую

    # Получаем предметы с новой сортировкой
    items = fetch_shop_items(sort_type)

    if not items:
        await callback_query.answer("Магазин пуст.", show_alert=True)
        return

    # Обновляем сообщение
    await update_shop_message(client, callback_query.message, user_id, items, 0)
    await callback_query.answer(f"Сортировка изменена на: {sort_type}")

# Обработчик нажатия на предмет
@app.on_callback_query(filters.regex(r"shop_info:"))
async def shop_item_info(client, callback_query):
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /shop
    if SHOP_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш магазин!", show_alert=True)
        return

    parts = callback_query.data.split(":")
    item_name, page = parts[1], int(parts[2])

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT name, price, quantity FROM shop WHERE name = ?", (item_name,))
    item = cursor.fetchone()
    db.close()

    if not item:
        await callback_query.answer("Предмет не найден.", show_alert=True)
        return

    name, price, quantity = item
    # Проверяем, закончился ли предмет
    is_out_of_stock = quantity == 0
    display_qty = "❌" if is_out_of_stock else f"Всего: {quantity if quantity > 0 else '∞'}"
    user_data = get_user_data(user_id)
    balance = user_data["money"]

    text = (f"🛒 <b>Предмет: {name}</b>\n"
            f"💰 Цена: {format_balance(price)} \n"
            f"📦 {display_qty}\n"
            f"💼 Ваш баланс: {format_balance(balance)} ")

    # Если предмет закончился, показываем только кнопку "Назад"
    if is_out_of_stock:
        buttons = [[InlineKeyboardButton("Назад", callback_data=f"shop_back:{page}")]]
    else:
        buttons = [
            [InlineKeyboardButton("Купить", callback_data=f"shop_buy:{name}:{page}")],
            [InlineKeyboardButton("Назад", callback_data=f"shop_back:{page}")]
        ]

    reply_markup = InlineKeyboardMarkup(buttons)

    await client.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=message_id,
        text=text,
        reply_markup=reply_markup
    )
    await callback_query.answer()

# Обработчик кнопки "Купить"
@app.on_callback_query(filters.regex(r"shop_buy:"))
async def shop_buy_item(client, callback_query):
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /shop
    if SHOP_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш магазин!", show_alert=True)
        return

    parts = callback_query.data.split(":")
    item_name, page = parts[1], int(parts[2])

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT name, price, quantity FROM shop WHERE name = ?", (item_name,))
    item = cursor.fetchone()

    if not item:
        await callback_query.answer("Предмет не найден.", show_alert=True)
        return

    name, price, quantity = item
    user_data = get_user_data(user_id)

    # Проверяем, закончился ли предмет
    if quantity == 0:
        await callback_query.answer("Товар нет в наличии!", show_alert=True)
        return
    if user_data["money"] < price:
        await callback_query.answer("Недостаточно монет!", show_alert=True)
        return

    # Выполняем покупку
    with sqlite3.connect(DBB) as db:
        cursor = db.cursor()
        # Списываем деньги
        cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(price), user_id))
        if quantity != -1:
            cursor.execute("UPDATE shop SET quantity = quantity - 1 WHERE name = ?", (item_name,))
        cursor.execute("""
            INSERT INTO inv_user (user_id, item_name, quantity)
            VALUES (?, ?, 1)
            ON CONFLICT(user_id, item_name)
            DO UPDATE SET quantity = quantity + 1
        """, (user_id, item_name))
        db.commit()

    cursor.execute("SELECT name, price, quantity FROM shop WHERE name = ?", (item_name,))
    updated_item = cursor.fetchone()
    db.close()

    name, price, quantity = updated_item
    # Проверяем, закончился ли предмет после покупки
    is_out_of_stock = quantity == 0
    display_qty = "Товар нет в наличии" if is_out_of_stock else f"Осталось в магазине: {quantity if quantity > 0 else '∞'}"
    new_balance = user_data["money"] - price

    text = (f"🎉 <b>Вы купили: {name}</b>\n"
            f"💰 Цена: {format_balance(price)} \n"
            f"📦 {display_qty}\n"
            f"💼 Ваш баланс: {format_balance(new_balance)}")

    await app.send_message("-1004869586301", f"""
    <b>Момент: Покупка</b>
    <b>Покупатель:</b> {callback_query.from_user.first_name} (@{callback_query.from_user.username} #{callback_query.from_user.id})
    <b>Предмет:</b> {name}
    <b>Сумма:</b> {format_balance(price)} ({price})
    <b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)

    # Если после покупки предмет закончился, убираем кнопку "Купить еще"
    if is_out_of_stock:
        buttons = [[InlineKeyboardButton("Назад", callback_data=f"shop_back:{page}")]]
    else:
        buttons = [
            [InlineKeyboardButton("Купить еще", callback_data=f"shop_buy:{name}:{page}")],
            [InlineKeyboardButton("Назад", callback_data=f"shop_back:{page}")]
        ]

    reply_markup = InlineKeyboardMarkup(buttons)

    await client.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=message_id,
        text=text,
        reply_markup=reply_markup
    )
    await callback_query.answer()

# Обработчик кнопки "Назад"
@app.on_callback_query(filters.regex(r"shop_back:"))
async def shop_back(client, callback_query):
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /shop
    if SHOP_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш магазин!", show_alert=True)
        return

    page = int(callback_query.data.split(":")[1])

    # Получаем предметы с учетом текущей сортировки
    items = fetch_shop_items(SHOP_SORT.get(user_id, "default"))

    if not items:
        await callback_query.answer("Магазин пуст.", show_alert=True)
        return

    # Возвращаемся к списку предметов
    await update_shop_message(client, callback_query.message, user_id, items, page)
    await callback_query.answer()

async def sell_item(client, message):
    args = message.text.split()
    if len(args) < 4:
        return await message.reply("Использование: /sell [название_предмета] [цена] [кол-во]")

    # Извлекаем название (все слова кроме последних двух), цену и количество
    item = " ".join(args[1:-2])
    try:
        price = float(parse_bet_amount((args[-2]), args[-2]))
        amount = parse_bet_amount((args[-1]), args[-1])
    except ValueError:
        return await message.reply("Цена и количество должны быть числами.")

    if price < 0 or amount <= 0:
        return await message.reply("Цена и количество должны быть положительными.")

    user_id = message.from_user.id

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT quantity FROM inv_user WHERE user_id = ? AND item_name = ?", (user_id, item))
    result = cursor.fetchone()

    if not result or result[0] < amount:
        db.close()
        return await message.reply("❌ У вас недостаточно предметов для продажи.")

    # вычитаем из инвентаря
    cursor.execute("UPDATE inv_user SET quantity = quantity - ? WHERE user_id = ? AND item_name = ?", (amount, user_id, item))

    # добавляем на маркет
    cursor.execute("INSERT INTO MARKETPLACE (seller_id, item_name, price, quantity) VALUES (?, ?, ?, ?)",
                   (user_id, item, price, float(amount)))
    db.commit()
    db.close()

    await message.reply(f"✅ Вы выставили <b>{item}</b> ×{amount} по <b>{format_balance(price)}</b> монет.")

# 

async def view_market(client, message):
    
    if message.chat.type == ChatType.PRIVATE:
        pass
    else:
        await message.reply("Магазин доступен только в личных сообщениях.")
        return
    
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛍 Купить", callback_data="market_buy")],
        [InlineKeyboardButton("📦 Продать", callback_data="market_sell")],
        [InlineKeyboardButton("📋 Мои лоты", callback_data="market_my_offers")]
    ])
    await message.reply("🛒 <b>Маркет:</b>", reply_markup=markup)

@app.on_callback_query(filters.regex(r"market_buy"))
async def market_buy_main(client, callback_query):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT item_name FROM MARKETPLACE")
    items = cursor.fetchall()
    db.close()

    if not items:
        return await callback_query.message.edit_text("❌ На рынке нет товаров.")

    buttons = [
        [InlineKeyboardButton(f"{item[0]}", callback_data=f"buy_item_group_{item[0]}")] for item in items
    ]
    markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text("🔎 Выберите предмет для покупки:", reply_markup=markup)
# helper: безпечне отримання імені продавця
async def safe_username(client, seller_id):
    """
    Повертає читабельне ім'я продавця.
    Перевага: пробує client.get_users, при помилці дивиться в БД, якщо і там нічого — повертає '#id'.
    """
    try:
        # Пробуємо привести до int — якщо в БД лежить рядок, але там число в тексті
        seller_id_int = int(seller_id)
    except Exception:
        seller_id_int = seller_id  # залишаємо як є

    # спроба отримати через API
    try:
        user = await client.get_users(seller_id_int)
        if user:
            return f"@{user.username}" if getattr(user, "username", None) else user.first_name or str(seller_id)
    except Exception as e:
        logger.debug(f"safe_username: client.get_users failed for {seller_id} -> {e}")

    # fallback: беремо з локальної БД (якщо є таблиця crash з username)
    try:
        from contextlib import closing
        with closing(sqlite3.connect(DBB)) as db:
            cur = db.cursor()
            cur.execute("SELECT username FROM crash WHERE id = ?", (seller_id,))
            r = cur.fetchone()
            if r and r[0]:
                return f"@{r[0]}"
    except Exception as e:
        logger.debug(f"safe_username: db fallback failed for {seller_id} -> {e}")

    # останній fallback — просто показуємо id
    return f"#{seller_id}"

# Виправлений market_item_group
@app.on_callback_query(filters.regex(r"buy_item_group_(.+)"))
@rate_limit
async def market_item_group(client, callback_query):
    """Отображение списка предложений для выбранного предмета."""
    try:
        item = callback_query.data.split("_", 3)[-1]
        user_id = callback_query.from_user.id

        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            cursor.execute(
                "SELECT rowid, seller_id, price, quantity FROM MARKETPLACE WHERE item_name = ?",
                (item,)
            )
            offers = cursor.fetchall()

        if not offers:
            try:
                await callback_query.message.edit_text("❌ Этих предметов больше нет в продаже.")
            except MessageNotModified:
                pass
            return

        buttons = []
        text = f"📦 <b>{item}</b> — предложения:\n\n"
        for rowid, seller_id, price, quantity in offers:
            # Використовуємо безпечний хелпер замість прямого get_users
            seller_display = await safe_username(client, seller_id)
            name = seller_display[:10]  # обрізаємо довге ім'я
            buttons.append([InlineKeyboardButton(
                f"{name} | {format_balance(price)} | {quantity} шт",
                callback_data=f"buy_confirm_{rowid}_{item}"
            )])
        buttons.append([InlineKeyboardButton("Назад", callback_data="market_view_edit")])

        try:
            await callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except MessageNotModified:
            logger.debug("market_item_group: message not modified, ignoring.")
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка в market_item_group (item={item}): {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)


@app.on_callback_query(filters.regex(r"buy_confirm_(\d+)_(.+)"))
@rate_limit
async def buy_confirm(client, callback_query):
    """Отображение подтверждения покупки (изменено: используется safe_username)."""
    try:
        data = callback_query.data.split("_")
        market_id = int(data[2])
        item = data[3]
        user_id = callback_query.from_user.id

        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            cursor.execute(
                "SELECT seller_id, price, quantity FROM MARKETPLACE WHERE rowid = ? AND item_name = ?",
                (market_id, item)
            )
            offer = cursor.fetchone()

            if not offer:
                try:
                    await callback_query.message.edit_text("❌ Предмет уже куплен или удален.")
                except MessageNotModified:
                    pass
                return

            seller_id, price, quantity = offer

            # Получаем отображаемое имя продавца безопасно
            seller_name = await safe_username(client, seller_id)

            # Проверяем баланс покупателя
            cursor.execute("SELECT money FROM crash WHERE id = ?", (user_id,))
            buyer_money = cursor.fetchone()
            if not buyer_money or buyer_money[0] < price:
                try:
                    await callback_query.message.edit_text("❌ Недостаточно монет.")
                except MessageNotModified:
                    pass
                return

        # Формируем сообщение подтверждения
        text = (
            f"📦 <b>Подтверждение покупки</b>\n\n"
            f"Предмет: <b>{item}</b>\n"
            f"Продавец: <b>{seller_name}</b>\n"
            f"Цена за единицу: <b>{format_balance(price)}</b> монет\n"
            f"Количество: <b>1</b> (доступно: {quantity})\n"
            f"Ваш баланс: <b>{format_balance(buyer_money[0])}</b> монет\n\n"
            f"Подтвердите покупку:"
        )
        buttons = [
            [InlineKeyboardButton("Подтвердить покупку", callback_data=f"buy_final_{market_id}_{item}")],
            [InlineKeyboardButton("Назад", callback_data=f"buy_item_group_{item}")]
        ]

        try:
            await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified:
            logger.debug("buy_confirm: message not modified, ignoring.")
        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка в buy_confirm (market_id={locals().get('market_id')}, item={locals().get('item')}): {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)


@app.on_callback_query(filters.regex(r"buy_final_(\d+)_(.+)"))
@rate_limit
async def buy_final(client, callback_query):
    """Финализация покупки предмета (изменено: safe уведомления продавцу и защита от ошибок)."""
    try:
        data = callback_query.data.split("_")
        market_id = int(data[2])
        item = data[3]
        user_id = callback_query.from_user.id
        current_message_text = callback_query.message.text or ""

        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            # Получаем информацию о предложении
            cursor.execute(
                "SELECT seller_id, price, quantity FROM MARKETPLACE WHERE rowid = ? AND item_name = ?",
                (market_id, item)
            )
            offer = cursor.fetchone()

            if not offer:
                try:
                    await callback_query.message.edit_text("❌ Предмет уже куплен или удален.")
                except MessageNotModified:
                    pass
                return

            seller_id, price, quantity = offer

            # Проверяем, что покупатель не покупает у самого себя
            if user_id == seller_id:
                try:
                    await callback_query.message.edit_text("❌ Нельзя купить предмет у самого себя.")
                except MessageNotModified:
                    pass
                return

            # Проверяем баланс покупателя
            cursor.execute("SELECT money FROM crash WHERE id = ?", (user_id,))
            buyer_money = cursor.fetchone()
            if not buyer_money or buyer_money[0] < price:
                try:
                    await callback_query.message.edit_text("❌ Недостаточно монет.")
                except MessageNotModified:
                    pass
                return

            # Формируем новое сообщение
            new_message_text = f"🎉 Вы купили <b>{item}</b> за <b>{format_balance(price)}</b> монет."

            # Проверяем, нужно ли редактировать сообщение
            if new_message_text == current_message_text:
                logger.debug(f"Сообщение не изменено для market_id={market_id}, пропускаем редактирование.")
                await callback_query.answer("Покупка уже обработана.", show_alert=True)
                return

            try:
                # Обновляем деньги
                cursor.execute(
                    "UPDATE crash SET money = money - ? WHERE id = ?",
                    (price, user_id)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось списать деньги с баланса покупателя.")

                cursor.execute(
                    "UPDATE crash SET money = money + ? WHERE id = ?",
                    (price, seller_id)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось начислить деньги продавцу.")

                # Обновляем инвентарь покупателя
                cursor.execute(
                    "INSERT OR IGNORE INTO inv_user (user_id, item_name, quantity) VALUES (?, ?, 0)",
                    (user_id, item)
                )
                cursor.execute(
                    "UPDATE inv_user SET quantity = quantity + 1 WHERE user_id = ? AND item_name = ?",
                    (user_id, item)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось обновить инвентарь покупателя.")

                # Обновляем маркетплейс
                if quantity <= 1:
                    cursor.execute("DELETE FROM MARKETPLACE WHERE rowid = ?", (market_id,))
                else:
                    cursor.execute(
                        "UPDATE MARKETPLACE SET quantity = quantity - 1 WHERE rowid = ?",
                        (market_id,)
                    )
                    if cursor.rowcount == 0:
                        raise Exception("Не удалось обновить количество на маркетплейсе.")

                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Ошибка при покупке предмета (market_id={market_id}): {e}")
                try:
                    await callback_query.message.edit_text("❌ Ошибка при покупке предмета.")
                except MessageNotModified:
                    pass
                return

        # Формируем кнопки (без "Купить еще", если предмет удален)
        buttons = [[InlineKeyboardButton("Назад", callback_data="market_view_edit")]]
        if quantity > 1:
            buttons.append([InlineKeyboardButton("Купить еще", callback_data=f"buy_item_group_{item}")])

        try:
            await callback_query.message.edit_text(new_message_text, reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified:
            logger.debug("buy_final: message not modified, ignoring.")

        # Уведомление админа/лог
        try:
            seller_info = get_user_data(seller_id) or {"username": str(seller_id), "id": seller_id}
            await app.send_message("-1004869586301", f"""
<b>Момент: Покупка у игрока</b>
<b>Покупатель:</b> {callback_query.from_user.first_name} (@{callback_query.from_user.username} #{callback_query.from_user.id})
<b>Продавец:</b> {seller_info.get('username')} (#{seller_info.get('id')})
<b>Предмет:</b> {item}
<b>Сумма:</b> {format_balance(price)} ({price})
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """)
        except Exception as e:
            logger.warning(f"Не удалось отправить лог о покупке админу: {e}")

        # Пытаемся уведомить продавца — оборачиваем в try/except и приводим id к int
        try:
            seller_id_int = int(seller_id)
            try:
                await app.send_message(seller_id_int, f"Ваш предмет {item} продан игроку {callback_query.from_user.first_name} за {format_balance(price)} монет.")
            except RPCError as e:
                logger.warning(f"Не удалось уведомить продавца {seller_id}: {e}")
        except (ValueError, TypeError):
            logger.warning(f"Неверный seller_id при уведомлении: {seller_id}")

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка в buy_final (market_id={locals().get('market_id')}, item={locals().get('item')}): {e}")
        try:
            await callback_query.message.edit_text("Произошла ошибка.")
        except MessageNotModified:
            pass

from typing import List, Tuple
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from contextlib import closing
import sqlite3
import logging

logger = logging.getLogger(__name__)

async def validate_item_quantity(user_id: int, item: str, quantity: int) -> Tuple[bool, str]:
    """Проверка наличия достаточного количества предметов у пользователя."""
    with closing(sqlite3.connect(DBB)) as db:
        cursor = db.cursor()
        cursor.execute("SELECT quantity FROM inv_user WHERE user_id = ? AND item_name = ?", 
                      (user_id, item))
        result = cursor.fetchone()
        if not result or result[0] < quantity:
            return False, "Недостаточно предметов для продажи."
        return True, ""

def create_quantity_keyboard(item: str, max_quantity: int, current_quantity: int) -> InlineKeyboardMarkup:
    """Создание клавиатуры для выбора количества предметов."""
    buttons = [
        [
            InlineKeyboardButton("+1", callback_data=f"sell_qty:{item}:{min(max_quantity, current_quantity + 1)}"),
            InlineKeyboardButton("+10", callback_data=f"sell_qty:{item}:{min(max_quantity, current_quantity + 10)}"),
            InlineKeyboardButton("+100", callback_data=f"sell_qty:{item}:{min(max_quantity, current_quantity + 100)}"),
            InlineKeyboardButton("Все", callback_data=f"sell_qty:{item}:{max_quantity}")
        ],
        [
            InlineKeyboardButton("-1", callback_data=f"sell_qty:{item}:{max(0, current_quantity - 1)}"),
            InlineKeyboardButton("-10", callback_data=f"sell_qty:{item}:{max(0, current_quantity - 10)}"),
            InlineKeyboardButton("-100", callback_data=f"sell_qty:{item}:{max(0, current_quantity - 100)}"),
            InlineKeyboardButton("Сброс", callback_data=f"sell_qty:{item}:0")
        ]
    ]
    if current_quantity > 0:
        buttons.append([InlineKeyboardButton("Подтвердить", callback_data=f"sell_price:{item}:{current_quantity}")])
    buttons.append([InlineKeyboardButton("Отмена", callback_data="sell_cancel")])
    return InlineKeyboardMarkup(buttons)

def create_price_keyboard(item: str, quantity: int, current_price: int) -> InlineKeyboardMarkup:
    """Создание клавиатуры для выбора цены."""
    buttons = [
        [
            InlineKeyboardButton("+100", callback_data=f"sell_price_adj:{item}:{quantity}:{current_price + 100}"),
            InlineKeyboardButton("+1к", callback_data=f"sell_price_adj:{item}:{quantity}:{current_price + 1000}"),
            InlineKeyboardButton("+10к", callback_data=f"sell_price_adj:{item}:{quantity}:{current_price + 10000}"),
            InlineKeyboardButton("+100к", callback_data=f"sell_price_adj:{item}:{quantity}:{current_price + 100000}"),
            InlineKeyboardButton("+1кк", callback_data=f"sell_price_adj:{item}:{quantity}:{current_price + 1000000}")
        ],
        [
            InlineKeyboardButton("-100", callback_data=f"sell_price_adj:{item}:{quantity}:{max(0, current_price - 100)}"),
            InlineKeyboardButton("-1к", callback_data=f"sell_price_adj:{item}:{quantity}:{max(0, current_price - 1000)}"),
            InlineKeyboardButton("-10к", callback_data=f"sell_price_adj:{item}:{quantity}:{max(0, current_price - 10000)}"),
            InlineKeyboardButton("-100к", callback_data=f"sell_price_adj:{item}:{quantity}:{max(0, current_price - 100000)}"),
            InlineKeyboardButton("-1кк", callback_data=f"sell_price_adj:{item}:{quantity}:{max(0, current_price - 1000000)}")
        ]
    ]
    buttons.append([InlineKeyboardButton("Сброс", callback_data=f"sell_price_adj:{item}:{quantity}:0")])
    if current_price > 0:
        buttons.append([InlineKeyboardButton("Подтвердить цену", callback_data=f"sell_confirm:{item}:{quantity}:{current_price}")])
    buttons.append([InlineKeyboardButton("Назад", callback_data=f"sell_qty:{item}:{quantity}")])
    return InlineKeyboardMarkup(buttons)

@app.on_callback_query(filters.regex(r"market_sell"))
@rate_limit
async def market_sell_choose_item(client, callback_query):
    """Отображение доступных предметов для продажи."""
    user_id = callback_query.from_user.id
    try:
        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            cursor.execute("SELECT item_name, quantity FROM inv_user WHERE user_id = ? AND quantity > 0", 
                          (user_id,))
            items = cursor.fetchall()

        if not items:
            await callback_query.message.edit_text("❌ У вас нет предметов для продажи.")
            return

        buttons = [
            [InlineKeyboardButton(f"{name} ×{qty}", callback_data=f"sell_item_{name}")]
            for name, qty in sorted(items, key=lambda x: x[0])  # Сортировка предметов по алфавиту
        ]
        buttons.append([InlineKeyboardButton("Назад", callback_data="market_view_edit")])
        
        await callback_query.message.edit_text(
            "📤 Выберите предмет для продажи:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в market_sell_choose_item: {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)

@app.on_callback_query(filters.regex(r"sell_item_(.+)"))
@rate_limit
async def sell_item_selection(client, callback_query):
    """Обработка выбора предмета и отображение выбора количества."""
    item = callback_query.data.split("_", 2)[-1]
    user_id = callback_query.from_user.id

    try:
        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            cursor.execute("SELECT quantity FROM inv_user WHERE user_id = ? AND item_name = ?", 
                          (user_id, item))
            result = cursor.fetchone()

        if not result or result[0] <= 0:
            await callback_query.message.edit_text("❌ У вас нет этого предмета для продажи.")
            return

        quantity = result[0]
        await callback_query.message.edit_text(
            f"📦 <b>{item}</b>\n"
            f"Доступно: {quantity} шт.\n"
            f"Выберите количество для продажи:",
            reply_markup=create_quantity_keyboard(item, quantity, 0)
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в sell_item_selection: {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)

@app.on_callback_query(filters.regex(r"sell_qty:(.+):(\d+)"))
@rate_limit
async def update_quantity(client, callback_query):
    """Обновление выбранного количества."""
    try:
        item, quantity = callback_query.data.split(":")[1:]
        quantity = int(quantity)
        user_id = callback_query.from_user.id

        valid, error = await validate_item_quantity(user_id, item, quantity)
        if not valid:
            await callback_query.answer(error, show_alert=True)
            return

        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            cursor.execute("SELECT quantity FROM inv_user WHERE user_id = ? AND item_name = ?", 
                          (user_id, item))
            max_quantity = cursor.fetchone()[0]

        await callback_query.message.edit_text(
            f"📦 <b>{item}</b>\n"
            f"Выбрано: {quantity} шт. из {max_quantity} доступных\n"
            f"Выберите количество для продажи:",
            reply_markup=create_quantity_keyboard(item, max_quantity, quantity)
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в update_quantity: {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)

@app.on_callback_query(filters.regex(r"sell_price:(.+):(\d+)"))
@rate_limit
async def select_price(client, callback_query):
    """Отображение интерфейса для выбора цены."""
    try:
        item, quantity = callback_query.data.split(":")[1:]
        quantity = int(quantity)
        user_id = callback_query.from_user.id

        valid, error = await validate_item_quantity(user_id, item, quantity)
        if not valid:
            await callback_query.answer(error, show_alert=True)
            return

        await callback_query.message.edit_text(
            f"📦 <b>{item}</b>\n"
            f"Количество: {quantity} шт.\n"
            f"Выберите цену за единицу:",
            reply_markup=create_price_keyboard(item, quantity, 0)
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в select_price: {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)

@app.on_callback_query(filters.regex(r"sell_price_adj:(.+):(\d+):(\d+)"))
@rate_limit
async def adjust_price(client, callback_query):
    """Обновление выбранной цены."""
    try:
        item, quantity, price = callback_query.data.split(":")[1:]
        quantity = int(quantity)
        price = int(price)
        user_id = callback_query.from_user.id

        valid, error = await validate_item_quantity(user_id, item, quantity)
        if not valid:
            await callback_query.answer(error, show_alert=True)
            return

        await callback_query.message.edit_text(
            f"📦 <b>{item}</b>\n"
            f"Количество: {quantity} шт.\n"
            f"Текущая цена за единицу: {format_balance(price)} монет\n"
            f"Выберите цену:",
            reply_markup=create_price_keyboard(item, quantity, price)
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в adjust_price: {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)

@app.on_callback_query(filters.regex(r"sell_confirm:(.+):(\d+):(\d+)"))
@rate_limit
async def confirm_sell(client, callback_query):
    """Подтверждение продажи и добавление на маркетплейс."""
    try:
        item, quantity, price = callback_query.data.split(":")[1:]
        quantity = int(quantity)
        price = int(price)
        user_id = callback_query.from_user.id

        if quantity <= 0:
            await callback_query.answer("❌ Выберите количество больше 0.", show_alert=True)
            return
        if price <= 0:
            await callback_query.answer("❌ Цена должна быть больше 0.", show_alert=True)
            return

        valid, error = await validate_item_quantity(user_id, item, quantity)
        if not valid:
            await callback_query.answer(error, show_alert=True)
            return

        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            try:
                # Вычитаем из инвентаря
                cursor.execute(
                    "UPDATE inv_user SET quantity = quantity - ? WHERE user_id = ? AND item_name = ?",
                    (quantity, user_id, item)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось обновить инвентарь: предмет не найден или количество не обновлено.")

                # Добавляем на маркетплейс
                cursor.execute(
                    "INSERT INTO MARKETPLACE (seller_id, item_name, price, quantity) VALUES (?, ?, ?, ?)",
                    (user_id, item, price, quantity)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось добавить предмет на маркетплейс.")

                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Ошибка при продаже предмета: {e}")
                await callback_query.answer("❌ Ошибка при добавлении предмета на маркетплейс.", show_alert=True)
                return

        await callback_query.message.edit_text(
            f"✅ Вы выставили <b>{item}</b> ×{quantity} по <b>{format_balance(price)}</b> монет за единицу.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Назад", callback_data="market_view_edit")]
            ])
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в confirm_sell: {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)
    
    await app.send_message("-1004869586301", f"""
<b>Момент: Выставление на маркетплейс</b>
<b>Продавец:</b> {callback_query.from_user.first_name} (@{callback_query.from_user.username} #{callback_query.from_user.id})
<b>Предмет:</b> {item}
<b>Сумма:</b> {format_balance(price)} ({price})
<b>Количество:</b> {quantity}
<b>Дата:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """)

@app.on_callback_query(filters.regex(r"sell_cancel"))
@rate_limit
async def cancel_sell(client, callback_query):
    """Отмена операции продажи."""
    await callback_query.message.edit_text(
        "❌ Продажа отменена.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Назад", callback_data="market_view_edit")]
        ])
    )
    await callback_query.answer()
    
@app.on_callback_query(filters.regex("market_my_offers"))
async def market_my_lots(client, callback_query):
    user_id = callback_query.from_user.id
    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT rowid, item_name, price, quantity FROM MARKETPLACE WHERE seller_id = ?", (user_id,))
    offers = cursor.fetchall()
    db.close()

    if not offers:
        return await callback_query.message.edit_text("📋 У вас нет выставленных предметов.")

    text = "📋 <b>Ваши лоты:</b>\n"
    buttons = []
    for rowid, item, price, quantity in offers:
        text += f"• {item} — {price} мон. ×{quantity}\n"
        buttons.append([InlineKeyboardButton(f"❌ Убрать {item}", callback_data=f"remove_offer_{rowid}")])
    
    buttons.append([InlineKeyboardButton(f"Назад", callback_data=f"market_view_edit")])

    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex(r"remove_offer_(\d+)"))
async def remove_offer(client, callback_query):
    rowid = int(callback_query.data.split("_")[-1])
    user_id = callback_query.from_user.id

    db = sqlite3.connect(DBB)
    cursor = db.cursor()
    cursor.execute("SELECT item_name, quantity FROM MARKETPLACE WHERE rowid = ? AND seller_id = ?", (rowid, user_id))
    offer = cursor.fetchone()
    if not offer:
        db.close()
        return await callback_query.answer("❌ Лот не найден.")

    item, quantity = offer
    cursor.execute("UPDATE inv_user SET quantity = quantity + ? WHERE user_id = ? AND item_name = ?", (quantity, user_id, item))
    cursor.execute("DELETE FROM MARKETPLACE WHERE rowid = ?", (rowid,))
    db.commit()
    db.close()

    await callback_query.answer("✅ Лот удалён.")
    await market_my_lots(client, callback_query)

from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import MessageNotModified

@app.on_callback_query(filters.regex(r"buy_(\d+)"))
@rate_limit
async def buy_item(client, callback_query):
    """Обработка покупки предмета на маркетплейсе."""
    try:
        logger.debug(f"Обработка callback_query.data: {callback_query.data}")
        market_id = int(callback_query.data.split("_")[1])
        user_id = callback_query.from_user.id
        current_message_text = callback_query.message.text or ""

        with closing(sqlite3.connect(DBB)) as db:
            cursor = db.cursor()
            # Получаем информацию о предложении
            cursor.execute(
                "SELECT seller_id, item_name, price, quantity FROM MARKETPLACE WHERE rowid = ?",
                (market_id,)
            )
            offer = cursor.fetchone()

            if not offer:
                await callback_query.answer("❌ Предмет уже куплен или удален.", show_alert=True)
                return

            seller_id, item, price, quantity = offer

            # Проверяем, что покупатель не покупает у самого себя
            if user_id == seller_id:
                await callback_query.answer("❌ Нельзя купить предмет у самого себя.", show_alert=True)
                return

            # Проверяем баланс покупателя
            cursor.execute("SELECT money FROM crash WHERE id = ?", (user_id,))
            buyer_money = cursor.fetchone()
            if not buyer_money or buyer_money[0] < price:
                await callback_query.answer("❌ Недостаточно монет.", show_alert=True)
                return

            # Формируем новое сообщение
            new_message_text = f"🎉 Вы купили <b>{item}</b> за <b>{format_balance(price)}</b> монет."

            # Проверяем, нужно ли редактировать сообщение
            if new_message_text == current_message_text:
                logger.debug(f"Сообщение не изменено для market_id={market_id}, пропускаем редактирование.")
                await callback_query.answer("Покупка уже обработана.", show_alert=True)
                return

            try:
                # Обновляем деньги
                cursor.execute(
                    "UPDATE crash SET money = money - ? WHERE id = ?",
                    (price, user_id)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось списать деньги с баланса покупателя.")

                cursor.execute(
                    "UPDATE crash SET money = money + ? WHERE id = ?",
                    (price, seller_id)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось начислить деньги продавцу.")

                # Обновляем инвентарь покупателя
                cursor.execute(
                    "INSERT OR IGNORE INTO inv_user (user_id, item_name, quantity) VALUES (?, ?, 0)",
                    (user_id, item)
                )
                cursor.execute(
                    "UPDATE inv_user SET quantity = quantity + 1 WHERE user_id = ? AND item_name = ?",
                    (user_id, item)
                )
                if cursor.rowcount == 0:
                    raise Exception("Не удалось обновить инвентарь покупателя.")

                # Обновляем маркетплейс
                if quantity <= 1:
                    cursor.execute("DELETE FROM MARKETPLACE WHERE rowid = ?", (market_id,))
                else:
                    cursor.execute(
                        "UPDATE MARKETPLACE SET quantity = quantity - 1 WHERE rowid = ?",
                        (market_id,)
                    )
                    if cursor.rowcount == 0:
                        raise Exception("Не удалось обновить количество на маркетплейсе.")

                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Ошибка при покупке предмета (market_id={market_id}): {e}")
                await callback_query.answer("❌ Ошибка при покупке предмета.", show_alert=True)
                return

        # Формируем кнопки (без "Купить еще", если предмет удален)
        buttons = [[InlineKeyboardButton("Назад", callback_data="market_buy")]]

        try:
            await callback_query.message.edit_text(
                new_message_text,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except MessageNotModified:
            logger.debug(f"Сообщение не изменено для market_id={market_id}, игнорируем ошибку.")
            await callback_query.answer("Покупка уже обработана.", show_alert=True)
            return

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Ошибка в buy_item (market_id={market_id}): {e}")
        await callback_query.answer("Произошла ошибка.", show_alert=True)

@app.on_callback_query(filters.regex(r"market_view_edit"))
async def market_view_edit(client, callback_query):
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛍 Купить", callback_data="market_buy")],
        [InlineKeyboardButton("📦 Продать", callback_data="market_sell")],
        [InlineKeyboardButton("📋 Мои лоты", callback_data="market_my_offers")]
    ])
    await callback_query.message.edit_text("🛒 <b>Маркет:</b>", reply_markup=markup)

@app.on_callback_query(filters.regex(r"market_view"))
async def market_view(client, callback_query):
    await view_market(client, callback_query.message)
    await callback_query.answer()


async def reset_data_command(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        return

    try:
        db = sqlite3.connect(DBB)
        cursor = db.cursor()
        cursor.execute("""
            UPDATE crash
            SET money = 0,
                stone = 0,
                iron = 0,
                gold = 0,
                diamond = 0,
                emerald = 0,
                lose_count = 0,
                win_count = 0,
                total_games = 0
        """)
        db.commit()
        db.close()
        await message.reply("УСПЕХ УУСУКА")
    except Exception as e:
        await message.reply(e)
        
async def reset_all_command(client, message):
    user_id = message.from_user.id
    if user_id not in API_OWNER:
        return

    try:
        db = sqlite3.connect(DBB)
        cursor = db.cursor()

        cursor.execute("""
            UPDATE inv
            SET details = 0,
                stone_sediments = 0,
                iron_ingots = 0,
                gold_bars = 0,
                diamond_cores = 0,
                emerald_dust = 0,
                iron_ingots_dense = 0,
                gold_nuggets = 0
        """)

        cursor.execute("DELETE FROM user_promos")
        cursor.execute("DELETE FROM promos")

        cursor.execute("UPDATE crash SET status = 0")

        db.commit()
        db.close()

        await message.reply("УСПЕХ УУСУКА")
    except Exception as e:
        await message.reply(e)

INV_PAGE_SIZE = 10
INV_PAGES = {}    
INV_CALLER = {}   
INV_SORT = {}     

async def show_inventory(client, message):
    user_id = message.from_user.id
    if not await check_user(user_id):
        await message.reply("Вы не зарегистрированы.\nИспользуйте /ss или рег для регистрации.")
        return
    if await is_banned_user(user_id):
        await message.reply("Вы забанены.")
        return

    # Инициализируем страницу и сортировку для пользователя
    INV_PAGES[user_id] = 0
    INV_SORT[user_id] = "default"  # По умолчанию сортировка "по умолчанию"

    # Получаем предметы из инвентаря пользователя
    items = fetch_inventory_items(user_id, INV_SORT[user_id])

    if not items:
        await message.reply("Ваш инвентарь пуст.")
        return

    # Отправляем первую страницу
    msg = await message.reply("🎒 Загружаю инвентарь...")
    INV_CALLER[msg.id] = user_id  # Сохраняем, кто вызвал /inv
    await update_inventory_message(client, msg, user_id, items, 0)

# Функция для получения предметов из инвентаря с учетом сортировки
def fetch_inventory_items(user_id, sort_type):
    db = sqlite3.connect(DBB)
    cursor = db.cursor()

    # Базовый запрос: соединяем inv_user с shop, чтобы получить цену и created_at
    base_query = """
        SELECT inv_user.item_name, inv_user.quantity, shop.price, shop.created_at
        FROM inv_user
        JOIN shop ON inv_user.item_name = shop.name
        WHERE inv_user.user_id = ?
    """

    # Определяем ORDER BY в зависимости от типа сортировки
    if sort_type == "cheap":
        order_by = "ORDER BY shop.price ASC"
    elif sort_type == "expensive":
        order_by = "ORDER BY shop.price DESC"
    elif sort_type == "new":
        order_by = "ORDER BY shop.created_at DESC"
    elif sort_type == "old":
        order_by = "ORDER BY shop.created_at ASC"
    elif sort_type == "quantity_high":
        order_by = "ORDER BY inv_user.quantity DESC"
    elif sort_type == "quantity_low":
        order_by = "ORDER BY inv_user.quantity ASC"
    else:
        order_by = ""  # По умолчанию без сортировки

    query = f"{base_query} {order_by}"
    cursor.execute(query, (user_id,))
    items = cursor.fetchall()
    db.close()
    return items

# Функция для обновления сообщения инвентаря
async def update_inventory_message(client, message, user_id, items, page):
    start_idx = page * INV_PAGE_SIZE
    end_idx = start_idx + INV_PAGE_SIZE
    items_page = items[start_idx:end_idx]

    # Определяем текущий критерий сортировки
    current_sort = INV_SORT.get(user_id, "default")
    sort_display = {
        "default": "По умолчанию",
        "cheap": "Дешевые",
        "expensive": "Дорогие",
        "new": "Новые",
        "old": "Старые",
        "quantity_high": "Кол-во (много)",
        "quantity_low": "Кол-во (мало)"
    }
    text = (f"🎒 <b>Ваш инвентарь (страница {page + 1}/{((len(items) - 1) // INV_PAGE_SIZE) + 1}):</b>\n"
            f"Всего предметов: {len(items)}\n"
            f"Сортировка: {sort_display[current_sort]}\n\n"
            f"Список предметов:")
    
    # Создаем кнопки для предметов
    buttons = []
    for item_name, quantity, price, created_at in items_page:
        if quantity == 0:
            pass
        else:
            button_text = f"{item_name} — ({quantity})"
            buttons.append([InlineKeyboardButton(button_text, callback_data=f"inv_info:{item_name}:{page}")])

    # Добавляем кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅ Назад", callback_data=f"inv_page:{page - 1}"))
    if end_idx < len(items):
        nav_buttons.append(InlineKeyboardButton("Вперед ➡", callback_data=f"inv_page:{page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    # Добавляем кнопки сортировки с выделением текущей
    sort_buttons = [
        [InlineKeyboardButton(f"Дешевые{' ✅' if current_sort == 'cheap' else ''}", callback_data="inv_sort:cheap"),
         InlineKeyboardButton(f"Дорогие{' ✅' if current_sort == 'expensive' else ''}", callback_data="inv_sort:expensive")],
        [InlineKeyboardButton(f"Новые{' ✅' if current_sort == 'new' else ''}", callback_data="inv_sort:new"),
         InlineKeyboardButton(f"Старые{' ✅' if current_sort == 'old' else ''}", callback_data="inv_sort:old")],
        [InlineKeyboardButton(f"Кол-во (много){' ✅' if current_sort == 'quantity_high' else ''}", callback_data="inv_sort:quantity_high"),
         InlineKeyboardButton(f"Кол-во (мало){' ✅' if current_sort == 'quantity_low' else ''}", callback_data="inv_sort:quantity_low")],
        [InlineKeyboardButton(f"По умолчанию{' ✅' if current_sort == 'default' else ''}", callback_data="inv_sort:default")]
    ]
    buttons.extend(sort_buttons)

    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
    await client.edit_message_text(
        chat_id=message.chat.id,
        message_id=message.id,
        text=text,
        reply_markup=reply_markup
    )

# Обработчик кнопок для смены страниц
@app.on_callback_query(filters.regex(r"inv_page:(\d+)"))
async def change_inventory_page(client, callback_query):
    page = int(callback_query.matches[0].group(1))
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /inv
    if INV_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш инвентарь!", show_alert=True)
        return

    # Получаем предметы с учетом текущей сортировки
    items = fetch_inventory_items(user_id, INV_SORT.get(user_id, "default"))

    if not items:
        await callback_query.answer("Ваш инвентарь пуст.", show_alert=True)
        return

    # Обновляем текущую страницу пользователя
    INV_PAGES[user_id] = page

    # Редактируем сообщение
    await update_inventory_message(client, callback_query.message, user_id, items, page)
    await callback_query.answer()

# Обработчик кнопок сортировки
@app.on_callback_query(filters.regex(r"inv_sort:(\w+)"))
async def change_inventory_sort(client, callback_query):
    sort_type = callback_query.matches[0].group(1)
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /inv
    if INV_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш инвентарь!", show_alert=True)
        return

    # Проверяем, не выбрана ли уже эта сортировка
    current_sort = INV_SORT.get(user_id, "default")
    if current_sort == sort_type:
        await callback_query.answer("Эта сортировка уже выбрана!", show_alert=True)
        return

    # Обновляем критерий сортировки
    INV_SORT[user_id] = sort_type
    INV_PAGES[user_id] = 0  # Сбрасываем страницу на первую

    # Получаем предметы с новой сортировкой
    items = fetch_inventory_items(user_id, sort_type)

    if not items:
        await callback_query.answer("Ваш инвентарь пуст.", show_alert=True)
        return

    # Обновляем сообщение
    await update_inventory_message(client, callback_query.message, user_id, items, 0)
    await callback_query.answer(f"Сортировка изменена на: {sort_type}")

# Обработчик нажатия на предмет
@app.on_callback_query(filters.regex(r"inv_info:"))
async def inventory_item_info(client, callback_query):
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /inv
    if INV_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш инвентарь!", show_alert=True)
        return

    parts = callback_query.data.split(":")
    item_name, page = parts[1], int(parts[2])

    db = sqlite3.connect(DBB)
    cursor = db.cursor()

    # Получаем данные из инвентаря (сколько у пользователя предмета)
    cursor.execute("SELECT quantity FROM inv_user WHERE user_id = ? AND item_name = ?", (user_id, item_name))
    user_item = cursor.fetchone()

    # Получаем данные из магазина (цена и общее кол-во)
    cursor.execute("SELECT name, price, quantity FROM shop WHERE name = ?", (item_name,))
    shop_item = cursor.fetchone()

    db.close()

    if not shop_item:
        await callback_query.answer("Предмет не найден.", show_alert=True)
        return

    name, price, all_quantity = shop_item
    user_quantity = user_item[0] if user_item else 0

    text = (f"🎒 <b>Предмет: <code>{name}</code></b>\n"
            f"💰 Цена: {price} монет\n"
            f"📦 У вас: {user_quantity} шт.\n"
            f"📦 Всего в магазине: {all_quantity} шт.")

    buttons = [[InlineKeyboardButton("🔙 Назад", callback_data=f"inv_back:{page}")]]
    reply_markup = InlineKeyboardMarkup(buttons)

    await client.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=message_id,
        text=text,
        reply_markup=reply_markup
    )

    await callback_query.answer()

# Обработчик кнопки "Назад"
@app.on_callback_query(filters.regex(r"inv_back:"))
async def inventory_back(client, callback_query):
    user_id = callback_query.from_user.id
    message_id = callback_query.message.id

    # Проверяем, что кнопку нажимает тот, кто вызвал /inv
    if INV_CALLER.get(message_id) != user_id:
        await callback_query.answer("Это не ваш инвентарь!", show_alert=True)
        return

    page = int(callback_query.data.split(":")[1])

    # Получаем предметы с учетом текущей сортировки
    items = fetch_inventory_items(user_id, INV_SORT.get(user_id, "default"))

    if not items:
        await callback_query.answer("Ваш инвентарь пуст.", show_alert=True)
        return

    # Возвращаемся к списку предметов
    await update_inventory_message(client, callback_query.message, user_id, items, page)
    await callback_query.answer()
import os
import speech_recognition as sr
from pydub import AudioSegment

from pyrogram import Client, filters
import os
import speech_recognition as sr
import subprocess

FFMPEG_PATH = r"C:\Users\Yuukiro\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-7.1.1-full_build\bin\ffmpeg.exe"

# @app.on_message(filters.voice)
@rate_limit
async def handle_voice_command(client, message):
    user_id = message.from_user.id
    tes = await message.reply_text("Распознование голосового сообщение...")

    ogg_path = await message.download()
    wav_path = ogg_path.replace(".ogg", ".wav")

    try:
        subprocess.run([
            FFMPEG_PATH,
            "-i", ogg_path,
            wav_path
        ], check=True)

        recognizer = sr.Recognizer()
        with sr.AudioFile(wav_path) as source:
            audio_data = recognizer.record(source)

        text = recognizer.recognize_google(audio_data, language="ru-RU")
        print(f"[VOICE CMD] Распознано: {text}")

        message.text = "/" + text.lower()
        await handle_text_commands(client, message)
        await tes.edit(f"Распознано: \n```voice\n{text}```", parse_mode=enums.ParseMode.MARKDOWN)

    except subprocess.CalledProcessError:
        await tes.edit("❌ Ошибка при конвертации голосового сообщения.")
    except sr.UnknownValueError:
        await tes.edit("🤔 Не удалось распознать голосовое сообщение.")
    except sr.RequestError:
        await tes.edit("🚫 Ошибка соединения с сервисом распознавания.")
    finally:
        if os.path.exists(ogg_path):
            os.remove(ogg_path)
        if os.path.exists(wav_path):
            os.remove(wav_path)

import logging
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ChatMemberStatus

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def join_channel_command(client: Client, message):
    """Команда для подписки бота на канал."""
    
    # Проверка, что команда отправлена администратором (опционально)
    if message.from_user.id not in API_OWNER:  # Замените YOUR_ADMIN_ID на ваш ID
        await message.reply_text("Эта команда доступна только администратору.")
        return

    # ID канала (публичный канал можно указать как @username)
    channel_url = "@GG_dangerizardhe"  # Укажите нужный канал

    try:
        # Попытка подписаться на канал
        chat = await client.join_chat(channel_url)
        await message.reply_text(f"Бот успешно подписался на канал: {chat.title}!")
    except Exception as e:
        # Логируем ошибку для отладки
        logger.error(f"Ошибка при подписке на канал: {e}")
        if "USER_ALREADY_PARTICIPANT" in str(e):
            await message.reply_text("Бот уже подписан на этот канал.")
        elif "CHAT_ADMIN_REQUIRED" in str(e):
            await message.reply_text("Бот не может подписаться, так как требуется приглашение от администратора канала.")
        else:
            await message.reply_text(f"Произошла ошибка при подписке: {str(e)}")

async def save_log(log_data: dict):
    """
    Сохраняет лог-сообщение в JSON файл.
    
    Args:
        log_data (dict): Словарь с информацией для логирования
    """

    
    # Путь к файлу логов
    log_file = 'log.json'
    logs = []
    
    # Чтение существующих логов, если файл существует
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                logs = json.load(f)
            if not isinstance(logs, list):  # На случай повреждения файла
                logs = []
        except (json.JSONDecodeError, Exception):
            logs = []
    
    # Добавление временной метки, если её нет
    if 'time' not in log_data:
        log_data['time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Добавление новой записи
    logs.append(log_data)
    
    # Сохранение в файл
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка при сохранении лога: {e}")

# пример: log_msgs = {
    #     "chat_id": message.chat.id,
    #     "message_id": message.id,
    #     "user_id": message.from_user.id,
    #     "username": message.from_user.username,
    #     "first_name": message.from_user.first_name,
    #     "text": txt,
    #     "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # }
    # await save_log(log_msg)


antispam_cache = {}
from time import time
async def antispam(client: Client, message: Message):
    user_id = message.from_user.id

    if user_id in API_OWNER:
        return False

    now = time()
    
    if user_id not in antispam_cache:
        antispam_cache[user_id] = {'count': 1, 'time': now}
        return False

    # если прошло больше 3 секунд — сбрасываем счётчик
    if now - antispam_cache[user_id]['time'] > 3:
        antispam_cache[user_id]['count'] = 1
        antispam_cache[user_id]['time'] = now
        return False

    antispam_cache[user_id]['count'] += 1

    if antispam_cache[user_id]['count'] > 5:
        await message.delete()
        return True

    return False

safe_games = {}

def _is_safe_active(_, __, message):
    try:
        return message.from_user and (message.from_user.id in safe_games)
    except Exception:
        return False

from pyrogram import filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
import asyncio
import random as rnd

@app.on_message(
    (filters.command("safe") | filters.regex(r"^сейф(?:\s+\S+)?", flags=re.IGNORECASE))
)
async def start_safe(client, message: Message):
    user_id = message.from_user.id

    # Розбір ставки
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await message.reply_text("⚠ Использование: /safe <ставка> или сейф <ставка>")

    balance = await get_balance(user_id)
    bet = parse_bet_input(parts[1].strip(), balance)
    if bet == -1:
        return await message.reply_text("Неверная ставка.")
    if bet < 10:
        return await message.reply_text("Ставка должна быть не меньше 10 монет.")
    if bet > balance:
        return await message.reply_text("Ставка не может превышать ваш баланс.")

    # Перевірка cooldown
    now = asyncio.get_event_loop().time()
    if not hasattr(start_safe, "_cooldowns"):
        start_safe._cooldowns = {}
    cd = start_safe._cooldowns
    if user_id in cd and now - cd[user_id] < 5:
        return await message.reply_text("Подождите перед новой игрой.")
    cd[user_id] = now

    # Списуємо ставку
    await update_balance(user_id, -bet)

    # Генерація ключів
    correct_key = rnd.randint(1, 4)
    safe_games[user_id] = {"correct": correct_key, "bet": bet}

    # Створюємо кнопки
    buttons = [
        [InlineKeyboardButton(f"🔑 Ключ {i}", callback_data=f"safe_pick:{user_id}:{i}")]
        for i in range(1, 5)
    ]
    cancel_btn = [InlineKeyboardButton("❌ Отменить игру", callback_data=f"safe_cancel:{user_id}")]
    buttons.append(cancel_btn)

    await message.reply_text(
        f"🔒 Игра «Сейф» начата!\nВыбери один из 4 ключей.\n"
        f"Если угадаешь — получишь {format_balance(str(bet * 2.5))} монет!",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@app.on_callback_query(filters.regex(r"^safe_pick:(\d+):(\d+)$"))
async def safe_pick_cb(client, callback_query):
    user_id = callback_query.from_user.id
    game_owner_id = int(callback_query.data.split(":")[1])
    pick = int(callback_query.data.split(":")[2])

    # Перевірка на гравця
    if user_id != game_owner_id:
        return await callback_query.answer("⛔ Это не ваша игра!", show_alert=True)

    if user_id not in safe_games:
        return await callback_query.answer("У вас нет активной игры.", show_alert=True)

    game = safe_games.pop(user_id)
    correct_key = game["correct"]
    bet = game["bet"]

    if pick == correct_key:
        win_amount = int(bet * 2.5)
        await update_balance(user_id, win_amount)
        balance = await get_balance(user_id)
        await callback_query.message.edit_text(
            f"🎉 Поздравляем! Ключ {pick} подошёл!\nВы выиграли {format_balance(win_amount)} монет!\nБаланс: {str(format_balance(balance))}"
        )
    else:
        balance = await get_balance(user_id)
        await callback_query.message.edit_text(
            f"Неверно! Правильный был ключ {correct_key}.\nСтавка сгорела.\n{format_balance(str(balance))}"
        )


@app.on_callback_query(filters.regex(r"^safe_cancel:(\d+)$"))
async def safe_cancel_cb(client, callback_query):
    user_id = callback_query.from_user.id
    game_owner_id = int(callback_query.data.split(":")[1])

    if user_id != game_owner_id:
        return await callback_query.answer("⛔ Это не ваша игра!", show_alert=True)

    if user_id not in safe_games:
        return await callback_query.answer("У вас нет активной игры.", show_alert=True)

    game_data = safe_games.pop(user_id)
    refund = game_data["bet"]
    await update_balance(user_id, refund)

    await callback_query.message.edit_text(
        f"Игра отменена. Вам возвращено {format_balance(refund)} монет."
    )

@app.on_message(filters.text)
@rate_limit
async def handle_text_commands(client: Client, message):
    
    if not message.from_user:
        return

    parts = message.text.split()
    txt = parts[0].lower().lstrip('/')
    tt = 'GG_dangerizardhe'
    channel_url = f"@{tt}" 

    valid_commands = {
        'список', 'помощь', 'hb', 'кости', 'бонус', 'купить', 'рул', 'перевести', 'топ', 'минер', 'bb', 'ss', 'рег', "рулетка", "шоп", "shop", "магазин", "sell", "продать", "market", "маркет", "bank", "банк",
        'я', 'pr', 'meb', 'tb', 'краш', 'inv', 'инв', 'mine', 'добыть', 'drill', 'дрель', 'пирамида', "duel", "дуэль", "дуель", "банк", "bank", "miner", "минеры",
        'pay', 'sell', 'продать', 'market', 'маркет', 'new_promo', "qu", 'quests', "квесты", "реф", "my_ref", "start", "st", "скрыть", "башня", "сейф"
    }

    if txt not in valid_commands:
        return
    
    if await antispam(client, message):
        await app.send_message(message.chat.id, f" {message.from_user.first_name} не спамь!")
        return

    user_id = message.from_user.id
    username = message.from_user.username or f"User_{user_id}"
    first_name = message.from_user.first_name or "NoName"

    # Регистрация или обновление данных
    if not await check_user(user_id):
        if txt in ['ss', 'рег']:
            await register_command(client, message)
        elif txt == 'start':
            await start_handler(client, message)
        else:
            await message.reply_text("<b>Вы не зарегистрированы.</b>\nИспользуйте <code>/ss</code> или <code>/рег</code>.")
        return
    else:
        await update_user_data(user_id, username, first_name)

    if await is_banned_user(user_id):
        await message.reply_text("Вы забанены.")
        return

    try:
        member = await client.get_chat_member(channel_url, message.from_user.id)
        if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
            btn = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Подписаться", url=f"https://t.me/{tt}")]]
            )
            await message.reply_text(
                "Чтобы играть, подпишитесь на канал 👇",
                reply_markup=btn
            )
            return
    except Exception as e:
        logger.error(f"Ошибка при проверке подписки: {e}")
        if "ChatAdminRequired" in str(e):
            btn = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Подписаться", url=f"https://t.me/{tt}")]]
            )
            await message.reply_text(
                "Чтобы играть, подпишитесь на канал 👇",
                reply_markup=btn
            )
            return
        else:
            btn = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Подписаться", url=f"https://t.me/{tt}")]]
            )
            await message.reply_text(
                "Чтобы играть, подпишитесь на канал 👇",
                reply_markup=btn
            )
            return
    if message.forward_from:
        return

    log_msgs = {
        "chat_id": message.chat.id,
        "message_id": message.id,
        "user_id": message.from_user.id,
        "username": message.from_user.username,
        "first_name": message.from_user.first_name,
        "text": message.text,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    await save_log(log_msgs)

    # меняяем ё на е
    txt = txt.replace('ё', 'е') 
    txt = txt.replace('Ё', 'Е')
    command_map = {
        ('start',): start_handler,
        ('список', 'помощь', 'hb'): help_command,
        ('кости',): dice_command,
        ('бонус',): bonus_command,
        ('купить', 'bb'): buy_status_command,
        ('рул', "рулетка"): roulette_command,
        ('перевести', 'pay',): transfer_command,
        ('топ', 'tb'): top_balance_command,
        ('ss', 'рег'): register_command,
        ('минер', "miner", "минеры"): mines_command,
        ('я', 'meb'): profile_command_short,
        ('pr',): activate_promo_command,
        ('краш',): crash_command,
        ('duel', 'дуэль', 'дуель'): duel_command,
        ('inv', 'инв', 'инвентарь'): show_inventory,
        ('market', 'маркет'): view_market,
        ('shop', 'шоп', "магазин"): show_shop,
        ('sell', 'продать'): sell_item,
        ('башня',): start_tower,
        ('банк', "bank"): bank_command,
        ("new_promo"): create_promo_command,
        ('qu', 'quests', 'квесты'): show_quests,
        ('my_ref', 'реф'): my_refs_handler,
        ('ref', 'рефы'): my_refs_handler,

        ('st', 'скрыть'): toggle_top_status,
    }

    for keys, func in command_map.items():
        if txt in keys:
            await func(client, message)
            break   





from random import random, uniform

config = {
    "hard_crash_chance": 0.5,  # 30% шанс на жесткий слив
    "hard_crash_multiplier_min": 2.41,  # Множитель для жесткого слива
    "hard_crash_max_sub": 1.59,  # Максимальное снижение для жесткого слива
    "r_adjustment": {
        2: 0.05,
        3: 0.1,
        4: 0.15,
        5: 0.2
    }
}

# async def get_crash_point(multiplier: float, config=None):
#     """
#     Возвращает точку краша с улучшенной логикой.
#     - 60% шанс на "жёсткий слив" (низкий crash) при высоком множителе.
#     - Остальные случаи распределяются по весам: чаще даёт x2–x3, реже x5+, почти никогда x10+.
#     """

#     # Конфигурация по умолчанию, если не передана
#     if config is None:
#         config = {
#             'hard_crash_multiplier_min': 2.41,
#             'hard_crash_chance': 0.6,
#             'hard_crash_max_sub': 1.59,
#         }

#     chance = random()

#     if multiplier < 2.41:
#         rr = random()
#         if rr < 0.7:
#             return round(uniform(1.00, 2.50), 2)
#         else:
#             return round(uniform(1.00, 2.1  ), 2)
#     # 📉 Жёсткий слив
#     if multiplier >= config['hard_crash_multiplier_min']:
#         if chance < config['hard_crash_chance']:
#             max_crash = multiplier - config['hard_crash_max_sub']
#             return round(uniform(1.00, max_crash), 2) if max_crash > 1.01 else round(uniform(1.00, 1.20), 2)

#     # 📊 Распределение шансов (более реалистично, без жирных x)
#     r = random()
#     crash_tiers = [
#         (1.0, 0.1),               # 10% автокраш
#         (uniform(1.5, 2.5), 0.5), # 50% победа x2
#         (uniform(2.51, 3.5), 0.2),# 20% победа x3
#         (uniform(3.51, 5.0), 0.1),# 10% x4–x5
#         (uniform(5.01, 7.0), 0.07),# 7% x6–x7
#         (uniform(7.01, 10.0), 0.03)# 3% эпик x8–x10
#     ]

#     cumulative = 0
#     for value, weight in crash_tiers:
#         cumulative += weight
#         if r < cumulative:
#             return round(value, 2)

#     # fallback на всякий
#     return round(uniform(1.0, 2.0), 2)


async def generate_crash_point():
    rand = random()
    if rand <= 0.10:
        return 1.00
    elif rand <= 0.66:
        return round(uniform(1.01, 2.00), 2)
    elif rand <= 0.89:
        return round(uniform(2.01, 5.00), 2)
    elif rand <= 0.96:
        return round(uniform(5.01, 8.00), 2)
    else:
        return round(uniform(8.01, 15.00), 2)

async def crash_command(client, message):
    """Игра в краш с указанием ставки и множителя."""
    try:
        parts = message.text.split()
        if len(parts) != 3:
            await message.reply_text("<i>Используйте: краш (сумма ставки) (икс)</i>\nПример: краш 100к 2.5")
            return

        bet_amount_str, multiplier_str = parts[1], parts[2]
        if not re.match(r"^\d+(\.\d{1,2})?$", multiplier_str):
            await message.reply_text("<i>Неправильный формат множителя (например, 2.5).</i>")
            return

        multiplier = float(multiplier_str)
        if not 1.01 <= multiplier <= 15.00:
            await message.reply_text("<i>Множитель должен быть от 1.01 до 15.00.</i>")
            return

        user_id = message.from_user.id
        user_data = get_user_data(user_id)
        bet_amount = parse_bet_amount(bet_amount_str, user_data['money'])

        if bet_amount is None:
            await message.reply_text("<i>Неправильный формат суммы ставки.</i>")
            return
        if bet_amount < 10:
            await message.reply_text("<i>Минимальная ставка 10 монет.</i>")
            return
        if user_data['money'] < bet_amount:
            await message.reply_text("<i>Недостаточно монет.</i>")
            return

        crash_point = await generate_crash_point()

        with sqlite3.connect(DBB) as db:
            cursor = db.cursor()
            if crash_point >= multiplier:
                win_amount = int(bet_amount * multiplier)
                cursor.execute("UPDATE crash SET money = money + ? WHERE id = ?", (float(win_amount - bet_amount), user_id))
                db.commit()
                await set_win_monet(user_id, bet_amount)
                mmoney = format_balance(user_data['money'] - bet_amount)
                bet_amount1 = format_balance(bet_amount)
                await message.reply_text(
                    f"🎉 <b>Вы выиграли</b> <code>{format_balance(win_amount)}</code> <b>монет</b>\n"
                    f"📈 <b>Точка краша:</b> <code>{crash_point:.2f}</code>\n"
                    f"🎯 <b>Множитель:</b> <code>{multiplier:.2f}</code>\n"
                    f"💸 <b>Ставка:</b> <code>{bet_amount1}</code>\n"
                    f"💰 <b>Баланс:</b> <code>{format_balance(user_data['money'] + win_amount - bet_amount)}</code>"
                )
            else:
                cursor.execute("UPDATE crash SET money = money - ? WHERE id = ?", (float(bet_amount), user_id))
                db.commit()
                await set_lose_monet(user_id, bet_amount)
                bet_amount1 = format_balance(bet_amount)
                
                await message.reply_text(
                    f"😔 <b>Вы проиграли!</b>\n"
                    f"📈 <b>Точка краша:</b> <code>{crash_point:.2f}</code>\n"
                    f"🎯 <b>Множитель:</b> <code>{multiplier:.2f}</code>\n"
                    f"💸 <b>Ставка:</b> <code>{bet_amount1}</code>\n"
                    f"💰 <b>Баланс:</b> <code>{format_balance(user_data['money'] - bet_amount)}</code>"
                )
    except Exception as e:
        await message.reply_text(f"Произошла ошибка: {e}")

from datetime import datetime, timedelta
import asyncio

async def update_bank_interest():
    moscow_tz = pytz.timezone('Europe/Moscow')
    while True:
        now = datetime.now(moscow_tz)
        # Проверяем, понедельник ли сегодня и время 00:00 по МСК
        if now.weekday() == 0 and now.hour == 0 and now.minute == 0:
            with sqlite3.connect(DBB) as db:
                cursor = db.cursor()
                cursor.execute("SELECT deposit_id, current_amount FROM bank_deposits WHERE is_active = 1")
                deposits = cursor.fetchall()
                for deposit_id, current_amount in deposits:
                    new_amount = int(current_amount * 1.05)  # +5%
                    cursor.execute("UPDATE bank_deposits SET current_amount = ? WHERE deposit_id = ?",
                                  (new_amount, deposit_id))
                db.commit()
            print(f"[BANK] Начислены проценты для {len(deposits)} депозитов в {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        # Ждем 1 минуту до следующей проверки
        await asyncio.sleep(60)


async def on_startup():
    print("Бот запущен!")
    asyncio.create_task(update_bank_interest()) 

app.startup_function = on_startup

app.run()
