import math
import os
import sqlite3
import json
import threading
import time
import pathlib
import webbrowser
import requests
from typing import Optional
import secrets

from flask import Flask, jsonify, render_template, request
from pyngrok import ngrok, conf
from typing import Dict

# ---------------- CONFIG ----------------
NGROK_AUTHTOKEN = "33leiRqpGWtKq9SUCP62LYGjM9T_7g5EjQd9hkkxpv4EfzNZ2"
RESERVED_DOMAIN = "irving-uncondoled-wriggly.ngrok-free.dev"
TELEGRAM_BOT_TOKEN = "8257726098:AAFD7pUUjChgkw2Ncj2Mik9zPJcjtxFjEbg"
PORT = 8000
DB_PATH = "data.db"

_username_cache: Dict[str, Optional[str]] = {}

# ---------------- Flask ----------------
app = Flask(__name__, template_folder="templates", static_folder="static")
PUBLIC_URL: Optional[str] = None


# ---------------- DB helpers ----------------

def save_user_data(user_id: str, data: dict):
    """Зберегти user data у sqlite (key -> json string)."""
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO json_data(key, value) VALUES (?, ?)",
            (str(user_id), json.dumps(data, ensure_ascii=False))
        )
        conn.commit()

def ensure_db():
    need_create = not os.path.exists(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        if need_create:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            conn.commit()
    finally:
        conn.close()

def save_user_data_sync(user_id: str, data: dict):
    """Синхронно зберегти user data (json) у sqlite (той самий формат)."""
    ensure_db()
    json_str = json.dumps(data, ensure_ascii=False)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO json_data(key, value) VALUES (?, ?)", (str(user_id), json_str))
        conn.commit()

def load_user_data_sync(user_id: str):
    """Синхронно завантажити user data (повертає dict або None)."""
    return get_user_data(str(user_id))  # у тебе вже є get_user_data - він читає з БД

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

def get_user_data(user_id: str):
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT value FROM json_data WHERE key = ?", (str(user_id),))
        row = cur.fetchone()
    if row:
        try:
            return json.loads(row["value"])
        except Exception:
            return None
    return None

def resolve_username_for_chat(chat_identifier) -> Optional[str]:
    """
    Повертає username (без @) або None.
    chat_identifier може бути числом (int or str) або строкою username.
    Використовує кеш, щоб не лімітувати Telegram API.
    """
    key = str(chat_identifier)
    if key in _username_cache:
        return _username_cache[key]

    # якщо це вже строка з username (не число) і починається не з '-' — приймаємо як username
    try:
        int(key)
        is_numeric = True
    except Exception:
        is_numeric = False

    # якщо не numeric і схоже на username — повертаємо без змін
    if not is_numeric:
        uname = key.lstrip("@")
        _username_cache[key] = uname
        return uname

    # numeric — пробуємо викликати getChat через Telegram Bot API (як await bot.get_chat)
    if not TELEGRAM_BOT_TOKEN:
        _username_cache[key] = None
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat"
    params = {"chat_id": key}
    try:
        r = requests.get(url, params=params, timeout=6)
        j = r.json()
        if not j.get("ok"):
            _username_cache[key] = None
            return None
        result = j.get("result", {})
        username = result.get("username")
        if username:
            username = username.lstrip("@")
            _username_cache[key] = username
            return username
    except Exception:
        pass

    _username_cache[key] = None
    return None


# ---------------- After request (headers for WebView) ----------------
@app.after_request
def add_security_headers(response):
    csp = "frame-ancestors 'self' https://t.me https://*.telesco.pe https://web.telegram.org https://telegram.me https://*.telegram.org;"
    response.headers['Content-Security-Policy'] = csp
    response.headers['X-Frame-Options'] = 'ALLOWALL'
    return response


# ---------------- Routes ----------------
@app.route("/")
def root():
    host = PUBLIC_URL or f"https://{RESERVED_DOMAIN}"
    return render_template("balance.html", public_url=host)

@app.route("/api/balance/<user_id>")
def api_balance(user_id):
    user = get_user_data(user_id) or {}
    mdrops = user.get("coins", 0)   # coins = mDrops
    ggs = user.get("GGs", 0)

    def sanitize_py(s):
        if s is None:
            return ""
        s = str(s)
        # прибрати replacement char і контрольні символи
        return "".join(ch for ch in s if ch != '\ufffd' and ord(ch) >= 0x20).strip()

    # в api_balance:
    m_form = format_balance(mdrops)
    m_form = sanitize_py(m_form)
    try:
        ggs_int = int(ggs)
    except Exception:
        ggs_int = 0

    return jsonify({
        "user_id": user_id,
        "mDrops": m_form,
        "GGs": ggs_int
    })

@app.route("/api/tasks")
def api_tasks():
    tasks_path = pathlib.Path("tasks.json")
    if not tasks_path.exists():
        return jsonify([])

    try:
        with tasks_path.open("r", encoding="utf-8") as f:
            tasks = json.load(f)
            if isinstance(tasks, dict):
                tasks = tasks.get("tasks", []) or []
            if not isinstance(tasks, list):
                tasks = []
    except Exception:
        tasks = []

    # доповнимо кожну задачу username та url (якщо відсутні)
    for t in tasks:
        # пропустимо, якщо username або url вже є
        if t.get("username"):
            t["url"] = t.get("url") or f"https://t.me/{t['username'].lstrip('@')}"
            continue
        if t.get("url"):
            continue
        ch = t.get("channel_id") or t.get("channel") or None
        if not ch:
            t["url"] = t.get("url") or None
            continue
        # спробуємо резолвнути username
        uname = resolve_username_for_chat(ch)
        if uname:
            t["username"] = uname
            t["url"] = f"https://t.me/{uname}"
        else:
            # fallback: збережемо просту t.me/<id> (хоча іноді бот клієнт може вести на telegram.org)
            t["url"] = t.get("url") or f"https://t.me/{ch}"
    return jsonify(tasks)

@app.route("/api/check_sub", methods=["POST"])
def api_check_sub():
    payload = request.get_json(silent=True) or {}
    user_id = str(payload.get("user_id") or request.args.get("user_id") or "")
    channel_id = payload.get("channel_id") or request.args.get("channel_id")
    if not user_id or not channel_id:
        return jsonify({"error": "missing user_id or channel_id"}), 400

    # Try to use Telegram API if token present
    if TELEGRAM_BOT_TOKEN:
        chat_param = channel_id
        try:
            int(chat_param)
        except Exception:
            # non-numeric -> ensure starts with @
            if isinstance(chat_param, str) and not chat_param.startswith("@"):
                chat_param = "@" + str(chat_param)

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChatMember"
        params = {"chat_id": chat_param, "user_id": user_id}
        try:
            r = requests.get(url, params=params, timeout=6)
            j = r.json()
            if not j.get("ok"):
                return jsonify({"subscribed": False, "error": j}), 200
            status = j.get("result", {}).get("status")
            subscribed = status in ("member", "administrator", "creator")
            return jsonify({"subscribed": subscribed, "status": status}), 200
        except Exception as e:
            return jsonify({"subscribed": False, "error": str(e)}), 500

    # Fallback: check DB subscriptions entry (simulation)
    user = get_user_data(user_id) or {}
    subs = user.get("subscriptions", {}) or {}
    sub = subs.get(str(channel_id))
    subscribed = bool(sub and sub.get("active"))
    return jsonify({"subscribed": subscribed})


@app.route("/api/task_complete", methods=["POST"])
def api_task_complete():
    payload = request.get_json(silent=True) or {}
    user_id = str(payload.get("user_id") or "")
    channel_id = payload.get("channel_id")
    created_at = payload.get("created_at") or ""

    if not channel_id:
        return jsonify({"error": f"missing channel_id"}), 400

    tasks_path = pathlib.Path("tasks.json")
    if not tasks_path.exists():
        return jsonify({"error": "no tasks file"}), 404

    # читаємо tasks
    try:
        with tasks_path.open("r", encoding="utf-8") as f:
            tasks = json.load(f)
            if isinstance(tasks, dict):
                tasks = tasks.get("tasks", []) or []
            if not isinstance(tasks, list):
                tasks = []
    except Exception:
        return jsonify({"error": "could not read tasks"}), 500

    # Запобігання повторного виконання: перевіримо user.completed_tasks
    if user_id:
        user = get_user_data(user_id) or {}
        completed = user.get("completed_tasks", {}) or {}
        task_key = f"{channel_id}:{created_at}"
        if completed.get(task_key):
            return jsonify({"ok": False, "msg": "already completed"}), 200

    # знаходимо задачу
    found = False
    action = None
    removed_task = None
    for idx, t in enumerate(tasks):
        if str(t.get("channel_id")) == str(channel_id) and (not created_at or t.get("created_at") == created_at):
            remaining = int(t.get("remaining", 0))
            removed_task = dict(t)
            if remaining > 1:
                tasks[idx]["remaining"] = remaining - 1
                action = "decremented"
            else:
                tasks.pop(idx)
                action = "removed"
            found = True
            break

    if not found:
        return jsonify({"ok": False, "msg": "task not found"}), 404

    # запишемо tasks назад
    try:
        with tasks_path.open("w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)
    except Exception:
        return jsonify({"error": "could not write tasks"}), 500

    # обчислимо reward (GGs)
    reward = 0
    if removed_task:
        # беремо можливі поля, або 0
        reward = int(removed_task.get("price_per_sub") or removed_task.get("reward") or removed_task.get("price") or 0)

    # Якщо винагорода не вказана — виставимо 1 GGs за замовчуванням
    if reward <= 0:
        reward = 1


    # якщо вказано user_id — оновимо user: додамо GGs, підписку і помітку completed_tasks
    if user_id:
        user = get_user_data(user_id) or {}
        user["GGs"] = int(user.get("GGs", 0)) + int(reward)
        subs = user.get("subscriptions", {}) or {}
        subs[str(channel_id)] = {"start": int(time.time()), "active": True}
        user["subscriptions"] = subs
        # completed tasks map
        completed = user.get("completed_tasks", {}) or {}
        task_key = f"{channel_id}:{created_at}"
        completed[task_key] = {"at": int(time.time()), "reward": reward}
        user["completed_tasks"] = completed

        # save
        save_user_data_sync(user_id, user)

    return jsonify({"ok": True, "action": action, "reward": reward})

_user_display_cache: Dict[str, Optional[str]] = {}

# налаштування: скільки чекати між запитами до Telegram (секунди)
SLEEP_BETWEEN = 0.05  # постав 0 якщо хочеш без затримки

def fetch_display_name_from_telegram(user_id: str) -> str:
    """
    Робить запит getChat до Telegram, повертає "First Last" або "@username" або "" якщо не знайшло.
    Кешує відповідь у _user_display_cache.
    """
    uid = str(user_id)
    if uid in _user_display_cache:
        return _user_display_cache[uid] or ""

    # якщо токен відсутній — нічого не робимо
    if not TELEGRAM_BOT_TOKEN:
        _user_display_cache[uid] = ""
        return ""

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat"
        params = {"chat_id": uid}
        r = requests.get(url, params=params, timeout=6)
        j = r.json()
        if j.get("ok"):
            res = j.get("result", {}) or {}
            fname = (res.get("first_name") or "").strip()
            lname = (res.get("last_name") or "").strip()
            uname = (res.get("username") or "").strip()
            if fname or lname:
                full = (fname + " " + lname).strip()
                _user_display_cache[uid] = full
                return full
            if uname:
                name = "@" + uname
                _user_display_cache[uid] = name
                return name
    except Exception:
        # тихо прогинаємось при помилці запиту
        pass

    # fallback: пустий рядок — фронтенд покаже скорочений id
    _user_display_cache[uid] = ""
    return ""

TOP_CACHE = {}          # ключ -> {"ts": timestamp, "data": [...]}
TOP_CACHE_LOCK = threading.Lock()
TOP_CACHE_TTL = 300     # секунди (за замовчуванням 5 хв). Змінити за потреби.
# --------------------------------------------------------------------

# Якщо у вас немає функції для прямого запиту імені — додаємо просту
def fetch_display_name_from_telegram_direct(user_id: str) -> Optional[str]:
    """Запит getChat до Telegram — повертає display name або None."""
    if not TELEGRAM_BOT_TOKEN or not user_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat"
        params = {"chat_id": user_id}
        r = requests.get(url, params=params, timeout=6)
        j = r.json()
        if not j.get("ok"):
            return None
        res = j.get("result", {}) or {}
        title = res.get("title")
        if title:
            return title
        first = res.get("first_name")
        last = res.get("last_name")
        if first or last:
            return ((first or "") + " " + (last or "")).strip()
        username = res.get("username")
        if username:
            return "@" + username.lstrip("@")
    except Exception:
        pass
    return None

# --- ОНОВЛЕНИЙ /api/top з in-memory кешем ---
@app.route("/api/top")
def api_top():
    """
    Топ по coins (mDrops) з in-memory кешем (TOP_CACHE).
    Параметр: ?limit=50 (default).
    Повертає [{ user_id, display_name, mDrops, GGs }, ...]
    """
    try:
        limit = int(request.args.get("limit", 50))
    except Exception:
        limit = 50
    limit = max(1, min(200, limit))

    cache_key = str(limit)

    # Перевіряємо кеш
    now = int(time.time())
    with TOP_CACHE_LOCK:
        entry = TOP_CACHE.get(cache_key)
        if entry and (now - entry.get("ts", 0)) <= TOP_CACHE_TTL:
            # повертаємо кешовані дані
            return jsonify(entry["data"])

    # Якщо тут — кеш відсутній або прострочений -> генеруємо новий список
    rows_out = []
    ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # 1) json_data
        try:
            cur.execute("SELECT key, value FROM json_data")
            for row in cur.fetchall():
                key = str(row["key"])
                try:
                    data = json.loads(row["value"]) if row["value"] else {}
                except Exception:
                    data = {}
                try:
                    coins_raw = float(data.get("coins", 0) or 0)
                except Exception:
                    coins_raw = 0.0
                try:
                    ggs_raw = int(data.get("GGs", 0) or 0)
                except Exception:
                    ggs_raw = 0
                rows_out.append({
                    "user_id": key,
                    "coins_raw": coins_raw,
                    "GGs": ggs_raw
                })
        except Exception:
            pass

        # 2) fallback: users table
        if not rows_out:
            try:
                cur.execute("SELECT id, coins, GGs FROM users")
                for row in cur.fetchall():
                    uid = str(row["id"])
                    try:
                        coins_raw = float(row["coins"] or 0)
                    except Exception:
                        coins_raw = 0.0
                    try:
                        ggs_raw = int(row["GGs"] or 0)
                    except Exception:
                        ggs_raw = 0
                    rows_out.append({
                        "user_id": uid,
                        "coins_raw": coins_raw,
                        "GGs": ggs_raw
                    })
            except Exception:
                pass

    # сортуємо та обираємо топ
    rows_out.sort(key=lambda x: x.get("coins_raw", 0), reverse=True)
    top_candidates = rows_out[:limit]

    # для кожного з топ отримуємо ім'я (через Telegram) — НЕ кешуємо імена тут в БД,
    # але можна легко додати локальний username_cache якщо потрібно
    result = []
    for u in top_candidates:
        uid = u.get("user_id") or ""
        display_name = fetch_display_name_from_telegram_direct(uid) or (f"#{uid[-6:]}" if uid else "")
        mDrops = format_balance(u.get("coins_raw", 0))
        result.append({
            "user_id": uid,
            "display_name": display_name,
            "mDrops": mDrops,
            "GGs": int(u.get("GGs", 0) or 0)
        })
        # невелика пауза між запитами щоб знизити ризик rate-limit
        try:
            if SLEEP_BETWEEN:
                time.sleep(SLEEP_BETWEEN)
        except Exception:
            pass

    # записуємо в кеш
    with TOP_CACHE_LOCK:
        TOP_CACHE[cache_key] = {"ts": int(time.time()), "data": result}

    return jsonify(result)

MIN_BET = 10
MAX_BET = 99999999999999999

@app.route("/api/flip", methods=["POST"])
def api_flip():
    """
    Тіло: JSON { user_id, amount, choice }
    choice: 'heads' або 'tails'
    Відповідь: { ok, win, outcome, payout, balance_raw, mDrops }
    """
    payload = request.get_json(silent=True) or {}
    user_id = str(payload.get("user_id") or "")
    try:
        amount = float(payload.get("amount"))
    except Exception:
        return jsonify({"ok": False, "error": "invalid amount"}), 400
    choice = str(payload.get("choice") or "").lower()
    if choice not in ("heads", "tails"):
        return jsonify({"ok": False, "error": "invalid choice"}), 400

    if not user_id:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    if amount <= 0 or amount < MIN_BET or amount > MAX_BET:
        return jsonify({"ok": False, "error": f"bet must be between {MIN_BET} and {MAX_BET}"}), 400

    # Атомарна операція з sqlite
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)  # isolation_level=None дозволяє ручне BEGIN
        cur = conn.cursor()
        cur.execute("BEGIN IMMEDIATE")  # Блокуємо для запису, щоб уникнути гонок
        # отримуємо поточний user data
        cur.execute("SELECT value FROM json_data WHERE key = ?", (user_id,))
        row = cur.fetchone()
        if row:
            try:
                user = json.loads(row[0])
            except Exception:
                user = {}
        else:
            user = {}

        balance = float(user.get("coins", 0) or 0)
        if balance < amount:
            conn.rollback()
            conn.close()
            return jsonify({"ok": False, "error": "insufficient balance", "balance": balance}), 400

        # результат: server-side RNG
        outcome = "heads" if secrets.choice([0,1]) == 0 else "tails"
        win = 1 if outcome == choice else 0

        if win:
            # Виграш: х2 ставки (повертається ставка + прибуток = ставка)
            payout = amount * 2.0  # означає, що баланс буде +amount (тому з балансу ми не віднімаємо ставку перед підрахунком)
            # як ми поводимось з балансом: віднімаємо ставку, потім додаємо payout
            new_balance = balance - amount + payout
        else:
            payout = 0.0
            new_balance = balance - amount

        # Оновлюємо user data
        user["coins"] = new_balance
        # зберігаємо назад
        cur.execute(
            "INSERT OR REPLACE INTO json_data(key, value) VALUES (?, ?)",
            (str(user_id), json.dumps(user, ensure_ascii=False))
        )

        # записуємо в bets лог
        try:
            cur.execute(
                "INSERT INTO bets(user_id, bet_amount, choice, outcome, win, payout) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, float(amount), choice, outcome, int(win), float(payout))
            )
        except Exception:
            # якщо таблиці немає, то ігноруємо (але краще створити таблицю)
            pass

        conn.commit()
        conn.close()

        return jsonify({
            "ok": True,
            "win": bool(win),
            "outcome": outcome,
            "payout": payout,
            "balance_raw": new_balance,
            "mDrops": format_balance(new_balance)
        })
    except sqlite3.OperationalError as e:
        # блокування або інша помилка sqlite
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "db error", "msg": str(e)}), 500
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "internal error", "msg": str(e)}), 500








# ---------------- Runner ----------------
def run_flask(open_user_id: Optional[str] = None, open_browser: bool = False, timeout_wait: float = 0.5):
    global PUBLIC_URL
    ensure_db()

    try:
        conf.get_default().log_event_handler = None
    except Exception:
        pass

    try:
        ngrok.set_auth_token(NGROK_AUTHTOKEN)
    except Exception:
        pass

    public_url = None
    try:
        print(f"📡 Спроба підняти тунель на hostname: {RESERVED_DOMAIN} ...")
        tunnel = ngrok.connect(addr=PORT, proto="http", hostname=RESERVED_DOMAIN)
        public_url = tunnel.public_url
        print("✅ Підключено на reserved domain:", public_url)
    except Exception as e:
        print("⚠ Не вдалося підняти reserved hostname:", e)
        try:
            tunnel = ngrok.connect(addr=PORT, proto="http")
            public_url = tunnel.public_url
            print("✅ Підключено (рандомний URL):", public_url)
        except Exception as e2:
            print("❌ Помилка при створенні ngrok тунеля:", e2)
            public_url = None

    PUBLIC_URL = public_url or f"https://{RESERVED_DOMAIN}"
    print("🌍 Public URL:", PUBLIC_URL)

    target = f"{PUBLIC_URL}/" if not open_user_id else f"{PUBLIC_URL}/"
    if open_browser and PUBLIC_URL:
        time.sleep(timeout_wait)
        try:
            webbrowser.open(target)
            print("🔗 Відкриваю в браузері:", target)
        except Exception as e:
            print("⚠ Не вдалося відкрити браузер автоматично:", e)

    app.run(port=PORT, host="127.0.0.1")


if __name__ == "__main__":
    run_flask(open_user_id=None, open_browser=True)
