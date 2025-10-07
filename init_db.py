import sqlite3

DB_PATH = "data.db"

def init_db():
    """Ініціалізація бази даних з мінімальними таблицями для твоєї логіки"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Таблиця користувачів для збереження балансу та статистики
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        coins REAL DEFAULT 0,
        GGs REAL DEFAULT 0,
        lost_coins REAL DEFAULT 0,
        won_coins REAL DEFAULT 0,
        status INTEGER DEFAULT 0,
        checks TEXT DEFAULT '[]'
    )
    """)

    # Таблиця для глобальних чеків (якщо треба)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS checks (
        code TEXT PRIMARY KEY,
        creator_id TEXT NOT NULL,
        per_user REAL NOT NULL,
        remaining INTEGER NOT NULL,
        claimed TEXT DEFAULT '[]',
        password TEXT DEFAULT NULL
    )
    """)

    # Таблиця для промокодів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS promos (
        name TEXT PRIMARY KEY,
        reward REAL NOT NULL,
        claimed TEXT DEFAULT '[]'
    )
    """)

    # Таблиця для біржових ордерів
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

    # Таблиця для кланів
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        coffres REAL DEFAULT 0,
        level INTEGER DEFAULT 1,
        rating INTEGER DEFAULT 0,
        members TEXT DEFAULT '[]',
        admins TEXT DEFAULT '[]',
        owner TEXT UNIQUE NOT NULL
    )
    """)

    # Таблиця для заявок у клан
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clan_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        clan_name TEXT NOT NULL,
        status TEXT DEFAULT 'pending' -- pending / accepted / rejected
    )
    """)


    conn.commit()
    conn.close()
    print("✅ DB initialized (users, checks, promos, exchange_orders, clans)")

if __name__ == "__main__":
    init_db()
