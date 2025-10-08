import sqlite3
import shutil
import os
import time
from pathlib import Path

DB_PATH = "data.db"

def backup_db(db_path: str) -> str:
    """Створити резервну копію БД та повернути шлях до бекапу"""
    ts = time.strftime("%Y%m%d-%H%M%S")
    src = Path(db_path)
    if not src.exists():
        raise FileNotFoundError(f"DB file not found: {db_path}")
    dst = src.with_suffix(f"{src.suffix}.bak.{ts}")
    shutil.copy2(src, dst)
    return str(dst)

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cur.fetchone() is not None

def clear_all_checks(db_path: str, dry_run: bool = False) -> dict:
    """
    Якщо dry_run=True — лише підрахує й виведе, що будуть зроблено.
    Повертає словник зі статистикою.
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB file not found: {db_path}")

    # Підключення
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    stats = {
        "checks_table_exists": False,
        "checks_rows_before": 0,
        "users_with_nonempty_checks_before": 0,
        "deleted_checks_rows": 0,
        "updated_users_count": 0,
    }

    try:
        # Чи є таблиця checks?
        stats["checks_table_exists"] = table_exists(conn, "checks")

        if stats["checks_table_exists"]:
            cur.execute("SELECT COUNT(*) AS cnt FROM checks")
            stats["checks_rows_before"] = cur.fetchone()["cnt"]
        else:
            stats["checks_rows_before"] = 0

        # Користувачі з checks != '[]' або NULL
        cur.execute("SELECT COUNT(*) AS cnt FROM users WHERE checks IS NULL OR checks != '[]'")
        stats["users_with_nonempty_checks_before"] = cur.fetchone()["cnt"]

        if dry_run:
            # Не вносити змін — повернути лише підрахунки
            return stats

        # Резервна копія бази
        backup_path = backup_db(db_path)
        print(f"Backup created: {backup_path}")

        # Почати транзакцію
        cur.execute("BEGIN")

        # Якщо є таблиця checks — видалити всі рядки
        if stats["checks_table_exists"]:
            cur.execute("DELETE FROM checks")
            # sqlite3's rowcount may be -1 for some drivers; використаємо pre-count
            stats["deleted_checks_rows"] = stats["checks_rows_before"]
        else:
            stats["deleted_checks_rows"] = 0

        # Оновити users.checks -> '[]' у всіх користувачів де потрібно
        cur.execute("UPDATE users SET checks = '[]' WHERE checks IS NULL OR checks != '[]'")
        stats["updated_users_count"] = cur.rowcount if cur.rowcount >= 0 else stats["users_with_nonempty_checks_before"]

        # commit
        conn.commit()
        print("✅ Changes committed.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats

if __name__ == "__main__":
    print("Starting clear_all_checks...")
    try:
        # Поставте dry_run=True щоб подивитися, що буде зроблено без змін
        result = clear_all_checks(DB_PATH, dry_run=False)
        print("=== Result ===")
        print(f"Checks table exists: {result['checks_table_exists']}")
        print(f"Checks rows before: {result['checks_rows_before']}")
        print(f"Deleted checks rows: {result['deleted_checks_rows']}")
        print(f"Users with non-empty checks before: {result['users_with_nonempty_checks_before']}")
        print(f"Users updated (checks -> '[]'): {result['updated_users_count']}")
    except Exception as err:
        print("❌ Error:", err)