import pymysql
from pymysql import Error
from lib.db_manager import DB_CONFIG, create_tables


def create_sandbox_db(sandbox_db_name="NIRbase_sandbox"):
    conn_params = {
        "host": DB_CONFIG["host"],
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"]
    }

    try:
        conn = pymysql.connect(**conn_params)
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {sandbox_db_name};")
            print(f"Старая песочница '{sandbox_db_name}' (если была) удалена.")

            # Создаем новую
            cur.execute(f"CREATE DATABASE {sandbox_db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            print(f"Песочница '{sandbox_db_name}' успешно создана.")

    except Error as e:
        print(f"Ошибка при создании песочницы: {e}")
    finally:
        if conn:
            conn.close()


def setup_sandbox_tables(sandbox_db_name="NIRbase_sandbox"):
    original_db = DB_CONFIG['database']
    DB_CONFIG['database'] = sandbox_db_name

    try:
        print(f"Создание таблиц в песочнице '{sandbox_db_name}'...")
        create_tables()
    finally:
        DB_CONFIG['database'] = original_db


def drop_sandbox_db(sandbox_db_name="NIRbase_sandbox"):
    conn_params = {
        "host": DB_CONFIG["host"],
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"]
    }

    try:
        conn = pymysql.connect(**conn_params)
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {sandbox_db_name};")
            print(f"Песочница '{sandbox_db_name}' успешно удалена.")
    except Error as e:
        print(f"Ошибка при удалении песочницы: {e}")
    finally:
        if conn:
            conn.close()