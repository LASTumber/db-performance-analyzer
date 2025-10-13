import pymysql
from pymysql import Error
from lib.db_manager import DB_CONFIG, create_tables  # Мы можем переиспользовать нашу функцию создания таблиц!


def create_sandbox_db(sandbox_db_name="NIRbase_sandbox"):
    """
    Создает новую базу данных ("песочницу") для исследований.
    """
    # Подключаемся к MySQL серверу без указания конкретной БД для выполнения CREATE DATABASE
    conn_params = {
        "host": DB_CONFIG["host"],
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"]
    }

    try:
        conn = pymysql.connect(**conn_params)
        conn.autocommit(True)  # Включаем автокоммит для команд DDL
        with conn.cursor() as cur:
            # Сначала удаляем старую песочницу, если она есть
            cur.execute(f"DROP DATABASE IF EXISTS {sandbox_db_name};")
            print(f"Старая песочница '{sandbox_db_name}' (если была) удалена.")

            # Создаем новую
            cur.execute(f"CREATE DATABASE {sandbox_db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            print(f"Песочница '{sandbox_db_name}' успешно создана.")

            # Выдаем права нашему пользователю на новую песочницу
            # Важно: это сработает, только если пользователь, от которого запущен скрипт, имеет GRANT OPTION.
            # Обычно это 'root'. Если нет, то права нужно будет выдать вручную один раз.
            # Мы предполагаем, что права уже есть.

    except Error as e:
        print(f"Ошибка при создании песочницы: {e}")
    finally:
        if conn:
            conn.close()


def setup_sandbox_tables(sandbox_db_name="NIRbase_sandbox"):
    """
    Создает структуру таблиц в песочнице.
    """
    # Временно переключаем DB_CONFIG на песочницу
    original_db = DB_CONFIG['database']
    DB_CONFIG['database'] = sandbox_db_name

    try:
        print(f"Создание таблиц в песочнице '{sandbox_db_name}'...")
        create_tables()  # Используем нашу уже готовую функцию!
    finally:
        # Возвращаем DB_CONFIG к основной базе данных
        DB_CONFIG['database'] = original_db


def drop_sandbox_db(sandbox_db_name="NIRbase_sandbox"):
    """
    Удаляет базу данных песочницы.
    """
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