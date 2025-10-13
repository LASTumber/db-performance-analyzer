import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager
import os
import subprocess

# Конфигурация БД для MySQL
DB_CONFIG = {
    "host": "localhost",
    "database": "NIRbase",
    "user": "artem",
    "password": "Dont_rush_plz"
}

@contextmanager
def get_db_connection():
    """
    Контекстный менеджер для получения и управления соединением с БД MySQL.
    Автоматически коммитит изменения и закрывает соединение при выходе,
    откатывает транзакцию при исключении.
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            yield conn
            conn.commit()
        else:
            raise Error("Не удалось подключиться к базе данных MySQL")
    except Error as e:
        if conn and conn.is_connected():
            conn.rollback()
        print(f"Ошибка БД MySQL: {e}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()

def create_tables():
    """
    Создает таблицы для интернет-магазина в базе данных MySQL,
    если они еще не существуют.
    """
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS clients (
            client_id INT AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS client_details (
            client_id INT PRIMARY KEY,
            phone_number VARCHAR(50),
            address VARCHAR(512),
            birth_date DATE,
            FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sections (
            section_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS categories (
            category_id INT AUTO_INCREMENT PRIMARY KEY,
            section_id INT NOT NULL,
            name VARCHAR(255) UNIQUE NOT NULL,
            label VARCHAR(255),
            FOREIGN KEY (section_id) REFERENCES sections(section_id) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cards (
            card_id INT AUTO_INCREMENT PRIMARY KEY,
            category_id INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            image_url VARCHAR(512),
            price DECIMAL(10, 2) NOT NULL,
            stock_quantity INT NOT NULL DEFAULT 0,
            purchases_count INT DEFAULT 0,
            FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT AUTO_INCREMENT PRIMARY KEY,
            client_id INT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            total_amount DECIMAL(10, 2) DEFAULT 0.00,
            FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NOT NULL,
            card_id INT NOT NULL,
            quantity INT NOT NULL,
            price_at_purchase DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
            FOREIGN KEY (card_id) REFERENCES cards(card_id) ON DELETE CASCADE,
            UNIQUE (order_id, card_id)
        );
        """
    ]

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for query in create_table_queries:
                    cur.execute(query)
                print("Таблицы успешно созданы или уже существуют в MySQL.")
    except Error as e:
        print(f"Ошибка при создании таблиц в MySQL: {e}")

def drop_tables():
    """
    Удаляет все таблицы, используемые в проекте из MySQL.
    Использовать с осторожностью!
    """
    drop_table_queries = [
        "SET FOREIGN_KEY_CHECKS = 0;", # Отключаем проверку внешних ключей для безопасного удаления
        "DROP TABLE IF EXISTS order_items;",
        "DROP TABLE IF EXISTS orders;",
        "DROP TABLE IF EXISTS cards;",
        "DROP TABLE IF EXISTS categories;",
        "DROP TABLE IF EXISTS sections;",
        "DROP TABLE IF EXISTS client_details;",
        "DROP TABLE IF EXISTS clients;",
        "SET FOREIGN_KEY_CHECKS = 1;" # Включаем проверку обратно
    ]
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for query in drop_table_queries:
                    cur.execute(query)
                print("Таблицы успешно удалены (если существовали) из MySQL.")
    except Error as e:
        print(f"Ошибка при удалении таблиц из MySQL: {e}")

def backup_database(backup_file="backup.sql"):
    """
    Создает бэкап схемы и данных базы данных MySQL в SQL файл.
    Требует утилиты mysqldump, которая должна быть доступна в PATH.
    """
    db_name = DB_CONFIG["database"]
    user = DB_CONFIG["user"]
    password = DB_CONFIG["password"]
    host = DB_CONFIG["host"]

    try:
        command = [
            "mysqldump",
            f"--host={host}",
            f"--user={user}",
            f"--password={password}",
            db_name
        ]
        with open(backup_file, "w") as f:
            subprocess.run(command, stdout=f, check=True)
        print(f"Бэкап базы данных MySQL сохранен в {backup_file}")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при создании бэкапа MySQL (mysqldump завершился с ошибкой): {e}")
    except FileNotFoundError:
        print("Ошибка: mysqldump не найден. Убедитесь, что MySQL клиент установлен и mysqldump доступен в PATH.")
    except Exception as e:
        print(f"Неизвестная ошибка при создании бэкапа MySQL: {e}")

def restore_database(backup_file="backup.sql"):
    """
    Восстанавливает базу данных MySQL из SQL файла.
    Требует утилиты mysql клиента, которая должна быть доступна в PATH.
    Использовать с осторожностью! Удалит существующие данные.
    """
    if not os.path.exists(backup_file):
        print(f"Файл бэкапа {backup_file} не найден.")
        return

    db_name = DB_CONFIG["database"]
    user = DB_CONFIG["user"]
    password = DB_CONFIG["password"]
    host = DB_CONFIG["host"]

    try:
        command = [
            "mysql",
            f"--host={host}",
            f"--user={user}",
            f"--password={password}",
            db_name
        ]
        with open(backup_file, "r") as f:
            subprocess.run(command, stdin=f, check=True)
        print(f"База данных MySQL успешно восстановлена из {backup_file}")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при восстановлении базы данных MySQL (mysql клиент завершился с ошибкой): {e}")
    except FileNotFoundError:
        print("Ошибка: mysql клиент не найден. Убедитесь, что MySQL клиент установлен и mysql доступен в PATH.")
    except Exception as e:
        print(f"Неизвестная ошибка при восстановлении базы данных MySQL: {e}")


# Вспомогательная функция для удаления всех данных из таблицы
def delete_all_data_from_table(table_name: str):
    """Удаляет все данные из указанной таблицы MySQL."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # В MySQL TRUNCATE TABLE также сбрасывает AUTO_INCREMENT
                cur.execute(f"TRUNCATE TABLE {table_name};")
                print(f"Все данные из таблицы '{table_name}' удалены.")
    except Error as e:
        print(f"Ошибка при удалении данных из таблицы '{table_name}' в MySQL: {e}")

def delete_data_by_condition(table_name: str, column_name: str, value):
    """Удаляет данные из указанной таблицы MySQL по условию."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Используем параметризованный запрос для безопасности и правильного экранирования
                cur.execute(f"DELETE FROM {table_name} WHERE {column_name} = %s;", (value,))
                print(f"Данные из таблицы '{table_name}' удалены по условию {column_name} = {value}.")
    except Error as e:
        print(f"Ошибка при удалении данных из таблицы '{table_name}' по условию в MySQL: {e}")