import pymysql
from pymysql import Error
from contextlib import contextmanager
import os
import subprocess

DB_CONFIG = {
    "host": "localhost",
    "database": "nirbase",
    "user": "artem",
    "password": "Dont_rush_plz"
}

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"]
        )
        yield conn
        conn.commit()
    except Error as e:
        if conn:
            conn.rollback()
        print(f"Ошибка БД PyMySQL: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_tables():
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
    drop_table_queries = [
        "SET FOREIGN_KEY_CHECKS = 0;",
        "DROP TABLE IF EXISTS order_items;",
        "DROP TABLE IF EXISTS orders;",
        "DROP TABLE IF EXISTS cards;",
        "DROP TABLE IF EXISTS categories;",
        "DROP TABLE IF EXISTS sections;",
        "DROP TABLE IF EXISTS client_details;",
        "DROP TABLE IF EXISTS clients;",
        "SET FOREIGN_KEY_CHECKS = 1;"
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


def delete_all_data_from_table(table_name: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name};")
                print(f"Все данные из таблицы '{table_name}' удалены.")
    except Error as e:
        print(f"Ошибка при удалении данных из таблицы '{table_name}' в MySQL: {e}")

def delete_data_by_condition(table_name: str, column_name: str, value):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name} WHERE {column_name} = %s;", (value,))
                print(f"Данные из таблицы '{table_name}' удалены по условию {column_name} = {value}.")
    except Error as e:
        print(f"Ошибка при удалении данных из таблицы '{table_name}' по условию в MySQL: {e}")



def perform_inserts(table_name, columns, data):
    if not data:
        return

    placeholders = ', '.join(['%s'] * len(data[0]))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, data)


def perform_selects(table_name, pk_column, ids):
    query = f"SELECT * FROM {table_name} WHERE {pk_column} = %s"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for entity_id in ids:
                cur.execute(query, (entity_id,))
                cur.fetchone()


def perform_deletes(table_name, pk_column, ids):
    if not ids:
        return

    query = f"DELETE FROM {table_name} WHERE {pk_column} IN (%s)"

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (ids,))

def perform_inserts_ignore(table_name, columns, data_rows):
    if not data_rows:
        return
    placeholders = ', '.join(['%s'] * len(data_rows[0]))
    query = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, data_rows)