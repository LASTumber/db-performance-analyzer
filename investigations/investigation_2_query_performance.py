import sys
import os
import random

# --- Настройка путей ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

from lib import db_manager, data_generator, sandbox_manager
from investigations.perf_analyzer import PerformanceAnalyzer
from investigations.plotter import Plotter


# --- Функции-обертки для выполнения SQL-операций для этого исследования ---

def perform_inserts_for_clients(table_name, data):
    """Выполняет вставку данных в таблицу clients."""
    if not data:
        return
    columns = "email, password_hash, name, created_at, updated_at"
    placeholders = ', '.join(['%s'] * len(data[0]))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, data)


def perform_selects_by_id(table_name, primary_key_column, ids_to_select):
    """Выполняет SELECT по списку ID."""
    query = f"SELECT * FROM {table_name} WHERE {primary_key_column} = %s"
    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            for entity_id in ids_to_select:
                cursor.execute(query, (entity_id,))
                cursor.fetchone()


def perform_deletes_by_id(table_name, primary_key_column, ids_to_delete):
    """Выполняет DELETE по списку ID."""
    query = f"DELETE FROM {table_name} WHERE {primary_key_column} = %s"
    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            for entity_id in ids_to_delete:
                cursor.execute(query, (entity_id,))


def perform_join_select():
    """Выполняет сложный SELECT с JOIN'ами."""
    query = """
        SELECT
            clients.name AS client_name,
            orders.order_id,
            orders.status,
            cards.title AS product_title,
            order_items.quantity,
            order_items.price_at_purchase
        FROM clients
        JOIN orders ON clients.client_id = orders.client_id
        JOIN order_items ON orders.order_id = order_items.order_id
        JOIN cards ON order_items.card_id = cards.card_id
        WHERE orders.status = 'delivered'
        LIMIT 1000;
    """
    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            cursor.fetchall()


# --- Основной скрипт исследования ---
def run_query_investigation():
    print("--- НАЧАЛО ИССЛЕДОВАНИЯ 2: ПРОИЗВОДИТЕЛЬНОСТЬ SQL-ЗАПРОСОВ ---")

    sandbox_name = "NIRbase_sandbox"
    # --- ИЗМЕНЕНИЕ: Инициализируем Plotter с правильным базовым путем ---
    plotter = Plotter(base_output_dir=PLOTS_DIR)
    analyzer = PerformanceAnalyzer(number=1, repeat=3)
    generator = data_generator.DataGenerator()

    print(f"\n[Подготовка] Создание и настройка песочницы '{sandbox_name}'...")
    sandbox_manager.create_sandbox_db(sandbox_name)
    original_database_name = db_manager.DB_CONFIG['database']
    db_manager.DB_CONFIG['database'] = sandbox_name
    sandbox_manager.setup_sandbox_tables(sandbox_name)
    print("Песочница готова.")

    # --- Исследование INSERT ---
    print("\n[Часть 1] Исследование производительности INSERT...")
    insert_counts = [100, 500, 1000, 2500, 5000]

    def measure_single_insert_run(table_to_insert, number_of_rows):
        with db_manager.get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute(f"TRUNCATE TABLE {table_to_insert}")
                if table_to_insert == 'clients':
                    cursor.execute("TRUNCATE TABLE client_details")
                    cursor.execute("TRUNCATE TABLE orders")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        generator.clear_uniques()
        client_data_to_insert = generator.generate_client_data(number_of_rows)
        perform_inserts_for_clients(table_to_insert, client_data_to_insert)

    results_insert_clients = []
    for count in insert_counts:
        print(f"  - Замер INSERT для {count} clients...")
        time_taken = analyzer.measure_time(measure_single_insert_run, 'clients', count)
        results_insert_clients.append(time_taken)

    plotter.build_plot(
        x_data=insert_counts,
        y_data_dict={"INSERT clients": results_insert_clients},
        title="Производительность INSERT для таблицы 'clients'",
        x_label="Количество вставляемых строк",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_insert_clients",
        # --- ИЗМЕНЕНИЕ: Указываем подпапку для этого исследования ---
        sub_dir="5c_query_performance"
    )

    # --- Подготовка к SELECT и DELETE ---
    print("\n[Подготовка] Заполнение песочницы большим объемом данных...")
    db_manager.drop_tables()
    db_manager.create_tables()
    TOTAL_CLIENTS = 2000
    generator.populate_database(num_clients=TOTAL_CLIENTS, num_sections=5, num_categories_per_section=10,
                                num_cards_per_category=20, num_orders_per_client=5)
    print(f"Песочница заполнена. Всего клиентов: {TOTAL_CLIENTS}")

    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT client_id FROM clients")
            all_client_ids = [row[0] for row in cursor.fetchall()]

    # --- Исследование SELECT ---
    print("\n[Часть 2] Исследование производительности SELECT по PK...")
    select_counts = [10, 50, 100, 200, 500]
    results_select_clients = []
    for count in select_counts:
        print(f"  - Замер SELECT для {count} clients...")
        ids_to_select = random.sample(all_client_ids, count)
        time_taken = analyzer.measure_time(perform_selects_by_id, 'clients', 'client_id', ids_to_select)
        results_select_clients.append(time_taken)

    plotter.build_plot(
        x_data=select_counts,
        y_data_dict={"SELECT clients": results_select_clients},
        title="Производительность SELECT по PK для таблицы 'clients'",
        x_label="Количество запрашиваемых строк (по одной)",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_select_clients",
        # --- ИЗМЕНЕНИЕ: Указываем подпапку для этого исследования ---
        sub_dir="5c_query_performance"
    )

    # --- Исследование DELETE ---
    print("\n[Часть 3] Исследование производительности DELETE по PK...")
    delete_counts = [10, 50, 100, 200, 500]
    results_delete_clients = []
    available_ids_for_delete = list(all_client_ids)
    for count in delete_counts:
        print(f"  - Замер DELETE для {count} clients...")
        if len(available_ids_for_delete) < count:
            print(f"    Пропуск: недостаточно ID для удаления ({len(available_ids_for_delete)} осталось).")
            continue
        ids_to_delete = random.sample(available_ids_for_delete, count)
        available_ids_for_delete = [client_id for client_id in available_ids_for_delete if
                                    client_id not in ids_to_delete]
        time_taken = analyzer.measure_time(perform_deletes_by_id, 'clients', 'client_id', ids_to_delete)
        results_delete_clients.append(time_taken)

    plotter.build_plot(
        x_data=delete_counts[:len(results_delete_clients)],
        y_data_dict={"DELETE clients": results_delete_clients},
        title="Производительность DELETE по PK для таблицы 'clients'",
        x_label="Количество удаляемых строк (по одной)",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_delete_clients",
        # --- ИЗМЕНЕНИЕ: Указываем подпапку для этого исследования ---
        sub_dir="5c_query_performance"
    )

    # --- Исследование JOIN ---
    print("\n[Часть 4] Исследование производительности SELECT с JOIN...")
    join_times = []
    for i in range(5):
        print(f"  - Замер JOIN #{i + 1}...")
        time_taken = analyzer.measure_time(perform_join_select)
        if time_taken != float('inf'):
            join_times.append(time_taken)
    if join_times:
        average_join_time = sum(join_times) / len(join_times)
        print(f"  -> Среднее время выполнения сложного JOIN-запроса: {average_join_time:.6f} секунд")
    else:
        print("  -> Не удалось измерить время выполнения JOIN-запроса.")

    # --- Очистка ---
    print("\n[Очистка] Удаление песочницы...")
    db_manager.DB_CONFIG['database'] = original_database_name
    sandbox_manager.drop_sandbox_db(sandbox_name)

    print("\n--- ИССЛЕДОВАНИЕ 2 ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '5c_query_performance')}'.")


if __name__ == "__main__":
    run_query_investigation()