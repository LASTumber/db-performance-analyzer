import sys
import os
import random
import time

# --- 1. НАСТРОЙКА ПУТЕЙ И ИМПОРТЫ ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

from lib import db_manager, data_generator, sandbox_manager
from investigations.perf_analyzer import PerformanceAnalyzer
from investigations.plotter import Plotter


# --- 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ УНИКАЛЬНЫХ ЗАПРОСОВ ---
def select_by_phone_pattern(pattern):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("SELECT * FROM client_details WHERE phone_number LIKE %s LIMIT 100",
                                            (pattern,)); cur.fetchall()


def select_by_label(label):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("SELECT * FROM categories WHERE label = %s", (label,)); cur.fetchall()


def select_by_price_range(min_price):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("SELECT * FROM cards WHERE price > %s LIMIT 100",
                                            (min_price,)); cur.fetchall()


def select_by_status(status):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("SELECT * FROM orders WHERE status = %s LIMIT 200",
                                            (status,)); cur.fetchall()


def delete_by_status(status):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("DELETE FROM orders WHERE status = %s", (status,))


def delete_by_price_range(max_price):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("DELETE FROM cards WHERE price < %s", (max_price,))


def delete_by_name(name):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("DELETE FROM categories WHERE name = %s", (name,))


def delete_by_id_range(max_id):
    with db_manager.get_db_connection() as c:
        with c.cursor() as cur: cur.execute("DELETE FROM sections WHERE section_id < %s", (max_id,))


def truncate_all_tables():
    with db_manager.get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
            tables = ['order_items', 'orders', 'cards', 'categories', 'sections', 'client_details', 'clients']
            for table in tables:
                cur.execute(f"TRUNCATE TABLE {table};")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1;")


# --- 3. ОСНОВНОЙ СКРИПТ ИССЛЕДОВАНИЯ ---
def run_final_investigation():
    print("--- НАЧАЛО ФИНАЛЬНОГО ИССЛЕДОВАНИЯ SQL-ЗАПРОСОВ ---")

    sandbox_name = "NIRbase_final_sandbox"
    plotter = Plotter(base_output_dir=PLOTS_DIR)
    analyzer = PerformanceAnalyzer(number=1, repeat=3)
    generator = data_generator.DataGenerator()

    print(f"\n[Подготовка] Создание песочницы '{sandbox_name}'...")
    sandbox_manager.create_sandbox_db(sandbox_name)
    original_db = db_manager.DB_CONFIG['database']
    db_manager.DB_CONFIG['database'] = sandbox_name
    db_manager.create_tables()

    # === ГРАФИК 1: ИССЛЕДОВАНИЕ INSERT ===
    print("\n[ИССЛЕДОВАНИЕ 1/3] Сравнение производительности INSERT...")
    insert_counts = [100, 500, 1000, 2500]
    results_insert = {"clients": [], "client_details": [], "sections": [], "categories": [], "cards": [], "orders": []}

    def measure_manually(insert_func):
        times = []
        for _ in range(3):
            truncate_all_tables()
            generator.clear_uniques()
            start = time.perf_counter()
            insert_func()
            end = time.perf_counter()
            times.append(end - start)
        return min(times)

    for count in insert_counts:
        print(f"  - Замер INSERT для {count} строк...")

        def do_insert_sections(): db_manager.perform_inserts('sections', 'name', generator.generate_section_data(count))

        results_insert["sections"].append(measure_manually(do_insert_sections))

        def do_insert_clients(): db_manager.perform_inserts('clients',
                                                            'email, password_hash, name, created_at, updated_at',
                                                            generator.generate_client_data(count))

        results_insert["clients"].append(measure_manually(do_insert_clients))

        def do_insert_client_details():
            parent_ids = generator.insert_data('clients', generator.generate_client_data(count))
            data = generator.generate_client_details_data(parent_ids)
            db_manager.perform_inserts('client_details', 'client_id, phone_number, address, birth_date', data)

        results_insert["client_details"].append(measure_manually(do_insert_client_details))

        def do_insert_orders():
            parent_ids = generator.insert_data('clients', generator.generate_client_data(10))
            data = generator.generate_order_data(count, parent_ids)
            db_manager.perform_inserts('orders', 'client_id, created_at, status, total_amount', data)

        results_insert["orders"].append(measure_manually(do_insert_orders))

        def do_insert_categories():
            parent_ids = generator.insert_data('sections', generator.generate_section_data(10))
            data = generator.generate_category_data(count, parent_ids)
            db_manager.perform_inserts('categories', 'section_id, name, label', data)

        results_insert["categories"].append(measure_manually(do_insert_categories))

        def do_insert_cards():
            s_ids = generator.insert_data('sections', generator.generate_section_data(10))
            c_ids = generator.insert_data('categories', generator.generate_category_data(10, s_ids))
            data = generator.generate_card_data(count, c_ids)
            db_manager.perform_inserts('cards',
                                       'category_id, title, description, image_url, price, stock_quantity, purchases_count',
                                       data)

        results_insert["cards"].append(measure_manually(do_insert_cards))

    plotter.build_plot(x_data=insert_counts, y_data_dict=results_insert, title="Сравнение производительности INSERT",
                       x_label="Количество строк", y_label="Время (секунды)", filename="perf_insert_all_tables",
                       sub_dir="5c_final_comparison")

    # === ПОДГОТОВКА К SELECT / DELETE / JOIN ===
    print("\n[Подготовка] Заполнение всех таблиц большим объемом данных...")
    truncate_all_tables()
    generator.populate_database(num_clients=2000, num_sections=50, num_categories_per_section=10,
                                num_cards_per_category=40, num_orders_per_client=5)
    print("Песочница заполнена.")

    # === ГРАФИК 2: ИССЛЕДОВАНИЕ SELECT ===
    print("\n[ИССЛЕДОВАНИЕ 2/3] Сравнение производительности SELECT...")
    select_counts = [10, 50, 100, 200, 500]
    results_select = {
        "clients (по PK)": [], "client_details (по LIKE phone)": [], "sections (по точному имени)": [],
        "categories (по label)": [], "cards (по диапазону price)": [], "orders (по статусу)": []
    }

    with db_manager.get_db_connection() as c:
        with c.cursor() as cur:
            cur.execute("SELECT client_id FROM clients");
            all_client_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT name FROM sections");
            all_section_names = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT label FROM categories");
            all_category_labels = [r[0] for r in cur.fetchall()]

    for count in select_counts:
        print(f"  - Замер SELECT для {count} запросов...")

        results_select["clients (по PK)"].append(
            analyzer.get_mean_time(db_manager.perform_selects, 'clients', 'client_id',
                                   random.sample(all_client_ids, count)))
        results_select["client_details (по LIKE phone)"].append(analyzer.get_mean_time(select_by_phone_pattern, "+7%"))
        results_select["sections (по точному имени)"].append(
            analyzer.get_mean_time(db_manager.perform_selects, 'sections', 'name',
                                   random.sample(all_section_names, min(count, len(all_section_names)))))
        results_select["categories (по label)"].append(
            analyzer.get_mean_time(select_by_label, random.choice(all_category_labels)))
        results_select["cards (по диапазону price)"].append(
            analyzer.get_mean_time(select_by_price_range, random.uniform(1000.0, 4000.0)))
        results_select["orders (по статусу)"].append(analyzer.get_mean_time(select_by_status, 'shipped'))

    plotter.build_plot(x_data=select_counts, y_data_dict=results_select, title="Сравнение производительности SELECT",
                       x_label="Количество запросов", y_label="Время (секунды)", filename="perf_select_all_tables",
                       sub_dir="5c_final_comparison")

    # === ГРАФИК 3: ИССЛЕДОВАНИЕ DELETE ===
    print("\n[ИССЛЕДОВАНИЕ 3/3] Сравнение производительности DELETE...")
    delete_iterations = [1, 2, 3]
    results_delete = {
        "clients (по PK)": [], "client_details (по PK)": [], "sections (по диапазону ID)": [],
        "categories (по имени)": [], "cards (по диапазону price)": [], "orders (по статусу)": []
    }

    print("  - Замер DELETE для 'clients'...")
    times = []
    for _ in range(3):
        db_manager.drop_tables();
        db_manager.create_tables()
        generator.populate_database(num_clients=2000)
        with db_manager.get_db_connection() as c:
            with c.cursor() as cur: cur.execute("SELECT client_id FROM clients"); ids = [r[0] for r in cur.fetchall()]
        id_to_del = [random.choice(ids)]

        start = time.perf_counter()
        db_manager.perform_deletes('clients', 'client_id', id_to_del)
        end = time.perf_counter()
        times.append(end - start)
    results_delete["clients (по PK)"] = times

    print("  - Замер DELETE для 'client_details'...")
    times = []
    for _ in range(3):
        db_manager.drop_tables();
        db_manager.create_tables()
        generator.populate_database(num_clients=2000)
        with db_manager.get_db_connection() as c:
            with c.cursor() as cur: cur.execute("SELECT client_id FROM client_details"); ids = [r[0] for r in
                                                                                                cur.fetchall()]
        id_to_del = [random.choice(ids)]

        start = time.perf_counter()
        db_manager.perform_deletes('client_details', 'client_id', id_to_del)
        end = time.perf_counter()
        times.append(end - start)
    results_delete["client_details (по PK)"] = times

    print("  - Замер DELETE для 'sections'...")
    times = []
    for _ in range(3):
        db_manager.drop_tables();
        db_manager.create_tables()
        generator.populate_database(num_sections=50)
        start = time.perf_counter()
        delete_by_id_range(25)
        end = time.perf_counter()
        times.append(end - start)
    results_delete["sections (по диапазону ID)"] = times

    print("  - Замер DELETE для 'categories'...")
    times = []
    for _ in range(3):
        db_manager.drop_tables();
        db_manager.create_tables()
        generator.populate_database(num_sections=10, num_categories_per_section=20)
        with db_manager.get_db_connection() as c:
            with c.cursor() as cur: cur.execute("SELECT name FROM categories"); names = [r[0] for r in cur.fetchall()]
        name_to_del = random.choice(names)

        start = time.perf_counter()
        delete_by_name(name_to_del)
        end = time.perf_counter()
        times.append(end - start)
    results_delete["categories (по имени)"] = times

    print("  - Замер DELETE для 'cards'...")
    times = []
    for _ in range(3):
        db_manager.drop_tables();
        db_manager.create_tables()
        generator.populate_database(num_sections=10, num_categories_per_section=10, num_cards_per_category=200)
        start = time.perf_counter()
        delete_by_price_range(100.0)
        end = time.perf_counter()
        times.append(end - start)
    results_delete["cards (по диапазону price)"] = times

    print("  - Замер DELETE для 'orders'...")
    times = []
    for _ in range(3):
        db_manager.drop_tables();
        db_manager.create_tables()
        generator.populate_database(num_clients=500, num_orders_per_client=10)
        start = time.perf_counter()
        delete_by_status('cancelled')
        end = time.perf_counter()
        times.append(end - start)
    results_delete["orders (по статусу)"] = times

    plotter.build_plot(
        x_data=delete_iterations,
        y_data_dict=results_delete,
        title="Сравнение производительности DELETE для всех таблиц",
        x_label="Номер замера",
        y_label="Время выполнения (секунды)",
        filename="perf_delete_all_tables",
        sub_dir="5c_final_comparison"
    )

    # === ИССЛЕДОВАНИЕ JOIN ===
    print("\n[Часть 4] Исследование производительности JOIN-запросов...")

    def measure_join_query(query):
        with db_manager.get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                cursor.fetchall()

    join_1_query = "SELECT c.name, cd.address FROM clients c JOIN client_details cd ON c.client_id = cd.client_id LIMIT 1000;"
    time_join_1 = analyzer.get_mean_time(measure_join_query, join_1_query)
    print(f"  - Среднее время JOIN (один к одному): {time_join_1:.6f} секунд")

    join_2_query = "SELECT s.name, c.name FROM sections s JOIN categories c ON s.section_id = c.section_id LIMIT 5000;"
    time_join_2 = analyzer.get_mean_time(measure_join_query, join_2_query)
    print(f"  - Среднее время JOIN (один ко многим): {time_join_2:.6f} секунд")

    join_3_query = "SELECT o.order_id, c.title FROM orders o JOIN order_items oi ON o.order_id = oi.order_id JOIN cards c ON oi.card_id = c.card_id LIMIT 5000;"
    time_join_3 = analyzer.get_mean_time(measure_join_query, join_3_query)
    print(f"  - Среднее время JOIN (многие ко многим): {time_join_3:.6f} секунд")

    # === ОЧИСТКА ===
    print("\n[Очистка] Удаление песочницы...")
    db_manager.DB_CONFIG['database'] = original_db
    sandbox_manager.drop_sandbox_db(sandbox_name)

    print("\n--- ФИНАЛЬНОЕ ИССЛЕДОВАНИЕ ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '5c_final_comparison')}'.")


if __name__ == "__main__":
    run_final_investigation()