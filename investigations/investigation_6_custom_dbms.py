import sys
import os
import random
import shutil

# --- Настройка путей ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

from lib.custom_dbms.engine import SimpleDB
from investigations.perf_analyzer import PerformanceAnalyzer
from investigations.plotter import Plotter


# --- Основной скрипт ---
def run_custom_db_investigation():
    print("--- НАЧАЛО ИССЛЕДОВАНИЯ 4: НАША СОБСТВЕННАЯ СУБД ---")

    db_path = "my_custom_db_test"
    # Полная очистка перед тестом
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = SimpleDB(db_path=db_path)
    plotter = Plotter(base_output_dir=PLOTS_DIR)
    analyzer = PerformanceAnalyzer(number=1, repeat=3)

    # --- 1. Подготовка таблиц ---
    print("\n[Подготовка] Создание таблиц в нашей СУБД...")
    db.execute("CREATE TABLE users_indexed (id INT, name VARCHAR(50));")
    db.execute("CREATE TABLE users_no_index (id INT, name VARCHAR(50));")

    # Создаем индекс для одной из таблиц
    db.execute("CREATE INDEX idx_id ON users_indexed (id);")

    # --- 2. Исследование INSERT ---
    print("\n[Часть 1] Сравнение производительности INSERT...")
    insert_counts = [100, 500, 1000, 2500, 5000]
    results_insert = {"С индексом": [], "Без индекса": []}

    def measure_insert(table, count):
        for i in range(count):
            db.execute(f"INSERT INTO {table} VALUES ({i}, 'user_{i}');")

    for count in insert_counts:
        print(f"  - Замер INSERT для {count} строк...")
        # Очищаем файлы таблиц перед каждым тестом
        if os.path.exists(db._get_table_path("users_indexed")): os.remove(db._get_table_path("users_indexed"))
        if os.path.exists(db._get_table_path("users_no_index")): os.remove(db._get_table_path("users_no_index"))

        time_idx = analyzer.measure_time(measure_insert, "users_indexed", count)
        results_insert["С индексом"].append(time_idx)

        time_noidx = analyzer.measure_time(measure_insert, "users_no_index", count)
        results_insert["Без индекса"].append(time_noidx)

    plotter.build_plot(
        x_data=insert_counts, y_data_dict=results_insert,
        title="Собственная СУБД: Сравнение INSERT (с индексом vs без)",
        x_label="Количество вставляемых строк", y_label="Среднее время выполнения (секунды)",
        filename="custom_db_perf_insert", sub_dir="7_custom_dbms"
    )

    # --- 3. Исследование SELECT ---
    print("\n[Часть 2] Сравнение производительности SELECT...")
    TOTAL_RECORDS = 10000
    print(f"  - Заполнение таблиц {TOTAL_RECORDS} записями...")
    # Очищаем файлы перед заполнением
    if os.path.exists(db._get_table_path("users_indexed")): os.remove(db._get_table_path("users_indexed"))
    if os.path.exists(db._get_table_path("users_no_index")): os.remove(db._get_table_path("users_no_index"))
    # Пересоздаем индекс, так как он был в памяти
    db.execute("CREATE INDEX idx_id ON users_indexed (id);")

    for i in range(TOTAL_RECORDS):
        db.execute(f"INSERT INTO users_indexed VALUES ({i}, 'user_{i}');")
        db.execute(f"INSERT INTO users_no_index VALUES ({i}, 'user_{i}');")

    select_counts = [10, 50, 100, 200, 500]
    results_select = {"С индексом": [], "Без индекса": []}

    def measure_select(table, ids):
        for i in ids:
            db.execute(f"SELECT * FROM {table} WHERE id = {i};")

    for count in select_counts:
        print(f"  - Замер SELECT для {count} случайных строк...")
        ids_to_select = random.sample(range(TOTAL_RECORDS), count)

        time_idx = analyzer.measure_time(measure_select, "users_indexed", ids_to_select)
        results_select["С индексом"].append(time_idx)

        time_noidx = analyzer.measure_time(measure_select, "users_no_index", ids_to_select)
        results_select["Без индекса"].append(time_noidx)

    plotter.build_plot(
        x_data=select_counts, y_data_dict=results_select,
        title="Собственная СУБД: Сравнение SELECT WHERE (с индексом vs без)",
        x_label="Количество запросов SELECT", y_label="Среднее время выполнения (секунды)",
        filename="custom_db_perf_select", sub_dir="7_custom_dbms"
    )

    # --- 4. Очистка ---
    print("\n[Очистка] Удаление тестовой базы данных...")
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print("\n--- ИССЛЕДОВАНИЕ 4 ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '7_custom_dbms')}'.")


if __name__ == "__main__":
    run_custom_db_investigation()