import sys
import os
import random

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

from lib import db_manager, data_generator, sandbox_manager
from investigations.perf_analyzer import PerformanceAnalyzer
from investigations.plotter import Plotter

def create_table_without_primary_key(original_table_name, new_table_name):
    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE {original_table_name}")
            create_table_sql = cursor.fetchone()[1]

            create_table_sql = create_table_sql.replace(f"CREATE TABLE `{original_table_name}`",
                                                        f"CREATE TABLE `{new_table_name}`")
            lines = create_table_sql.split('\n')
            new_lines = []
            for line in lines:
                if "PRIMARY KEY" in line:
                    continue
                line = line.replace(" AUTO_INCREMENT", "")
                new_lines.append(line)

            modified_sql = '\n'.join(new_lines)

            cursor.execute(f"DROP TABLE IF EXISTS {new_table_name}")
            cursor.execute(modified_sql)
            print(f"Таблица '{new_table_name}' создана без первичного ключа.")


def perform_range_select(table_name, column_name, max_value):
    query = f"SELECT * FROM {table_name} WHERE {column_name} < %s"
    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (max_value,))
            cursor.fetchall()


def run_primary_key_index_investigation():
    print("--- НАЧАЛО ИССЛЕДОВАНИЯ 3.1: ЭФФЕКТИВНОСТЬ ПЕРВИЧНОГО КЛЮЧА ---")

    sandbox_name = "NIRbase_sandbox_pk"
    plotter = Plotter(base_output_dir=PLOTS_DIR)
    analyzer = PerformanceAnalyzer(number=1, repeat=3)
    generator = data_generator.DataGenerator()

    print(f"\n[Подготовка] Создание песочницы '{sandbox_name}'...")
    sandbox_manager.create_sandbox_db(sandbox_name)
    original_database_name = db_manager.DB_CONFIG['database']
    db_manager.DB_CONFIG['database'] = sandbox_name
    sandbox_manager.setup_sandbox_tables(sandbox_name)
    create_table_without_primary_key('clients', 'clients_no_pk')

    print("\n[Часть 1] Сравнение производительности INSERT...")
    insert_counts = [100, 500, 1000, 2500, 5000]
    results_insert = {"Таблица с Primary Key": [], "Таблица без Primary Key": []}

    def measure_clean_insert(table_name, columns, number_of_rows, has_primary_key=True):
        generator.clear_uniques()
        client_data_to_insert = generator.generate_client_data(number_of_rows)
        if not has_primary_key:
            client_data_to_insert = [row + (i + 1,) for i, row in enumerate(client_data_to_insert)]
        with db_manager.get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                if table_name == 'clients':
                    cursor.execute("TRUNCATE TABLE client_details")
                    cursor.execute("TRUNCATE TABLE orders")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        db_manager.perform_inserts(table_name, columns, client_data_to_insert)

    for count in insert_counts:
        print(f"  - Замер INSERT для {count} строк...")
        time_with_pk = analyzer.measure_time(measure_clean_insert, table_name='clients',
                                             columns="email, password_hash, name, created_at, updated_at",
                                             number_of_rows=count, has_primary_key=True)
        results_insert["Таблица с Primary Key"].append(time_with_pk)
        time_no_pk = analyzer.measure_time(measure_clean_insert, table_name='clients_no_pk',
                                           columns="email, password_hash, name, created_at, updated_at, client_id",
                                           number_of_rows=count, has_primary_key=False)
        results_insert["Таблица без Primary Key"].append(time_no_pk)

    plotter.build_plot(
        x_data=insert_counts,
        y_data_dict=results_insert,
        title="Сравнение производительности INSERT (с PK vs без PK)",
        x_label="Количество вставляемых строк",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_index_insert_pk",
        sub_dir="6a_primary_key_index"
    )

    print("\n[Часть 2] Сравнение производительности SELECT...")
    print("  - Заполнение таблиц данными для тестов...")
    TOTAL_RECORDS = 20000
    generator.clear_uniques()
    client_data_large = generator.generate_client_data(TOTAL_RECORDS)
    db_manager.perform_inserts('clients', "email, password_hash, name, created_at, updated_at", client_data_large)
    client_data_no_pk_large = [row + (i + 1,) for i, row in enumerate(client_data_large)]
    db_manager.perform_inserts('clients_no_pk', "email, password_hash, name, created_at, updated_at, client_id",
                               client_data_no_pk_large)

    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT client_id FROM clients")
            all_ids = [row[0] for row in cursor.fetchall()]

    select_counts = [10, 100, 500, 1000]
    results_select_equality = {"Таблица с Primary Key": [], "Таблица без Primary Key": []}
    for count in select_counts:
        print(f"  - Замер SELECT по равенству для {count} строк...")
        ids_to_select = random.sample(all_ids, count)
        time_with_pk = analyzer.measure_time(db_manager.perform_selects, 'clients', 'client_id', ids_to_select)
        results_select_equality["Таблица с Primary Key"].append(time_with_pk)
        time_no_pk = analyzer.measure_time(db_manager.perform_selects, 'clients_no_pk', 'client_id', ids_to_select)
        results_select_equality["Таблица без Primary Key"].append(time_no_pk)

    plotter.build_plot(
        x_data=select_counts,
        y_data_dict=results_select_equality,
        title="Сравнение SELECT по равенству (с PK vs без PK)",
        x_label="Количество запрашиваемых строк (по одной)",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_index_select_eq_pk",
        sub_dir="6a_primary_key_index"
    )

    range_percents = [0.01, 0.05, 0.1, 0.2]
    results_select_range = {"Таблица с Primary Key": [], "Таблица без Primary Key": []}
    x_axis_range = [int(p * TOTAL_RECORDS) for p in range_percents]
    for i, percent in enumerate(range_percents):
        max_id_value = x_axis_range[i]
        if max_id_value == 0: max_id_value = 1
        print(f"  - Замер SELECT по диапазону до ID {max_id_value}...")
        time_with_pk = analyzer.measure_time(perform_range_select, 'clients', 'client_id', max_id_value)
        results_select_range["Таблица с Primary Key"].append(time_with_pk)
        time_no_pk = analyzer.measure_time(perform_range_select, 'clients_no_pk', 'client_id', max_id_value)
        results_select_range["Таблица без Primary Key"].append(time_no_pk)

    plotter.build_plot(
        x_data=x_axis_range,
        y_data_dict=results_select_range,
        title="Сравнение SELECT по диапазону (с PK vs без PK)",
        x_label="Количество строк в диапазоне (WHERE id < N)",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_index_select_range_pk",
        sub_dir="6a_primary_key_index"
    )

    print("\n[Очистка] Удаление песочницы...")
    db_manager.DB_CONFIG['database'] = original_database_name
    sandbox_manager.drop_sandbox_db(sandbox_name)

    print("\n--- ИССЛЕДОВАНИЕ 3.1 ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '6a_primary_key_index')}'.")


if __name__ == "__main__":
    run_primary_key_index_investigation()