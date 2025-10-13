import sys
import os
import random
import matplotlib.pyplot as plt

# --- Настройка путей ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

from lib import db_manager, data_generator, sandbox_manager
from investigations.perf_analyzer import PerformanceAnalyzer
from investigations.plotter import Plotter


# --- Вспомогательные функции ---

def perform_string_select(table_name, column_name, search_value, operator="="):
    """Выполняет SELECT по строковому полю с разными операторами."""
    if operator.upper() == 'LIKE':
        query = f"SELECT * FROM {table_name} WHERE {column_name} LIKE %s"
    else:
        query = f"SELECT * FROM {table_name} WHERE {column_name} = %s"

    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (search_value,))
            cursor.fetchall()


# --- Основной скрипт ---
def run_string_index_investigation():
    print("--- НАЧАЛО ИССЛЕДОВАНИЯ 3.2: ЭФФЕКТИВНОСТЬ СТРОКОВОГО ИНДЕКСА ---")

    sandbox_name = "NIRbase_sandbox_str"
    # --- ИЗМЕНЕНИЕ: Инициализируем Plotter с правильным базовым путем ---
    plotter = Plotter(base_output_dir=PLOTS_DIR)
    analyzer = PerformanceAnalyzer(number=1, repeat=3)
    generator = data_generator.DataGenerator()

    print(f"\n[Подготовка] Создание песочницы '{sandbox_name}'...")
    sandbox_manager.create_sandbox_db(sandbox_name)
    original_database_name = db_manager.DB_CONFIG['database']
    db_manager.DB_CONFIG['database'] = sandbox_name
    sandbox_manager.setup_sandbox_tables(sandbox_name)
    print("Базовая структура таблиц в песочнице создана.")

    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS cards_with_index;")
            cursor.execute("CREATE TABLE cards_with_index LIKE cards;")
            cursor.execute("ALTER TABLE cards_with_index ADD INDEX idx_title (title);")
            print("Таблица 'cards_with_index' (с B-Tree индексом) создана.")
            cursor.execute("DROP TABLE IF EXISTS cards_no_index;")
            cursor.execute("CREATE TABLE cards_no_index LIKE cards;")
            print("Таблица 'cards_no_index' (без индекса) создана.")

    print("  - Заполнение таблиц данными для тестов...")
    TOTAL_RECORDS = 50000
    generator.clear_uniques()
    fake_category_ids = list(range(1, 101))
    cards_data = generator.generate_card_data(TOTAL_RECORDS, fake_category_ids)

    cards_columns = "category_id, title, description, image_url, price, stock_quantity, purchases_count"
    db_manager.perform_inserts('cards_with_index', cards_columns, cards_data)
    db_manager.perform_inserts('cards_no_index', cards_columns, cards_data)
    print(f"Обе таблицы заполнены {TOTAL_RECORDS} записями.")

    # --- Исследование SELECT ---
    print("\n[Часть 1] Сравнение производительности SELECT...")
    all_titles = [row[1] for row in cards_data]
    search_titles = random.sample(all_titles, 20)

    print("  - Замер SELECT по равенству (WHERE title = '...')...")
    time_equality_with_index = sum(
        analyzer.measure_time(perform_string_select, 'cards_with_index', 'title', title) for title in
        search_titles) / len(search_titles)
    time_equality_no_index = sum(
        analyzer.measure_time(perform_string_select, 'cards_no_index', 'title', title) for title in
        search_titles) / len(search_titles)

    print("  - Замер SELECT по префиксу (WHERE title LIKE '...%')...")
    search_prefixes = [title[:len(title) // 2] + '%' for title in search_titles]
    time_prefix_with_index = sum(
        analyzer.measure_time(perform_string_select, 'cards_with_index', 'title', prefix, 'LIKE') for prefix in
        search_prefixes) / len(search_prefixes)
    time_prefix_no_index = sum(
        analyzer.measure_time(perform_string_select, 'cards_no_index', 'title', prefix, 'LIKE') for prefix in
        search_prefixes) / len(search_prefixes)

    print("  - Замер SELECT по подстроке (WHERE title LIKE '%...%')...")
    search_substrings = ['%' + title[len(title) // 4: len(title) // 2] + '%' for title in search_titles if
                         len(title) > 4]
    if not search_substrings: search_substrings = ['%test%']
    time_substring_with_index = sum(
        analyzer.measure_time(perform_string_select, 'cards_with_index', 'title', subs, 'LIKE') for subs in
        search_substrings) / len(search_substrings)
    time_substring_no_index = sum(
        analyzer.measure_time(perform_string_select, 'cards_no_index', 'title', subs, 'LIKE') for subs in
        search_substrings) / len(search_substrings)

    # Построение столбчатой диаграммы
    sub_directory_name = "6b_string_index"
    output_folder = os.path.join(PLOTS_DIR, sub_directory_name)
    os.makedirs(output_folder, exist_ok=True)

    labels = ["Равенство (=)", "Префикс (LIKE '...%')", "Подстрока (LIKE '%...%')"]
    times_with_index = [time_equality_with_index, time_prefix_with_index, time_substring_with_index]
    times_no_index = [time_equality_no_index, time_prefix_no_index, time_substring_no_index]

    figure, axes = plt.subplots(figsize=(12, 8))
    x_positions = range(len(labels))
    axes.bar([pos - 0.2 for pos in x_positions], times_with_index, width=0.4, label='С B-Tree индексом',
             color='skyblue')
    axes.bar([pos + 0.2 for pos in x_positions], times_no_index, width=0.4, label='Без индекса', color='salmon')
    axes.set_xticks(x_positions, labels)
    axes.set_title('Сравнение производительности SELECT для строкового поля')
    axes.set_yscale('log')
    axes.set_ylabel('Среднее время выполнения (секунды, логарифмическая шкала)')
    axes.legend()

    filename = "perf_index_select_string_comparison"
    figure.savefig(os.path.join(output_folder, f"{filename}.png"), dpi=300)
    print(f"График сравнения SELECT сохранен в '{output_folder}'.")
    plt.close(figure)

    # --- Исследование INSERT ---
    print("\n[Часть 2] Сравнение производительности INSERT...")
    insert_counts = [100, 500, 1000, 2500, 5000]
    results_insert = {"Таблица с B-Tree индексом": [], "Таблица без индекса": []}
    for count in insert_counts:
        print(f"  - Замер INSERT для {count} строк...")
        generator.clear_uniques()
        new_cards_data = generator.generate_card_data(count, fake_category_ids)
        with db_manager.get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE cards_with_index")
                cursor.execute("TRUNCATE TABLE cards_no_index")

        time_with_index = analyzer.measure_time(db_manager.perform_inserts, 'cards_with_index', cards_columns,
                                                new_cards_data)
        results_insert["Таблица с B-Tree индексом"].append(time_with_index)
        time_no_index = analyzer.measure_time(db_manager.perform_inserts, 'cards_no_index', cards_columns,
                                              new_cards_data)
        results_insert["Таблица без индекса"].append(time_no_index)

    plotter.build_plot(
        x_data=insert_counts,
        y_data_dict=results_insert,
        title="Сравнение производительности INSERT (строковый индекс)",
        x_label="Количество вставляемых строк",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_index_insert_string",
        # --- ИЗМЕНЕНИЕ: Указываем подпапку ---
        sub_dir="6b_string_index"
    )

    # --- Очистка ---
    print("\n[Очистка] Удаление песочницы...")
    db_manager.DB_CONFIG['database'] = original_database_name
    sandbox_manager.drop_sandbox_db(sandbox_name)

    print("\n--- ИССЛЕДОВАНИЕ 3.2 ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '6b_string_index')}'.")


if __name__ == "__main__":
    run_string_index_investigation()