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

def perform_fulltext_select(table_name, column_name, search_query, use_match_against=True):
    """
    Выполняет SELECT для поиска текста через MATCH...AGAINST или LIKE.
    """
    if use_match_against:
        query = f"SELECT * FROM {table_name} WHERE MATCH({column_name}) AGAINST(%s IN NATURAL LANGUAGE MODE)"
    else:
        query = f"SELECT * FROM {table_name} WHERE {column_name} LIKE %s"
        # Для LIKE имитируем поиск слова, оборачивая в '%'
        search_query = f"%{search_query}%"

    with db_manager.get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (search_query,))
            cursor.fetchall()


# --- Основной скрипт ---
def run_fulltext_index_investigation():
    print("--- НАЧАЛО ИССЛЕДОВАНИЯ 3.3: ЭФФЕКТИВНОСТЬ FULLTEXT ИНДЕКСА ---")

    sandbox_name = "NIRbase_sandbox_ft"
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
            # T5: Таблица с FULLTEXT индексом
            cursor.execute("DROP TABLE IF EXISTS cards_with_ft_index;")
            cursor.execute("CREATE TABLE cards_with_ft_index LIKE cards;")
            cursor.execute("ALTER TABLE cards_with_ft_index ADD FULLTEXT INDEX idx_ft_description (description);")
            print("Таблица 'cards_with_ft_index' (с FULLTEXT индексом) создана.")

            # T6: Таблица без индекса
            cursor.execute("DROP TABLE IF EXISTS cards_no_ft_index;")
            cursor.execute("CREATE TABLE cards_no_ft_index LIKE cards;")
            print("Таблица 'cards_no_ft_index' (без индекса) создана.")

    print("  - Заполнение таблиц данными для тестов...")
    TOTAL_RECORDS = 20000
    generator.clear_uniques()
    fake_category_ids = list(range(1, 101))
    cards_data = generator.generate_card_data(TOTAL_RECORDS, fake_category_ids)

    cards_columns = "category_id, title, description, image_url, price, stock_quantity, purchases_count"
    db_manager.perform_inserts('cards_with_ft_index', cards_columns, cards_data)
    db_manager.perform_inserts('cards_no_ft_index', cards_columns, cards_data)
    print(f"Обе таблицы заполнены {TOTAL_RECORDS} записями.")

    # --- Исследование SELECT ---
    print("\n[Часть 1] Сравнение производительности SELECT для поиска слов...")

    all_words_in_descriptions = ' '.join([row[2] for row in cards_data if row[2]]).split()
    single_search_words = random.sample(all_words_in_descriptions, 20)
    multi_search_phrases = [' '.join(random.sample(all_words_in_descriptions, 2)) for _ in range(20)]

    print("  - Замер поиска одного слова...")
    time_single_word_with_index = sum(
        analyzer.measure_time(perform_fulltext_select, 'cards_with_ft_index', 'description', word, True) for word in
        single_search_words) / len(single_search_words)
    time_single_word_no_index = sum(
        analyzer.measure_time(perform_fulltext_select, 'cards_no_ft_index', 'description', word, False) for word in
        single_search_words) / len(single_search_words)

    print("  - Замер поиска двух слов...")
    time_multi_word_with_index = sum(
        analyzer.measure_time(perform_fulltext_select, 'cards_with_ft_index', 'description', phrase, True) for phrase in
        multi_search_phrases) / len(multi_search_phrases)
    time_multi_word_no_index = sum(
        analyzer.measure_time(perform_fulltext_select, 'cards_no_ft_index', 'description', phrase, False) for phrase in
        multi_search_phrases) / len(multi_search_phrases)

    # Построение столбчатой диаграммы
    sub_directory_name = "6c_fulltext_index"
    output_folder = os.path.join(PLOTS_DIR, sub_directory_name)
    os.makedirs(output_folder, exist_ok=True)

    labels = ["Поиск одного слова", "Поиск двух слов"]
    times_with_index = [time_single_word_with_index, time_multi_word_with_index]
    times_no_index = [time_single_word_no_index, time_multi_word_no_index]

    figure, axes = plt.subplots(figsize=(12, 8))
    x_positions = range(len(labels))
    axes.bar([pos - 0.2 for pos in x_positions], times_with_index, width=0.4,
             label='С FULLTEXT индексом (MATCH...AGAINST)', color='forestgreen')
    axes.bar([pos + 0.2 for pos in x_positions], times_no_index, width=0.4, label='Без индекса (LIKE)',
             color='orangered')
    axes.set_xticks(x_positions, labels)
    axes.set_title('Сравнение производительности полнотекстового поиска')
    axes.set_yscale('log')
    axes.set_ylabel('Среднее время выполнения (секунды, логарифмическая шкала)')
    axes.legend()

    filename = "perf_index_select_fulltext"
    figure.savefig(os.path.join(output_folder, f"{filename}.png"), dpi=300)
    print(f"График сравнения SELECT сохранен в '{output_folder}'.")
    plt.close(figure)

    # --- Исследование INSERT ---
    print("\n[Часть 2] Сравнение производительности INSERT...")
    insert_counts = [100, 250, 500, 750, 1000]
    results_insert = {"Таблица с FULLTEXT индексом": [], "Таблица без индекса": []}

    for count in insert_counts:
        print(f"  - Замер INSERT для {count} строк...")
        generator.clear_uniques()
        new_cards_data = generator.generate_card_data(count, fake_category_ids)

        with db_manager.get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE cards_with_ft_index")
                cursor.execute("TRUNCATE TABLE cards_no_ft_index")

        time_with_index = analyzer.measure_time(db_manager.perform_inserts, 'cards_with_ft_index', cards_columns,
                                                new_cards_data)
        results_insert["Таблица с FULLTEXT индексом"].append(time_with_index)

        time_no_index = analyzer.measure_time(db_manager.perform_inserts, 'cards_no_ft_index', cards_columns,
                                              new_cards_data)
        results_insert["Таблица без индекса"].append(time_no_index)

    plotter.build_plot(
        x_data=insert_counts,
        y_data_dict=results_insert,
        title="Сравнение производительности INSERT (FULLTEXT индекс)",
        x_label="Количество вставляемых строк",
        y_label="Среднее время выполнения (секунды)",
        filename="perf_index_insert_fulltext",
        # --- ИЗМЕНЕНИЕ: Указываем подпапку ---
        sub_dir="6c_fulltext_index"
    )

    # --- Очистка ---
    print("\n[Очистка] Удаление песочницы...")
    db_manager.DB_CONFIG['database'] = original_database_name
    sandbox_manager.drop_sandbox_db(sandbox_name)

    print("\n--- ИССЛЕДОВАНИЕ 3.3 ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '6c_fulltext_index')}'.")


if __name__ == "__main__":
    run_fulltext_index_investigation()