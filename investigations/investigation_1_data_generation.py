import sys
import os
import time

# --- Настройка путей ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
PLOTS_DIR = os.path.join(ROOT_DIR, "plots")

from investigations.perf_analyzer import PerformanceAnalyzer
from investigations.plotter import Plotter
from lib.data_generator import DataGenerator


# --- Функции-обертки для измерения ---
def measure_simple_generation(generator_func, count):
    generator_func(count)


def measure_linked_generation_one_to_many(parent_func, child_func, parent_count, child_per_parent):
    parent_data = parent_func(parent_count)
    fake_parent_ids = list(range(1, parent_count + 1))
    child_count = parent_count * child_per_parent
    child_func(child_count, fake_parent_ids)


# --- Основной скрипт исследования ---
def run_generation_investigation():
    print("--- НАЧАЛО ИССЛЕДОВАНИЯ 1: ВРЕМЯ ГЕНЕРАЦИИ ДАННЫХ ---")

    row_counts = [100, 500, 1000, 2500, 5000, 10000]
    analyzer = PerformanceAnalyzer(number=1, repeat=3)
    plotter = Plotter(base_output_dir=PLOTS_DIR)
    generator = DataGenerator()

    print("\n[Часть 1] Измерение времени для отдельных таблиц...")
    single_table_results = {
        "clients (с уникальным email)": [],
        "sections (с уникальным именем)": [],
        "cards (простая генерация)": []
    }

    for count in row_counts:
        print(f"  - Замеры для {count} строк...")

        generator.clear_uniques()
        time_clients = analyzer.measure_time(measure_simple_generation, generator.generate_client_data, count)
        single_table_results["clients (с уникальным email)"].append(time_clients)

        generator.clear_uniques()
        time_sections = analyzer.measure_time(measure_simple_generation, generator.generate_section_data, count)
        single_table_results["sections (с уникальным именем)"].append(time_sections)

        generator.clear_uniques()
        fake_category_ids = list(range(1, 101))
        time_cards = analyzer.measure_time(generator.generate_card_data, count, fake_category_ids)
        single_table_results["cards (простая генерация)"].append(time_cards)

    plotter.build_plot(
        x_data=row_counts,
        y_data_dict=single_table_results,
        title="Время генерации данных для отдельных таблиц",
        x_label="Количество генерируемых строк",
        y_label="Среднее время выполнения (секунды)",
        filename="generation_time_single_tables",
        sub_dir="5b_data_generation"
    )

    print("\n[Часть 2] Измерение времени для связанных таблиц (один ко многим)...")
    linked_table_results = {"sections + categories (1 родитель: 5 потомков)": []}
    total_rows_linked = []

    for count in row_counts:
        parent_count = count
        child_per_parent = 5
        total_rows = parent_count + (parent_count * child_per_parent)
        total_rows_linked.append(total_rows)
        print(
            f"  - Замеры для {parent_count} sections и {parent_count * child_per_parent} categories (всего {total_rows} строк)...")

        generator.clear_uniques()
        time_linked = analyzer.measure_time(
            measure_linked_generation_one_to_many,
            generator.generate_section_data,
            generator.generate_category_data,
            parent_count,
            child_per_parent
        )
        linked_table_results["sections + categories (1 родитель: 5 потомков)"].append(time_linked)

    plotter.build_plot(
        x_data=total_rows_linked,
        y_data_dict=linked_table_results,
        title="Время генерации данных для связанных таблиц (один ко многим)",
        x_label="Общее количество генерируемых строк",
        y_label="Среднее время выполнения (секунды)",
        filename="generation_time_linked_tables",
        sub_dir="5b_data_generation"
    )

    print("\n--- ИССЛЕДОВАНИЕ 1 ЗАВЕРШЕНО ---")
    print(f"Результаты сохранены в папку '{os.path.join(PLOTS_DIR, '5b_data_generation')}'.")


if __name__ == "__main__":
    run_generation_investigation()