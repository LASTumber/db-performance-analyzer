# run_setup.py (можно удалить после настройки)
from lib.db_manager import create_tables, drop_tables, backup_database
from lib.data_generator import DataGenerator
import os

if __name__ == "__main__":
    # Убедимся, что DB_CONFIG в db_manager.py настроен правильно

    print("Проверяем/создаем таблицы...")
    create_tables()

    # Удалить все данные перед заполнением, если таблицы уже были
    print("Очищаем таблицы перед новой генерацией данных...")
    drop_tables()  # Удаляем таблицы, чтобы создать их заново и сбросить AUTO_INCREMENT
    create_tables()  # Создаем таблицы снова

    generator = DataGenerator()
    generator.populate_database(
        num_clients=50,
        num_sections=5,
        num_categories_per_section=4,
        num_cards_per_category=20,
        num_orders_per_client=10
    )

    print("Попробуем сделать бэкап базы данных...")
    backup_database("initial_backup.sql")

    print("\nНастройка базы данных и заполнение данными завершены.")
    # Чтобы проверить данные, можно подключиться к MySQL через Workbench или клиент
    # и выполнить SELECT * FROM clients; и т.д.