import sys
import os

# Добавляем корневую директорию проекта в sys.path,
# чтобы можно было импортировать модули из lib и investigations
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import db_manager, data_generator, sandbox_manager

def run_all_tests():
    """
    Запускает последовательную проверку всех основных функций.
    """
    print("--- НАЧАЛО ТЕСТИРОВАНИЯ ---")

    # --- Тест 1: db_manager ---
    print("\n[ТЕСТ 1] Проверка db_manager: удаление и создание таблиц в основной БД.")
    try:
        db_manager.drop_tables()
        db_manager.create_tables()
        print("[ТЕСТ 1] УСПЕХ: Таблицы успешно пересозданы.")
    except Exception as e:
        print(f"[ТЕСТ 1] ПРОВАЛ: Ошибка при работе с таблицами: {e}")
        return # Прекращаем тесты, если база не работает

    # --- Тест 2: data_generator ---
    print("\n[ТЕСТ 2] Проверка data_generator: заполнение основной БД небольшим количеством данных.")
    try:
        generator = data_generator.DataGenerator()
        generator.populate_database(
            num_clients=5,
            num_sections=2,
            num_categories_per_section=2,
            num_cards_per_category=3,
            num_orders_per_client=2
        )
        print("[ТЕСТ 2] УСПЕХ: База данных успешно заполнена тестовыми данными.")
    except Exception as e:
        print(f"[ТЕСТ 2] ПРОВАЛ: Ошибка при генерации данных: {e}")
        return

    # --- Тест 3: db_manager (бэкап и восстановление) ---
    print("\n[ТЕСТ 3] Проверка db_manager: бэкап и восстановление.")
    backup_file = "test_backup.sql"
    try:
        db_manager.backup_database(backup_file)
        print("Бэкап создан.")
        # Для проверки восстановления, мы удаляем таблицы и восстанавливаем из бэкапа
        db_manager.drop_tables()
        print("Таблицы удалены перед восстановлением.")
        db_manager.restore_database(backup_file)
        print("Восстановление из бэкапа выполнено.")
        # Простая проверка: если create_tables не выдаст ошибку, значит, таблицы существуют
        db_manager.create_tables()
        print("[ТЕСТ 3] УСПЕХ: Бэкап и восстановление прошли без ошибок.")
    except Exception as e:
        print(f"[ТЕСТ 3] ПРОВАЛ: Ошибка при бэкапе/восстановлении: {e}")
    finally:
        if os.path.exists(backup_file):
            os.remove(backup_file)

    # --- Тест 4: sandbox_manager ---
    print("\n[ТЕСТ 4] Проверка sandbox_manager: создание и удаление песочницы.")
    sandbox_name = "NIRbase_test_sandbox"
    try:
        sandbox_manager.create_sandbox_db(sandbox_name)
        sandbox_manager.setup_sandbox_tables(sandbox_name)
        print("Песочница и таблицы в ней созданы.")
        sandbox_manager.drop_sandbox_db(sandbox_name)
        print("[ТЕСТ 4] УСПЕХ: Песочница успешно создана и удалена.")
    except Exception as e:
        print(f"[ТЕСТ 4] ПРОВАЛ: Ошибка при работе с песочницей: {e}")


    print("\n--- ТЕСТИРОВАНИЕ ЗАВЕРШЕНО ---")

if __name__ == "__main__":
    run_all_tests()