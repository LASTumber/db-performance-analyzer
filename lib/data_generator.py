from faker import Faker
import random
from datetime import datetime, timedelta
import bcrypt # Для хеширования паролей
from lib.db_manager import get_db_connection
from pymysql import Error

fake = Faker('ru_RU') # Используем русскую локаль для более реалистичных имен

class DataGenerator:
    def __init__(self):
        self.generated_ids = {
            'clients': [],
            'client_details': [],
            'sections': [],
            'categories': [],
            'cards': [],
            'orders': [],
            'order_items': []
        }
        # Убедимся, что Faker генерирует уникальные email'ы
        self.unique_emails = set()

    def _generate_password_hash(self, password):
        """Генерирует хеш пароля."""
        # bcrypt хеширует строку байтов, поэтому кодируем пароль
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=4))
        return hashed.decode('utf-8') # Декодируем обратно в строку для хранения

    def generate_client_data(self, count):
        """Генерирует данные для таблицы clients с гарантированно уникальным email."""
        clients_data = []
        for i in range(count):
            # Генерируем email с уникальным префиксом
            # Это намного быстрее и надежнее, чем fake.unique.email() на больших объемах
            email = f"{i}_{fake.email()}"

            password = fake.password()
            password_hash = self._generate_password_hash(password)
            name = fake.name()
            created_at = fake.date_time_between(start_date='-5y', end_date='now')
            updated_at = fake.date_time_between(start_date=created_at, end_date='now')

            clients_data.append((email, password_hash, name, created_at, updated_at))
        return clients_data

    def generate_client_details_data(self, client_ids):
        """Генерирует данные для таблицы client_details, опираясь на client_ids."""
        client_details_data = []
        for client_id in client_ids:
            phone_number = fake.phone_number()
            address = fake.address()
            birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90)
            client_details_data.append((client_id, phone_number, address, birth_date))
        return client_details_data

    def generate_section_data(self, count):
        """Генерирует данные для таблицы sections."""
        sections_data = []
        for i in range(count):
            # Создаем гарантированно уникальное имя
            name = f"Секция Наград {i + 1}"
            sections_data.append((name,))
        return sections_data

    def generate_category_data(self, count, section_ids):
        """Генерирует данные для таблицы categories, опираясь на section_ids."""
        categories_data = []
        if not section_ids:
            print("Предупреждение: Нет секций для привязки категорий.")
            return []

        for i in range(count):
            section_id = random.choice(section_ids)
            # Создаем гарантированно уникальные имя и метку
            name = f"Категория {i + 1}"
            label = f"category-{i + 1}"
            categories_data.append((section_id, name, label))
        return categories_data

    def generate_card_data(self, count, category_ids):
        """Генерирует данные для таблицы cards, опираясь на category_ids."""
        cards_data = []
        if not category_ids:
            print("Предупреждение: Нет категорий для привязки товаров.")
            return []
        for _ in range(count):
            category_id = random.choice(category_ids)
            title = fake.catch_phrase() + " Награда"
            description = fake.paragraph(nb_sentences=3)
            image_url = fake.image_url()
            price = round(random.uniform(10.0, 5000.0), 2)
            stock_quantity = random.randint(0, 500)
            purchases_count = random.randint(0, stock_quantity) # Purchases_count не может быть больше stock_quantity

            cards_data.append((category_id, title, description, image_url, price, stock_quantity, purchases_count))
        return cards_data

    def generate_order_data(self, count, client_ids):
        """Генерирует данные для таблицы orders, опираясь на client_ids."""
        orders_data = []
        if not client_ids:
            print("Предупреждение: Нет клиентов для создания заказов.")
            return []
        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        for _ in range(count):
            client_id = random.choice(client_ids)
            created_at = fake.date_time_between(start_date='-2y', end_date='now')
            status = random.choice(statuses)
            total_amount = round(random.uniform(50.0, 10000.0), 2) # Примерная сумма, будет уточнена order_items

            orders_data.append((client_id, created_at, status, total_amount))
        return orders_data

    def generate_order_item_data(self, count, order_ids, card_ids):
        """Генерирует данные для таблицы order_items, опираясь на order_ids и card_ids."""
        order_items_data = []
        if not order_ids or not card_ids:
            print("Предупреждение: Нет заказов или товаров для создания позиций заказа.")
            return []

        # Для каждого заказа создадим несколько позиций
        for order_id in order_ids:
            num_items = random.randint(1, 5) # От 1 до 5 позиций в заказе
            selected_cards = random.sample(card_ids, min(num_items, len(card_ids)))

            for card_id in selected_cards:
                quantity = random.randint(1, 10)
                # Предполагаем, что цена на момент покупки может быть случайной для теста,
                # или мы можем получить ее из таблицы cards (более реалистично).
                # Для простоты генерации, пока сделаем случайной.
                price_at_purchase = round(random.uniform(50.0, 1000.0), 2)
                order_items_data.append((order_id, card_id, quantity, price_at_purchase))
        return order_items_data

    def insert_data(self, table_name, data):
        """Вставляет сгенерированные данные в указанную таблицу и возвращает ID вставленных записей."""
        if not data:
            print(f"Нет данных для вставки в таблицу '{table_name}'.")
            return []

        # Формируем SQL запрос и список столбцов в зависимости от таблицы
        if table_name == 'clients':
            columns = "email, password_hash, name, created_at, updated_at"
        elif table_name == 'client_details':
            columns = "client_id, phone_number, address, birth_date"
        elif table_name == 'sections':
            columns = "name"
        elif table_name == 'categories':
            columns = "section_id, name, label"
        elif table_name == 'cards':
            columns = "category_id, title, description, image_url, price, stock_quantity, purchases_count"
        elif table_name == 'orders':
            columns = "client_id, created_at, status, total_amount"
        elif table_name == 'order_items':
            columns = "order_id, card_id, quantity, price_at_purchase"
        else:
            raise ValueError(f"Неизвестная таблица: {table_name}")

        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        inserted_ids = []
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(query, data)
                    # Для MySQL, чтобы получить ID, нужно это делать по одной записи или использовать LAST_INSERT_ID()
                    # Если auto_increment PK, то обычно cursor.lastrowid даст первый ID, а потом можно сгенерировать диапазон
                    # Но executemany возвращает только количество строк, а не ID.
                    # Более надежно: если нужно много ID, делать по одной записи.
                    # Для упрощения пока не будем получать все ID из executemany
                    # Для теста, если нужны ID, можем запрашивать их.
                    # Но для FK нам нужны ID из предыдущих таблиц, поэтому будем их сохранять вручную.
                    print(f"Успешно вставлено {len(data)} записей в '{table_name}'.")

                    # Если нужны ID для FK, их придется получать SELECT-ом после вставки,
                    # или вставлять по одной записи и получать cur.lastrowid.
                    # Для этого задания сделаем проще: будем считать, что при генерации
                    # ID для FK уже есть, и нам не нужно их возвращать из insert_data,
                    # кроме как для первичных таблиц (clients, sections и т.д.).
                    # Для целей тестирования мы можем запросить последние ID после вставки
                    pk_column_map = {
                        'clients': 'client_id',
                        'sections': 'section_id',
                        'categories': 'category_id',  # <-- Вот здесь исправлена опечатка
                        'cards': 'card_id',
                        'orders': 'order_id',
                        'order_items': 'order_item_id'
                    }

                    if table_name in pk_column_map:
                        pk_column = pk_column_map[table_name]
                        cur.execute(
                            f"SELECT {pk_column} FROM {table_name} ORDER BY {pk_column} DESC LIMIT {len(data)};")
                        # В PyMySQL с DictCursor результат будет словарем, поэтому обращаемся по ключу.
                        # Если вы не использовали DictCursor, оставьте row[0]. Давайте сделаем универсально.
                        # Для этого перенастроим get_db_connection, чтобы он не использовал DictCursor, так как это влияет на код.
                        # Давайте вернемся к простому варианту, предполагая, что курсор стандартный.
                        fetched_ids = [row[0] for row in cur.fetchall()]
                        inserted_ids = fetched_ids[::-1]  # Возвращаем в том же порядке, в каком вставляли

            self.generated_ids[table_name].extend(inserted_ids)
            return inserted_ids
        except Error as e:
            print(f"Ошибка при вставке данных в таблицу '{table_name}': {e}")
            return []

    def populate_database(self, num_clients=10, num_sections=5, num_categories_per_section=3, num_cards_per_category=10, num_orders_per_client=5):
        """
        Заполняет всю базу данных сгенерированными данными.
        Порядок вставки важен из-за внешних ключей.
        """
        print("Начинаем заполнение базы данных...")

        # 1. Clients
        print(f"Генерация и вставка {num_clients} клиентов...")
        clients_data = self.generate_client_data(num_clients)
        client_ids = self.insert_data('clients', clients_data)
        print(f"Вставлено клиентов, ID: {client_ids}")

        # 2. Client Details (зависит от Clients)
        if client_ids:
            print(f"Генерация и вставка деталей для {len(client_ids)} клиентов...")
            client_details_data = self.generate_client_details_data(client_ids)
            self.insert_data('client_details', client_details_data)

        # 3. Sections
        print(f"Генерация и вставка {num_sections} секций...")
        sections_data = self.generate_section_data(num_sections)
        section_ids = self.insert_data('sections', sections_data)
        print(f"Вставлено секций, ID: {section_ids}")

        # 4. Categories (зависит от Sections)
        if section_ids:
            num_categories = num_sections * num_categories_per_section
            print(f"Генерация и вставка {num_categories} категорий...")
            # Генерируем категории, распределяя их по секциям
            all_categories_data = []
            for _ in range(num_categories):
                all_categories_data.extend(self.generate_category_data(1, section_ids)) # generate 1 category at a time
            # all_categories_data = self.generate_category_data(num_categories, section_ids) # Это неправильно, нужно по 1 чтобы random.choice работал
            category_ids = self.insert_data('categories', all_categories_data)
            print(f"Вставлено категорий, ID: {category_ids}")
        else:
            category_ids = []

        # 5. Cards (зависит от Categories)
        if category_ids:
            num_cards = num_categories * num_cards_per_category
            print(f"Генерация и вставка {num_cards} товаров...")
            all_cards_data = []
            for _ in range(num_cards):
                 all_cards_data.extend(self.generate_card_data(1, category_ids))
            # all_cards_data = self.generate_card_data(num_cards, category_ids)
            card_ids = self.insert_data('cards', all_cards_data)
            print(f"Вставлено товаров, ID: {card_ids}")
        else:
            card_ids = []

        # 6. Orders (зависит от Clients)
        if client_ids:
            num_orders = num_clients * num_orders_per_client
            print(f"Генерация и вставка {num_orders} заказов...")
            all_orders_data = []
            for _ in range(num_orders):
                all_orders_data.extend(self.generate_order_data(1, client_ids))
            #all_orders_data = self.generate_order_data(num_orders, client_ids)
            order_ids = self.insert_data('orders', all_orders_data)
            print(f"Вставлено заказов, ID: {order_ids}")
        else:
            order_ids = []

        # 7. Order Items (зависит от Orders и Cards)
        if order_ids and card_ids:
            print(f"Генерация и вставка позиций для {len(order_ids)} заказов...")
            order_items_data = self.generate_order_item_data(num_orders * 2, order_ids, card_ids) # Произвольное количество позиций
            self.insert_data('order_items', order_items_data)
            print(f"Вставлено позиций заказа.")
        else:
            print("Не удалось сгенерировать позиции заказа: нет заказов или товаров.")

        print("Заполнение базы данных завершено.")

    # Внутри класса DataGenerator в lib/data_generator.py

    def clear_uniques(self):
        """Сбрасывает состояние генератора уникальных значений Faker."""
        fake.unique.clear()
        self.unique_emails.clear()