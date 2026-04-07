from faker import Faker
import random
import bcrypt
from lib.db_manager import get_db_connection
from pymysql import Error

fake = Faker('ru_RU')

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

    def clear_uniques(self):
        try:
            fake.unique.clear()
        except AttributeError:
            pass

    def _generate_password_hash(self, password):
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=4))
        return hashed_password.decode('utf-8')

    def generate_client_data(self, count):
        clients_data = []
        for i in range(count):
            email = f"{i}_{fake.email()}"
            password = fake.password()
            password_hash = self._generate_password_hash(password)
            name = fake.name()
            created_at = fake.date_time_between(start_date='-5y', end_date='now')
            updated_at = fake.date_time_between(start_date=created_at, end_date='now')
            clients_data.append((email, password_hash, name, created_at, updated_at))
        return clients_data

    def generate_client_details_data(self, client_ids):
        client_details_data = []
        for client_id in client_ids:
            phone_number = fake.phone_number()
            address = fake.address()
            birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90)
            client_details_data.append((client_id, phone_number, address, birth_date))
        return client_details_data

    def generate_section_data(self, count):
        sections_data = []
        for i in range(count):
            name = f"Секция Наград {i + 1}"
            sections_data.append((name,))
        return sections_data

    def generate_category_data(self, count, section_ids):
        if not section_ids:
            print("Предупреждение: Нет секций для привязки категорий.")
            return []
        categories_data = []
        for i in range(count):
            section_id = random.choice(section_ids)
            name = f"Категория {i + 1}"
            label = f"category-{i + 1}"
            categories_data.append((section_id, name, label))
        return categories_data

    def generate_card_data(self, count, category_ids):
        if not category_ids:
            print("Предупреждение: Нет категорий для привязки товаров.")
            return []
        cards_data = []
        for i in range(count):
            category_id = random.choice(category_ids)
            title = f"{fake.catch_phrase()} Награда #{i}"
            description = fake.paragraph(nb_sentences=3)
            image_url = fake.image_url()
            price = round(random.uniform(10.0, 5000.0), 2)
            stock_quantity = random.randint(0, 500)
            purchases_count = random.randint(0, stock_quantity)
            cards_data.append((category_id, title, description, image_url, price, stock_quantity, purchases_count))
        return cards_data

    def generate_order_data(self, count, client_ids):
        if not client_ids:
            print("Предупреждение: Нет клиентов для создания заказов.")
            return []
        orders_data = []
        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        for _ in range(count):
            client_id = random.choice(client_ids)
            created_at = fake.date_time_between(start_date='-2y', end_date='now')
            status = random.choice(statuses)
            total_amount = round(random.uniform(50.0, 10000.0), 2)
            orders_data.append((client_id, created_at, status, total_amount))
        return orders_data

    def generate_order_item_data(self, order_ids, card_ids):
        if not order_ids or not card_ids:
            print("Предупреждение: Нет заказов или товаров для создания позиций заказа.")
            return []
        order_items_data = []
        for order_id in order_ids:
            num_items = random.randint(1, 5)
            if len(card_ids) < num_items:
                num_items = len(card_ids)
            selected_card_ids = random.sample(card_ids, num_items)
            for card_id in selected_card_ids:
                quantity = random.randint(1, 10)
                price_at_purchase = round(random.uniform(50.0, 1000.0), 2)
                order_items_data.append((order_id, card_id, quantity, price_at_purchase))
        return order_items_data

    def insert_data(self, table_name, data):
        if not data:
            return []

        columns_map = {
            'clients': "email, password_hash, name, created_at, updated_at",
            'client_details': "client_id, phone_number, address, birth_date",
            'sections': "name",
            'categories': "section_id, name, label",
            'cards': "category_id, title, description, image_url, price, stock_quantity, purchases_count",
            'orders': "client_id, created_at, status, total_amount",
            'order_items': "order_id, card_id, quantity, price_at_purchase",
        }
        if table_name not in columns_map:
            raise ValueError(f"Неизвестная таблица: {table_name}")

        columns = columns_map[table_name]
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            with get_db_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.executemany(query, data)

                    pk_column_map = {
                        'clients': 'client_id',
                        'sections': 'section_id',
                        'categories': 'category_id',
                        'cards': 'card_id',
                        'orders': 'order_id',
                        'order_items': 'order_item_id',
                    }
                    if table_name in pk_column_map:
                        pk_column = pk_column_map[table_name]
                        cursor.execute(
                            f"SELECT {pk_column} FROM {table_name} ORDER BY {pk_column} DESC LIMIT {len(data)};")
                        fetched_ids = [row[0] for row in cursor.fetchall()]
                        inserted_ids = fetched_ids[::-1]
                        self.generated_ids[table_name].extend(inserted_ids)
                        return inserted_ids
            return []
        except Error as e:
            print(f"Ошибка при вставке данных в таблицу '{table_name}': {e}")
            raise

    def populate_database(self, num_clients=10, num_sections=5, num_categories_per_section=3, num_cards_per_category=10,
                          num_orders_per_client=5):
        self.clear_uniques()
        print("Начинаем заполнение базы данных...")

        self.clear_uniques()

        clients_data = self.generate_client_data(num_clients)
        client_ids = self.insert_data('clients', clients_data)

        if client_ids:
            client_details_data = self.generate_client_details_data(client_ids)
            self.insert_data('client_details', client_details_data)

        sections_data = self.generate_section_data(num_sections)
        section_ids = self.insert_data('sections', sections_data)

        category_ids = []
        if section_ids:
            num_categories = num_sections * num_categories_per_section
            all_categories_data = self.generate_category_data(num_categories, section_ids)
            category_ids = self.insert_data('categories', all_categories_data)

        card_ids = []
        if category_ids:
            num_cards = len(category_ids) * num_cards_per_category
            all_cards_data = self.generate_card_data(num_cards, category_ids)
            card_ids = self.insert_data('cards', all_cards_data)

        order_ids = []
        if client_ids:
            num_orders = num_clients * num_orders_per_client
            all_orders_data = self.generate_order_data(num_orders, client_ids)
            order_ids = self.insert_data('orders', all_orders_data)

        if order_ids and card_ids:
            order_items_data = self.generate_order_item_data(order_ids, card_ids)
            self.insert_data('order_items', order_items_data)
        else:
            print("Пропуск генерации позиций заказа: нет заказов или товаров.")

        print("Заполнение базы данных завершено.")