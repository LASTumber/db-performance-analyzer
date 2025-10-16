import os
import struct
import json
import re
import pickle


class SimpleDB:
    def __init__(self, db_path="custom_database"):
        self.db_path = db_path
        self.schema_path = os.path.join(self.db_path, "schema.json")
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
        self.schema = self._load_schema()

    def _load_schema(self):
        if os.path.exists(self.schema_path):
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        return {}

    def _save_schema(self):
        with open(self.schema_path, 'w') as f:
            json.dump(self.schema, f, indent=4)

    def _get_table_path(self, table_name):
        return os.path.join(self.db_path, f"{table_name}.db")

    def _get_index_path(self, table_name, column_name):
        return os.path.join(self.db_path, f"{table_name}_{column_name}.idx")

    def execute(self, query):
        query = query.strip().lower()

        if query.startswith("create table"):
            return self._execute_create_table(query)
        elif query.startswith("insert into"):
            return self._execute_insert(query)
        elif query.startswith("select"):
            return self._execute_select(query)
        elif query.startswith("delete from"):
            return self._execute_delete(query)
        elif query.startswith("create index"):
            return self._execute_create_index(query)
        else:
            raise ValueError(f"Неподдерживаемый тип запроса: {query}")

    # --- Методы выполнения запросов ---

    def _execute_create_table(self, query):
        match = re.match(r"create table (\w+) \((.+)\);?", query)
        if not match:
            raise ValueError("Неверный синтаксис CREATE TABLE")

        table_name, columns_str = match.groups()
        if table_name in self.schema:
            raise ValueError(f"Таблица '{table_name}' уже существует.")

        columns = []
        row_format = ""
        row_size = 0

        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            if "int" in col_def:
                col_name = col_def.split()[0]
                columns.append({"name": col_name, "type": "INT"})
                row_format += "Q"  # 8-байтовый unsigned integer
                row_size += 8
            elif "varchar" in col_def:
                col_match = re.match(r"(\w+) varchar\((\d+)\)", col_def)
                if not col_match:
                    raise ValueError(f"Неверный синтаксис VARCHAR: {col_def}")
                col_name, length = col_match.groups()
                length = int(length)
                columns.append({"name": col_name, "type": "VARCHAR", "len": length})
                row_format += f"{length}s"  # Строка фиксированной длины
                row_size += length

        self.schema[table_name] = {
            "columns": columns,
            "format": row_format,
            "size": row_size,
            "indexes": {}
        }
        self._save_schema()
        print(f"Таблица '{table_name}' успешно создана.")

    def _execute_insert(self, query):
        match = re.match(r"insert into (\w+) values \((.+)\);?", query)
        if not match:
            raise ValueError("Неверный синтаксис INSERT")

        table_name, values_str = match.groups()
        if table_name not in self.schema:
            raise ValueError(f"Таблица '{table_name}' не найдена.")

        table_info = self.schema[table_name]
        values = [v.strip().strip("'\"") for v in values_str.split(',')]

        packed_values = []
        for i, col_info in enumerate(table_info["columns"]):
            value = values[i]
            if col_info["type"] == "INT":
                packed_values.append(int(value))
            elif col_info["type"] == "VARCHAR":
                encoded_str = value.encode('utf-8')
                padded_str = encoded_str.ljust(col_info["len"], b'\0')
                packed_values.append(padded_str)

        row_data = struct.pack(table_info["format"], *packed_values)

        table_path = self._get_table_path(table_name)
        with open(table_path, 'ab') as f:
            row_position = f.tell()
            f.write(row_data)

        for col_name, index in table_info["indexes"].items():
            col_index = [c["name"] for c in table_info["columns"]].index(col_name)
            key = packed_values[col_index]
            row_index = row_position // table_info["size"]
            index[key] = row_index

            index_path = self._get_index_path(table_name, col_name)
            with open(index_path, 'wb') as f:
                pickle.dump(index, f)

    def _execute_select(self, query):
        match = re.match(r"select (.+) from (\w+)(?: where (.+))?;?", query)
        if not match:
            raise ValueError("Неверный синтаксис SELECT")

        cols_str, table_name, where_clause = match.groups()
        if table_name not in self.schema:
            raise ValueError(f"Таблица '{table_name}' не найдена.")

        table_info = self.schema[table_name]
        table_path = self._get_table_path(table_name)

        results = []

        where_col, where_val = None, None
        if where_clause:
            where_match = re.match(r"(\w+) = (.+)", where_clause.strip())
            if not where_match:
                raise ValueError("Неподдерживаемый синтаксис WHERE")
            where_col, where_val = where_match.groups()
            where_val = where_val.strip("'\"").strip(";")

            # Преобразуем значение для поиска
            col_info = next(c for c in table_info["columns"] if c["name"] == where_col)
            if col_info["type"] == "INT":
                where_val = int(where_val)

        row_indices_to_read = []
        if where_col and where_col in table_info["indexes"]:
            print(f"Используется индекс по полю '{where_col}'...")
            index = table_info["indexes"][where_col]
            if where_val in index:
                row_indices_to_read.append(index[where_val])
        else:
            if os.path.exists(table_path):
                file_size = os.path.getsize(table_path)
                total_rows = file_size // table_info["size"]
                row_indices_to_read = range(total_rows)

        with open(table_path, 'rb') as f:
            for row_index in row_indices_to_read:
                f.seek(row_index * table_info["size"])
                row_data = f.read(table_info["size"])
                if not row_data: continue

                unpacked_row = list(struct.unpack(table_info["format"], row_data))

                for i, col_info in enumerate(table_info["columns"]):
                    if col_info["type"] == "VARCHAR":
                        unpacked_row[i] = unpacked_row[i].strip(b'\0').decode('utf-8', errors='ignore')

                row_as_dict = {col["name"]: val for col, val in zip(table_info["columns"], unpacked_row)}

                if where_col and not (where_col in table_info["indexes"]):
                    if str(row_as_dict.get(where_col)) == str(where_val):
                        results.append(row_as_dict)
                else:
                    results.append(row_as_dict)

        if cols_str.strip() == "*":
            return results
        else:
            selected_cols = [c.strip() for c in cols_str.split(',')]
            return [{col: row[col] for col in selected_cols} for row in results]

    def _execute_delete(self, query):
        match = re.match(r"delete from (\w+)(?: where (.+))?;?", query)
        if not match:
            raise ValueError("Неверный синтаксис DELETE")

        table_name, where_clause = match.groups()
        if table_name not in self.schema:
            raise ValueError(f"Таблица '{table_name}' не найдена.")

        table_info = self.schema[table_name]
        table_path = self._get_table_path(table_name)

        if not os.path.exists(table_path):
            print("Таблица пуста, удалять нечего.")
            return

        if not where_clause:
            os.remove(table_path)
            print(f"Все записи из таблицы '{table_name}' удалены.")
            for col_name in table_info["indexes"]:
                index_path = self._get_index_path(table_name, col_name)
                if os.path.exists(index_path):
                    os.remove(index_path)
                table_info["indexes"][col_name].clear()
            return

        # --- Логика для DELETE с WHERE ---

        where_match = re.match(r"(\w+) = (.+)", where_clause.strip())
        if not where_match:
            raise ValueError("Неподдерживаемый синтаксис WHERE для DELETE")
        where_col, where_val = where_match.groups()
        where_val = where_val.strip("'\"").strip(";")

        col_info = next((c for c in table_info["columns"] if c["name"] == where_col), None)
        if not col_info:
            raise ValueError(f"Столбец '{where_col}' не найден в таблице.")
        if col_info["type"] == "INT":
            where_val = int(where_val)

        rows_to_keep = []
        file_size = os.path.getsize(table_path)
        total_rows = file_size // table_info["size"]

        with open(table_path, 'rb') as f:
            for i in range(total_rows):
                row_data = f.read(table_info["size"])
                if not row_data: continue

                unpacked_row = list(struct.unpack(table_info["format"], row_data))
                row_as_dict = {col["name"]: val for col, val in zip(table_info["columns"], unpacked_row)}

                if str(row_as_dict.get(where_col)) != str(where_val):
                    rows_to_keep.append(row_data)

        with open(table_path, 'wb') as f:
            for row_data in rows_to_keep:
                f.write(row_data)

        print(f"Записи из таблицы '{table_name}' удалены. Требуется перестройка индексов.")
        for col_name in table_info["indexes"]:
            table_info["indexes"][col_name].clear()
            self._rebuild_index(table_name, col_name)

    def _rebuild_index(self, table_name, column_name):
        table_info = self.schema[table_name]
        index_name = f"idx_{column_name}"  # Пример имени
        self.execute(f"CREATE INDEX {index_name} ON {table_name} ({column_name});")

    def _execute_create_index(self, query):
        match = re.match(r"create index (\w+) on (\w+) \((\w+)\);?", query)
        if not match:
            raise ValueError("Неверный синтаксис CREATE INDEX")

        index_name, table_name, column_name = match.groups()
        if table_name not in self.schema:
            raise ValueError(f"Таблица '{table_name}' не найдена.")

        table_info = self.schema[table_name]
        col_names = [c["name"] for c in table_info["columns"]]
        if column_name not in col_names:
            raise ValueError(f"Столбец '{column_name}' не найден в таблице '{table_name}'.")

        print(f"Создание индекса '{index_name}' для {table_name}.{column_name}...")
        index = {}
        table_path = self._get_table_path(table_name)

        if os.path.exists(table_path):
            file_size = os.path.getsize(table_path)
            total_rows = file_size // table_info["size"]
            col_index = col_names.index(column_name)

            with open(table_path, 'rb') as f:
                for i in range(total_rows):
                    row_data = f.read(table_info["size"])
                    unpacked_row = struct.unpack(table_info["format"], row_data)
                    key = unpacked_row[col_index]
                    index[key] = i

        # Сохраняем и в память, и на диск
        table_info["indexes"][column_name] = index
        index_path = self._get_index_path(table_name, column_name)
        with open(index_path, 'wb') as f:
            pickle.dump(index, f)

        self._save_schema()
        print("Индекс успешно создан.")