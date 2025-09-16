# tools/smoke_insert.py
import json, sys
from lib.db.connection import DBConnection

def main(cfg_path):
    cfg = json.load(open(cfg_path))
    with DBConnection(cfg) as db:
        # SELECT
        cur = db.execute("SELECT COUNT(*) AS cnt FROM students;")
        row = cur.fetchone()
        print("students count:", row["cnt"])

        # INSERT (проверка вставки)
        cur = db.execute("INSERT INTO students (full_name) VALUES (%s);", ("smoke_test_user",))
        last_id = db.cur.lastrowid
        print("Inserted id (pending commit):", last_id)

        # Откатим, чтобы не оставлять тестовые данные
        db.rollback()
        print("Rolled back inserted row.")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python -m tools.smoke_insert configs/db_config.json")
    else:
        main(sys.argv[1])
