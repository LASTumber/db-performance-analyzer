# lib/db/schema.py
"""
Создание idempotent-схемы для проекта (MySQL, InnoDB, utf8mb4)
и проверка корректности внешних ключей.

Функции:
- create_schema(cfg): создаёт таблицы IF NOT EXISTS.
- check_foreign_keys(cfg): проверяет, что FK заданные в expected_fks существуют в БД и возвращает отчёт.
"""

from typing import Dict, Any, List, Tuple
import mysql.connector
import logging

logger = logging.getLogger(__name__)


SCHEMA_SQL = [
    # profiles
    """
    CREATE TABLE IF NOT EXISTS profiles (
      profile_id INT AUTO_INCREMENT PRIMARY KEY,
      passport VARCHAR(30) UNIQUE,
      address VARCHAR(200),
      birthdate DATE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # departments
    """
    CREATE TABLE IF NOT EXISTS departments (
      department_id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(100)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # students (profile_id is UNIQUE -> 1:1)
    """
    CREATE TABLE IF NOT EXISTS students (
      student_id INT AUTO_INCREMENT PRIMARY KEY,
      full_name VARCHAR(200),
      profile_id INT UNIQUE,
      department_id INT,
      FOREIGN KEY (profile_id) REFERENCES profiles(profile_id),
      FOREIGN KEY (department_id) REFERENCES departments(department_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # courses
    """
    CREATE TABLE IF NOT EXISTS courses (
      course_id INT AUTO_INCREMENT PRIMARY KEY,
      title VARCHAR(150),
      credits INT,
      department_id INT,
      FOREIGN KEY (department_id) REFERENCES departments(department_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # enrollments many-to-many
    """
    CREATE TABLE IF NOT EXISTS enrollments (
      student_id INT,
      course_id INT,
      enrolled_on DATE,
      PRIMARY KEY (student_id, course_id),
      FOREIGN KEY (student_id) REFERENCES students(student_id),
      FOREIGN KEY (course_id) REFERENCES courses(course_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
]


# Expected foreign keys mapping (for automated check)
# format: (table, column, referenced_table, referenced_column)
EXPECTED_FKS = [
    ("students", "profile_id", "profiles", "profile_id"),
    ("students", "department_id", "departments", "department_id"),
    ("courses", "department_id", "departments", "department_id"),
    ("enrollments", "student_id", "students", "student_id"),
    ("enrollments", "course_id", "courses", "course_id"),
]


def get_connection(cfg: Dict[str, Any]):
    cfg = cfg.copy()
    # Ensure unicode/charset options
    if "charset" not in cfg:
        cfg["charset"] = "utf8mb4"
    if "use_unicode" not in cfg:
        cfg["use_unicode"] = True
    return mysql.connector.connect(**cfg)


def create_schema(cfg: Dict[str, Any]) -> None:
    """
    Выполняет CREATE TABLE IF NOT EXISTS для всех SQL-скриптов в SCHEMA_SQL.
    Открывает соединение, выполняет каждый statement и коммитит.
    """
    conn = get_connection(cfg)
    try:
        cur = conn.cursor()
        for stmt in SCHEMA_SQL:
            logger.info("Executing statement:\n%s", stmt.strip().splitlines()[0])
            cur.execute(stmt)
        conn.commit()
        logger.info("Schema creation finished.")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def check_foreign_keys(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Проверяет наличие ожидаемых FK в INFORMATION_SCHEMA.KEY_COLUMN_USAGE.
    Возвращает список записей с полями:
      table, column, referenced_table, referenced_column, exists (True/False), details (if exists)
    """
    conn = get_connection(cfg)
    try:
        cur = conn.cursor(dictionary=True)
        results = []
        for tbl, col, ref_tbl, ref_col in EXPECTED_FKS:
            sql = """
            SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
              AND REFERENCED_TABLE_NAME = %s
              AND REFERENCED_COLUMN_NAME = %s
            """
            cur.execute(sql, (cfg.get("database"), tbl, col, ref_tbl, ref_col))
            row = cur.fetchone()
            exists = row is not None
            results.append({
                "table": tbl,
                "column": col,
                "referenced_table": ref_tbl,
                "referenced_column": ref_col,
                "exists": exists,
                "details": row
            })
        return results
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, help="path to json/yaml db config (json recommended)")
    args = p.parse_args()

    # простая загрузка JSON
    import os
    import io
    cfg_path = args.config
    cfg = {}
    if cfg_path.endswith(".yaml") or cfg_path.endswith(".yml"):
        try:
            import yaml
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
        except Exception as e:
            raise
    else:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    create_schema(cfg)
    fk_report = check_foreign_keys(cfg)
    print(json.dumps(fk_report, indent=2, ensure_ascii=False))
