# lib/generator/generator.py
"""
Генератор тестовых данных + быстрые вставки (executemany и LOAD DATA LOCAL INFILE).

Содержит:
- fake row generators (использует Faker, fallback простые строки если не установлен)
- insert_batch_executemany(cfg, table, cols, rows, batch_size)
- load_data_local_infile(cfg, table, cols, rows_iterable, tmp_csv_path=None)
- helper write_csv_rows(rows, cols, csv_path)

Примеры в конце файла.
"""

from typing import Dict, Any, List, Tuple, Iterable, Optional
import csv
import os
import tempfile
import mysql.connector
import logging
logger = logging.getLogger(__name__)

# попытка импортировать Faker
try:
    from faker import Faker
    faker = Faker()
except Exception:
    faker = None
    # можно сгенерировать примитивные строки, если Faker не установлен


def get_connection(cfg: Dict[str, Any]):
    cfg = cfg.copy()
    if "charset" not in cfg:
        cfg["charset"] = "utf8mb4"
    if "use_unicode" not in cfg:
        cfg["use_unicode"] = True
    return mysql.connector.connect(**cfg)


# ----------------------
# Простые генераторы
# ----------------------
def gen_profile() -> Tuple[Optional[int], str, str, str]:
    """Возвращает (profile_id, passport, address, birthdate) - profile_id оставляем None (AUTO_INCREMENT)."""
    if faker:
        passport = faker.bothify(text='??######')  # пример
        address = faker.address().replace("\n", ", ")
        birthdate = faker.date_of_birth(minimum_age=18, maximum_age=60).isoformat()
    else:
        passport = "P" + os.urandom(6).hex()
        address = "Some Address"
        birthdate = "1990-01-01"
    return (None, passport, address, birthdate)


def gen_department() -> Tuple[Optional[int], str]:
    if faker:
        name = faker.word().capitalize() + " Dept"
    else:
        name = "Dept_" + os.urandom(2).hex()
    return (None, name)


def gen_student(profile_id: Optional[int], department_id: Optional[int]) -> Tuple[Optional[int], str, Optional[int], Optional[int]]:
    if faker:
        full_name = faker.name()
    else:
        full_name = "Name_" + os.urandom(2).hex()
    return (None, full_name, profile_id, department_id)


def gen_course(department_id: Optional[int]) -> Tuple[Optional[int], str, Optional[int], Optional[int]]:
    if faker:
        title = faker.sentence(nb_words=3).rstrip(".")
        credits = faker.random_int(min=1, max=6)
    else:
        title = "Course_" + os.urandom(2).hex()
        credits = 3
    return (None, title, credits, department_id)


def write_csv_rows(rows: Iterable[Tuple], cols: List[str], csv_path: str) -> None:
    """
    Записывает rows в CSV файл, пригодный для LOAD DATA.
    Использует quotechar='"', delimiter=',' и экранирует None как \N (MySQL NULL).
    """
    # ensure directory
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            # MySQL expects \N for NULL when using LOAD DATA and default settings
            out = [r if r is not None else "\\N" for r in row]
            writer.writerow(out)


# ----------------------
# Insert helpers
# ----------------------
def insert_batch_executemany(cfg: Dict[str, Any], table: str, cols: List[str],
                             rows: Iterable[Tuple], batch_size: int = 5000) -> None:
    """
    Вставляет rows в таблицу батчами через executemany.
    rows: iterable of tuples (matching cols order).
    batch_size: количество записей в одном executemany.
    """
    conn = get_connection(cfg)
    try:
        cur = conn.cursor()
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        batch = []
        count = 0
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                conn.commit()  # commit per batch — безопасно и снижает нагрузку
                count += len(batch)
                logger.info("Inserted %d rows into %s (total %d)", len(batch), table, count)
                batch = []
        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            count += len(batch)
            logger.info("Inserted final %d rows into %s (total %d)", len(batch), table, count)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def load_data_local_infile(cfg: Dict[str, Any], table: str, cols: List[str],
                           rows: Iterable[Tuple], tmp_csv_path: Optional[str] = None) -> str:
    """
    Подготавливает CSV (временный файл) и загружает его через LOAD DATA LOCAL INFILE.
    Возвращает путь к использованному CSV (если временный — caller может удалить).
    ВАЖНО: cfg должен содержать allow_local_infile=True и сервер MySQL должен разрешать local_infile.
    """
    # подготовка CSV
    temp_file = tmp_csv_path
    remove_after = False
    if temp_file is None:
        fd, temp_file = tempfile.mkstemp(prefix=f"load_{table}_", suffix=".csv")
        os.close(fd)
        remove_after = True

    write_csv_rows(rows, cols, temp_file)

    # Выполнение LOAD DATA
    conn = get_connection(cfg)
    try:
        cur = conn.cursor()
        # Опции: FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
        # NULL обозначается \N (мы использовали это в write_csv_rows)
        sql = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {table}
        FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
        LINES TERMINATED BY '\n'
        ({', '.join(cols)})
        """
        # Для mysql-connector placeholder %s
        cur.execute(sql, (temp_file,))
        conn.commit()
        logger.info("Loaded CSV %s into table %s, affected=%s", temp_file, table, cur.rowcount)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    return temp_file if not remove_after else temp_file  # возвращаем путь, caller может удалить


# ----------------------
# Пример генерации данных в правильном порядке (parents first)
# ----------------------
def generate_and_insert_example(cfg: Dict[str, Any], n_departments: int = 10, n_profiles: int = 1000,
                                n_students: int = 1000, batch_size: int = 5000, use_load_data: bool = False):
    """
    Генерирует простые данные и вставляет их в БД. Это демонстрация, не идеальная модель для всех случаев.
    Порядок вставки: departments -> profiles -> students -> courses -> enrollments.
    При использовании use_load_data: генерируем CSV и загружаем через LOAD DATA.
    """
    # 1) Departments
    departments = [gen_department() for _ in range(n_departments)]
    # We remove the first None column (AUTO_INCREMENT): gen_department returns (None, name)
    dep_rows = [(d[1],) for d in departments]  # only name
    dep_cols = ["name"]
    if use_load_data:
        csv_path = load_data_local_infile(cfg, "departments", dep_cols, dep_rows)
        logger.info("Departments loaded via LOAD DATA from %s", csv_path)
    else:
        # Using executemany (we need to insert and then read the ids)
        insert_batch_executemany(cfg, "departments", dep_cols, dep_rows, batch_size=batch_size)

    # fetch department ids to assign to students/courses
    conn = get_connection(cfg)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT department_id FROM departments ORDER BY department_id LIMIT %s", (n_departments,))
        deps = [r["department_id"] for r in cur.fetchall()]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    # 2) Profiles
    profile_rows = []
    for _ in range(n_profiles):
        _, passport, address, birthdate = gen_profile()
        profile_rows.append((passport, address, birthdate))
    profile_cols = ["passport", "address", "birthdate"]

    if use_load_data:
        path = load_data_local_infile(cfg, "profiles", profile_cols, profile_rows)
        logger.info("Profiles loaded via LOAD DATA from %s", path)
    else:
        insert_batch_executemany(cfg, "profiles", profile_cols, profile_rows, batch_size=batch_size)

    # 3) Students (we need profile ids). Simplest: pick random profile ids from inserted profiles.
    conn = get_connection(cfg)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT profile_id FROM profiles ORDER BY profile_id LIMIT %s", (n_profiles,))
        profiles_ids = [r["profile_id"] for r in cur.fetchall()]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    import random
    student_rows = []
    for i in range(n_students):
        profile_id = profiles_ids[i % len(profiles_ids)]
        department_id = deps[i % len(deps)]
        _, full_name, _, _ = gen_student(profile_id, department_id)
        student_rows.append((full_name, profile_id, department_id))
    student_cols = ["full_name", "profile_id", "department_id"]

    if use_load_data:
        path = load_data_local_infile(cfg, "students", student_cols, student_rows)
        logger.info("Students loaded via LOAD DATA from %s", path)
    else:
        insert_batch_executemany(cfg, "students", student_cols, student_rows, batch_size=batch_size)

    # 4) Courses
    course_rows = []
    for i in range(len(deps) * 3):  # few courses per department
        dept = deps[i % len(deps)]
        _, title, credits, _ = gen_course(dept)
        course_rows.append((title, credits, dept))
    course_cols = ["title", "credits", "department_id"]
    if use_load_data:
        path = load_data_local_infile(cfg, "courses", course_cols, course_rows)
        logger.info("Courses loaded via LOAD DATA from %s", path)
    else:
        insert_batch_executemany(cfg, "courses", course_cols, course_rows, batch_size=batch_size)

    # 5) Enrollments: create few enrollments mapping students to random courses
    conn = get_connection(cfg)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT student_id FROM students ORDER BY student_id LIMIT %s", (n_students,))
        student_ids = [r["student_id"] for r in cur.fetchall()]
        cur.execute("SELECT course_id FROM courses")
        course_ids = [r["course_id"] for r in cur.fetchall()]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    enroll_rows = []
    for sid in student_ids:
        # each student enrolls in 1..3 courses
        k = random.randint(1, 3)
        choices = random.sample(course_ids, k)
        for cid in choices:
            enroll_rows.append((sid, cid, None))  # enrolled_on = NULL for simplicity
    enroll_cols = ["student_id", "course_id", "enrolled_on"]
    if use_load_data:
        path = load_data_local_infile(cfg, "enrollments", enroll_cols, enroll_rows)
        logger.info("Enrollments loaded via LOAD DATA from %s", path)
    else:
        insert_batch_executemany(cfg, "enrollments", enroll_cols, enroll_rows, batch_size=batch_size)

    logger.info("Data generation finished.")


# ----------------------
# Простой self-test / пример использования
# ----------------------
if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, help="path to json config")
    p.add_argument("--use_load", action="store_true", help="use LOAD DATA instead of executemany")
    p.add_argument("--n_profiles", type=int, default=100)
    p.add_argument("--n_students", type=int, default=200)
    p.add_argument("--n_departments", type=int, default=5)
    args = p.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    generate_and_insert_example(cfg,
                                n_departments=args.n_departments,
                                n_profiles=args.n_profiles,
                                n_students=args.n_students,
                                batch_size=1000,
                                use_load_data=args.use_load)
