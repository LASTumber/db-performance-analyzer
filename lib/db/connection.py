import mysql.connector
from mysql.connector import MySQLConnection
from typing import Optional

class DBConnection:
    def __init__(self, config: dict):
        self.config = config
        self.conn: Optional[MySQLConnection] = None
        self.cur = None

    def __enter__(self):
        self.conn = mysql.connector.connect(**self.config)
        # явно выключаем autocommit, чтобы контролировать транзакии
        self.conn.autocommit = False
        self.cur = self.conn.cursor(buffered=True, dictionary=True)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()

    def execute(self, sql, params=None):
        self.cur.execute(sql, params or ())
        return self.cur

    def executemany(self, sql, seq_of_params):
        self.cur.executemany(sql, seq_of_params)
        return self.cur

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()
