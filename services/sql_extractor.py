import pymssql
import pymysql
import json
from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    @abstractmethod
    def get_databases(self): pass
    @abstractmethod
    def get_tables(self, db_name): pass
    @abstractmethod
    def get_views(self, db_name): pass
    @abstractmethod
    def get_procedures(self, db_name): pass
    @abstractmethod
    def get_triggers(self, db_name): pass
    @abstractmethod
    def get_table_ddl(self, db_name, table_name): pass
    @abstractmethod
    def get_table_sample(self, db_name, table_name): pass
    @abstractmethod
    def get_object_definition(self, db_name, object_name): pass

class MSSQLExtractor(BaseExtractor):
    def __init__(self, c):
        self.host, self.user, self.pw = f"{c.host}:{c.port}", c.user, c.password
    def _conn(self, db='master'):
        return pymssql.connect(server=self.host, user=self.user, password=self.pw, database=db)
    def get_databases(self):
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM sys.databases WHERE state_desc='ONLINE' AND name NOT IN ('master','model','msdb','tempdb')")
                return [r[0] for r in cur.fetchall()]
    def get_tables(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
                return [r[0] for r in cur.fetchall()]
    def get_views(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
                return [r[0] for r in cur.fetchall()]
    def get_procedures(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM sys.procedures")
                return [r[0] for r in cur.fetchall()]
    def get_triggers(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM sys.triggers")
                return [r[0] for r in cur.fetchall()]
    def get_table_ddl(self, db_name, table_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}'")
                cols = cur.fetchall()
                return f"CREATE TABLE [{table_name}] (\n" + ",\n".join([f" [{c[0]}] {c[1]}({c[2]})" for c in cols]) + "\n);"
    def get_table_sample(self, db_name, table_name):
        with self._conn(db_name) as conn:
            with conn.cursor(as_dict=True) as cur:
                cur.execute(f"SELECT TOP 20 * FROM [{table_name}]")
                return cur.fetchall()
    def get_object_definition(self, db_name, object_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT definition FROM sys.sql_modules WHERE object_id=OBJECT_ID('{object_name}')")
                r = cur.fetchone()
                return r[0] if r else ""

class MySQLExtractor(BaseExtractor):
    def __init__(self, c):
        self.host, self.port, self.user, self.pw = c.host, c.port, c.user, c.password
    def _conn(self, db=None):
        return pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.pw, database=db, charset='utf8mb4')
    def get_databases(self):
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW DATABASES")
                return [r[0] for r in cur.fetchall() if r[0] not in ('information_schema','mysql','performance_schema','sys')]
    def get_tables(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
                return [r[0] for r in cur.fetchall()]
    def get_views(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW FULL TABLES WHERE Table_type = 'VIEW'")
                return [r[0] for r in cur.fetchall()]
    def get_procedures(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW PROCEDURE STATUS WHERE Db = %s", (db_name,))
                return [r[1] for r in cur.fetchall()]
    def get_triggers(self, db_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW TRIGGERS")
                return [r[0] for r in cur.fetchall()]
    def get_table_ddl(self, db_name, table_name):
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SHOW CREATE TABLE `{table_name}`")
                return cur.fetchone()[1]
    def get_table_sample(self, db_name, table_name):
        with self._conn(db_name) as conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(f"SELECT * FROM `{table_name}` LIMIT 20")
                return cur.fetchall()
    def get_object_definition(self, db_name, object_name):
        # MySQL requires specific 'SHOW CREATE' per object type. Simplified here for View:
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"SHOW CREATE VIEW `{object_name}`")
                    return cur.fetchone()[1]
                except:
                    try:
                        cur.execute(f"SHOW CREATE PROCEDURE `{object_name}`")
                        return cur.fetchone()[2]
                    except: return "-- Source not found"

def get_extractor(conn_info):
    if conn_info.db_type == 'mssql': return MSSQLExtractor(conn_info)
    return MySQLExtractor(conn_info)
