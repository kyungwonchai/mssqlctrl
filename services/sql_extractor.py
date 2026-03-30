import pymssql
import pymysql
from abc import ABC, abstractmethod
from datetime import datetime, timezone

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

    @abstractmethod
    def get_database_health_snapshot(self, db_name):
        """반환: 진단·용량 분석용 테이블별 행수/용량/인덱스 요약 (dict, JSON 직렬화 가능)."""
        pass

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

    def get_database_health_snapshot(self, db_name):
        collected_at = datetime.now(timezone.utc).isoformat()
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.name, t.name, SUM(p.rows) AS row_count,
                           CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS total_mb
                    FROM sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    INNER JOIN sys.indexes i ON t.object_id = i.object_id
                    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                    WHERE t.is_ms_shipped = 0 AND i.index_id IN (0,1)
                    GROUP BY s.name, t.object_id, t.name
                    """
                )
                rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT s.name, t.name, COUNT(i.index_id) AS index_count
                    FROM sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    INNER JOIN sys.indexes i ON t.object_id = i.object_id
                    WHERE i.index_id > 0 AND t.is_ms_shipped = 0
                    GROUP BY s.name, t.object_id, t.name
                    """
                )
                idx_map = {(r[0], r[1]): int(r[2]) for r in cur.fetchall()}
        tables = []
        for schema_name, table_name, row_count, total_mb in rows:
            tables.append(
                {
                    "schema": schema_name,
                    "name": table_name,
                    "row_count": int(row_count or 0),
                    "total_mb": float(total_mb or 0),
                    "index_count": idx_map.get((schema_name, table_name), 0),
                }
            )
        tables.sort(key=lambda x: x["total_mb"], reverse=True)
        return {
            "database": db_name,
            "engine": "mssql",
            "collected_at": collected_at,
            "tables": tables,
        }

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

    def get_database_health_snapshot(self, db_name):
        collected_at = datetime.now(timezone.utc).isoformat()
        with self._conn(db_name) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT TABLE_NAME, TABLE_ROWS,
                           ROUND(COALESCE(DATA_LENGTH,0) / 1024 / 1024, 2) AS data_mb,
                           ROUND(COALESCE(INDEX_LENGTH,0) / 1024 / 1024, 2) AS index_mb
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                    """,
                    (db_name,),
                )
                rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT TABLE_NAME, COUNT(DISTINCT INDEX_NAME) AS idx_count
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = %s
                    GROUP BY TABLE_NAME
                    """,
                    (db_name,),
                )
                idx_map = {r[0]: int(r[1]) for r in cur.fetchall()}
        tables = []
        for table_name, row_est, data_mb, index_mb in rows:
            tables.append(
                {
                    "schema": None,
                    "name": table_name,
                    "row_count": int(row_est or 0),
                    "data_mb": float(data_mb or 0),
                    "index_mb": float(index_mb or 0),
                    "total_mb": float((data_mb or 0) + (index_mb or 0)),
                    "index_count": idx_map.get(table_name, 0),
                }
            )
        tables.sort(key=lambda x: x["total_mb"], reverse=True)
        return {
            "database": db_name,
            "engine": "mysql",
            "collected_at": collected_at,
            "tables": tables,
        }

def get_extractor(conn_info):
    if conn_info.db_type == 'mssql': return MSSQLExtractor(conn_info)
    return MySQLExtractor(conn_info)
