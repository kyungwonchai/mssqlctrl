import pymssql
import json

class SQLExtractorService:
    def __init__(self, conn_info):
        self.host = f"{conn_info.host}:{conn_info.port}"
        self.user = conn_info.user
        self.password = conn_info.password

    def _get_connection(self, database='master'):
        return pymssql.connect(
            server=self.host,
            user=self.user,
            password=self.password,
            database=database,
            charset='UTF-8'
        )

    def get_databases(self):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name FROM sys.databases WHERE state_desc = 'ONLINE'")
                return [row[0] for row in cursor.fetchall()]

    def get_tables(self, db_name):
        with self._get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
                return [row[0] for row in cursor.fetchall()]

    def get_views(self, db_name):
        with self._get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
                return [row[0] for row in cursor.fetchall()]

    def get_procedures(self, db_name):
        with self._get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name FROM sys.procedures")
                return [row[0] for row in cursor.fetchall()]

    def get_triggers(self, db_name):
        with self._get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT name FROM sys.triggers")
                return [row[0] for row in cursor.fetchall()]

    def get_table_ddl(self, db_name, table_name):
        # Simplistic approach to get CREATE TABLE-like info using sp_help or sys.columns
        # Since sp_help is verbose, we'll fetch column info to build a dummy DDL or just use sys.sql_modules if available
        # Real DDL extraction in SQL Server is usually done via SMO, but here we'll use T-SQL
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        with self._get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                cols = cursor.fetchall()
                ddl = f"CREATE TABLE [{table_name}] (\n"
                col_defs = []
                for col in cols:
                    name, dtype, length, is_null = col
                    len_str = f"({length})" if length and length != -1 else ""
                    null_str = "NULL" if is_null == 'YES' else "NOT NULL"
                    col_defs.append(f"    [{name}] {dtype}{len_str} {null_str}")
                ddl += ",\n".join(col_defs) + "\n);"
                return ddl

    def get_table_sample(self, db_name, table_name):
        # Detect possible date columns for sorting
        query_cols = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        date_col = None
        
        with self._get_connection(db_name) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query_cols)
                cols = [row['COLUMN_NAME'] for row in cursor.fetchall()]
                
                # Heuristic: find a date column to sort by
                date_hints = ['date', 'time', 'created', 'reg', 'modified', 'updated']
                for c in cols:
                    if any(hint in c.lower() for hint in date_hints):
                        date_col = c
                        break
                
                sort_clause = f"ORDER BY [{date_col}] DESC" if date_col else ""
                sample_query = f"SELECT TOP 20 * FROM [{table_name}] {sort_clause}"
                
                try:
                    cursor.execute(sample_query)
                    return cursor.fetchall()
                except:
                    # Fallback if sorting fails
                    cursor.execute(f"SELECT TOP 20 * FROM [{table_name}]")
                    return cursor.fetchall()

    def get_object_definition(self, db_name, object_name):
        # Works for Views, Procedures, and Triggers
        query = f"SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID('{object_name}')"
        with self._get_connection(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                return row[0] if row else "-- Definition not found"
