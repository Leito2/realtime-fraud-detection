"""Postgres adapter. One transaction per table per batch; reconnects lazily after a failure."""
import psycopg

from auditor.sql import insert_sql


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self._dsn, self._conn = dsn, None

    def _connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, autocommit=False)
        return self._conn

    def write(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        columns = list(rows[0])
        sql = insert_sql(table, columns)
        try:
            conn = self._connection()
            with conn.cursor() as cur:
                cur.executemany(sql, [tuple(r[c] for c in columns) for r in rows])
            conn.commit()
        except psycopg.Error:
            if self._conn is not None:
                self._conn.close()
            self._conn = None
            raise
