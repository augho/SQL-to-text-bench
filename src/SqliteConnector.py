from __future__ import annotations
import sqlite3
from enum import Enum
from typing import Callable, Any, TypeVar


T = TypeVar("T")


class FetchType(Enum):
    ALL = (1,)
    ONE = (2,)
    MANY = 3


class SqliteConnector:
    def __init__(self, conn_string: str, do_logging: bool = True) -> None:
        self.conn_string: str = conn_string
        self.do_logging: bool = do_logging

    def _raw_dog_conn(
        self, cb: Callable[[sqlite3.Connection], T]
    ) -> tuple[bool, T | None]:
        try:
            with sqlite3.connect(self.conn_string) as conn:
                return True, cb(conn)
        except sqlite3.OperationalError as err:
            self.log(f"[Warning] Sql Error: {err}")
            return False, None

    def log(self, msg: str) -> None:
        if self.do_logging:
            print(msg)

    def select(
        self, sql_query: str, fetch: FetchType | tuple[FetchType, int] = FetchType.ALL
    ) -> list | tuple | None:
        def execute(conn: sqlite3.Connection) -> list | tuple | None:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            match fetch:
                case FetchType.ALL:
                    return cursor.fetchall()
                case FetchType.ONE:
                    return cursor.fetchone()
                case (FetchType.MANY, result_count) if isinstance(result_count, int):
                    return cursor.fetchmany(result_count)
                case _:
                    raise Exception("[Warning] Incorrect fetch type: ", fetch)

        success, data = self._raw_dog_conn(execute)

        if data is None or not success:
            return None
        else:
            return data

    def execute(self, sql_query: str) -> bool:
        def select(conn: sqlite3.Connection) -> None:
            cursor = conn.cursor()
            cursor.execute(sql_query)

        success, _ = self._raw_dog_conn(select)

        return success

    def insert(self, insert_query: str, data: list[tuple]) -> bool:
        def insert(conn: sqlite3.Connection) -> bool:
            cursor = conn.cursor()
            cursor.executemany(insert_query, data)
            conn.commit()
            self.log(f"[LOG] Inserted {len(data)} rows")
            return True

        success, _ = self._raw_dog_conn(insert)
        return success

    def test(self) -> bool:
        def test(conn: sqlite3.Connection):
            cursor = conn.cursor()

            self.log("[LOG] DB conn init")

            query = "SELECT sqlite_version();"
            cursor.execute(query)

            result = cursor.fetchall()
            self.log(f"[LOG] SQLite Version is {result[0][0]}")

        success, _ = self._raw_dog_conn(test)

        return success

    def has_table(self, tablename: str) -> bool:
        table_list = self.select(f"""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name='{tablename}';""")
        if table_list is None:
            return False

        return len(table_list) != 0

    def list_tables(self) -> list[str]:
        name_list = self.select("""
            SELECT name
            FROM sqlite_master
            WHERE type='table';""")

        if name_list is None:
            return []

        return list(map(lambda x: x[0], name_list))

    # -----------------------------------------------
    # Stats for the profiler

    def table_columns(self, tablename: str) -> list[dict[str, Any]]:
        # Returns list of dicts: {cid, name, type, notnull, dflt_value, pk}
        q = f"PRAGMA table_info({tablename})"
        result = self.select(q, FetchType.ALL)
        assert result is not None, f"query: {q}"

        r = self.select(f"PRAGMA table_info({tablename})", FetchType.ALL)
        assert r is not None

        cols = []
        for row in result:
            cols.append(
                {
                    "column_id": row[0],
                    "column_name": row[1],
                    "column_type": row[2],
                    "allows_null": not bool(row[3]),
                    "default_value": row[4],
                    "is_pk": bool(row[5]),
                }
            )
        return cols

    def table_row_count(self, tablename: str) -> int:
        result = self.select(f'SELECT COUNT(*) FROM "{tablename}"', fetch=FetchType.ONE)
        assert result is not None, f"[ASSERT] tablename={tablename}"
        return result[0]

    def count_nulls_and_nonnulls(
        self, tablename: str, column_name: str
    ) -> tuple[int, int]:
        q = (
            f"SELECT SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as nulls, "
            f"SUM(CASE WHEN {column_name} IS NOT NULL THEN 1 ELSE 0 END) as nonnulls FROM {tablename}"
        )

        result = self.select(q, FetchType.ONE)
        assert result is not None, (
            f"[ASSERT] tablename={tablename}, colname={column_name}"
        )

        nulls, nonnulls = result

        # SQLite returns None for SUM over empty set sometimes; coerce
        nulls = int(nulls or 0)
        nonnulls = int(nonnulls or 0)

        return nulls, nonnulls

    def distinct_count(self, tablename: str, column_name: str) -> int:
        q = f"SELECT COUNT(DISTINCT {column_name}) FROM {tablename}"

        result = self.select(q, FetchType.ONE)
        assert result is not None, (
            f"[ASSERT] tablename={tablename}, colname={column_name}"
        )
        # check type of result
        return int(result[0] or 0)

    def min_max_for_column(
        self, tablename, column_name
    ) -> tuple[int | None, int | None]:
        # We attempt MIN/MAX directly; for mixed types SQLite will try to compare.
        q = f"SELECT MIN({column_name}), MAX({column_name}) FROM {tablename} WHERE {column_name} IS NOT NULL"

        result = self.select(q, FetchType.ONE)
        assert result is not None, (
            f"[ASSERT] tablename={tablename}, colname={column_name}"
        )

        if result is None:
            return None, None
        return int(result[0]), int(result[1])

    def length_stats_sql(
        self, tablename: str, column_name: str
    ) -> tuple[int | None, float | None, int | None]:
        # For text-like values compute min/avg/max length using LENGTH().
        q = (
            f"SELECT MIN(LENGTH({column_name})), AVG(LENGTH({column_name})), MAX(LENGTH({column_name})) "
            f"FROM {tablename} WHERE {column_name} IS NOT NULL"
        )

        result = self.select(q, FetchType.ONE)
        assert result is not None, (
            f"[ASSERT] tablename={tablename}, colname={column_name}"
        )

        if result is None:
            return None, None, None
        # AVG returns float or None
        return (
            int(result[0]) if result[0] is not None else None,
            float(result[1]) if result[1] is not None else None,
            int(result[2]) if result[2] is not None else None,
        )

    def sample_values(
        self,
        tablename: str,
        column_name: str,
        sample_size: int,
        force_random: bool = False,
    ):
        # Fetch up to sample_size non-null values. Using simple LIMIT for sampling is biased,
        # but it's fast and avoids full table scans; for better randomness, could ORDER BY RANDOM()
        # but that's expensive on large tables. We'll use ORDER BY RANDOM() if sample_size is small relative.

        total = self.table_row_count(tablename)

        use_random = (
            sample_size >= 100 and sample_size < 10000 and sample_size > total * 0.05
        )
        use_random = use_random or force_random

        if use_random:
            q = f"SELECT {column_name} FROM {tablename} WHERE {column_name} IS NOT NULL ORDER BY RANDOM() LIMIT {sample_size}"
        else:
            q = f"SELECT {column_name} FROM {tablename} WHERE {column_name} IS NOT NULL LIMIT {sample_size}"

        result = self.select(q, FetchType.ALL)
        assert result is not None, (
            f"[ASSERT] tablename={tablename}, colname={column_name}"
        )
        return [r[0] for r in result]


if __name__ == "__main__":
    db_filepath = "./db/Chinook.db"
    db = SqliteConnector(db_filepath)

    db.do_logging = False
    assert db.select("this is not sql") is None
    db.do_logging = True

    assert db.select("SELECT * FROM Album LIMIT 3;") is not None

    tablename = "Customer"
    string_column_name = "Company"
    int_column_name = "SupportRepId"

    assert db.count_nulls_and_nonnulls(tablename, string_column_name) == (49, 10)
    assert db.table_row_count(tablename) == 59, (
        f"row count = {db.table_row_count(tablename)}"
    )

    assert db.distinct_count(tablename, string_column_name) == 10

    assert db.min_max_for_column(tablename, int_column_name) == (3, 5)

    assert db.length_stats_sql(tablename, string_column_name) == (5, 16.6, 48)

    assert db.sample_values(tablename, string_column_name, 3) == [
        "Embraer - Empresa Brasileira de Aeronáutica S.A.",
        "JetBrains s.r.o.",
        "Woodstock Discos",
    ]

    [print(d) for d in db.table_columns(tablename)]
