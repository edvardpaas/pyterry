import time
from enum import Enum, auto
from typing import Any

from sqlalchemy import Connection, text


class Tag(Enum):
    FACT_COUNT = auto()
    COMPILER_INIT = auto()
    MAT_NONREC = auto()
    MAT_REC = auto()
    DRAIN = auto()
    SPJ_SELECT = auto()
    SPJ_JOIN = auto()
    SPJ_PROJECT = auto()
    SPJ_CLEAR = auto()


class StatementData:
    def __init__(
        self, test_run: int, iter: int, tag: Tag, elapsed: int, rule: str = ""
    ) -> None:
        self.test_run = test_run
        self.iter = iter
        self.tag = tag
        self.elapsed = elapsed
        self.rule = rule

    def serialize(self) -> tuple[int, int, str, int, str]:
        return (self.test_run, self.iter, self.tag.name, self.elapsed, self.rule)


class ConnectionProfiler:
    def __init__(self, conn: Connection, test_run: int) -> None:
        self.conn = conn
        self.test_run = test_run
        self.iter = -1
        self.statements: list[tuple[int, int, str, int, str]] = []

    def execute(self, tag: Tag, stmt: str, rule: str = "") -> Any:
        t1 = time.perf_counter()
        r = self.conn.execute(text(stmt))
        t2 = time.perf_counter()
        elapsed_time = int(round((t2 - t1) * 1000))
        self.save_point(tag, elapsed_time, rule)
        return r

    def save_point(self, tag: Tag, elapsed: int, rule):
        stmt_data = StatementData(self.test_run, self.iter, tag, elapsed, rule)
        self.statements.append(stmt_data.serialize())

    def increment_iter(self):
        self.iter += 1

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
