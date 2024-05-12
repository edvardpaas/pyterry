from typing import Any
import sqlglot
import sqlglot.expressions

from conn_profiler import ConnectionProfiler, Tag
from datalog import Rule
from delta_program import DELTA_PREFIX
from stack import (
    Join,
    Move,
    Project,
    ProjectionInputColumn,
    ProjectionInputValue,
    Select,
    Stack,
    stringify_join,
    stringify_select,
)


class RuleEvaluator:
    def __init__(self, conn: ConnectionProfiler, rule: Rule) -> None:
        self.conn = conn
        self.rule = rule
        self.join_counter = 0
        self.select_counter = 0
        self.temp_tables: list[str] = []
        # Maps relation names to a list of column names
        self.base_relations: dict[str, list[str]] = {}
        self.gen_base_idx_list(rule)
        # Maps join temporary names to a list of column names
        self.tmp_relations: dict[str, list[str]] = {}

    def execute(self, tag: Tag, stmt: str) -> Any:
        return self.conn.execute(tag, stmt, self.rule.serialize())

    def gen_base_idx_list(self, rule: Rule):
        self.base_relations[rule.head.symbol] = [
            f"{rule.head.symbol.strip(DELTA_PREFIX)}_{i}"
            for i in range(len(rule.head.terms))
        ]
        for body_atom in rule.body:
            sym = body_atom.symbol
            if sym in self.base_relations:
                continue
            self.base_relations[sym] = [
                f"{sym.strip(DELTA_PREFIX)}_{i}" for i in range(len(body_atom.terms))
            ]

    def get_idx_list(self, relation: str) -> list[str]:
        if relation in self.tmp_relations:
            return self.tmp_relations[relation]
        else:
            return self.base_relations[relation]

    def create_alias_cols(self, relation: str, cols: int) -> list[str]:
        return [f"{relation}_{i}_alias" for i in range(cols)]

    def create_join_cols(self, left: str, right: str) -> list[str]:
        left_cols = self.get_idx_list(left)
        right_cols = self.get_idx_list(right)
        right_cols = self.create_alias_cols(right, len(right_cols))
        join_cols = left_cols + right_cols
        return join_cols

    def execute_select(self, sql: str):
        self.execute(Tag.SPJ_SELECT, sql)
        self.conn.commit()

    def execute_join(self, sql: str):
        self.execute(Tag.SPJ_JOIN, sql)
        self.conn.commit()

    def step(self):
        stack = Stack(self.rule)
        penultimate_operation = len(stack) - 2
        relation_symbol_to_be_projected = self.rule.head.symbol
        for idx, op in enumerate(stack):
            if isinstance(op, Move):
                if idx == penultimate_operation:
                    relation_symbol_to_be_projected = op.symbol
                pass
            elif isinstance(op, Select):
                index_name = f"{stringify_select(op)}"
                select_result_name = index_name
                if idx == penultimate_operation:
                    relation_symbol_to_be_projected = select_result_name
                select_cols = self.get_idx_list(op.symbol)
                temp_table_name = f"{select_result_name}"
                # If value is a string, then surround it with single quotes
                if isinstance(op.value, str):
                    select_filter = f"'{op.value}'"
                else:
                    select_filter = op.value
                sql = (
                    sqlglot.expressions.Select()
                    .select("*")
                    .from_(f"{op.symbol}")
                    .where(f"{select_cols[op.column]} = {select_filter}")
                )
                sql.set("exists", True)
                sql = sql.sql()

                ct_cols = []
                for i in range(len(select_cols)):
                    ct_cols.append(select_cols[i] + ' INTEGER')
                sql_str = f"CREATE TABLE IF NOT EXISTS {temp_table_name} ({", ".join(ct_cols)})"
                self.execute(Tag.SPJ_SELECT, sql_str)
                self.conn.commit()
                sql_str = f"INSERT INTO {temp_table_name} {sql}"
                self.execute(Tag.SPJ_SELECT, sql_str)

                self.temp_tables.append(temp_table_name)
                self.tmp_relations[select_result_name] = select_cols
                self.select_counter += 1
            elif isinstance(op, Join):
                join_result_name = f"{stringify_join(op)}"
                if idx == penultimate_operation:
                    relation_symbol_to_be_projected = join_result_name
                temp_table_name = f"{join_result_name}"  # sql name
                left_cols = self.get_idx_list(op.left_symbol)
                right_cols = self.get_idx_list(op.right_symbol)
                left_alias = "X"
                right_alias = "Y"
                select_list = []
                for col in left_cols:
                    select_list.append(f"{left_alias}.{col}")
                alias_cols = self.create_alias_cols(op.right_symbol, len(right_cols))
                for i in range(len(right_cols)):
                    select_list.append(
                        f"{right_alias}.{right_cols[i]} AS {alias_cols[i]}"
                    )
                join_cols = self.create_join_cols(
                    op.left_symbol,
                    op.right_symbol,
                )
                self.tmp_relations[join_result_name] = join_cols
                condition_list = []

                for left_key, right_key in op.keys:
                    condition_list.append(
                        sqlglot.condition(
                            f"{left_alias}.{left_cols[left_key]} = {right_alias}.{right_cols[right_key]}"
                        )
                    )
                sql = (
                    sqlglot.expressions.Select()
                    .select(*select_list)
                    .from_(f"{op.left_symbol} as {left_alias}")
                    .join(
                        sqlglot.expressions.alias_(
                            f"{op.right_symbol}", f"{right_alias}"
                        ),
                        on=condition_list,  # type: ignore
                    )
                )
                sql = sql.sql()
                ct_cols = []
                for i in range(len(join_cols)):
                    ct_cols.append(join_cols[i] + ' INTEGER')
                sql_str = f"CREATE TABLE IF NOT EXISTS {temp_table_name} ({", ".join(ct_cols)})"
                self.execute(Tag.SPJ_JOIN, sql_str)
                self.conn.commit()
                sql_str = f"INSERT INTO {temp_table_name} {sql}"
                self.execute(Tag.SPJ_JOIN, sql_str)

                self.temp_tables.append(temp_table_name)
                # self.execute_join(sql)
                self.join_counter += 1

            elif isinstance(op, Project):
                column_list = []
                from_symbol = f"{relation_symbol_to_be_projected}"
                projected_cols = self.get_idx_list(relation_symbol_to_be_projected)
                for input in op.projection_inputs:
                    if isinstance(input, ProjectionInputColumn):
                        column_list.append(f"{projected_cols[input.value]}")
                    elif isinstance(input, ProjectionInputValue):
                        column_list.append(input.value)
                into_symbol = op.symbol
                sql = sqlglot.expressions.insert(
                    sqlglot.select(*column_list)
                    .from_(f"{from_symbol}")  # type: ignore
                    .distinct(),  # TODO!: Performance issue
                    f"{DELTA_PREFIX}{into_symbol}",
                ).sql()
                self.execute(Tag.SPJ_PROJECT, sql)
                self.conn.commit()
        # Delete temporary tables
        drop_sql = set()
        for table_name in self.temp_tables:
            drop_sql.add(f"DROP TABLE {table_name}")
        for drop in drop_sql:
            self.execute(Tag.SPJ_CLEAR, drop)
        self.conn.commit()
