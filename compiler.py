from typing import Any

import sqlalchemy

from conn_profiler import ConnectionProfiler, Tag
from datalog import Program, Symbol
from delta_program import DELTA_PREFIX, make_delta_program
from dependency_graph import sort_program
from evaluator import RuleEvaluator
from helpers import split_program


def get_table_row_count(conn: ConnectionProfiler, table_name: str) -> int:
    result = conn.execute(Tag.FACT_COUNT, f"SELECT COUNT(*) FROM {table_name}")
    return list(result.first())[0]


def get_table_names(program: Program) -> set[str]:
    table_names: set[str] = set()
    for rule in program:
        table_names.add(rule.head.symbol)
    return table_names


def get_total_fact_count(connection, table_names: set[str]) -> int:
    fact_count = 0
    for table in table_names:
        fact_count += get_table_row_count(connection, table)
    return fact_count


class Compiler:
    def __init__(
        self, db_type: str, db_data: dict[str, Any], program: Program, test_run: int
    ):
        self.setup_connection(db_type, db_data, test_run)
        self.base_relations: dict[str, list[str]] = {}
        self.gen_base_idx_list(program)

        self.relations: set[str] = set()
        self.delta_relations: set[str] = set()
        self.current_delta_relations: set[str] = set()

        for rule in program:
            relation = rule.head.symbol
            delta_relation = f"{DELTA_PREFIX}{rule.head.symbol}"
            current_delta_relation = f"{DELTA_PREFIX}{DELTA_PREFIX}{rule.head.symbol}"
            self.relations.add(rule.head.symbol)
            self.delta_relations.add(delta_relation)
            self.create_table_like(delta_relation, relation)
            self.current_delta_relations.add(current_delta_relation)
            self.create_table_like(current_delta_relation, relation)
            for body_atom in rule.body:
                body_relation = body_atom.symbol
                body_delta_relation = f"{DELTA_PREFIX}{body_atom.symbol}"
                self.relations.add(body_atom.symbol)
                self.create_table_like(body_delta_relation, body_relation)
                self.delta_relations.add(body_delta_relation)
        self.init_programs(program)

    def dump_benchmark(self) -> list[tuple[int, int, str, int, str]]:
        return self.conn.statements

    def gen_base_idx_list(self, program: Program):
        for rule in program:
            if rule.head.symbol not in self.base_relations:
                self.base_relations[rule.head.symbol] = [
                    f"{rule.head.symbol}_{i} INTEGER"
                    for i in range(len(rule.head.terms))
                ]
            for body_atom in rule.body:
                sym = body_atom.symbol
                if sym in self.base_relations:
                    continue
                self.base_relations[sym] = [
                    f"{sym}_{i} INTEGER" for i in range(len(body_atom.terms))
                ]

    def get_idx_list(self, relation: str) -> list[str]:
        relation = relation.strip(DELTA_PREFIX)
        return self.base_relations[relation]

    def setup_connection(self, db_type: str, db_data: dict[str, Any], test_run: int):
        if db_type == "sqlite":
            self.engine = sqlalchemy.create_engine(f"sqlite:///{db_data['db']}")
        if db_type == "duckdb":
            self.engine = sqlalchemy.create_engine(f"duckdb:///{db_data['db']}")
        elif db_type == "mysql":
            self.engine = sqlalchemy.create_engine(
                f"mysql+mysqldb://{db_data['user']}:{db_data['password']}@{db_data['host']}/{db_data['db']}",
            )
        elif db_type == "postgres":
            self.engine = sqlalchemy.create_engine(
                f"postgresql+psycopg://{db_data['user']}:{db_data['password']}@{db_data['host']}:{db_data['port']}/{db_data['db']}"
            )
        elif db_type == "materialize":
            self.engine = sqlalchemy.create_engine(
                f"postgresql+pg8000://{db_data['user']}@{db_data['host']}:{db_data['port']}/{db_data['db']}",
                isolation_level="AUTOCOMMIT",
            )
        sqla_conn = self.engine.connect()
        self.conn = ConnectionProfiler(sqla_conn, test_run)
        if db_type == "materialize":
            sqla_conn.exec_driver_sql("SET SESSION statement_timeout = '6000s'")

    def create_table_like(self, new_relation: str, relation: str):
        col_list = self.get_idx_list(relation)
        sql_str = f"CREATE TABLE IF NOT EXISTS {new_relation} ({', '.join(col_list)})"
        self.conn.execute(Tag.COMPILER_INIT, sql_str)
        self.conn.commit()

    def init_programs(self, program: Program):
        self.nonrecursive_delta_program, self.recursive_delta_program = split_program(
            make_delta_program(program, True)
        )
        self.nonrecursive_delta_program = sort_program(self.nonrecursive_delta_program)

    def get_delta_fact_count(self):
        fact_count = 0
        for delta_relation in self.delta_relations:
            fact_count += get_table_row_count(self.con, delta_relation)
        return fact_count

    def materialize_nonrecursive_delta_program(self, nonrecursive_program: Program):
        for idx, rule in enumerate(nonrecursive_program):
            RuleEvaluator(self.conn, rule).step()
            delta_relation_symbol = rule.head.symbol
            # diff = list of newly evaluated facts that are NOT inside delta_relation
            # new_facts = select * from ddRelation
            # cur_facts = select * from dRelation
            eval_table = f"{DELTA_PREFIX}{delta_relation_symbol}"
            relation_symbol = delta_relation_symbol.strip(DELTA_PREFIX)
            self.conn.execute(
                Tag.MAT_NONREC,
                f"INSERT INTO {relation_symbol} SELECT * FROM  {eval_table} EXCEPT SELECT * FROM {delta_relation_symbol}",
            )
            self.conn.commit()
            if idx == 0:
                self.conn.execute(
                    Tag.MAT_NONREC,
                    f"ALTER TABLE {delta_relation_symbol} RENAME TO TEMP_{delta_relation_symbol}",
                )
                sql_str = f"CREATE TABLE {delta_relation_symbol} ({', '.join(self.get_idx_list(eval_table))})"
                self.conn.execute(Tag.MAT_NONREC, sql_str)
                self.conn.execute(
                    Tag.MAT_NONREC,
                    f"INSERT INTO {delta_relation_symbol} SELECT * FROM  {eval_table} EXCEPT SELECT * FROM TEMP_{delta_relation_symbol}",
                )
                self.conn.execute(
                    Tag.MAT_NONREC, f"DROP TABLE TEMP_{delta_relation_symbol}"
                )
                self.conn.commit()
            else:
                # insert all diff into delta table
                self.conn.execute(
                    Tag.MAT_NONREC,
                    f"INSERT INTO {delta_relation_symbol} SELECT * FROM  {eval_table} EXCEPT SELECT * FROM {delta_relation_symbol}",
                )
                self.conn.commit()

            # clear eval table
            self.conn.execute(Tag.MAT_NONREC, f"DELETE FROM {eval_table}")
            self.conn.commit()

    def materialize_recursive_delta_program(self, recursive_program: Program):
        eval_relations: set[Symbol] = set()
        for idx, rule in enumerate(recursive_program):
            RuleEvaluator(self.conn, rule).step()
            delta_relation_symbol = rule.head.symbol
            eval_relations.add(delta_relation_symbol)
            # diff = evaluated facts that are NOT in delta_relation
            # new_facts = select * from ddRelation
            # cur_facts = select * from dRelation
        for idx, delta_relation_symbol in enumerate(eval_relations):
            relation_symbol = delta_relation_symbol.strip(DELTA_PREFIX)
            eval_table = f"{DELTA_PREFIX}{delta_relation_symbol}"
            diff_table = f"DIFF_{eval_table}"
            sql_str = f"CREATE TABLE {diff_table} ({', '.join(self.get_idx_list(relation_symbol))})"
            self.conn.execute(Tag.MAT_REC, sql_str)
            self.conn.commit()
            # insert diff into real table
            self.conn.execute(
                Tag.MAT_REC,
                f"""INSERT INTO {diff_table} SELECT * FROM
                    (
                        SELECT * FROM {eval_table} AS _DERIVE_3
                            EXCEPT SELECT * FROM {delta_relation_symbol} AS _DERIVE_2
                    ) AS _DERIVE_4
                    EXCEPT SELECT * FROM {relation_symbol} AS _DERIVE_1
            """,
            )
            self.conn.execute(
                Tag.MAT_REC, f"INSERT INTO {relation_symbol} SELECT * FROM {diff_table}"
            )
            if idx == 0:
                self.conn.execute(Tag.MAT_REC, f"DROP TABLE {delta_relation_symbol}")
                self.conn.commit()
                sql_str = f"CREATE TABLE {delta_relation_symbol} ({', '.join(self.get_idx_list(relation_symbol))})"
                self.conn.execute(Tag.MAT_REC, sql_str)
                self.conn.commit()
                self.conn.execute(
                    Tag.MAT_REC,
                    f"INSERT INTO {delta_relation_symbol} SELECT * FROM  {diff_table}",
                )
            else:
                self.conn.execute(
                    Tag.MAT_REC,
                    f"INSERT INTO {delta_relation_symbol} SELECT * FROM {diff_table}",
                )
            # clear eval table
            self.conn.execute(
                Tag.MAT_REC, f"DELETE FROM {DELTA_PREFIX}{delta_relation_symbol}"
            )
            self.conn.execute(Tag.MAT_REC, f"DROP TABLE {diff_table}")
            self.conn.commit()

    def semi_naive_evaluation(
        self,
        nonrecursive_delta_program: Program,
        recursive_delta_program: Program,
    ):
        self.conn.increment_iter()
        self.materialize_nonrecursive_delta_program(nonrecursive_delta_program)
        while True:
            self.conn.increment_iter()
            prev_nondelta_facts = get_total_fact_count(self.conn, self.relations)
            self.materialize_recursive_delta_program(recursive_delta_program)
            cur_nondelta_facts = get_total_fact_count(self.conn, self.relations)
            new_facts = cur_nondelta_facts - prev_nondelta_facts
            if new_facts == 0:
                break

    def get_unprocessed_insertions(self) -> dict[str, list[Any]]:
        unprocessed_insertions: dict[str, list[Any]] = {}
        for relation in self.delta_relations:
            result = self.conn.execute(Tag.COMPILER_INIT, f"SELECT * FROM {relation}")
            unprocessed_insertions[relation] = list(result.fetchall())
        return unprocessed_insertions

    def drain_deltas(self):
        # Get all delta symbols
        # For each delta symbol,
        #   Drain delta relation
        #       Get all facts from delta relation
        #       Clear delta relation
        #   Get nondelta symbol
        #   Insert all facts into nondelta symbol
        for delta_relation in self.delta_relations:
            relation = delta_relation.strip(DELTA_PREFIX)
            self.conn.execute(
                Tag.DRAIN,
                f"INSERT INTO {relation} SELECT * FROM {delta_relation} EXCEPT SELECT * FROM {relation}",
            )
            self.conn.execute(Tag.DRAIN, f"DELETE FROM {delta_relation}")
            self.conn.commit()

    def poll(self):
        unprocessed_insertions = self.get_unprocessed_insertions()
        if len(unprocessed_insertions) > 0:
            # Additions
            # for each drain relation
            #  dump all unprocessed EDB relations into delta EDB relations
            #  And in their respective place
            for relation in self.relations:
                self.conn.execute(
                    Tag.COMPILER_INIT,
                    f"INSERT INTO {DELTA_PREFIX}{relation} SELECT * FROM {relation}",
                )
            self.conn.commit()
            # Evaluate
            self.semi_naive_evaluation(
                self.nonrecursive_delta_program, self.recursive_delta_program
            )
            self.drain_deltas()
        self.conn.close()
        self.engine.dispose()
