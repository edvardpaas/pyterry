import os
import time
import unittest
from typing import Final

import sqlalchemy
from sqlalchemy import Connection, text

from compiler import Compiler
from datalog import Atom, Program, Rule, TermConstant, TermVariable
from dotenv import load_dotenv

load_dotenv()


def get_or_intern(mapping: dict[str, int], value: str):
    if value not in mapping:
        mapping[value] = len(mapping)
    return mapping[value]


class TestCompiler(unittest.TestCase):

    def setup_connection(self, db_path: str) -> Connection:
        # Reset database
        try:
            os.remove(db_path)
        except FileNotFoundError:
            pass
        # Establish connection
        engine = sqlalchemy.create_engine(f"duckdb:///{db_path}")
        conn = engine.connect()
        return conn

    def test_unary_rule(self):
        db_name = "test/data/test_unary_rule.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            """CREATE TABLE E (
                E_0 INTEGER,
                E_1 INTEGER
            )""",
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )""",
            "INSERT INTO E (E_0, E_1) VALUES (1, 2)",
            "INSERT INTO E (E_0, E_1) VALUES (1, 3)",
            "INSERT INTO E (E_0, E_1) VALUES (2, 4)",
            "INSERT INTO E (E_0, E_1) VALUES (3, 5)",
            "INSERT INTO E (E_0, E_1) VALUES (5, 7)",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()
        # T(x, y) <- E(x, y)
        rule = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="E",
                ),
            ],
        )
        compiler = Compiler("duckdb", {"db": db_name}, Program([rule]))
        compiler.poll()
        expected_output = set([(1, 2), (1, 3), (2, 4), (3, 5), (5, 7)])
        r = conn.execute(text("SELECT * FROM T"))
        output = r.fetchall()
        assert expected_output, set(output)
        conn.close()

    def test_simple_binary_rule(self):
        db_name = "test/data/test_simple_binary_rule.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )""",
            "INSERT INTO T (T_0, T_1) VALUES (1, 2)",
            "INSERT INTO T (T_0, T_1) VALUES (2, 3)",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()
        program = Program(
            [
                Rule(
                    head=Atom(
                        terms=[TermVariable("x"), TermVariable("z")],
                        symbol="T",
                    ),
                    body=[
                        Atom(terms=[TermVariable("x"), TermVariable("y")], symbol="T"),
                        Atom(terms=[TermVariable("y"), TermVariable("z")], symbol="T"),
                    ],
                )
            ]
        )
        compiler = Compiler("duckdb", {"db": db_name}, program)
        compiler.poll()
        expected_output = set([(1, 2), (2, 3), (1, 3)])
        r = conn.execute(text("SELECT * FROM T"))
        output = r.fetchall()
        self.assertEqual(expected_output, set(output))
        conn.close()

    def test_tc_multi_relation(self):
        rule1 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="E",
                ),
            ],
        )
        rule2 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("z")],
                    symbol="T",
                ),
                Atom(
                    terms=[TermVariable("z"), TermVariable("y")],
                    symbol="E",
                ),
            ],
        )
        program = Program([])
        program.append(rule1)
        program.append(rule2)
        db_name = "test/data/test_tc_multi_relation.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )""",
            """CREATE TABLE E (
                E_0 INTEGER,
                E_1 INTEGER
            )""",
            "INSERT INTO E (E_0, E_1) VALUES (1, 2)",
            "INSERT INTO E (E_0, E_1) VALUES (1, 3)",
            "INSERT INTO E (E_0, E_1) VALUES (2, 4)",
            "INSERT INTO E (E_0, E_1) VALUES (3, 5)",
            "INSERT INTO E (E_0, E_1) VALUES (5, 7)",
            "INSERT INTO E (E_0, E_1) VALUES (7, 8)",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()

        compiler = Compiler("duckdb", {"db": db_name}, program)
        compiler.poll()
        expected = {
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 5),
            (5, 7),
            (1, 4),
            (1, 5),
            (3, 7),
            (1, 7),
            (1, 8),
            (3, 8),
            (5, 8),
            (7, 8),
        }
        r = conn.execute(text("SELECT * FROM T"))
        output = r.fetchall()
        self.assertEqual(expected, set(output))
        conn.close()

    def test_binary_rule_constants(self):
        db_name = "test/data/test_binary_rule_constants.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            "DROP TABLE IF EXISTS T",
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER,
                T_2 INTEGER
            )""",
            "INSERT INTO T (T_0, T_1, T_2) VALUES (10, 2, 20)",
            "INSERT INTO T (T_0, T_1, T_2) VALUES (20, 2, 30)",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()

        # T(y, 0, x) <- T(x, 2, y), T(y, 2, z)
        rule = Rule(
            head=Atom(
                terms=[
                    TermVariable("y"),
                    TermConstant(0),
                    TermVariable("x"),
                ],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[
                        TermVariable("x"),
                        TermConstant(2),
                        TermVariable("y"),
                    ],
                    symbol="T",
                ),
                Atom(
                    terms=[
                        TermVariable("y"),
                        TermConstant(2),
                        TermVariable("z"),
                    ],
                    symbol="T",
                ),
            ],
        )
        compiler = Compiler("duckdb", {"db": db_name}, Program([rule]))
        compiler.poll()
        expected_output = {(10, 2, 20), (20, 2, 30), (20, 0, 10)}
        r = conn.execute(text("SELECT * FROM T"))
        output = r.fetchall()
        self.assertEqual(expected_output, set(output))
        conn.close()

    def test_ternary_rule(self):
        # Setup Database
        db_name = "test/data/test_ternary_rule.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER,
                T_2 INTEGER
            )""",
            "INSERT INTO T (T_0, T_1, T_2) VALUES (1, 2, 4)",
            "INSERT INTO T (T_0, T_1, T_2) VALUES (4, 2, 5)",
            "INSERT INTO T (T_0, T_1, T_2) VALUES (3, 5, 6)",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()

        # T(y, 0, w) <- T(x, 2, y), T(y, 2, z), T(3, z, w)
        rule = Rule(
            head=Atom(
                terms=[TermVariable("y"), TermConstant(0), TermVariable("w")],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[
                        TermVariable("x"),
                        TermConstant(2),
                        TermVariable("y"),
                    ],
                    symbol="T",
                ),
                Atom(
                    terms=[
                        TermVariable("y"),
                        TermConstant(2),
                        TermVariable("z"),
                    ],
                    symbol="T",
                ),
                Atom(
                    terms=[
                        TermConstant(3),
                        TermVariable("z"),
                        TermVariable("w"),
                    ],
                    symbol="T",
                ),
            ],
        )
        program = Program([rule])
        compiler = Compiler("duckdb", {"db": db_name}, program)
        compiler.poll()
        expected_output = {(1, 2, 4), (4, 2, 5), (3, 5, 6), (4, 0, 6)}
        r = conn.execute(text("SELECT * FROM T"))
        output = r.fetchall()
        self.assertEqual(expected_output, set(output))
        conn.close()

    def test_rdf(self) -> None:
        TYPE: Final[str] = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        SUB_CLASS_OF: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
        SUB_PROPERTY_OF: Final[str] = (
            "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>"
        )
        DOMAIN: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#domain>"
        RANGE: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#range>"
        PROPERTY: Final[str] = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>"
        PREFIX: Final[str] = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"
        mapping: dict[str, int] = {}
        get_or_intern(mapping, TYPE)
        get_or_intern(mapping, SUB_CLASS_OF)
        get_or_intern(mapping, SUB_PROPERTY_OF)
        get_or_intern(mapping, DOMAIN)
        get_or_intern(mapping, RANGE)
        get_or_intern(mapping, PROPERTY)
        get_or_intern(mapping, PREFIX)

        db_name = "test/data/lubm1.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            """
            CREATE TABLE RDF (
                RDF_0 INTEGER,
                RDF_1 INTEGER,
                RDF_2 INTEGER
            )
        """,
            """
            CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER,
                T_2 INTEGER
            )
        """,
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()

        program = Program(
            [
                Rule.create("T", ["?s", "?p", "?o"], [("RDF", ["?s", "?p", "?o"])]),
                Rule.create(
                    "T",
                    ["?y", 0, "?x"],
                    [("T", ["?a", 3, "?x"]), ("T", ["?y", "?a", "?z"])],
                ),
                Rule.create(
                    "T",
                    ["?z", 0, "?x"],
                    [("T", ["?a", 4, "?x"]), ("T", ["?y", "?a", "?z"])],
                ),
                Rule.create(
                    "T",
                    ["?x", 2, "?z"],
                    [("T", ["?x", 2, "?y"]), ("T", ["?y", 2, "?z"])],
                ),
                Rule.create(
                    "T",
                    ["?x", 1, "?z"],
                    [("T", ["?x", 1, "?y"]), ("T", ["?y", 1, "?z"])],
                ),
                Rule.create(
                    "T",
                    ["?z", 0, "?y"],
                    [("T", ["?x", 1, "?y"]), ("T", ["?z", 0, "?x"])],
                ),
                Rule.create(
                    "T",
                    ["?x", "?b", "?y"],
                    [("T", ["?a", 2, "?b"]), ("T", ["?x", "?a", "?y"])],
                ),
            ]
        )

        compiler = Compiler("duckdb", {"db": db_name}, program)

        with open("test/data/lubm1.nt", "r") as file:
            data = file.readlines()

        # ensure no duplicate input
        input_set = set()

        for line in data:
            if "genid" not in line:
                if line not in input_set:
                    input_set.add(line)
                else:
                    continue
                triple = line.split()
                s = get_or_intern(mapping, triple[0])
                p = get_or_intern(mapping, triple[1])
                o = get_or_intern(mapping, triple[2])
                conn.execute(
                    text(
                        f"INSERT INTO RDF (RDF_0, RDF_1, RDF_2) VALUES ({s}, {p}, {o})"
                    )
                )
        conn.commit()

        t1 = time.perf_counter()
        compiler.poll()
        t2 = time.perf_counter()
        print(f"time elapsed: {t2 - t1}")
        r = conn.execute(text("SELECT COUNT(*) FROM T"))
        self.assertEqual(124518, list(r.first())[0])  # type: ignore
        conn.close()

        # pr.dump_stats("program.prof")

    def test_dense(self):
        rule = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("z")],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T",
                ),
                Atom(
                    terms=[TermVariable("y"), TermVariable("z")],
                    symbol="T",
                ),
            ],
        )
        program = Program([])
        program.append(rule)
        db_name = "test/data/test_dense.db"
        conn = self.setup_connection(db_name)

        init_queries = [
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )""",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()

        # compiler = Compiler(con, program)
        compiler = Compiler("duckdb", {"db": db_name}, program)

        with open("test/data/dense.txt", "r") as file:
            data = file.readlines()

        # ensure no duplicate input
        input_set = set()

        for line in data:
            if line not in input_set:
                input_set.add(line)
            else:
                continue
            triple = line.split()
            conn.execute(
                text(f"INSERT INTO T (T_0, T_1) VALUES ({triple[0]}, {triple[1]})")
            )
        conn.commit()
        compiler.poll()
        r = conn.execute(text("SELECT COUNT(*) FROM T"))
        output = list(r.first())[0]
        self.assertEqual(output, 11532)
        conn.close()

    def test_sparse(self):
        rule = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("z")],
                symbol="T",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T",
                ),
                Atom(
                    terms=[TermVariable("y"), TermVariable("z")],
                    symbol="T",
                ),
            ],
        )
        program = Program([])
        program.append(rule)
        db_name = "test/data/test_sparse.db"
        conn = self.setup_connection(db_name)
        init_queries = [
            "DROP TABLE IF EXISTS T",
            """CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )""",
        ]
        for query in init_queries:
            conn.execute(text(query))
        conn.commit()

        compiler = Compiler("duckdb", {"db": db_name}, program)

        with open("test/data/sparse.txt", "r") as file:
            data = file.readlines()

        # ensure no duplicate input
        input_set = set()

        for line in data:
            if line not in input_set:
                input_set.add(line)
            else:
                continue
            triple = line.split()
            conn.execute(
                text(f"INSERT INTO T (T_0, T_1) VALUES ({triple[0]}, {triple[1]})")
            )
        conn.commit()
        compiler.poll()
        r = conn.execute(text("SELECT COUNT(*) FROM T"))
        output = list(r.first())[0]
        self.assertEqual(output, 262144)
        conn.close()
