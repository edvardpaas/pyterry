import time
from typing import Final

import sqlalchemy
from sqlalchemy import text

from compiler import Compiler
from datalog import Atom, Program, Rule, TermConstant, TermVariable
from delta_program import DELTA_PREFIX


def get_or_intern(mapping: dict[str, int], value: str):
    if value not in mapping:
        mapping[value] = len(mapping)
    return mapping[value]


class TestMaterialize():

    def clear_db(self, program: Program) -> None:
        drop_tables: list[str] = []
        for rule in program:
            drop_tables.append(str(rule.head.symbol).strip(DELTA_PREFIX))
            for atom in rule.body:
                drop_tables.append(str(atom.symbol).strip(DELTA_PREFIX))
                for term in atom.terms:
                    if isinstance(term, TermVariable):
                        drop_tables.append(term.name.strip(DELTA_PREFIX))
        delta_drop_tables: list[str] = []
        for tbl in drop_tables:
            delta_drop_tables.append(f'temp_{tbl}')
            delta_drop_tables.append(f'{DELTA_PREFIX}{tbl}')
            delta_drop_tables.append(f'temp_{DELTA_PREFIX}{tbl}')
            delta_drop_tables.append(f'{DELTA_PREFIX}{DELTA_PREFIX}{tbl}')
            delta_drop_tables.append(f'temp_{DELTA_PREFIX}{DELTA_PREFIX}{tbl}')
        drop_tables.extend(delta_drop_tables)
        for tbl in drop_tables:
            self.conn.execute(text(f'DROP TABLE IF EXISTS materialize.public.{tbl}'))
            self.conn.commit()

    def setup_method(self, method) -> None:
        self.db_data = {
            "user": "materialize",
            # "password": "postgres",
            "host": "localhost",
            "port": 6875,
            "db": "materialize"
        }
        self.engine = sqlalchemy.create_engine(
            f"postgresql+pg8000://{self.db_data["user"]}@{self.db_data["host"]}:{self.db_data["port"]}/{self.db_data["db"]}", client_encoding='utf8', isolation_level="READ COMMITTED"
        )
        self.conn = self.engine.connect()

    def teardown_method(self, method) -> None:
        self.conn.close()

    def test_unary_rule(self):
        # T(x, y) <- E(x, y)
        program = Program([Rule(
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
        )])
        self.clear_db(program)
        init_queries = [
            '''CREATE TABLE E (
                E_0 INTEGER,
                E_1 INTEGER
            )''',
            '''CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )''',
            'INSERT INTO E (E_0, E_1) VALUES (1, 2)',
            'INSERT INTO E (E_0, E_1) VALUES (1, 3)',
            'INSERT INTO E (E_0, E_1) VALUES (2, 4)',
            'INSERT INTO E (E_0, E_1) VALUES (3, 5)',
            'INSERT INTO E (E_0, E_1) VALUES (5, 7)',
        ]
        for query in init_queries:
            self.conn.execute(text(query))
            self.conn.commit()
        compiler = Compiler("materialize", self.db_data, program)
        compiler.poll()
        expected_output = set([(1, 2), (1, 3), (2, 4), (3, 5), (5, 7)])
        r = self.conn.execute(text('SELECT * FROM T'))
        output = r.fetchall()
        assert expected_output, set(output)

    def test_simple_binary_rule(self):
        program = Program([Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("z")],
                symbol="T",
            ),
            body=[
                Atom(terms=[TermVariable("x"), TermVariable("y")], symbol="T"),
                Atom(terms=[TermVariable("y"), TermVariable("z")], symbol="T"),
            ],
        )])
        self.clear_db(program)
        init_queries = [
            '''CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )''',
            'INSERT INTO T (T_0, T_1) VALUES (1, 2)',
            'INSERT INTO T (T_0, T_1) VALUES (2, 3)',
        ]
        for query in init_queries:
            self.conn.execute(text(query))
            self.conn.commit()
        compiler = Compiler("materialize", self.db_data, program)
        compiler.poll()
        expected_output = set([(1, 2), (2, 3), (1, 3)])
        r = self.conn.execute(text('SELECT * FROM T'))
        output = r.fetchall()
        assert expected_output, set(output)

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
        program = Program([rule1, rule2])
        self.clear_db(program)
        init_queries = [
            '''CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER
            )''',
            '''CREATE TABLE E (
                E_0 INTEGER,
                E_1 INTEGER
            )''',
            'INSERT INTO E (E_0, E_1) VALUES (1, 2)',
            'INSERT INTO E (E_0, E_1) VALUES (1, 3)',
            'INSERT INTO E (E_0, E_1) VALUES (2, 4)',
            'INSERT INTO E (E_0, E_1) VALUES (3, 5)',
            'INSERT INTO E (E_0, E_1) VALUES (5, 7)',
            'INSERT INTO E (E_0, E_1) VALUES (7, 8)',
        ]
        for query in init_queries:
            self.conn.execute(text(query))
            self.conn.commit()

        compiler = Compiler("materialize", self.db_data, program)
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
        r = self.conn.execute(text('SELECT * FROM T'))
        output = r.fetchall()
        assert expected, set(output)

    def test_binary_rule_constants(self):
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
        program = Program([rule])
        self.clear_db(program)

        init_queries = [
            '''CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER,
                T_2 INTEGER
            )''',
            'INSERT INTO T (T_0, T_1, T_2) VALUES (10, 2, 20)',
            'INSERT INTO T (T_0, T_1, T_2) VALUES (20, 2, 30)',
        ]
        for query in init_queries:
            self.conn.execute(text(query))
            self.conn.commit()
        compiler = Compiler(
            "materialize",
            self.db_data,
            program,
        )
        compiler.poll()
        expected_output = {(10, 2, 20), (20, 2, 30), (20, 0, 10)}
        result = self.conn.execute(text('SELECT * FROM T'))
        assert expected_output, set(result.fetchall())

    def test_ternary_rule(self):
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
        self.clear_db(program)

        init_queries = [
            '''CREATE TABLE T (
                T_0 INTEGER,
                T_1 INTEGER,
                T_2 INTEGER
            )''',
            'INSERT INTO T (T_0, T_1, T_2) VALUES (1, 2, 4)',
            'INSERT INTO T (T_0, T_1, T_2) VALUES (4, 2, 5)',
            'INSERT INTO T (T_0, T_1, T_2) VALUES (3, 5, 6)',
        ]
        for query in init_queries:
            self.conn.execute(text(query))
            self.conn.commit()

        compiler = Compiler(
            "materialize",
            self.db_data,
            program,
        )
        compiler.poll()
        expected_output = {(1, 2, 4), (4, 2, 5), (3, 5, 6), (4, 0, 6)}
        result = self.conn.execute(text('SELECT * FROM T'))
        assert expected_output, set(result.fetchall())

    def test_rdf(self) -> None:
        # pr = cProfile.Profile()
        # pr.enable()

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
        self.clear_db(program)

        self.conn.execute(text('''CREATE TABLE T (
            T_0 INTEGER,
            T_1 INTEGER,
            T_2 INTEGER
        )'''))
        self.conn.commit()
        self.conn.execute(text('''CREATE TABLE RDF (
            RDF_0 INTEGER,
            RDF_1 INTEGER,
            RDF_2 INTEGER
        )'''))
        self.conn.commit()
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
                print(f'INSERT INTO RDF (RDF_0, RDF_1, RDF_2) VALUES ({s}, {p}, {o})')
                self.conn.execute(sqlalchemy.text(
                    f'INSERT INTO RDF (RDF_0, RDF_1, RDF_2) VALUES ({s}, {p}, {o})')
                )
        self.conn.commit()

        compiler = Compiler(
            "materialize",
            self.db_data,
            program,
        )
        t1 = time.perf_counter()
        compiler.poll()
        t2 = time.perf_counter()
        print(f"time elapsed: {t2 - t1}")
        result = self.conn.execute(text('SELECT COUNT(*) FROM T'))
        assert 124518, list(result.first())[0]  # type: ignore
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
        program = Program([rule])
        self.clear_db(program)
        self.conn.execute(text('''CREATE TABLE T (
            T_0 INTEGER,
            T_1 INTEGER
        )'''))
        self.conn.commit()
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
            self.conn.execute(text(f'INSERT INTO T (T_0, T_1) VALUES ({triple[0]}, {triple[1]})'))
        self.conn.commit()

        compiler = Compiler(
            "materialize",
            self.db_data,
            program,
        )
        compiler.poll()
        result = self.conn.execute(text('SELECT COUNT(*) FROM T'))
        assert list(result.first())[0], 11532

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
        program = Program([rule])
        self.clear_db(program)
        self.conn.execute(text('''CREATE TABLE T (
            T_0 INTEGER,
            T_1 INTEGER
        )'''))
        self.conn.commit()
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
            self.conn.execute(text(f'INSERT INTO T (T_0, T_1) VALUES ({triple[0]}, {triple[1]})'))
        self.conn.commit()

        compiler = Compiler(
            "materialize",
            self.db_data,
            program,
        )
        compiler.poll()
        result = self.conn.execute(text('SELECT COUNT(*) FROM T'))
        assert list(result.first())[0], 262144
