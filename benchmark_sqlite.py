import json
import os
import time
from typing import Final
from sqlalchemy import text
import sqlalchemy

from compiler import Compiler
from datalog import Program, Rule


def setup_database(db_path: str, input_path: str) -> None:
    # Establish connection
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    conn = engine.connect()

    init_queries = [
        """CREATE TABLE E (
            E_0 INTEGER,
            E_1 INTEGER
        )""",
        """CREATE TABLE T (
            T_0 INTEGER,
            T_1 INTEGER
        )""",
    ]
    for query in init_queries:
        conn.execute(text(query))
    conn.commit()

    with open(input_path) as file:
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
            text(f"INSERT INTO E (E_0, E_1) VALUES ({triple[0]}, {triple[1]})")
        )
    conn.commit()
    conn.close()
    engine.dispose()


def get_or_intern(mapping: dict[str, int], value: str):
    if value not in mapping:
        mapping[value] = len(mapping)
    return mapping[value]


def setup_database_rdf(db_path: str, input_path: str) -> None:
    # Establish connection
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    conn = engine.connect()

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

    with open(input_path, "r") as file:
        data = file.readlines()
    # ensure no duplicate input
    input_set = set()
    mapping: dict[str, int] = {}
    TYPE: Final[str] = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    SUB_CLASS_OF: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
    SUB_PROPERTY_OF: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>"
    DOMAIN: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#domain>"
    RANGE: Final[str] = "<http://www.w3.org/2000/01/rdf-schema#range>"
    PROPERTY: Final[str] = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>"
    PREFIX: Final[str] = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#"
    get_or_intern(mapping, TYPE)
    get_or_intern(mapping, SUB_CLASS_OF)
    get_or_intern(mapping, SUB_PROPERTY_OF)
    get_or_intern(mapping, DOMAIN)
    get_or_intern(mapping, RANGE)
    get_or_intern(mapping, PROPERTY)
    get_or_intern(mapping, PREFIX)
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
                text(f"INSERT INTO RDF (RDF_0, RDF_1, RDF_2) VALUES ({s}, {p}, {o})")
            )
    conn.commit()
    conn.close()
    engine.dispose()


class SqliteBenchmark:

    def reset_db(self, path: str):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def run_dense(self, iters: int) -> None:
        db_name = "test/data/sqlite/test_dense.db"
        input_path = "test/data/dense.txt"
        program = Program(
            [
                Rule.create("T", ["?x", "?y"], [("E", ["?x", "?y"])]),
                Rule.create(
                    "T", ["?x", "?z"], [("T", ["?x", "?y"]), ("E", ["?y", "?z"])]
                ),
            ]
        )

        data: list[list[tuple[int, int, str, int, str]]] = []
        poll_times: list[float] = []
        for i in range(1, iters + 1):
            self.reset_db(db_name)
            setup_database(db_name, input_path)
            compiler = Compiler("sqlite", {"db": db_name}, program, i)
            t1 = time.perf_counter()
            compiler.poll()
            t2 = time.perf_counter()
            data.append(compiler.dump_benchmark())
            poll_times.append(int(round((t2 - t1) * 1000)))

        with open("test/data/sqlite/dense.json", "w") as f:
            json.dump(data, f)

        with open("test/data/sqlite/dense_time.json", "w") as f:
            json.dump(poll_times, f)

    def run_sparse(self, iters: int) -> None:
        db_name = "test/data/sqlite/test_sparse.db"
        input_path = "test/data/sparse.txt"
        program = Program(
            [
                Rule.create("T", ["?x", "?y"], [("E", ["?x", "?y"])]),
                Rule.create(
                    "T", ["?x", "?z"], [("T", ["?x", "?y"]), ("E", ["?y", "?z"])]
                ),
            ]
        )

        data: list[list[tuple[int, int, str, int, str]]] = []
        poll_times: list[float] = []
        for i in range(1, iters + 1):
            self.reset_db(db_name)
            setup_database(db_name, input_path)
            compiler = Compiler("sqlite", {"db": db_name}, program, i)
            t1 = time.perf_counter()
            compiler.poll()
            t2 = time.perf_counter()
            data.append(compiler.dump_benchmark())
            poll_times.append(int(round((t2 - t1) * 1000)))

        with open("test/data/sqlite/sparse.json", "w") as f:
            json.dump(data, f)

        with open("test/data/sqlite/sparse_time.json", "w") as f:
            json.dump(poll_times, f)

    def run_rdf(self, iters: int) -> None:
        db_name = "test/data/sqlite/test_rdf.db"
        input_path = "test/data/lubm1.nt"
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

        data: list[list[tuple[int, int, str, int, str]]] = []
        poll_times: list[float] = []
        for i in range(1, iters + 1):
            self.reset_db(db_name)
            setup_database_rdf(db_name, input_path)
            compiler = Compiler("sqlite", {"db": db_name}, program, i)
            t1 = time.perf_counter()
            compiler.poll()
            t2 = time.perf_counter()
            data.append(compiler.dump_benchmark())
            poll_times.append(int(round((t2 - t1) * 1000)))

        with open("test/data/sqlite/rdf.json", "w") as f:
            json.dump(data, f)

        with open("test/data/sqlite/rdf_time.json", "w") as f:
            json.dump(poll_times, f)


def main():
    SqliteBenchmark().run_dense(100)
    SqliteBenchmark().run_sparse(100)
    SqliteBenchmark().run_rdf(100)


if __name__ == "__main__":
    main()
