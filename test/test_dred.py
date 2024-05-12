import unittest

from datalog import Program, Rule
from dred import make_overdeletion_program, make_rederivation_program


class DredTest(unittest.TestCase):
    def test_make_overdeletion_program(self):
        program = Program(
            [
                Rule.create("tc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "tc", ["?x", "?z"], [("tc", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
            ]
        )

        expected_program = Program(
            [
                Rule.create("delete_tc", ["?x", "?y"], [("delete_e", ["?x", "?y"])]),
                Rule.create(
                    "delete_tc",
                    ["?x", "?z"],
                    [("delete_tc", ["?x", "?y"]), ("tc", ["?y", "?z"])],
                ),
                Rule.create(
                    "delete_tc",
                    ["?x", "?z"],
                    [("tc", ["?x", "?y"]), ("delete_tc", ["?y", "?z"])],
                ),
            ]
        )

        actual_program = make_overdeletion_program(program)

        self.assertEqual(str(actual_program), str(expected_program))

    def test_make_rederivation_program(self):
        program = Program(
            [
                Rule.create("tc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "tc", ["?x", "?z"], [("tc", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
            ]
        )

        expected_program = Program(
            [
                Rule.create(
                    "rederive_tc",
                    ["?x", "?y"],
                    [
                        ("delete_tc", ["?x", "?y"]),
                        ("e", ["?x", "?y"]),
                    ],
                ),
                Rule.create(
                    "rederive_tc",
                    ["?x", "?z"],
                    [
                        ("delete_tc", ["?x", "?z"]),
                        ("tc", ["?x", "?y"]),
                        ("tc", ["?y", "?z"]),
                    ],
                ),
            ]
        )

        actual_program = make_rederivation_program(program)

        self.assertEqual(str(actual_program), str(expected_program))
