import unittest

from datalog import Program, Rule
from delta_program import make_delta_program


class TestDeltaProgram(unittest.TestCase):
    def test_make_sne_program_nonlinear_update(self):
        program = Program(
            [
                Rule.create("tc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "tc", ["?x", "?z"], [("tc", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
            ]
        )
        delta_program = make_delta_program(program, True)
        expected_program = Program(
            [
                Rule.create("Δtc", ["?x", "?y"], [("Δe", ["?x", "?y"])]),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("Δtc", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("tc", ["?x", "?y"]), ("Δtc", ["?y", "?z"])]
                ),
            ]
        )
        self.assertEqual(str(delta_program), str(expected_program))

    def test_make_sne_program_nonlinear_initial(self):
        program = Program(
            [
                Rule.create("tc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "tc", ["?x", "?z"], [("tc", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
            ]
        )

        actual_program = make_delta_program(program, False)
        expected_program = Program(
            [
                Rule.create("Δtc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("Δtc", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("tc", ["?x", "?y"]), ("Δtc", ["?y", "?z"])]
                ),
            ]
        )

        self.assertEqual(str(actual_program), str(expected_program))

    def test_make_sne_program_linear_initial(self):
        program = Program(
            [
                Rule.create("tc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "tc", ["?x", "?z"], [("e", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
            ]
        )

        actual_program = make_delta_program(program, False)
        expected_program = Program(
            [
                Rule.create("Δtc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("e", ["?x", "?y"]), ("Δtc", ["?y", "?z"])]
                ),
            ]
        )

        self.assertEqual(str(actual_program), str(expected_program))

    def test_make_sne_program_linear_update(self):
        program = Program(
            [
                Rule.create("tc", ["?x", "?y"], [("e", ["?x", "?y"])]),
                Rule.create(
                    "tc", ["?x", "?z"], [("e", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
            ]
        )

        actual_program = make_delta_program(program, True)
        expected_program = Program(
            [
                Rule.create("Δtc", ["?x", "?y"], [("Δe", ["?x", "?y"])]),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("Δe", ["?x", "?y"]), ("tc", ["?y", "?z"])]
                ),
                Rule.create(
                    "Δtc", ["?x", "?z"], [("e", ["?x", "?y"]), ("Δtc", ["?y", "?z"])]
                ),
            ]
        )

        self.assertEqual(str(actual_program), str(expected_program))
