import unittest

from datalog import Atom, Program, Rule, TermVariable
from helpers import split_program


class TestProgramSplit(unittest.TestCase):
    def test_tc_split(self):
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
        nonrecursive, recursive = split_program(program)
        # tc dg
        expected_recursive = Program(
            [
                Rule(
                    Atom("T", [TermVariable("x"), TermVariable("y")]),
                    [
                        Atom("T", [TermVariable("x"), TermVariable("z")]),
                        Atom("E", [TermVariable("z"), TermVariable("y")]),
                    ],
                ),
            ]
        )
        expected_nonrecursive = Program(
            [
                Rule(
                    Atom("T", [TermVariable("x"), TermVariable("y")]),
                    [Atom("E", [TermVariable("x"), TermVariable("y")])],
                ),
            ]
        )
        # print(f"sorted_program = {sorted_program}")
        self.assertEqual(str(expected_recursive), str(recursive))
        self.assertEqual(str(expected_nonrecursive), str(nonrecursive))
