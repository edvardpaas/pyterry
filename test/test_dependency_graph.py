import unittest
from copy import deepcopy

from datalog import Atom, Program, Rule, TermVariable
from dependency_graph import sort_program


class TestDependencyGraph(unittest.TestCase):
    def test_tc_dg(self):
        rule1 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_1",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_0",
                ),
            ],
        )
        rule2 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_2",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_1",
                ),
            ],
        )
        rule3 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_3",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_2",
                ),
            ],
        )
        rule4 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_4",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_3",
                ),
            ],
        )
        rule5 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_5",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_4",
                ),
            ],
        )
        rule6 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_6",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_4",
                ),
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_5",
                ),
            ],
        )
        rule7 = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("y")],
                symbol="T_7",
            ),
            body=[
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_5",
                ),
                Atom(
                    terms=[TermVariable("x"), TermVariable("y")],
                    symbol="T_7",
                ),
            ],
        )
        rules_sort = [
            deepcopy(rule7),
            deepcopy(rule4),
            deepcopy(rule2),
            deepcopy(rule1),
            deepcopy(rule3),
            deepcopy(rule6),
            deepcopy(rule5),
        ]
        program = Program([])
        for rule in rules_sort:
            program.append(rule)
        sorted_program = sort_program(program)
        # tc dg
        expected_program = Program([])
        rules = [
            deepcopy(rule1),
            deepcopy(rule2),
            deepcopy(rule3),
            deepcopy(rule4),
            deepcopy(rule5),
            deepcopy(rule7),
            deepcopy(rule6),
        ]
        for rule in rules:
            expected_program.append(rule)
        # print(f"sorted_program = {sorted_program}")
        self.assertEqual(str(expected_program), str(sorted_program))
