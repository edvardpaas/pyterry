import unittest

from datalog import Atom, Symbol, TermConstant, TermVariable
from evaluator import (
    Join,
    Move,
    Project,
    ProjectionInputColumn,
    ProjectionInputValue,
    Rule,
    Select,
    Stack,
)


class TestStack(unittest.TestCase):
    def test_from_unary_rule_into_stack(self):
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
        stack = Stack(rule)
        expected_stack = [
            Move(Symbol("E")),
            Project(Symbol("T"), [ProjectionInputColumn(0), ProjectionInputColumn(1)]),
        ]
        self.assertEqual(stack, expected_stack)

    def test_from_simple_binary_rule_into_stack(self):
        rule = Rule(
            head=Atom(
                terms=[TermVariable("x"), TermVariable("z")],
                symbol="T",
            ),
            body=[
                Atom(terms=[TermVariable("x"), TermVariable("y")], symbol="T"),
                Atom(terms=[TermVariable("y"), TermVariable("z")], symbol="T"),
            ],
        )
        expected_stack = [
            Move(Symbol("T")),
            Move(Symbol("T")),
            Join(Symbol("T"), Symbol("T"), [(1, 0)]),
            Project(Symbol("T"), [ProjectionInputColumn(0), ProjectionInputColumn(3)]),
        ]
        stack = Stack(rule)
        self.assertEqual(stack, expected_stack)

    def test_from_binary_rule_into_stack(self):
        # T(y, 0, x) <- T(x, 2, y), T(y, 2, z)
        rule = Rule(
            head=Atom(
                terms=[TermVariable("y"), TermConstant(0), TermVariable("x")],
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
        expected_stack = [
            Select(Symbol("T"), 1, 2),
            Select(Symbol("T"), 1, 2),
            Join(Symbol("T_1=2"), Symbol("T_1=2"), [(2, 0)]),
            Project(
                Symbol("T"),
                [
                    ProjectionInputColumn(2),
                    ProjectionInputValue(0),
                    ProjectionInputColumn(0),
                ],
            ),
        ]
        stack = Stack(rule)
        self.assertEqual(stack, expected_stack)

    def test_from_ternary_rule_into_operations(self):
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
        expected_stack = [
            Select(Symbol("T"), 1, 2),
            Select(Symbol("T"), 1, 2),
            Join(Symbol("T_1=2"), Symbol("T_1=2"), [(2, 0)]),
            Select(Symbol("T"), 0, 3),
            Join(Symbol("T_1=2_T_1=2_2=0"), Symbol("T_0=3"), [(5, 1)]),
            Project(
                Symbol("T"),
                [
                    ProjectionInputColumn(2),
                    ProjectionInputValue(0),
                    ProjectionInputColumn(8),
                ],
            ),
        ]
        self.assertEqual(expected_stack, Stack(rule))
