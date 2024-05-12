import functools
from copy import deepcopy
from dataclasses import dataclass

from datalog import Rule, Symbol, Term, TermConstant, TermVariable, TypedValue, Variable


class ProjectionInput:
    pass


@dataclass
class ProjectionInputColumn(ProjectionInput):
    value: int


@dataclass
class ProjectionInputValue(ProjectionInput):
    value: TypedValue


class Instruction:
    pass


@dataclass
class Move(Instruction):
    symbol: Symbol


@dataclass
class Select(Instruction):
    symbol: Symbol
    column: int
    value: TypedValue


@dataclass
class Project(Instruction):
    symbol: Symbol
    projection_inputs: list[ProjectionInput]


@dataclass
class Join(Instruction):
    left_symbol: Symbol
    right_symbol: Symbol
    keys: list[tuple[int, int]]


def stringify_join(join: Join) -> Symbol:
    join_keys_list = []
    for left_column, right_column in join.keys:
        join_keys_list.append(f"{left_column}eq{right_column}")
    join_keys_format = functools.reduce(
        lambda x, y: f"{x}_{y}",
        join_keys_list,
    )
    return Symbol(f"{join.left_symbol}_{join.right_symbol}_{join_keys_format}")


def stringify_select(selection: Select) -> Symbol:
    return Symbol(f"{selection.symbol}_{selection.column}eq{selection.value}")


class Stack(list[Instruction]):
    def __init__(self, rule: Rule):
        rule = deepcopy(rule)
        i = 0
        last_join_result_name: Symbol = Symbol("")
        last_join_terms: list[Term] = []
        while i < len(rule.body):
            current_atom = rule.body[i]
            if i + 1 < len(rule.body):
                next_atom = rule.body[i + 1]
                left_symbol = current_atom.symbol
                left_terms = current_atom.terms
                right_symbol = next_atom.symbol
                right_terms = next_atom.terms
                if not last_join_result_name:
                    selection = self.get_selection(left_symbol, left_terms)
                    if selection:
                        left_symbol = stringify_select(selection)
                        self.append(selection)
                    else:
                        self.append(Move(left_symbol))
                else:
                    left_symbol = last_join_result_name
                    left_terms = last_join_terms
                selection = self.get_selection(right_symbol, right_terms)
                if selection:
                    right_symbol = stringify_select(selection)
                    self.append(selection)
                else:
                    self.append(Move(right_symbol))
                binary_join = self.get_join(
                    left_terms, right_terms, left_symbol, right_symbol
                )
                if binary_join:
                    last_join_result_name = stringify_join(binary_join)
                    last_join_terms = left_terms
                    last_join_terms.extend(right_terms)
                    self.append(binary_join)
            else:  # no next atom
                if not self:
                    self.append(Move(current_atom.symbol))
                # projection
                projection = self.get_projection(rule)
                self.append(projection)
            i += 1

    def get_selection(self, symbol: Symbol, terms: list[Term]) -> Select | None:
        selection: list[Select] = []
        for idx, t in enumerate(terms):
            if isinstance(t, TermConstant):
                selection.append(Select(symbol, idx, t.value))
        if selection:
            return selection[0]
        else:
            return None

    def get_variables(self, terms: list[Term]) -> dict[str, int]:
        variables = {}
        for idx, t in enumerate(terms):
            if isinstance(t, TermVariable):
                variables[t.name] = idx
        return variables

    def get_join(
        self,
        left_terms: list[Term],
        right_terms: list[Term],
        left_symbol: Symbol,
        right_symbol: Symbol,
    ) -> Join | None:
        left_variables_map = self.get_variables(left_terms)
        right_variables_map = self.get_variables(right_terms)

        join_keys: list[tuple[int, int]] = []

        for variable_name, left_position in left_variables_map.items():
            right_position = right_variables_map.get(variable_name)
            if right_position is not None:
                join_keys.append((left_position, right_position))
        if join_keys:
            return Join(left_symbol, right_symbol, join_keys)
        return None

    def get_projection(self, rule: Rule) -> Project:
        projection_variable_targets: set[str] = set()
        for t in rule.head.terms:
            if isinstance(t, TermVariable):
                projection_variable_targets.add(t.name)
        seen = set()
        variable_locations_assuming_joins_are_natural: dict[Variable, int] = {}
        position_assuming_joins_are_natural = 0

        for atom in rule.body:
            for t in atom.terms:
                if isinstance(t, TermVariable):
                    if t.name not in seen:
                        seen.add(t.name)
                        if t.name in projection_variable_targets:
                            variable_locations_assuming_joins_are_natural[
                                Variable(t.name)
                            ] = position_assuming_joins_are_natural
                position_assuming_joins_are_natural += 1

        projection: list[ProjectionInput] = []
        for t in rule.head.terms:
            if isinstance(t, TermVariable):
                projection.append(
                    ProjectionInputColumn(
                        variable_locations_assuming_joins_are_natural[Variable(t.name)]
                    )
                )
            elif isinstance(t, TermConstant):
                projection.append(ProjectionInputValue(t.value))

        return Project(rule.head.symbol, projection)
