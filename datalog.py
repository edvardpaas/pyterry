from dataclasses import dataclass
from typing import Any, NewType

type TypedValue = str | bool | int | float  # type: ignore


Symbol = NewType("Symbol", str)


class Term:
    pass


@dataclass
class TermVariable(Term):
    name: str

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __hash__(self):
        return hash(f"?{self.name}")

    def serialize(self):
        return f"?{self.name}"


@dataclass
class TermConstant(Term):
    value: TypedValue

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return str(self.value)

    def __hash__(self):
        return hash(self.value)

    def serialize(self):
        return str(self.value)


class Atom:
    def __init__(
        self,
        symbol: str,
        terms: list[Term],
    ) -> None:
        self.terms = terms
        self.symbol = Symbol(symbol)

    def __str__(self):
        return f"Atom({self.symbol}, {self.terms})"

    def __repr__(self):
        return f"Atom({self.symbol}, {self.terms})"

    def __hash__(self):
        return hash(tuple([self.symbol, tuple(self.terms)]))

    def serialize(self):
        terms = [term.serialize() for term in self.terms]
        return f"{self.symbol}({', '.join(terms)})"


class Rule:
    def __init__(self, head: Atom, body: list[Atom] = list()) -> None:
        self.head = head
        self.body = body
        self.id = 0

    def __str__(self):
        return f"Rule({self.head}, {self.body})"

    def __repr__(self):
        return f"Rule({self.head}, {self.body})"

    def __hash__(self):
        return hash(tuple([self.head, tuple(self.body)]))

    def serialize(self):
        atoms = [atom.serialize() for atom in self.body]
        return f"{self.head.serialize()} :- {', '.join(atoms)}"

    @classmethod
    def create(
        cls, head_symbol: str, head: list[Any], body: list[tuple[str, list[Any]]]
    ):
        head_atom = create_atom(head_symbol, head)
        body_atoms: list[Atom] = []
        for symbol, body_vals in body:
            body_atoms.append(create_atom(symbol, body_vals))
        return Rule(head_atom, body_atoms)


def create_atom(symbol: str, values: list[Any]) -> Atom:
    terms: list[Term] = []
    for val in values:
        if isinstance(val, str):
            if val.startswith("?"):
                terms.append(TermVariable(val[1:]))
                continue
        terms.append(TermConstant(val))
    return Atom(symbol, terms)


class Program(list[Rule]):
    pass

    def __init__(self, rules: list[Rule]):
        super().__init__(rules)
        self.sort(key=lambda r: str(r))
        for idx, rule in enumerate(self):
            rule.id = idx


Variable = NewType("Variable", str)
