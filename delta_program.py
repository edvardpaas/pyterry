from copy import deepcopy
from typing import Final

from datalog import Program, Symbol

DELTA_PREFIX: Final[str] = "d"


def make_delta_program(program: Program, update: bool) -> Program:
    idb_relation_symbols = set()
    for rule in program:
        idb_relation_symbols.add(rule.head.symbol)

    delta_rules_set = set()

    for rule in program:
        delta_rule = deepcopy(rule)
        delta_rule.head.symbol = Symbol(f"{DELTA_PREFIX}{delta_rule.head.symbol}")

        contains_idb = False
        for atom_body in rule.body:
            if atom_body.symbol in idb_relation_symbols:
                contains_idb = True

        if not contains_idb and not update:
            # If the body does not contain any IDB relation symbols
            # and it's not an update phase,
            # add the delta rule directly to the set.
            delta_rules_set.add(delta_rule)
        else:
            # Otherwise consider each body atom and deltaify if necessary.
            for idx, body_atom in enumerate(rule.body):
                if update or body_atom.symbol in idb_relation_symbols:
                    new_rule = deepcopy(delta_rule)
                    new_rule.body[idx].symbol = Symbol(
                        f"{DELTA_PREFIX}{new_rule.body[idx].symbol}"
                    )
                    delta_rules_set.add(new_rule)
    delta_program = Program(list(delta_rules_set))
    return delta_program
