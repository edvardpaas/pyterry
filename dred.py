from copy import deepcopy
from typing import Final

from datalog import Program, Symbol

OVERDELETION_PREFIX: Final[str] = "delete_"
REDERIVATION_PREFIX: Final[str] = "rederive_"


def make_overdeletion_program(program: Program) -> Program:
    overdeletion_rules_set = set()

    for rule in program:
        overdeletion_rule = deepcopy(rule)
        overdeletion_rule.head.symbol = Symbol(
            f"{OVERDELETION_PREFIX}{overdeletion_rule.head.symbol}"
        )
        for idx, _ in enumerate(rule.body):
            new_rule = deepcopy(overdeletion_rule)
            new_rule.body[idx].symbol = Symbol(
                f"{OVERDELETION_PREFIX}{new_rule.body[idx].symbol}"
            )
            overdeletion_rules_set.add(new_rule)

    overdeletion_program = Program(list(overdeletion_rules_set))
    return overdeletion_program


def make_rederivation_program(program: Program) -> Program:
    rederivation_rules_set = set()

    for rule in program:
        rederivation_rule = deepcopy(rule)

        rederivation_head = deepcopy(rederivation_rule.head)
        rederivation_head.symbol = Symbol(
            f"{OVERDELETION_PREFIX}{rederivation_head.symbol}"
        )
        rederivation_rule.body.insert(0, rederivation_head)

        rederivation_rule.head.symbol = Symbol(
            f"{REDERIVATION_PREFIX}{rederivation_rule.head.symbol}"
        )
        rederivation_rules_set.add(rederivation_rule)

    rederivation_program = Program(list(rederivation_rules_set))
    return rederivation_program
