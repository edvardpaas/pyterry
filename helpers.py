from copy import deepcopy

from datalog import Program, Rule


def split_program(program: Program) -> tuple[Program, Program]:
    nonrecursive: list[Rule] = []
    recursive: list[Rule] = []

    for rule in program:
        head_symbol = rule.head.symbol
        is_recursive = False

        for body_atom in rule.body:
            if body_atom.symbol == head_symbol:
                is_recursive = True

        if is_recursive:
            recursive.append(deepcopy(rule))
        else:
            nonrecursive.append(deepcopy(rule))
    return Program(nonrecursive), Program(recursive)
