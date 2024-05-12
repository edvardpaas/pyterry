from copy import deepcopy

import networkx as nx  # type: ignore

from datalog import Program, Rule, Symbol


def generate_rule_dependency_graph(program: Program) -> nx.DiGraph:
    idb_relations: dict[Symbol, list[Rule]] = {}
    output = nx.DiGraph()
    for rule in program:
        if idb_relations.get(rule.head.symbol):
            idb_relations[rule.head.symbol].append(rule)
        else:
            idb_relations[rule.head.symbol] = [rule]
        output.add_node(rule)
    for rule in program:
        for body_atom in rule.body:
            body_atom_rules = idb_relations.get(body_atom.symbol)
            if body_atom_rules:
                for body_atom_rule in body_atom_rules:
                    output.add_edge(body_atom_rule, rule)
    return output


def stratify(rule_graph: nx.DiGraph) -> list[list[Rule]]:
    sccs = nx.kosaraju_strongly_connected_components(rule_graph)
    # For each SCC, sort the rules based on a deterministic property, like their string representation
    sorted_sccs: list[list[Rule]] = [
        sorted(list(scc), key=lambda rule: rule.id) for scc in list(sccs)
    ]
    return list(sorted_sccs)


def sort_program(program: Program) -> Program:
    rule_graph = generate_rule_dependency_graph(deepcopy(program))
    stratification = stratify(rule_graph)
    sorted_program = Program([])
    for program_strat in stratification:
        for rule in program_strat:
            sorted_program.append(deepcopy(rule))
    sorted_program.reverse()
    return sorted_program
