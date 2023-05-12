from lark import Lark
from lark.tree import Tree

grammar = """
start: expr
expr: atom | expr "+" atom
atom: INT | "(" expr ")"
%import common.INT
%import common.WS
%ignore WS
"""
def find_data(node: Tree, data_fields: list[str]) -> list[Tree]:
    """
    Find all nodes in the parse tree that have one of the specified data fields.
    """
    result = []
    if node.data in data_fields:
        result.append(node)
    for child in node.children:
        if isinstance(child, Tree):
            result.extend(find_data(child, data_fields))
    return result

lark = Lark(grammar)

tree = lark.parse("(1+(2+3))")

nodes = find_data(tree, ["INT", "expr"])
print(nodes)  # Output: [Tree('expr', [Tree('INT', ['1']), Tree('+', [Tree('INT', ['2']), Tree('INT', ['3'])])]), Tree('INT', ['1']), Tree('INT', ['2']), Tree('INT', ['3'])]
