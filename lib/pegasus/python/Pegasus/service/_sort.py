import ast
import logging


class InvalidSortError(Exception):
    pass


def sort_parse(clause):
    """
    Parses sort clause.

    Sort Clause Identifier Rules:
        Prefix + Identifier

    Prefix Rules:
        Is optional
        Must start with a-z OR A-Z
        Can contain a-z OR A-Z OR 0-9 OR _
        Must end with .

    Identifier Rules:
        Is mandatory
        Must start with a-z OR A-Z
        Can contain a-z OR A-Z OR 0-9 OR _

    :param clause: The clause to be parsed.
    :type clause: str

    :raises InvalidSortError: Raised if the `clause` is not valid.

    :returns: list containing tuples of field name, and sort order.
    :rtype: list

    .. example::

        >>> sort_parse("-prefix.identifier")
        [('prefix', 'identifier', 'DESC')]

        >>> sort_parse("a, +a.b")
        [('a', 'ASC'), ('a', 'b', 'ASC')]
    """
    try:
        n = ast.parse(clause.strip(",") + ",", mode="eval").body
    except SyntaxError as e:
        raise InvalidSortError("Invalid sort: %s" % e)

    if not isinstance(n, ast.Tuple):
        raise InvalidSortError("Invalid condition: must evaluate to a boolean value")

    return _SortEvaluator().visit(n)


class _SortEvaluator(ast.NodeVisitor):
    def __init__(self):
        self._sort_dir = None
        self.reset_context()
        self._log = logging.getLogger(__name__)

    def reset_context(self):
        self._sort_dir = "ASC"

    def visit_Tuple(self, n):
        sort = []
        for elt in n.elts:
            self.reset_context()
            sort.append(tuple(self.visit(elt)) + (self._sort_dir,))
        return sort

    def visit_UnaryOp(self, n):
        self.visit(n.op)
        return self.visit(n.operand)

    def visit_UAdd(self, n):
        pass

    def visit_USub(self, n):
        self._sort_dir = "DESC"

    def visit_Attribute(self, n):
        self._log.info("Attribute <%s> <%s>", n.value, n.attr)
        value = self.visit(n.value)
        value.append(n.attr)
        return value

    def visit_Name(self, n):
        return [n.id]

    def generic_visit(self, n):
        raise InvalidSortError(
            "Invalid sort clause at Line <%d>, Col <%d>" % (n.lineno, n.col_offset)
        )


def _main():
    import sys

    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Expression <%s>", sys.argv[1])
    result = sort_parse(sys.argv[1])
    logging.debug("Evaluation Result <%s>", result)


if __name__ == "__main__":
    _main()
