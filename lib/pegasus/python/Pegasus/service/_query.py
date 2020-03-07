import ast
import logging

from sqlalchemy import and_, not_, or_
from sqlalchemy.sql.operators import ColumnOperators as operator

COMPARE_OPERATORS = {
    ast.Eq: operator.__eq__,
    ast.NotEq: operator.__ne__,
    ast.Lt: operator.__lt__,
    ast.LtE: operator.__le__,
    ast.Gt: operator.__gt__,
    ast.GtE: operator.__ge__,
    ast.Is: operator.is_,
    ast.IsNot: operator.isnot,
    ast.In: operator.in_,
    ast.NotIn: operator.notin_,
}

OPERATORS = {
    ast.Add: operator.__add__,
    ast.Sub: operator.__sub__,
    ast.Mult: operator.__mul__,
    ast.Div: operator.__div__,
}

BOOLEAN_OPERATORS = {ast.And: and_, ast.Or: or_}

UNARY_OPERATORS = {
    ast.Not: not_,
    # ast.UAdd: operator.pos,
    # ast.USub: operator.neg
}

BUILTINS = {"None": None}


class InvalidQueryError(Exception):
    pass


def query_parse(clause, **symbols):
    """
    Parses query clause.

    Query Clause Identifier Rules:
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

    Supported Comparators (case-insensitive)
        - ==
        - !=
        - <
        - <=
        - >
        - >=
        - in

    Supported Functions
        - like
        - ilike

    Supported Operands (case-insensitive)
        - and
        - or
        - not

    Null Handling

    <identifier> is None OR <identifier> == None

    Examples:

        - age > 1
        - user.age >= 10 and user.age < 100
        - ((user.age < 18 and movie.ratings in ("PG", "PG13") or user.age > 18)

    :param clause: The clause to be parsed.
    :type clause: str

    :raises InvalidQueryError: Raised if the `clause` is not valid.

    :returns: sqlalchemy boolean clause list.
    :rtype: tuple

    .. example::

        >>> str( query_parse("w.wf_id == 1") )
        'workflow.wf_id = :wf_id_1'

        >>> str( query_parse("w.wf_id > 1 and w.wf_id < 5") )
        'workflow.wf_id > :wf_id_1 AND workflow.wf_id < :wf_id_2'
    """

    try:
        n = ast.parse(clause.strip(",") + ",", mode="eval").body
    except SyntaxError as e:
        raise InvalidQueryError("Invalid query: %s" % e)

    if not isinstance(n, ast.Tuple):
        raise InvalidQueryError("Invalid condition: must evaluate to a boolean value")

    return _QueryEvaluator(**symbols).visit(n)


class _QueryEvaluator(ast.NodeVisitor):
    def __init__(self, **symbols):
        super().__init__()
        self._symbols = symbols
        self._log = logging.getLogger(__name__)

    # Compare
    def visit_Compare(self, n):
        # Compare(expr left, cmpop* ops, expr* comparators)
        left = self.visit(n.left)

        result = True
        for i, v in zip(n.ops, n.comparators):
            op = COMPARE_OPERATORS[i.__class__]
            comparator = self.visit(v)

            result = result and op(left, comparator)
            left = comparator

        return result

    # BoolOp
    def visit_BoolOp(self, n):
        # boolop = And | Or
        op = BOOLEAN_OPERATORS[n.op.__class__]
        return op(*[self.visit(i) for i in n.values])

    # BinOp
    def visit_BinOp(self, n):
        left = self.visit(n.left)
        right = self.visit(n.right)
        op = OPERATORS[n.op.__class__]
        return op(left, right)

    # UnaryOp
    def visit_UnaryOp(self, n):
        # UnaryOp(unaryop op, expr operand)
        self._log.info("UnaryOp <%s> <%s>", n.op.__class__, n.operand)
        op = UNARY_OPERATORS[n.op.__class__]
        operand = self.visit(n.operand)
        return op(operand)

    # Identifiers
    def visit_Attribute(self, n):
        self._log.info("Attribute <%s> <%s>", n.value, n.attr)
        value = self.visit(n.value)
        if isinstance(value, dict):
            return value[n.attr]
        return getattr(value, n.attr)

    def visit_Subscript(self, n):
        self._log.info("Subscript <%s> <%s>", n.value, n.slice)
        value = self.visit(n.value)
        slice = self.visit(n.slice)
        return value[slice]

    def visit_Index(self, n):
        return self.visit(n.value)

    def visit_keyword(self, n):
        return {n.arg: n.value}

    def visit_Name(self, n):
        self._log.info("Name <%s>", n.id)
        name = self._symbols.get(n.id, BUILTINS.get(n.id, None))
        if n.id not in self._symbols and n.id not in BUILTINS:
            raise NameError(
                "Invalid name <%s> at Line <%d> Col "
                "<%d>" % (n.id, n.lineno, n.col_offset)
            )

        return name

    # Literals
    def visit_Num(self, n):
        return n.n

    def visit_Str(self, n):
        return n.s

    def visit_NameConstant(self, n):
        self._log.info("NameConstant <%s>", n.value)
        return n.value

    def visit_List(self, n):
        return [self.visit(n) for n in n.elts]

    def visit_Tuple(self, n):
        return tuple([self.visit(e) for e in n.elts])

    def visit_Set(self, n):
        return {self.visit(n) for n in n.elts}

    def visit_Call(self, n):
        args = []
        try:
            func = self.visit(n.func)
        except AttributeError:
            raise InvalidQueryError(
                "Invalid name <%s> at Line <%d> Col <%d>"
                % (n.func.attr, n.lineno, n.col_offset)
            )

        # Args
        args.extend([self.visit(a) for a in n.args])

        # Keyword Args
        kwargs = {}
        for k in n.keywords:
            kwargs.update(self.visit(k))

        return func(*args, **kwargs)

    # Catch All
    def generic_visit(self, n):
        raise InvalidQueryError(
            "Invalid query at Line <%d> Col <%d>" % (n.lineno, n.col_offset)
        )


def _main():
    import sys

    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Expression <%s>", sys.argv[1])
    from Pegasus.db.schema import Workflow

    result = query_parse(sys.argv[1], Workflow)
    logging.debug("Evaluation Result <%s>", result[0])


if __name__ == "__main__":
    _main()
