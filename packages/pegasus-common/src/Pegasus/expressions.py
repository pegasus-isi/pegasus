class InvalidMandatoryClauseError(Exception):
    """Error."""


def mandatory_check(clause):
    try:
        compile(clause, "<string>", "eval")
        return True
    except SyntaxError as e:
        raise InvalidMandatoryClauseError("Invalid mandatory clause: %s" % e)


# Earlier range returned a list, but now it returns a range object,
# so it needs to be wrapped in a list call
def list_wrapped_range(*args):
    return list(range(*args))


def mandatory_parse(clause, symbols=None):
    """Parse mandatory expression.

    Filter Clause Identifier Rules:
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

    :raises InvalidMandatoryClauseError: Raised if the `clause` is not valid.

    :returns: list containing tuples of field name, and sort order.
    :rtype: list

    .. example::

        >>> mandatory_parse("d.id == 1")
        [('prefix', 'identifier', 'DESC')]

        >>> mandatory_parse("d.id > 1 and d.id < 5")
        [('a', 'ASC'), ('a', 'b', 'ASC')]

        >>> mandatory_parse("u.firt_name + u.last_name like '%abc%'")
        [('a', 'ASC'), ('a', 'b', 'ASC')]

    """
    try:
        return eval(clause, {}, symbols,)
    except (SyntaxError, KeyError, NameError, ValueError) as e:
        raise InvalidMandatoryClauseError("Invalid mandatory clause: %s" % e)


"""
# "karan" if queue == "test" else "vahi"
queue = "test"
newqueue = mandatory_parse(
    '"karan" if queue == "test" else "vahi"', symbols={"queue": queue}
)
print(newqueue)

dagnode_retry = 0
current_mem = 10
expr = "'100MB' if dagnode_retry == 1 else '10MB'"
memory = mandatory_parse(expr, symbols={"dagnode_retry": dagnode_retry})
print(memory)
"""
