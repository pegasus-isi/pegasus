class PegasusError(Exception):
    pass


class DuplicateError(PegasusError):
    pass


class NotFoundError(PegasusError):
    pass


class FormatError(PegasusError):
    pass


class ParseError(PegasusError):
    pass
