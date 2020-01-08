class DAX4Error(Exception):
    pass


class DuplicateError(DAX4Error):
    pass


class NotFoundError(DAX4Error):
    pass


class FormatError(DAX4Error):
    pass


class ParseError(DAX4Error):
    pass
