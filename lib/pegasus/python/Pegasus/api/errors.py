class PegasusError(Exception):
    pass


class DuplicateError(PegasusError):
    pass


class NotFoundError(PegasusError):
    pass
