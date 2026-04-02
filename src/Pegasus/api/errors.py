class PegasusError(Exception):
    """Base exception for all Pegasus API errors."""


class DuplicateError(PegasusError):
    """Raised when an attempt is made to add an object that already exists (e.g. duplicate job id, duplicate catalog entry)."""


class NotFoundError(PegasusError):
    """Raised when a requested object does not exist (e.g. a job id that is not in the workflow)."""
