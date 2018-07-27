__author__ = "Rafael Ferreira da Silva"

import abc
from future.utils import with_metaclass


class BaseVersion(with_metaclass(abc.ABCMeta, object)):
    def __init__(self, db):
        self.db = db

    @abc.abstractmethod
    def update(self, force=False):
        """Used for the update command."""

    @abc.abstractmethod
    def downgrade(self, force=False):
        """Used for the downgrade command."""
