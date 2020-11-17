__author__ = "Rafael Ferreira da Silva"

import abc


class BaseVersion(metaclass=abc.ABCMeta):
    def __init__(self, db):
        self.db = db

    @abc.abstractmethod
    def update(self, force=False):
        """Used for the update command."""

    @abc.abstractmethod
    def downgrade(self, force=False):
        """Used for the downgrade command."""
