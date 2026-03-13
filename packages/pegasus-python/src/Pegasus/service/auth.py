"""
Authentication backends for the Pegasus service.

:class:`NoAuthentication` is used in single-user deployments; :class:`PAMAuthentication`
delegates to the system PAM stack for multi-user or remote deployments.
"""

import logging
import os

import pamela
from flask import g

from Pegasus import user

log = logging.getLogger(__name__)


class BaseAuthentication:
    """Abstract base class for Pegasus service authentication backends."""

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def authenticate(self):
        raise Exception("Not implemented")

    def get_user(self):
        raise Exception("Not implemented")


class NoAuthentication(BaseAuthentication):
    """Authentication backend that always succeeds, used for single-user service deployments."""

    def __init__(self, *args):
        # For no auth. username and password is not required
        pass

    def authenticate(self):
        # Always authenticate the user
        return True

    def get_user(self):
        # Just return info for the user running the service
        if "username" in g:
            return user.get_user_by_username(g.username)
        else:
            return user.get_user_by_uid(os.getuid())


class PAMAuthentication(BaseAuthentication):
    """Authentication backend that validates credentials via the system PAM stack."""

    def authenticate(self):
        try:
            if not self.username:
                return False
            return pamela.authenticate(self.username, self.password) is None
        except Exception as e:
            log.exception(e)
            return False

    def get_user(self):
        return user.get_user_by_username(self.username)
