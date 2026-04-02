"""
Unix user lookup utilities for the Pegasus service layer.

Provides :class:`User` with helper methods for Pegasus-specific paths, and
module-level lookup functions :func:`get_user_by_uid` and
:func:`get_user_by_username`.
"""

import os
import pwd


class NoSuchUser(Exception):
    """Raised when a user cannot be found in the system password database."""


class User:
    """Represents a Unix user with Pegasus-specific path helpers."""

    def __init__(self, uid, gid, username, homedir):
        """
        :param uid: Unix user ID.
        :param gid: Unix group ID.
        :param username: Login name.
        :param homedir: Absolute path to the user's home directory.
        """
        self.uid = uid
        self.gid = gid
        self.username = username
        self.homedir = homedir

    def get_pegasus_dir(self):
        """Return the path to the user's ``~/.pegasus`` directory."""
        return os.path.join(self.homedir, ".pegasus")

    def get_ensembles_dir(self):
        """Return the path to the user's ``~/.pegasus/ensembles`` directory."""
        return os.path.join(self.homedir, ".pegasus", "ensembles")

    def get_master_db(self):
        """Return the path to the user's Pegasus master SQLite database file."""
        return os.path.join(self.homedir, ".pegasus", "workflow.db")

    def get_master_db_url(self):
        """Return a SQLAlchemy connection URL for the user's master database."""
        return "sqlite:///%s" % self.get_master_db()


def __user_from_pwd(pw):
    return User(pw.pw_uid, pw.pw_gid, pw.pw_name, pw.pw_dir)


def get_user_by_uid(uid):
    """Look up a user by numeric UID.

    :param uid: Unix user ID.
    :type uid: int
    :return: The matching :class:`User`.
    :rtype: User
    :raises NoSuchUser: If no user with that UID exists.
    """
    try:
        pw = pwd.getpwuid(uid)
        return __user_from_pwd(pw)
    except KeyError:
        raise NoSuchUser(uid)


def get_user_by_username(username):
    """Look up a user by login name.

    :param username: The login name to look up.
    :type username: str
    :return: The matching :class:`User`.
    :rtype: User
    :raises NoSuchUser: If no user with that name exists.
    """
    try:
        pw = pwd.getpwnam(username)
        return __user_from_pwd(pw)
    except KeyError:
        raise NoSuchUser(username)
    except TypeError:
        raise NoSuchUser(username)
