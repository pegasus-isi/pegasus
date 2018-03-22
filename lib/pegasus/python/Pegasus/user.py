import os
import pwd


class NoSuchUser(Exception):
    pass


class User(object):
    def __init__(self, uid, gid, username, homedir):
        self.uid = uid
        self.gid = gid
        self.username = username
        self.homedir = homedir

    def get_pegasus_dir(self):
        return os.path.join(self.homedir, ".pegasus")

    def get_ensembles_dir(self):
        return os.path.join(self.homedir, ".pegasus", "ensembles")

    def get_master_db(self):
        return os.path.join(self.homedir, ".pegasus", "workflow.db")

    def get_master_db_url(self):
        return "sqlite:///%s" % self.get_master_db()


def __user_from_pwd(pw):
    return User(pw.pw_uid, pw.pw_gid, pw.pw_name, pw.pw_dir)


def get_user_by_uid(uid):
    try:
        pw = pwd.getpwuid(uid)
        return __user_from_pwd(pw)
    except KeyError as e:
        raise NoSuchUser(uid)


def get_user_by_username(username):
    try:
        pw = pwd.getpwnam(username)
        return __user_from_pwd(pw)
    except KeyError:
        raise NoSuchUser(username)
    except TypeError:
        raise NoSuchUser(username)
