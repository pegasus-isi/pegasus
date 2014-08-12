import os
import getpass
from passlib.hash import pbkdf2_sha256 as passlib

from Pegasus.service import app, db

class UserExists(Exception): pass
class NoSuchUser(Exception): pass
class InvalidPassword(Exception): pass

class User(db.Model):
    __tablename__ = 'user'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    hashpass = db.Column(db.String(100))
    email = db.Column(db.String(120))

    def __init__(self, username, password, email):
        self.username = username
        self.set_password(password)
        self.email = email

    def set_password(self, password):
        "Set the user's password"
        validate_password(password)
        self.hashpass = hash_password(password)

    def password_matches(self, password):
        "Confirm that the password matches the user's password"
        return verify_password(password, self.hashpass)

    def __repr__(self):
        return '<User %r>' % (self.username)

    def get_userdata_dir(self):
        return os.path.join(app.config["STORAGE_DIR"],
                            "userdata", self.username)

def validate_password(password):
    # Password must be non-null
    if password is None:
        raise InvalidPassword("Invalid password: None")

    if not isinstance(password,basestring):
        raise InvalidPassword("Invalid password: Not a string")

    # Password must be > 3 characters
    if len(password) <= 3:
        raise InvalidPassword("Invalid password: Password length must be > 3 characters")

def hash_password(password):
    return passlib.encrypt(password)

def verify_password(password, hashpass):
    if password is None or password == "":
        return False
    if hashpass is None or hashpass == "":
        return False
    return passlib.verify(password, hashpass)

def create(username, password, email):
    if User.query.filter_by(username=username).count() > 0:
        raise UserExists("User exists: %s" % username)

    if password is None:
        password = getpass.getpass("New password: ")

    user = User(username, password, email)
    db.session.add(user)
    db.session.flush()

    return user

def passwd(username, password):
    "Change a user's password"
    user = getuser(username)
    if password is None:
        password = getpass.getpass("New password: ")
    user.set_password(password)
    db.session.flush()
    return user

def usermod(username, email):
    "Change a user's email address"
    user = getuser(username)
    user.email = email
    db.session.flush()
    return user

def all():
    "Get a list of all users"
    return User.query.order_by("username").all()

def getuser(username):
    "Get a user by username"
    user = User.query.filter_by(username=username).first()
    if user is None:
        raise NoSuchUser("No such user: %s" % username)
    return user

