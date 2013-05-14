from datetime import datetime
from sqlalchemy import Table
from sqlalchemy.orm import mapper
from passlib.hash import pbkdf2_sha256 as passlib

from pegasus.service import db

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IMPORTANT
# This is the current version of the schema. It should be incremented 
# each time a change is made to the schema and a migration script 
# should be created to go from the old version to the new version.
version = 1
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

class Schema(db.Model):
    __tablename__ = 'schema'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.Integer)
    timestamp = db.Column(db.DateTime)

    def __init__(self, version):
        self.version = version
        self.timestamp = datetime.utcnow()

    def __repr__(self):
        return '<Schema %d %s>' % (self.version, self.timestamp)

class User(db.Model):
    __tablename__ = 'user'
    __table_args__ = {'mysql_engine':'InnoDB'}

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True)
    hashpass = db.Column(db.String(100))
    email = db.Column(db.String(120), unique=True)

    @staticmethod
    def hash_password(password):
        "Compute cryptographic hash of password"
        User.validate_password(password)
        return passlib.encrypt(password)

    @staticmethod
    def validate_password(password):
        "Validate that a password conforms to the password policy"

        # Password must be non-null
        if password is None:
            raise Exception("Invalid password: None")

        # Password must be > 3 characters
        if len(password) <= 3:
            raise Exception("Invalid password: Password length must be > 3")

    def __init__(self, username, password, email):
        self.username = username
        User.validate_password(password)
        self.hashpass = User.hash_password(password)
        self.email = email

    def password_matches(self, password):
        "Confirm that the password matches the user's password"
        try:
            User.validate_password(password)
        except:
            return False
        return passlib.verify(password, self.hashpass)

    def __repr__(self):
        return '<User %r>' % (self.username)

