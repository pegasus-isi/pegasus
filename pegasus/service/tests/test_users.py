from pegasus.service import db, tests
from pegasus.service.models import User

from sqlalchemy.exc import IntegrityError

class TestUsers(tests.TestCase):
    def test_validpass(self):
        self.assertRaises(Exception, User.validate_password, None)
        self.assertRaises(Exception, User.validate_password, "")
        self.assertRaises(Exception, User.validate_password, "abc")
        User.validate_password("abcd")
        User.validate_password("secret")

    def test_hashpass(self):
        self.assertRaises(Exception, User.hash_password, None)
        self.assertRaises(Exception, User.hash_password, "")
        self.assertRaises(Exception, User.hash_password, "abc")
        shorthash = User.hash_password("abcd")
        self.assertEquals(len(shorthash),87)
        secrethash = User.hash_password("secret")
        self.assertEquals(len(secrethash),87)

    def test_userpass(self):
        u = User(username="gideon",password="secret",email="gideon@isi.edu")

        # Make sure the correct password is valid
        self.assertTrue(u.password_matches("secret"))

        # Make sure an incorrect password is not valid
        self.assertFalse(u.password_matches("secrets"))
        self.assertFalse(u.password_matches(""))
        self.assertFalse(u.password_matches(None))

class TestUsersDB(tests.DBTestCase):
    def test_usercreate(self):
        # Make sure we can insert a new user
        u1 = User(username="gideon", password="secret", email="gideon@isi.edu")
        db.session.add(u1)
        db.session.commit()

        # Make sure one user exists
        self.assertEquals(User.query.count(), 1)

        # Make sure the user matches what we specified
        u2 = User.query.first()
        self.assertEquals(u1.username, u2.username)
        self.assertEquals(u1.hashpass, u2.hashpass)
        self.assertEquals(u1.email, u2.email)

    def test_userdupes(self):
        u1 = User(username="gideon", password="secret", email="gideon@isi.edu")
        u2 = User(username="gideon", password="private", email="juve@usc.edu")

        db.session.add(u1)
        db.session.commit()

        # Adding a duplicate user should be an error
        db.session.add(u2)
        self.assertRaises(IntegrityError, db.session.commit)

