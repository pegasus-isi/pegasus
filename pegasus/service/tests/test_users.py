from pegasus.service import db, tests, users
from pegasus.service.users import User

from sqlalchemy.exc import IntegrityError

class TestUsers(tests.TestCase):
    def test_validate_password(self):
        self.assertRaises(users.InvalidPassword, users.validate_password, None)
        self.assertRaises(users.InvalidPassword, users.validate_password, "")
        self.assertRaises(users.InvalidPassword, users.validate_password, "abc")
        self.assertRaises(users.InvalidPassword, users.validate_password, self)
        users.validate_password("abcd")
        users.validate_password("secret")

    def test_hash_password(self):
        shorthash = users.hash_password("abcd")
        self.assertEquals(len(shorthash),87)
        secrethash = users.hash_password("secret")
        self.assertEquals(len(secrethash),87)

    def test_verify_password(self):
        self.assertTrue(users.verify_password("secret", users.hash_password("secret")))
        self.assertTrue(users.verify_password("abcd", users.hash_password("abcd")))

    def test_userpass(self):
        u = User(username="gideon",password="secret",email="gideon@isi.edu")

        # Make sure the correct password matches
        self.assertTrue(u.password_matches("secret"))

        # Make sure an incorrect password does not match
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

