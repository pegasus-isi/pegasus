from Pegasus.service import db, tests, users
from Pegasus.service.users import User

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
        u1 = users.create(username="gideon", password="secret", email="gideon@isi.edu")

        # Make sure one user exists
        self.assertEquals(User.query.count(), 1)

        # Make sure the user matches what we specified
        u2 = User.query.first()
        self.assertEquals(u1.username, u2.username)
        self.assertEquals(u1.hashpass, u2.hashpass)
        self.assertEquals(u1.email, u2.email)

    def test_userdupes(self):
        users.create(username="gideon", password="secret", email="gideon@isi.edu")
        self.assertRaises(users.UserExists, users.create, "gideon", "private", "juve@usc.edu")

    def test_passwd(self):
        gideon = users.create("gideon", "secret", "gideon@isi.edu")
        self.assertTrue(gideon.password_matches("secret")) # original passwd

        users.passwd("gideon", "newsecret")
        self.assertTrue(gideon.password_matches("newsecret")) # new passwd

        gideon2 = users.getuser("gideon")
        self.assertTrue(gideon2.password_matches("newsecret")) # new passwd

    def test_usermod(self):
        gideon = users.create("gideon", "secret", "gideon@isi.edu")
        self.assertEquals(gideon.email, "gideon@isi.edu") # original email

        users.usermod("gideon", "juve@usc.edu")
        self.assertEquals(gideon.email, "juve@usc.edu") # new email

    def test_all(self):
        l = users.all()
        self.assertEquals(len(l), 0) # should not be any users

        users.create("gideon", "secret", "gideon@isi.edu")
        l = users.all()
        self.assertEquals(len(l), 1) # should be 1 user

        users.create("rynge", "secret", "rynge@isi.edu")
        l = users.all()
        self.assertEquals(len(l), 2) # should be 2 users

    def test_getuser(self):
        gideon = users.create("gideon", "secret", "gideon@isi.edu")

        g2 = users.getuser("gideon")
        self.assertEquals(gideon.username, g2.username)
        self.assertEquals(gideon.hashpass, g2.hashpass)
        self.assertEquals(gideon.email, g2.email)

        self.assertRaises(users.NoSuchUser, users.getuser, "rynge")


class TestAuthentication(tests.APITestCase):

    def test_unauthorized(self):
        r = self.get("/", auth=False)
        self.assertEquals(r.status_code, 401)

    def test_authorized(self):
        r = self.get("/")
        self.assertEquals(r.status_code, 200)

