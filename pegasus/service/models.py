from pegasus.service import db


class User(db.Model):
    __tablename__ = 'users'
    
    ROLE_USER = 0
    ROLE_ADMIN = 1
    
    id = db.Column(db.Integer, primary_key = True)
    username = db.Column(db.String(64), index=True, unique=True)
    hashpass = db.Column(db.String(32))
    email = db.Column(db.String(120), index=True, unique=True)
    role = db.Column(db.SmallInteger, default = ROLE_USER)
    
    def __repr__(self):
        return '<User %r>' % (self.nickname)

