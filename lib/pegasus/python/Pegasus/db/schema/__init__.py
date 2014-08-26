from sqlalchemy import BigInteger
from sqlalchemy.dialects import postgresql, mysql, sqlite

KeyInteger = BigInteger()
KeyInteger = KeyInteger.with_variant(postgresql.BIGINT(), 'postgresql')
KeyInteger = KeyInteger.with_variant(mysql.BIGINT(), 'mysql')
KeyInteger = KeyInteger.with_variant(sqlite.INTEGER(), 'sqlite')

class SABase(object):
    """
    Base class for all the DB mapper objects.
    """
    def _commit(self, session, batch, merge=False):
        if merge:
            session.merge(self)
        else:
            session.add(self)
        if batch:
            return
        session.flush()
        session.commit()

    def commit_to_db(self, session, batch=False):
        """
        Commit the DB object/row to the database.

        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to commit row to.
        """ 
        self._commit(session, batch)

    def merge_to_db(self, session, batch=False):
        """
        Merge the DB object/row with an existing row in the database.

        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to merge row with.

        Using this method pre-supposes that the developer has already
        assigned any primary key information to the object before
        calling.
        """
        self._commit(session, batch, merge=True)

    def __repr__(self):
        retval = '%s:\n' % self.__class__
        for k,v in self.__dict__.items():
            if k == '_sa_instance_state':
                continue
            retval += '  * %s : %s\n' % (k,v)
        return retval

