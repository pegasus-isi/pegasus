"""
Base for schema modules
"""
class SABase(object):
    """
    Base class for all the DB mapper objects.
    """
    def commit_to_db(self, session):
        """
        Commit the DB object/row to the database.
        
        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to commit row to.
        """
        session.add(self)
        session.flush()
        session.commit()
        
    def merge_to_db(self, session):
        """
        Merge the DB object/row with an existing row in the database.
        
        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to merge row with.
        
        Using this method pre-supposes that the developer has already
        assigned any primary key information to the object before
        calling.
        """
        session.merge(self)
        session.flush()
        session.commit()
    def __repr__(self):
        retval = '%s:\n' % self.__class__
        for k,v in self.__dict__.items():
            if k == '_sa_instance_state':
                continue
            retval += '  * %s : %s\n' % (k,v)
        return retval
