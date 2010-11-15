"""
Base for schema modules
"""
from sqlalchemy import exceptions

class SchemaException(Exception):
    """Common base.
    """
    pass
    
class SchemaIntegrityError(SchemaException):
    """
    Raise this when an insert/update attempst to violate
    the schema - ie: violating a unique index or a column
    definition (NOT/NULL, etc).
    
    This is just a "wrapper" to unify handing multiple
    exceptions (ie: ones that violate defined schema) but
    being able to trap them all the same way in the calling
    code.
    """
    def __init__(self, value):
        self.value = 'SchemaIntegrityError: %s' % value
    def __str__(self):
        return repr(self.value)

class SABase(object):
    """
    Base class for all the DB mapper objects.
    """
    def _commit(self, session, merge=False):
        try:
            if merge:
                session.merge(self)
            else:
                session.add(self)
            session.flush()
            session.commit()
        except exceptions.IntegrityError, e:
            raise SchemaIntegrityError, e
            
    
    def commit_to_db(self, session):
        """
        Commit the DB object/row to the database.
        
        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to commit row to.
        """ 
        self._commit(session)
        
    def merge_to_db(self, session):
        """
        Merge the DB object/row with an existing row in the database.
        
        @type   session: sqlalchemy.orm.scoping.ScopedSession object
        @param  session: SQLAlch session to merge row with.
        
        Using this method pre-supposes that the developer has already
        assigned any primary key information to the object before
        calling.
        """
        self._commit(session, merge=True)

    def __repr__(self):
        retval = '%s:\n' % self.__class__
        for k,v in self.__dict__.items():
            if k == '_sa_instance_state':
                continue
            retval += '  * %s : %s\n' % (k,v)
        return retval
