import logging

from sqlalchemy import create_engine, orm, event
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQLite3Connection

from Pegasus.db import schema
#from Pegasus.db.errors import StampedeDBNotFoundError

__all__ = ['connect']

log = logging.getLogger(__name__)

# This turns on foreign keys for SQLite3 connections
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(conn, record):
    if isinstance(conn, SQLite3Connection):
        log.debug("Turning on foreign keys")
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()

def connect(dburi, echo=False):
    engine = create_engine(dburi, echo=echo, pool_recycle=True)

    # Create all the tables if they don't exist
    schema.metadata.create_all(engine)

    Session = orm.sessionmaker(bind=engine, autoflush=False, autocommit=False,
                               expire_on_commit=False)

    # TODO Check schema

    return orm.scoped_session(Session)

