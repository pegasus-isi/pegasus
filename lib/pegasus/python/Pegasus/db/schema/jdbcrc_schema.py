__author__ = "Rafael Ferreira da Silva"

import time
import warnings
import logging

from sqlalchemy import *
from sqlalchemy import MetaData, orm, exc

from Pegasus.db.schema import SABase, KeyInteger

log = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 1.3

metadata = MetaData()

# These are keywords that all tables should have
table_keywords = {}
table_keywords['mysql_charset'] = 'latin1'
table_keywords['mysql_engine'] = 'InnoDB'

def initializeToJDBCRC(db):
    # For SQLite
    warnings.filterwarnings('ignore', '.*does \*not\* support Decimal*.')
    
    # Create all the tables if they don't exist
    metadata.create_all(db)
    db.execute(rc_schema.insert(), name='JDBCRC', catalog='rc', version=1.3, creator='vahi')
    
# Empty classes that will be populated and mapped
# to tables via the SQLAlch mapper.
class Sequences(SABase):
    pass

class PegasusSchema(SABase):
    pass

class RCLFN(SABase):
    pass

class RCAttr(SABase):
    pass

rc_sequences = Table('sequences', metadata,
    Column('name', VARCHAR(32), nullable=False, primary_key=True),
    Column('currval', BIGINT, nullable=False),
    **table_keywords
)
orm.mapper(Sequences, rc_sequences)

rc_schema = Table('pegasus_schema', metadata,
    Column('name', VARCHAR(64), nullable=False, primary_key=True),
    Column('catalog', VARCHAR(16)),
    Column('version', FLOAT),
    Column('creator', VARCHAR(32)),
    Column('creation', NUMERIC(precision=16,scale=6), default=time.time()),
    **table_keywords
)
orm.mapper(PegasusSchema, rc_schema)

rc_lfn = Table('rc_lfn', metadata,
    Column('id', KeyInteger, primary_key=True, nullable=False),
    Column('lfn', VARCHAR(245), nullable=False),
    Column('pfn', VARCHAR(245), nullable=False),
    Column('site', VARCHAR(245)),
    UniqueConstraint('lfn', 'pfn', 'site', name='sk_rc_lfn'),
    **table_keywords
)

Index('ix_rc_lfn', rc_lfn.c.lfn, unique=True)
orm.mapper(RCLFN, rc_lfn)

rc_attr = Table('rc_attr', metadata,
    Column('id', KeyInteger, ForeignKey('rc_lfn.id', ondelete='CASCADE', name='fk_rc_attr'), primary_key=True, nullable=False),
    Column('name', VARCHAR(64), nullable=False, primary_key=True),
    Column('value', VARCHAR(32), nullable=False),
    **table_keywords
)

Index('ix_rc_attr', rc_attr.c.name, unique=True)
orm.mapper(RCAttr, rc_attr)