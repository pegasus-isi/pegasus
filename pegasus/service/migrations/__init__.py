from pegasus.service import db, schema
from pegasus.service.schema import Schema

def current_schema():
    """Return the version of the current database schema"""
    try:
        s = db.session.query(Schema).\
                       order_by(Schema.timestamp.desc()).\
                       first()
        return s.version
    except Exception, e:
        if "no such table: schema" in e.message:
            return None
        else:
            raise

def create():
    """Create all objects in the database"""
    if current_schema() is not None:
        raise Exception("Schema already exists. Try migrate.")

    db.create_all()
    db.session.add(Schema(schema.version))
    db.session.commit()

def drop():
    """Drop all objects from the database"""
    # We omit the dashboard tables because they are maintained
    # by monitord
    omit = ["workflow","workflowstate"]
    tables = [t for t in db.metadata.sorted_tables if t.name not in omit]
    db.metadata.drop_all(bind=db.engine, tables=tables)

def migrate(to):
    """Migrate the current schema to version"""
    start = current_schema()
    end = to

    if end < 0 or end > schema.version:
        raise Exception("Version out of range: %d" % to)

    if start == end:
        return

    if start < end:
        for i in range(start+1, end+1):
            _get_migration(i).upgrade()
    else:
        for i in range(start, end, -1):
            _get_migration(i).downgrade()

    db.session.add(Schema(end))
    db.session.commit()

def _get_migration(v):
    return __import__("pegasus.service.migrations.v%d" % v,
                      fromlist=["pegasus.service.migrations"])

