from sqlalchemy.exc import IntegrityError

from pegasus.service import db

class NoSuchMapping(Exception): pass
class MappingExists(Exception): pass

class ReplicaMapping(db.Model):
    __tablename__ = 'replica_mapping'
    __table_args__ = (
        db.UniqueConstraint('lfn', 'pfn'),
        {'mysql_engine':'InnoDB'}
    )

    lfn = db.Column(db.String(250), primary_key=True)
    pfn = db.Column(db.String(1000), primary_key=True)
    pool = db.Column(db.String(100), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    def __init__(self, user_id, lfn, pfn, pool="local"):
        self.user_id = user_id
        self.lfn = lfn
        self.pfn = pfn
        self.pool = pool

class PFN:
    def __init__(self, name, pool):
        self.name = name
        self.pool = pool

    def __repr__(self):
        return "<PFN %s>" % self.name

def create_mapping(user_id, lfn, pfn, pool):
    if ReplicaMapping.query.filter_by(user_id=user_id, lfn=lfn, pfn=pfn).count() > 0:
        raise MappingExists("%s -> %s" % (lfn, pfn))

    mapping = ReplicaMapping(user_id, lfn, pfn, pool)
    db.session.add(mapping)
    db.session.flush()

    return mapping

def update_mapping(user_id, lfn, pfn, pool):
    mapping = ReplicaMapping.query.filter_by(lfn=lfn, pfn=pfn).first()
    if mapping is None:
        raise NoSuchMapping("%s -> %s" % (lfn, pfn))
    mapping.pool = pool

def find_lfns(user_id):
    mappings = ReplicaMapping.query.filter_by(user_id=user_id).all()
    return [m.lfn for m in mappings]

def find_pfns(user_id, lfn):
    mappings = ReplicaMapping.query.filter_by(user_id=user_id, lfn=lfn).all()
    return [PFN(m.pfn, m.pool) for m in mappings]

def find_mappings(user_id):
    mappings = ReplicaMapping.query.filter_by(user_id=user_id).all()
    return mappings

