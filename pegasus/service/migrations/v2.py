from pegasus.service import db, replicas
from pegasus.service.replicas import ReplicaMapping

def upgrade():
    db.metadata.create_all(bind=db.engine,
            tables=[ReplicaMapping.__table__])

def downgrade():
    db.metadata.drop_all(bind=db.engine,
            tables=[ReplicaMapping.__table__])

