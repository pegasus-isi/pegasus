from Pegasus.service import db, users

def upgrade():
    db.metadata.create_all(bind=db.engine,
            tables=[users.User.__table__])

def downgrade():
    db.metadata.drop_all(bind=db.engine,
            tables=[users.User.__table__])

