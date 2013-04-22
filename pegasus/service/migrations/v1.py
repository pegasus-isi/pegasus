from pegasus.service import db, models

def upgrade():
    db.metadata.create_all(bind=db.engine,
            tables=[models.User.__table__])

def downgrade():
    db.metadata.drop_all(bind=db.engine,
            tables=[models.User.__table__])

