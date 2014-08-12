from pegasus.service import db, ensembles

def upgrade():
    db.metadata.create_all(bind=db.engine,
            tables=[ensembles.Ensemble.__table__,
                    ensembles.EnsembleWorkflow.__table__])

def downgrade():
    db.metadata.drop_all(bind=db.engine,
            tables=[ensembles.Ensemble.__table__,
                    ensembles.EnsembleWorkflow.__table__])

