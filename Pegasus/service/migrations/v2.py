from pegasus.service import db, catalogs

def upgrade():
    db.metadata.create_all(bind=db.engine,
            tables=[catalogs.ReplicaCatalog.__table__,
                    catalogs.SiteCatalog.__table__,
                    catalogs.TransformationCatalog.__table__])

def downgrade():
    db.metadata.drop_all(bind=db.engine,
            tables=[catalogs.ReplicaCatalog.__table__,
                    catalogs.SiteCatalog.__table__,
                    catalogs.TransformationCatalog.__table__])

