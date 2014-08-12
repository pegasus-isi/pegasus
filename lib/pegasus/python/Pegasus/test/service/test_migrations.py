from Pegasus.service import db, migrations, schema
from Pegasus.test.service import *

class TestMigrations(DBTestCase):
    def test_downgrade(self):
        self.assertEquals(schema.version, migrations.current_schema())
        migrations.migrate(0)
        self.assertEquals(0, migrations.current_schema())

    def test_upgrate(self):
        migrations.migrate(0)
        self.assertEquals(0, migrations.current_schema())
        migrations.migrate(1)
        self.assertEquals(1, migrations.current_schema())
        migrations.migrate(schema.version)
        self.assertEquals(schema.version, migrations.current_schema())

