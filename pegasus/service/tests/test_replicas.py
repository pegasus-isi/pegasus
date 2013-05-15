from pegasus.service import db, tests, replicas, users


class TestReplicasDB(tests.DBTestCase):
    def setUp(self):
        tests.DBTestCase.setUp(self)
        self.user = users.create("gideon", "secret", "gideon@isi.edu")

    def test_create_mapping(self):
        m = replicas.create_mapping(user_id=self.user.id, lfn="lfn", pfn="pfn", pool="local")

        lfns = replicas.find_lfns(self.user.id)
        self.assertEquals(len(lfns), 1)
        self.assertEquals(lfns[0], "lfn")

    def test_create_dupe(self):
        m1 = replicas.create_mapping(user_id=self.user.id, lfn="lfn", pfn="pfn", pool="local")
        self.assertRaises(replicas.MappingExists, replicas.create_mapping,
                          user_id=self.user.id, lfn="lfn", pfn="pfn",
                          pool="other")

    def test_find_pfns(self):
        replicas.create_mapping(user_id=self.user.id, lfn="lfn", pfn="pfn1", pool="local")
        replicas.create_mapping(user_id=self.user.id, lfn="lfn", pfn="pfn2", pool="local")
        replicas.create_mapping(user_id=self.user.id, lfn="lfn", pfn="pfn3", pool="local")

        pfns = replicas.find_pfns(self.user.id, "lfn")

        self.assertEquals(len(pfns), 3)

    def test_find_mappings(self):
        replicas.create_mapping(user_id=self.user.id, lfn="lfn1", pfn="pfn1", pool="local")
        replicas.create_mapping(user_id=self.user.id, lfn="lfn2", pfn="pfn2", pool="local")
        replicas.create_mapping(user_id=self.user.id, lfn="lfn3", pfn="pfn3", pool="local")

        mappings = replicas.find_mappings(self.user.id)

        self.assertEquals(len(mappings), 3)

