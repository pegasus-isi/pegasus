/*
 *
 *   Copyright 2007-2020 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.catalog.replica.classes;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for ReplicaStore
 *
 * @author Karan Vahi
 */
public class ReplicaStoreTest {

    private TestSetup mTestSetup;

    public ReplicaStoreTest() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterClass
    public static void tearDownClass() {}

    @After
    public void tearDown() {}

    @Test
    public void testSingleReplica() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n"
                        + "replicas:\n"
                        + "  # matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
        assertNotNull(store);
        assertEquals(1, store.getLFNCount());
        testBasicReplicaLocation(
                store.get("f.a"), "f.a", "file:///Volumes/data/input/f.a", "local");
    }

    @Test
    public void testMultipleReplicas() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n"
                        + "replicas:\n"
                        + "  # matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"\n"
                        + "  # matches \"f.b\"\n"
                        + "  - lfn: \"f.b\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.b\"\n"
                        + "        site: \"isi\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
        assertNotNull(store);
        assertEquals(2, store.getLFNCount());
        testBasicReplicaLocation(
                store.get("f.a"), "f.a", "file:///Volumes/data/input/f.a", "local");
        testBasicReplicaLocation(store.get("f.b"), "f.b", "file:///Volumes/data/input/f.b", "isi");
    }

    @Test
    public void testSingleReplicaWithChecksum() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n"
                        + "replicas:\n"
                        + "  # matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"\n"
                        + "    checksum:\n"
                        + "      sha256: \"a08d9d7769cffb96a910a4b6c2be7bfd85d461c9\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
        assertNotNull(store);
        assertEquals(1, store.getLFNCount());
        ReplicaLocation rl = store.get("f.a");
        assertNotNull(rl);
        assertEquals("f.a", rl.getLFN());
        ReplicaCatalogEntry rce = rl.getPFN(0);
        ReplicaCatalogEntry expected = new ReplicaCatalogEntry("file:///Volumes/data/input/f.a");
        expected.addAttribute("site", "local");
        assertEquals(expected, rce);
        assertEquals("sha256", rl.getMetadata(Metadata.CHECKSUM_TYPE_KEY));
        assertEquals(
                "a08d9d7769cffb96a910a4b6c2be7bfd85d461c9",
                rl.getMetadata(Metadata.CHECKSUM_VALUE_KEY));
    }

    @Test
    public void testSingleReplicaWithChecksumAndMetadata() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n"
                        + "replicas:\n"
                        + "  # matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"\n"
                        + "    checksum:\n"
                        + "      sha256: \"a08d9d7769cffb96a910a4b6c2be7bfd85d461c9\"\n"
                        + "    metadata:\n"
                        + "      user: \"karan\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
        assertNotNull(store);
        assertEquals(1, store.getLFNCount());
        ReplicaLocation rl = store.get("f.a");
        assertNotNull(rl);
        assertEquals("f.a", rl.getLFN());
        ReplicaCatalogEntry rce = rl.getPFN(0);
        ReplicaCatalogEntry expected = new ReplicaCatalogEntry("file:///Volumes/data/input/f.a");
        expected.addAttribute("site", "local");
        assertEquals(expected, rce);
        assertEquals("sha256", rl.getMetadata(Metadata.CHECKSUM_TYPE_KEY));
        assertEquals(
                "a08d9d7769cffb96a910a4b6c2be7bfd85d461c9",
                rl.getMetadata(Metadata.CHECKSUM_VALUE_KEY));
        assertEquals("karan", rl.getMetadata("user"));
    }

    @Test(expected = ReplicaCatalogException.class)
    public void replicaWithRegexTrue() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n"
                        + "replicas:\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    regex: true\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
    }

    @Test(expected = ReplicaCatalogException.class)
    public void replicaWithoutLFN() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n"
                        + "replicas:\n"
                        + "  - pfns:\n"
                        + "     - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "       site: \"local\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
    }

    @Test(expected = ReplicaCatalogException.class)
    public void replicaWithoutPFN() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "pegasus: \"5.0\"\n" + "replicas:\n" + "  - lfn: \"f.a\"\n" + "    site: \"local\"";

        ReplicaStore store = mapper.readValue(test, ReplicaStore.class);
    }

    private void testBasicReplicaLocation(
            ReplicaLocation actual, String lfn, String pfn, String site) {
        assertNotNull(actual);
        assertEquals(lfn, actual.getLFN());
        ReplicaCatalogEntry rce = actual.getPFN(0);
        ReplicaCatalogEntry expected = new ReplicaCatalogEntry(pfn);
        expected.addAttribute("site", site);
    }
}
