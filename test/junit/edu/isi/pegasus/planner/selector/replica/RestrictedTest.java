/**
 * Copyright 2007-2013 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.selector.replica;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Restricted replica selector. */
public class RestrictedTest {

    private Restricted mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Restricted(PegasusProperties.nonSingletonInstance());
    }

    @Test
    public void testDescription() {
        assertEquals("Restricted", mSelector.description(), "Description should be 'Restricted'");
    }

    @Test
    public void testSelectReplicaNoPreferredOrIgnored() {
        // Without preferred/ignored config, behaves like Default selector
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "site1", false);
        assertNotNull(rce, "Should select a replica");
    }

    @Test
    public void testSelectReplicaWithPreferredSite() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.replica.site1.prefer.stagein.sites", "site2");
        Restricted selector = new Restricted(props);

        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site3/test.txt", "site3"));

        ReplicaCatalogEntry rce = selector.selectReplica(rl, "site1", false);
        assertNotNull(rce, "Should select a replica");
        assertEquals("site2", rce.getResourceHandle(), "Should prefer configured site2");
    }

    @Test
    public void testSelectReplicaWithIgnoredSite() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.replica.site1.ignore.stagein.sites", "site2");
        Restricted selector = new Restricted(props);

        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site3/test.txt", "site3"));

        ReplicaCatalogEntry rce = selector.selectReplica(rl, "site1", false);
        assertNotNull(rce, "Should select a replica ignoring site2");
        assertEquals("site3", rce.getResourceHandle(), "Should select from site3 (not ignored)");
    }

    @Test
    public void testSelectReplicaThrowsWhenAllIgnored() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.replica.site1.ignore.stagein.sites", "site2");
        Restricted selector = new Restricted(props);

        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        assertThrows(
                RuntimeException.class,
                () -> selector.selectReplica(rl, "site1", false),
                "Should throw when only available site is ignored");
    }

    @Test
    public void testGloballyPreferredSite() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.replica.*.prefer.stagein.sites", "preferred-site");
        Restricted selector = new Restricted(props);

        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://preferred-site/test.txt", "preferred-site"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://other-site/test.txt", "other-site"));

        ReplicaCatalogEntry rce = selector.selectReplica(rl, "site1", false);
        assertNotNull(rce, "Should select a replica");
        assertEquals(
                "preferred-site", rce.getResourceHandle(), "Should select globally preferred site");
    }

    @Test
    public void testSelectReplicaFileURLFromPreferredSiteReturned() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        // file URL from preferred site should be returned immediately
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "site1"));

        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "site1", false);
        assertNotNull(rce);
        assertEquals(
                "file:///tmp/test.txt",
                rce.getPFN(),
                "File URL from preferred site should be returned");
    }
}
