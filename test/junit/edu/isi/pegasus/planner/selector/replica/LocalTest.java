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

/** Tests for the Local replica selector. */
public class LocalTest {

    private Local mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Local(PegasusProperties.nonSingletonInstance());
    }

    @Test
    public void testDescription() {
        assertNotNull(mSelector.description(), "Description should not be null");
        assertFalse(mSelector.description().isEmpty(), "Description should not be empty");
    }

    @Test
    public void testSanityCheckErrorMessagePrefix() {
        assertNotNull(
                Local.SANITY_CHECK_ERROR_MESSAGE_PREFIX, "Error message prefix should not be null");
    }

    @Test
    public void testSelectReplicaLocalFileURL() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "local"));

        // local preferred site with allow local file URLs
        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "local", true);
        assertNotNull(rce, "Should select local file URL");
        assertEquals("file:///tmp/test.txt", rce.getPFN(), "Should return the local file URL");
    }

    @Test
    public void testSelectReplicaSanityCheckFails() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "local"));

        // non-local preferred site with allowLocalFileURLs=false should throw
        assertThrows(
                RuntimeException.class,
                () -> mSelector.selectReplica(rl, "site1", false),
                "Should throw when preferred site is not local and local URLs not allowed");
    }

    @Test
    public void testSelectReplicaNoLocalFileURL() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://local/test.txt", "local"));

        // No file:// URLs at local site - should throw
        assertThrows(
                RuntimeException.class,
                () -> mSelector.selectReplica(rl, "local", true),
                "Should throw when no local file URLs found");
    }

    @Test
    public void testSelectAndOrderReplicasLocalSite() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://local/test.txt", "local"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        // with local preferred site
        ReplicaLocation result = mSelector.selectAndOrderReplicas(rl, "local", true);
        assertNotNull(result, "Result should not be null");
        assertTrue(result.getPFNCount() >= 1, "Should return at least 1 replica");
    }

    @Test
    public void testSelectAndOrderReplicasSanityCheckFails() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));

        assertThrows(
                RuntimeException.class,
                () -> mSelector.selectAndOrderReplicas(rl, "site1", false),
                "Should throw when non-local preferred site and local URLs disallowed");
    }

    @Test
    public void testDescriptionContainsLocalKeyword() {
        String desc = mSelector.description();
        assertTrue(
                desc.toLowerCase().contains("local"), "Description should mention 'local' keyword");
    }
}
