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
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Default replica selector. */
public class DefaultTest {

    private Default mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Default(PegasusProperties.nonSingletonInstance());
    }

    @Test
    public void testDescription() {
        assertEquals("Default", mSelector.description(), "Description should be 'Default'");
    }

    @Test
    public void testSelectReplicaPreferredSite() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "site1", false);
        assertNotNull(rce, "Selected replica should not be null");
        assertEquals("site1", rce.getResourceHandle(), "Should prefer site1");
    }

    @Test
    public void testSelectReplicaFallsBackToRandom() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "site1", false);
        assertNotNull(rce, "Should fall back to a random replica when preferred site not found");
    }

    @Test
    public void testSelectReplicaThrowsWhenEmpty() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        // only file URL from non-preferred, non-local site - will be removed
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "remote-site"));

        assertThrows(
                RuntimeException.class,
                () -> mSelector.selectReplica(rl, "site1", false),
                "Should throw when no valid replicas remain");
    }

    @Test
    public void testSelectAndOrderReplicasPreferredFirst() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));

        ReplicaLocation result = mSelector.selectAndOrderReplicas(rl, "site1", false);
        assertNotNull(result, "Ordered replicas should not be null");
        assertEquals("test.txt", result.getLFN());
        assertTrue(result.getPFNCount() >= 1, "Should have at least 1 replica");
        // preferred site should come first after ordering
        List<ReplicaCatalogEntry> pfns = result.getPFNList();
        assertEquals("site1", pfns.get(0).getResourceHandle(), "Preferred site should be first");
    }

    @Test
    public void testSelectAndOrderReplicasFileURLHighPriority() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));
        // File URLs from local site have highest priority when allowed
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "local"));

        ReplicaLocation result = mSelector.selectAndOrderReplicas(rl, "site1", true);
        assertNotNull(result);
        // file URL should be included when allowLocalFileURLs=true
        assertTrue(result.getPFNCount() >= 1);
    }

    @Test
    public void testRemoveFileURLNonPreferredSite() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("file:///tmp/test.txt", "site2");
        assertTrue(
                mSelector.removeFileURL(rce, "site1", false),
                "File URL from non-preferred site should be removed when local not allowed");
    }

    @Test
    public void testRemoveFileURLLocalSiteAllowed() {
        ReplicaCatalogEntry rce =
                new ReplicaCatalogEntry("file:///tmp/test.txt", ReplicaSelector.LOCAL_SITE_HANDLE);
        assertFalse(
                mSelector.removeFileURL(rce, "site1", true),
                "File URL from local site should be kept when local URLs allowed");
    }

    @Test
    public void testRemoveFileURLNotFileURL() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2");
        assertFalse(
                mSelector.removeFileURL(rce, "site1", false),
                "Non-file URLs should never be removed by removeFileURL");
    }
}
