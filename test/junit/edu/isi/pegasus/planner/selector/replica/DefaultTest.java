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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
        mSelector.mLogger.logEventStart("test.default.selector", "replica-selector", "default");
    }

    @Test
    public void testDescription() {
        assertThat(mSelector.description(), is("Default"));
    }

    @Test
    public void testSelectReplicaPreferredSite() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "site1", false);
        assertThat(rce, notNullValue());
        assertThat(rce.getResourceHandle(), is("site1"));
    }

    @Test
    public void testSelectReplicaFallsBackToRandom() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "site1", false);
        assertThat(rce, notNullValue());
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
        assertThat(result, notNullValue());
        assertThat(result.getLFN(), is("test.txt"));
        assertThat(result.getPFNCount() >= 1, is(true));
        // preferred site should come first after ordering
        List<ReplicaCatalogEntry> pfns = result.getPFNList();
        assertThat(pfns.get(0).getResourceHandle(), is("site1"));
    }

    @Test
    public void testSelectAndOrderReplicasFileURLHighPriority() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));
        // File URLs from local site have highest priority when allowed
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "local"));

        ReplicaLocation result = mSelector.selectAndOrderReplicas(rl, "site1", true);
        assertThat(result, notNullValue());
        // file URL should be included when allowLocalFileURLs=true
        assertThat(result.getPFNCount() >= 1, is(true));
    }

    @Test
    public void testRemoveFileURLNonPreferredSite() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("file:///tmp/test.txt", "site2");
        assertThat(mSelector.removeFileURL(rce, "site1", false), is(true));
    }

    @Test
    public void testRemoveFileURLLocalSiteAllowed() {
        ReplicaCatalogEntry rce =
                new ReplicaCatalogEntry("file:///tmp/test.txt", ReplicaSelector.LOCAL_SITE_HANDLE);
        assertThat(mSelector.removeFileURL(rce, "site1", true), is(false));
    }

    @Test
    public void testRemoveFileURLNotFileURL() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2");
        assertThat(mSelector.removeFileURL(rce, "site1", false), is(false));
    }

    @Test
    public void testProtectedRemoveFileURLWithNullSiteReturnsTrueForFileURL() {
        assertThat(mSelector.removeFileURL("file:///tmp/test.txt", null, "site1", false), is(true));
    }

    @Test
    public void testSelectAndOrderReplicasSetsPriorityAttributes() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");

        ReplicaCatalogEntry preferred = new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1");
        ReplicaCatalogEntry other = new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2");
        ReplicaCatalogEntry localFile = new ReplicaCatalogEntry("file:///tmp/test.txt", "local");

        rl.addPFN(preferred);
        rl.addPFN(other);
        rl.addPFN(localFile);

        ReplicaLocation result = mSelector.selectAndOrderReplicas(rl, "site1", true);

        assertThat(localFile.getAttribute(ReplicaSelector.PRIORITY_KEY), is("100"));
        assertThat(preferred.getAttribute(ReplicaSelector.PRIORITY_KEY), is("50"));
        assertThat(other.getAttribute(ReplicaSelector.PRIORITY_KEY), is("10"));
        assertThat(result.getPFNCount(), is(3));
    }

    @Test
    public void testWarnForFileURLMethodExists() throws Exception {
        assertThat(
                Default.class
                        .getDeclaredMethod(
                                "warnForFileURL",
                                ReplicaCatalogEntry.class,
                                String.class,
                                Boolean.TYPE)
                        .getReturnType(),
                is(Void.TYPE));
    }
}
