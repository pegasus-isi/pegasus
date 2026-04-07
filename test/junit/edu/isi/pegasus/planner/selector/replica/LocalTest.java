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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertThat(mSelector.description(), notNullValue());
        assertThat(mSelector.description(), not(isEmptyString()));
    }

    @Test
    public void testSanityCheckErrorMessagePrefix() {
        assertThat(Local.SANITY_CHECK_ERROR_MESSAGE_PREFIX, notNullValue());
    }

    @Test
    public void testSelectReplicaLocalFileURL() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", "local"));

        // local preferred site with allow local file URLs
        ReplicaCatalogEntry rce = mSelector.selectReplica(rl, "local", true);
        assertThat(rce, notNullValue());
        assertThat(rce.getPFN(), equalTo("file:///tmp/test.txt"));
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
        assertThat(result, notNullValue());
        assertThat(result.getPFNCount(), greaterThanOrEqualTo(1));
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
        assertThat(desc.toLowerCase(), containsString("local"));
    }

    @Test
    public void testDescriptionExactValue() {
        assertThat(mSelector.description(), equalTo("Local from submit host"));
    }

    @Test
    public void testSelectReplicaSkipsEntriesWithoutSiteHandle() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("file:///tmp/test.txt", (String) null));

        assertThrows(RuntimeException.class, () -> mSelector.selectReplica(rl, "local", true));
    }

    @Test
    public void testSelectAndOrderReplicasFallsBackWhenNoPreferredSiteMatches() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test.txt");
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site1/test.txt", "site1"));
        rl.addPFN(new ReplicaCatalogEntry("gsiftp://site2/test.txt", "site2"));

        ReplicaLocation result = mSelector.selectAndOrderReplicas(rl, "local", true);

        assertThat(result.getPFNCount(), equalTo(1));
        assertThat(result.getLFN(), equalTo("test.txt"));
    }
}
