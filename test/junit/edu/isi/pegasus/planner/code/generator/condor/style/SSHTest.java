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
package edu.isi.pegasus.planner.code.generator.condor.style;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import org.junit.jupiter.api.Test;

/** Tests for the SSH style class. */
public class SSHTest {

    private static final class TestSSH extends SSH {
        String gridResource(Job job) throws Exception {
            return constructGridResource(job);
        }

        void setSiteStore(SiteStore store) {
            mSiteStore = store;
        }
    }

    private TestSSH newStyleWithSite(
            String siteHandle,
            GridGateway.TYPE gatewayType,
            String contact,
            GridGateway.SCHEDULER_TYPE scheduler) {
        GridGateway gateway = new GridGateway(gatewayType, contact, scheduler);
        gateway.setJobType(GridGateway.JOB_TYPE.compute);

        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle(siteHandle);
        site.addGridGateway(gateway);

        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestSSH style = new TestSSH();
        style.setSiteStore(store);
        return style;
    }

    @Test
    public void testSSHExtendsGLite() {
        assertThat(GLite.class.isAssignableFrom(SSH.class), is(true));
    }

    @Test
    public void testSSHImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(SSH.class), is(true));
    }

    @Test
    public void testStyleNameConstant() {
        assertThat(SSH.STYLE_NAME, is("SSH"));
    }

    @Test
    public void testGridResourceKeyNotNull() {
        assertThat(SSH.GRID_RESOURCE_KEY, notNullValue());
    }

    @Test
    public void testInstantiation() {
        SSH style = new SSH();
        assertThat(style, notNullValue());
    }

    @Test
    public void testConstructGridResourceUsesGatewayTypeSchedulerAndContact() throws Exception {
        TestSSH style =
                newStyleWithSite(
                        "ssh",
                        GridGateway.TYPE.batch,
                        "user@submit.example",
                        GridGateway.SCHEDULER_TYPE.slurm);
        Job job = new Job();
        job.setSiteHandle("ssh");

        assertThat(style.gridResource(job), is("batch slurm user@submit.example "));
    }

    @Test
    public void testConstructGridResourceThrowsWhenContactMissing() {
        TestSSH style =
                newStyleWithSite(
                        "ssh", GridGateway.TYPE.batch, null, GridGateway.SCHEDULER_TYPE.slurm);
        Job job = new Job();
        job.setSiteHandle("ssh");

        CondorStyleException e =
                assertThrows(CondorStyleException.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Grid Gateway not specified"));
    }

    @Test
    public void testConstructGridResourceRejectsForkScheduler() {
        TestSSH style =
                newStyleWithSite(
                        "ssh",
                        GridGateway.TYPE.batch,
                        "user@submit.example",
                        GridGateway.SCHEDULER_TYPE.fork);
        Job job = new Job();
        job.setSiteHandle("ssh");

        RuntimeException e = assertThrows(RuntimeException.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Please specify a valid scheduler"));
    }

    @Test
    public void testConstructGridResourceNullGatewayCurrentlyThrowsNullPointerException() {
        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle("missing");
        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestSSH style = new TestSSH();
        style.setSiteStore(store);

        Job job = new Job();
        job.setSiteHandle("missing");

        assertThrows(NullPointerException.class, () -> style.gridResource(job));
    }
}
