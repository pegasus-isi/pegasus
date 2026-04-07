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

/** Tests for the Panda style class. */
public class PandaTest {

    private static final class TestPanda extends Panda {
        String gridResource(Job job) throws Exception {
            return constructGridResource(job);
        }

        void setSiteStore(SiteStore store) {
            mSiteStore = store;
        }
    }

    private TestPanda newStyleWithSite(
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

        TestPanda style = new TestPanda();
        style.setSiteStore(store);
        return style;
    }

    @Test
    public void testPandaExtendsGLite() {
        assertThat(GLite.class.isAssignableFrom(Panda.class), is(true));
    }

    @Test
    public void testPandaImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(Panda.class), is(true));
    }

    @Test
    public void testStyleNameConstant() {
        assertThat(Panda.STYLE_NAME, is("PANDA"));
    }

    @Test
    public void testGridResourceKeyNotNull() {
        assertThat(Panda.GRID_RESOURCE_KEY, notNullValue());
    }

    @Test
    public void testInstantiation() {
        Panda style = new Panda();
        assertThat(style, notNullValue());
    }

    @Test
    public void testConstructGridResourceUsesGatewayTypeAndPandaPrefix() throws Exception {
        TestPanda style =
                newStyleWithSite(
                        "panda",
                        GridGateway.TYPE.condor,
                        "submit.example",
                        GridGateway.SCHEDULER_TYPE.slurm);
        Job job = new Job();
        job.setSiteHandle("panda");

        assertThat(style.gridResource(job), is("condor panda submit.example "));
    }

    @Test
    public void testConstructGridResourceThrowsWhenContactMissing() {
        TestPanda style =
                newStyleWithSite(
                        "panda", GridGateway.TYPE.condor, null, GridGateway.SCHEDULER_TYPE.slurm);
        Job job = new Job();
        job.setSiteHandle("panda");

        CondorStyleException e =
                assertThrows(CondorStyleException.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Grid Gateway not specified"));
    }

    @Test
    public void testConstructGridResourceRejectsForkScheduler() {
        TestPanda style =
                newStyleWithSite(
                        "panda",
                        GridGateway.TYPE.batch,
                        "submit.example",
                        GridGateway.SCHEDULER_TYPE.fork);
        Job job = new Job();
        job.setSiteHandle("panda");

        RuntimeException e = assertThrows(RuntimeException.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Please specify a valid scheduler"));
    }

    @Test
    public void testConstructGridResourceNullGatewayCurrentlyThrowsNullPointerException() {
        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle("missing");
        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestPanda style = new TestPanda();
        style.setSiteStore(store);

        Job job = new Job();
        job.setSiteHandle("missing");

        assertThrows(NullPointerException.class, () -> style.gridResource(job));
    }
}
