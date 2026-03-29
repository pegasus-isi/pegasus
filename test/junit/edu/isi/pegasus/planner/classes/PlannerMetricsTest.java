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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Date;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PlannerMetricsTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    // -----------------------------------------------------------------------
    // construction defaults
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultExitcodeIsMinusOne() {
        PlannerMetrics pm = new PlannerMetrics();
        assertThat(pm.getExitcode(), is(-1));
    }

    @Test
    public void testDefaultMetricsTypeIsMetrics() {
        PlannerMetrics pm = new PlannerMetrics();
        assertThat(pm.getMetricsType(), is("metrics"));
    }

    @Test
    public void testDefaultUsesPMCIsFalse() {
        PlannerMetrics pm = new PlannerMetrics();
        assertFalse(pm.usesPMC());
    }

    @Test
    public void testDefaultPlannerArgumentsIsEmpty() {
        PlannerMetrics pm = new PlannerMetrics();
        assertThat(pm.getPlannerArguments(), is(""));
    }

    // -----------------------------------------------------------------------
    // UUID accessors
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetRootWorkflowUUID() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setRootWorkflowUUID("root-uuid-123");
        assertThat(pm.getRootWorkflowUUID(), is("root-uuid-123"));
    }

    @Test
    public void testSetAndGetWorkflowUUID() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setWorkflowUUID("wf-uuid-456");
        assertThat(pm.getWorkflowUUID(), is("wf-uuid-456"));
    }

    // -----------------------------------------------------------------------
    // user / vo group
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetUser() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setUser("alice");
        assertThat(pm.getUser(), is("alice"));
    }

    @Test
    public void testSetAndGetVOGroup() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setVOGroup("atlas");
        assertThat(pm.getVOGroup(), is("atlas"));
    }

    // -----------------------------------------------------------------------
    // paths
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetDAX() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setDAX("/data/workflow.dax");
        assertThat(pm.getDAX(), is("/data/workflow.dax"));
    }

    @Test
    public void testSetAndGetProperties() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setProperties("/etc/pegasus.conf");
        assertThat(pm.getProperties(), is("/etc/pegasus.conf"));
    }

    @Test
    public void testSetAndGetBaseSubmitDirectory() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setBaseSubmitDirectory("/submit/base");
        assertThat(pm.getBaseSubmitDirectory(), is("/submit/base"));
    }

    @Test
    public void testSetAndGetRelativeSubmitDirectory() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setRelativeSubmitDirectory("run001");
        assertThat(pm.getRelativeSubmitDirectory(), is("run001"));
    }

    @Test
    public void testSetAndGetMetricsFileLocation() {
        PlannerMetrics pm = new PlannerMetrics();
        File f = new File("/submit/base/run001/wf.metrics");
        pm.setMetricsFileLocationInSubmitDirectory(f);
        assertThat(pm.getMetricsFileLocationInSubmitDirectory(), is(f));
    }

    // -----------------------------------------------------------------------
    // data configuration
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetDataConfiguration() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setDataConfiguration("sharedfs");
        assertThat(pm.getDataConfiguration(), is("sharedfs"));
    }

    // -----------------------------------------------------------------------
    // planner arguments / metrics type
    // -----------------------------------------------------------------------

    @Test
    public void testSetPlannerOptionsFromString() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setPlannerOptions("--dax test.dax --sites local");
        assertThat(pm.getPlannerArguments(), is("--dax test.dax --sites local"));
    }

    @Test
    public void testSetMetricsType() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setMetricsType("custom");
        assertThat(pm.getMetricsType(), is("custom"));
    }

    @Test
    public void testSetMetricsTypeToError() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setMetricsTypeToError();
        assertThat(pm.getMetricsType(), is("error"));
    }

    // -----------------------------------------------------------------------
    // PMC flag
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetUsesPMC() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setUsesPMC(true);
        assertTrue(pm.usesPMC());
        pm.setUsesPMC(false);
        assertFalse(pm.usesPMC());
    }

    // -----------------------------------------------------------------------
    // timing
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetStartTimeDouble() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setStartTime(1234567890.0);
        assertThat(pm.getStartTime(), is(1234567890.0));
    }

    @Test
    public void testSetAndGetStartTimeDate() {
        PlannerMetrics pm = new PlannerMetrics();
        Date d = new Date(1000L); // epoch + 1 second
        pm.setStartTime(d);
        // 1000 ms → 1.0 seconds
        assertThat(pm.getStartTime(), closeTo(1.0, 0.001));
    }

    @Test
    public void testSetAndGetEndTimeDouble() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setEndTime(9876543210.0);
        assertThat(pm.getEndTime(), is(9876543210.0));
    }

    @Test
    public void testSetAndGetEndTimeDate() {
        PlannerMetrics pm = new PlannerMetrics();
        Date d = new Date(2000L);
        pm.setEndTime(d);
        assertThat(pm.getEndTime(), closeTo(2.0, 0.001));
    }

    @Test
    public void testSetAndGetDuration() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setDuration(42.5);
        assertThat(pm.getDuration(), is(42.5));
    }

    // -----------------------------------------------------------------------
    // exitcode
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetExitcode() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setExitcode(0);
        assertThat(pm.getExitcode(), is(0));
    }

    @Test
    public void testSetExitcodeNonZero() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setExitcode(1);
        assertThat(pm.getExitcode(), is(1));
    }

    // -----------------------------------------------------------------------
    // error message
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetErrorMessage() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setErrorMessage("something went wrong");
        assertThat(pm.getErrorMessage(), is("something went wrong"));
    }

    // -----------------------------------------------------------------------
    // workflow metrics association
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetWorkflowMetrics() {
        PlannerMetrics pm = new PlannerMetrics();
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setLabel("test-wf");
        pm.setWorkflowMetrics(wm);
        assertThat(pm.getWorkflowMetrics(), is(wm));
        assertThat(pm.getWorkflowMetrics().getLabel(), is("test-wf"));
    }

    // -----------------------------------------------------------------------
    // application metrics
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetApplicationMetrics() {
        PlannerMetrics pm = new PlannerMetrics();
        Properties props = new Properties();
        props.setProperty("key1", "val1");
        pm.setApplicationMetrics(props);
        assertThat(pm.getApplicationMetrics(), is(props));
        assertThat(pm.getApplicationMetrics().getProperty("key1"), is("val1"));
    }

    // -----------------------------------------------------------------------
    // WF API
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetWFAPI() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setWFAPI("python");
        assertThat(pm.getWFAPI(), is("python"));
    }

    // -----------------------------------------------------------------------
    // toString / toJson
    // -----------------------------------------------------------------------

    @Test
    public void testToStringContainsKnownFields() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setUser("bob");
        pm.setRootWorkflowUUID("root-1");
        pm.setWorkflowUUID("wf-1");
        pm.setWorkflowMetrics(new WorkflowMetrics());
        String s = pm.toString();
        assertThat(s, containsString("bob"));
        assertThat(s, containsString("root-1"));
        assertThat(s, containsString("wf-1"));
    }

    @Test
    public void testToJsonContainsClientField() {
        PlannerMetrics pm = new PlannerMetrics();
        String json = pm.toJson();
        assertThat(json, containsString("pegasus-plan"));
    }

    @Test
    public void testToPrettyJsonIsFormattedJson() {
        PlannerMetrics pm = new PlannerMetrics();
        String json = pm.toPrettyJson();
        // pretty-printed JSON contains newlines
        assertThat(json, containsString("\n"));
        assertThat(json, containsString("pegasus-plan"));
    }

    // -----------------------------------------------------------------------
    // clone
    // -----------------------------------------------------------------------

    @Test
    public void testCloneProducesEqualState() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setUser("carol");
        pm.setVOGroup("cms");
        pm.setDAX("/data/wf.dax");
        pm.setBaseSubmitDirectory("/submit");
        pm.setRelativeSubmitDirectory("run1");
        pm.setProperties("/etc/pegasus.conf");
        pm.setDataConfiguration("condorio");
        pm.setRootWorkflowUUID("root-uuid");
        pm.setWorkflowUUID("wf-uuid");
        pm.setStartTime(100.0);
        pm.setEndTime(200.0);
        pm.setDuration(100.0);
        pm.setExitcode(0);
        pm.setUsesPMC(true);
        pm.setErrorMessage("none");

        PlannerMetrics clone = (PlannerMetrics) pm.clone();

        assertThat(clone.getUser(), is("carol"));
        assertThat(clone.getVOGroup(), is("cms"));
        assertThat(clone.getDAX(), is("/data/wf.dax"));
        assertThat(clone.getBaseSubmitDirectory(), is("/submit"));
        assertThat(clone.getRelativeSubmitDirectory(), is("run1"));
        assertThat(clone.getProperties(), is("/etc/pegasus.conf"));
        assertThat(clone.getDataConfiguration(), is("condorio"));
        assertThat(clone.getRootWorkflowUUID(), is("root-uuid"));
        assertThat(clone.getWorkflowUUID(), is("wf-uuid"));
        assertThat(clone.getStartTime(), is(100.0));
        assertThat(clone.getEndTime(), is(200.0));
        assertThat(clone.getDuration(), is(100.0));
        assertThat(clone.getExitcode(), is(0));
        assertTrue(clone.usesPMC());
        assertThat(clone.getErrorMessage(), is("none"));
    }

    @Test
    public void testCloneIsIndependentOfOriginal() {
        PlannerMetrics pm = new PlannerMetrics();
        pm.setUser("original");
        PlannerMetrics clone = (PlannerMetrics) pm.clone();
        clone.setUser("cloned");
        assertThat(pm.getUser(), is("original"));
    }

    @Test
    public void testCloneWithApplicationMetricsIsIndependent() {
        PlannerMetrics pm = new PlannerMetrics();
        Properties props = new Properties();
        props.setProperty("app", "myapp");
        pm.setApplicationMetrics(props);

        PlannerMetrics clone = (PlannerMetrics) pm.clone();
        assertThat(clone.getApplicationMetrics().getProperty("app"), is("myapp"));

        // mutate clone properties; original should not change
        clone.getApplicationMetrics().setProperty("app", "changed");
        assertThat(pm.getApplicationMetrics().getProperty("app"), is("myapp"));
    }
}
