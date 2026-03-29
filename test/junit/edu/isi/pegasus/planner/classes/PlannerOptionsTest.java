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

import java.util.Collection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PlannerOptionsTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructor() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getBaseSubmitDirectory(), is("."));
        assertThat(opts.getRelativeDirectory(), nullValue());
        assertThat(opts.getDAX(), nullValue());
        assertFalse(opts.getForce());
        assertFalse(opts.getForceReplan());
        assertFalse(opts.submitToScheduler());
        assertFalse(opts.getHelp());
        assertFalse(opts.generateRandomDirectory());
        assertFalse(opts.partOfDeferredRun());
        assertThat(opts.getLoggingLevel(), is(PlannerOptions.DEFAULT_LOGGING_LEVEL));
        assertThat(
                opts.getNumberOfRescueTries(), is(PlannerOptions.DEFAULT_NUMBER_OF_RESCUE_TRIES));
        assertThat(opts.getVOGroup(), is("pegasus"));
        assertThat(opts.getExecutionSites(), empty());
        assertThat(opts.getOutputSites(), empty());
        assertThat(opts.getCacheFiles(), empty());
        assertThat(opts.getInheritedRCFiles(), empty());
    }

    @Test
    public void testSetAndGetDAX() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSanitizePath(false);
        opts.setDAX("/tmp/test.dax");
        assertThat(opts.getDAX(), is("/tmp/test.dax"));
    }

    @Test
    public void testSetAndGetForce() {
        PlannerOptions opts = new PlannerOptions();
        opts.setForce(true);
        assertTrue(opts.getForce());
        opts.setForce(false);
        assertFalse(opts.getForce());
    }

    @Test
    public void testSetAndGetForceReplan() {
        PlannerOptions opts = new PlannerOptions();
        opts.setForceReplan(true);
        assertTrue(opts.getForceReplan());
    }

    @Test
    public void testSetAndGetSubmitToScheduler() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSubmitToScheduler(true);
        assertTrue(opts.submitToScheduler());
    }

    @Test
    public void testSetAndGetHelp() {
        PlannerOptions opts = new PlannerOptions();
        opts.setHelp(true);
        assertTrue(opts.getHelp());
    }

    @Test
    public void testIncrementAndDecrementLoggingLevel() {
        PlannerOptions opts = new PlannerOptions();
        int initial = opts.getLoggingLevel();
        opts.incrementLogging();
        assertThat(opts.getLoggingLevel(), is(initial + 1));
        opts.decrementLogging();
        assertThat(opts.getLoggingLevel(), is(initial));
    }

    @Test
    public void testSetLoggingLevelString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setLoggingLevel("5");
        assertThat(opts.getLoggingLevel(), is(5));
    }

    @Test
    public void testSetLoggingLevelEmptyString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setLoggingLevel("");
        // empty string defaults to 1
        assertThat(opts.getLoggingLevel(), is(1));
    }

    @Test
    public void testSetAndGetBasenamePrefix() {
        PlannerOptions opts = new PlannerOptions();
        opts.setBasenamePrefix("myworkflow");
        assertThat(opts.getBasenamePrefix(), is("myworkflow"));
    }

    @Test
    public void testSetAndGetJobnamePrefix() {
        PlannerOptions opts = new PlannerOptions();
        opts.setJobnamePrefix("job_");
        assertThat(opts.getJobnamePrefix(), is("job_"));
    }

    @Test
    public void testSetAndGetVOGroup() {
        PlannerOptions opts = new PlannerOptions();
        opts.setVOGroup("atlas");
        assertThat(opts.getVOGroup(), is("atlas"));
    }

    @Test
    public void testSetAndGetNumberOfRescueTries() {
        PlannerOptions opts = new PlannerOptions();
        opts.setNumberOfRescueTries(5);
        assertThat(opts.getNumberOfRescueTries(), is(5));
    }

    @Test
    public void testSetNumberOfRescueTriesFromString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setNumberOfRescueTries("10");
        assertThat(opts.getNumberOfRescueTries(), is(10));
    }

    @Test
    public void testSetAndGetClusteringTechnique() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getClusteringTechnique(), nullValue());
        opts.setClusteringTechnique("horizontal");
        assertThat(opts.getClusteringTechnique(), is("horizontal"));
    }

    @Test
    public void testSetAndGetExecutionSitesFromString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setExecutionSites("siteA,siteB");
        Collection<String> sites = opts.getExecutionSites();
        assertThat(sites, hasSize(2));
        assertThat(sites, containsInAnyOrder("siteA", "siteB"));
    }

    @Test
    public void testSetAndGetOutputSitesFromString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setOutputSites("output1,output2");
        Collection<String> sites = opts.getOutputSites();
        assertThat(sites, hasSize(2));
        assertThat(sites, containsInAnyOrder("output1", "output2"));
    }

    @Test
    public void testAddOutputSite() {
        PlannerOptions opts = new PlannerOptions();
        opts.addOutputSite("siteX");
        assertThat(opts.getOutputSites(), hasItem("siteX"));
    }

    @Test
    public void testDoStageOutWithOutputSite() {
        PlannerOptions opts = new PlannerOptions();
        assertFalse(opts.doStageOut());
        opts.addOutputSite("siteX");
        assertTrue(opts.doStageOut());
    }

    @Test
    public void testDoStageOutWithOutputDirectory() {
        PlannerOptions opts = new PlannerOptions();
        assertFalse(opts.doStageOut());
        opts.setOutputDirectory("/some/output/dir");
        assertTrue(opts.doStageOut());
    }

    @Test
    public void testDoStageOutWithOutputMap() {
        PlannerOptions opts = new PlannerOptions();
        assertFalse(opts.doStageOut());
        opts.setOutputMap("/some/output.map");
        assertTrue(opts.doStageOut());
    }

    @Test
    public void testSetAndGetOutputDirectory() {
        PlannerOptions opts = new PlannerOptions();
        opts.setOutputDirectory("/data/output");
        assertThat(opts.getOutputDirectory(), is("/data/output"));
    }

    @Test
    public void testSetAndGetOutputMap() {
        PlannerOptions opts = new PlannerOptions();
        opts.setOutputMap("/data/output.map");
        assertThat(opts.getOutputMap(), is("/data/output.map"));
    }

    @Test
    public void testSetRandomDir() {
        PlannerOptions opts = new PlannerOptions();
        assertFalse(opts.generateRandomDirectory());
        opts.setRandomDir("mydir");
        assertTrue(opts.generateRandomDirectory());
        assertThat(opts.getRandomDir(), is("mydir"));
        assertThat(opts.getRandomDirName(), is("mydir"));
        assertTrue(opts.optionalArgSet());
    }

    @Test
    public void testSetRandomDirEmptyString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setRandomDir("");
        assertTrue(opts.generateRandomDirectory());
        // empty string → optional arg NOT set
        assertFalse(opts.optionalArgSet());
        // getRandomDirName returns the dir (could be empty) since flag is set
        assertThat(opts.getRandomDirName(), is(""));
    }

    @Test
    public void testGetRandomDirNameWhenNotSet() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getRandomDirName(), nullValue());
    }

    @Test
    public void testSetAndGetCleanupOption() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getCleanup(), nullValue());
        opts.setCleanup(PlannerOptions.CLEANUP_OPTIONS.inplace);
        assertThat(opts.getCleanup(), is(PlannerOptions.CLEANUP_OPTIONS.inplace));
    }

    @Test
    public void testSetCleanupFromString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setCleanup("leaf");
        assertThat(opts.getCleanup(), is(PlannerOptions.CLEANUP_OPTIONS.leaf));
    }

    @Test
    public void testSetAndGetConfFile() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getConfFile(), nullValue());
        opts.setConfFile("/etc/pegasus.conf");
        assertThat(opts.getConfFile(), is("/etc/pegasus.conf"));
    }

    @Test
    public void testSetAndGetPartOfDeferredRun() {
        PlannerOptions opts = new PlannerOptions();
        assertFalse(opts.partOfDeferredRun());
        opts.setPartOfDeferredRun(true);
        assertTrue(opts.partOfDeferredRun());
    }

    @Test
    public void testSetAndGetFinalOutputAsJSON() {
        PlannerOptions opts = new PlannerOptions();
        assertFalse(opts.logFinalOutputAsJSON());
        opts.setFinalOutputAsJSON(true);
        assertTrue(opts.logFinalOutputAsJSON());
    }

    @Test
    public void testAddAndGetForwardOptions() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getForwardOptions(), empty());
        opts.addToForwardOptions("verbose=true");
        assertThat(opts.getForwardOptions(), hasSize(1));
        assertThat(opts.getForwardOptions().get(0).getKey(), is("verbose"));
    }

    @Test
    public void testAddAndGetNonStandardJavaOptions() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getNonStandardJavaOptions(), empty());
        opts.addToNonStandardJavaOptions("mx1024m");
        assertThat(opts.getNonStandardJavaOptions(), hasItem("-Xmx1024m"));
    }

    @Test
    public void testStagingSiteMappingsWithExplicitKey() {
        PlannerOptions opts = new PlannerOptions();
        opts.addToStagingSitesMappings("condorpool=staging");
        assertThat(opts.getStagingSite("condorpool"), is("staging"));
    }

    @Test
    public void testStagingSiteMappingsWithStarNotation() {
        PlannerOptions opts = new PlannerOptions();
        // single value without '=' means wildcard
        opts.addToStagingSitesMappings("myStagingSite");
        // any execution site should resolve to wildcard staging site
        assertThat(opts.getStagingSite("anysite"), is("myStagingSite"));
    }

    @Test
    public void testGetStagingSiteReturnsNullWhenNotMapped() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getStagingSite("unmapped"), nullValue());
    }

    @Test
    public void testSetOriginalArgString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setOriginalArgString(new String[] {"--dax", "test.dax", "--sites", "local"});
        assertThat(opts.getOriginalArgString(), containsString("--dax"));
        assertThat(opts.getOriginalArgString(), containsString("test.dax"));
    }

    @Test
    public void testSetAndGetBaseSubmitDirectory() {
        PlannerOptions opts = new PlannerOptions();
        opts.setBaseSubmitDirectory("/submit");
        assertThat(opts.getBaseSubmitDirectory(), is("/submit"));
    }

    @Test
    public void testSetAndGetRelativeDirectory() {
        PlannerOptions opts = new PlannerOptions();
        opts.setRelativeDirectory("run001");
        assertThat(opts.getRelativeDirectory(), is("run001"));
        // getRelativeSubmitDirectory falls back to relativeDir when relativeSubmitDir not set
        assertThat(opts.getRelativeSubmitDirectory(), is("run001"));
    }

    @Test
    public void testSetRelativeSubmitDirectoryOverridesRelativeDir() {
        PlannerOptions opts = new PlannerOptions();
        opts.setRelativeDirectory("run001");
        opts.setRelativeSubmitDirectory("submit001");
        assertThat(opts.getRelativeSubmitDirectory(), is("submit001"));
        assertThat(opts.getRelativeSubmitDirectoryOption(), is("submit001"));
    }

    @Test
    public void testSubmitDirectoryWithBaseAndRelative() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSanitizePath(false);
        opts.setSubmitDirectory("/base", "run001");
        assertThat(opts.getBaseSubmitDirectory(), is("/base"));
        // getSubmitDirectory combines base + relative
        String submitDir = opts.getSubmitDirectory();
        assertThat(submitDir, containsString("run001"));
    }

    @Test
    public void testSetPropertyValid() {
        PlannerOptions opts = new PlannerOptions();
        // should not throw
        opts.setProperty("pegasus.mode=development");
    }

    @Test
    public void testSetPropertyInvalidThrows() {
        PlannerOptions opts = new PlannerOptions();
        assertThrows(RuntimeException.class, () -> opts.setProperty("invalidformat"));
    }

    @Test
    public void testSetInputDirectories() {
        PlannerOptions opts = new PlannerOptions();
        opts.setInputDirectories("/data/in1,/data/in2");
        assertThat(opts.getInputDirectories(), hasSize(2));
    }

    @Test
    public void testSetAndGetTransformationsDirectory() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSanitizePath(false);
        opts.setTransformationsDirectory("/transformations");
        assertThat(opts.getTransformationsDirectory(), is("/transformations"));
    }

    @Test
    public void testToStringContainsKeyFields() {
        PlannerOptions opts = new PlannerOptions();
        opts.setBasenamePrefix("wf");
        String s = opts.toString();
        assertThat(s, containsString("Planner Options"));
        assertThat(s, containsString("wf"));
    }
}
