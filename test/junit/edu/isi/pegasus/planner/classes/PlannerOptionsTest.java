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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PlannerOptionsTest {

    @Test
    public void testDefaultConstructor() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.getBaseSubmitDirectory(), is("."));
        assertThat(opts.getRelativeDirectory(), nullValue());
        assertThat(opts.getDAX(), nullValue());
        assertThat(opts.getForce(), is(false));
        assertThat(opts.getForceReplan(), is(false));
        assertThat(opts.submitToScheduler(), is(false));
        assertThat(opts.getHelp(), is(false));
        assertThat(opts.generateRandomDirectory(), is(false));
        assertThat(opts.partOfDeferredRun(), is(false));
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
        assertThat(opts.getForce(), is(true));
        opts.setForce(false);
        assertThat(opts.getForce(), is(false));
    }

    @Test
    public void testSetAndGetForceReplan() {
        PlannerOptions opts = new PlannerOptions();
        opts.setForceReplan(true);
        assertThat(opts.getForceReplan(), is(true));
    }

    @Test
    public void testSetAndGetSubmitToScheduler() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSubmitToScheduler(true);
        assertThat(opts.submitToScheduler(), is(true));
    }

    @Test
    public void testSetAndGetHelp() {
        PlannerOptions opts = new PlannerOptions();
        opts.setHelp(true);
        assertThat(opts.getHelp(), is(true));
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
        assertThat(opts.doStageOut(), is(false));
        opts.addOutputSite("siteX");
        assertThat(opts.doStageOut(), is(true));
    }

    @Test
    public void testDoStageOutWithOutputDirectory() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.doStageOut(), is(false));
        opts.setOutputDirectory("/some/output/dir");
        assertThat(opts.doStageOut(), is(true));
    }

    @Test
    public void testDoStageOutWithOutputMap() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.doStageOut(), is(false));
        opts.setOutputMap("/some/output.map");
        assertThat(opts.doStageOut(), is(true));
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
        assertThat(opts.generateRandomDirectory(), is(false));
        opts.setRandomDir("mydir");
        assertThat(opts.generateRandomDirectory(), is(true));
        assertThat(opts.getRandomDir(), is("mydir"));
        assertThat(opts.getRandomDirName(), is("mydir"));
        assertThat(opts.optionalArgSet(), is(true));
    }

    @Test
    public void testSetRandomDirEmptyString() {
        PlannerOptions opts = new PlannerOptions();
        opts.setRandomDir("");
        assertThat(opts.generateRandomDirectory(), is(true));
        // empty string → optional arg NOT set
        assertThat(opts.optionalArgSet(), is(false));
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
        assertThat(opts.partOfDeferredRun(), is(false));
        opts.setPartOfDeferredRun(true);
        assertThat(opts.partOfDeferredRun(), is(true));
    }

    @Test
    public void testSetAndGetFinalOutputAsJSON() {
        PlannerOptions opts = new PlannerOptions();
        assertThat(opts.logFinalOutputAsJSON(), is(false));
        opts.setFinalOutputAsJSON(true);
        assertThat(opts.logFinalOutputAsJSON(), is(true));
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

    @Test
    public void testSetExecutionSitesFromCollection() {
        PlannerOptions opts = new PlannerOptions();

        opts.setExecutionSites(Arrays.asList("siteA", "siteB", "siteA"));

        assertThat(opts.getExecutionSites(), hasSize(2));
        assertThat(opts.getExecutionSites(), containsInAnyOrder("siteA", "siteB"));
    }

    @Test
    public void testSetCacheFilesAppendsAndSanitizesRelativePaths() {
        PlannerOptions opts = new PlannerOptions();

        opts.setCacheFiles("cache-a.txt");
        opts.setCacheFiles("cache-b.txt");

        assertThat(opts.getCacheFiles(), hasSize(2));
        assertThat(opts.getCacheFiles(), everyItem(startsWith(System.getProperty("user.dir"))));
    }

    @Test
    public void testSetInheritedRCFilesAppendsAndSanitizesRelativePaths() {
        PlannerOptions opts = new PlannerOptions();

        opts.setInheritedRCFiles("rc-a.txt");
        opts.setInheritedRCFiles("rc-b.txt");

        assertThat(opts.getInheritedRCFiles(), hasSize(2));
        assertThat(
                opts.getInheritedRCFiles(), everyItem(startsWith(System.getProperty("user.dir"))));
    }

    @Test
    public void testSetDataReuseSubmitDirsSanitizesRelativePaths() {
        PlannerOptions opts = new PlannerOptions();

        opts.setDataReuseSubmitDirs("reuse-a,reuse-b");

        assertThat(opts.getDataReuseSubmitDirectories(), hasSize(2));
        assertThat(
                opts.getDataReuseSubmitDirectories(),
                everyItem(startsWith(System.getProperty("user.dir"))));
    }

    @Test
    public void testToJVMOptionsIncludesVdsPropsCommandLinePropsAndNonStandardJavaOptions() {
        PlannerOptions opts = new PlannerOptions();
        List<NameValue> vdsProps = new LinkedList<NameValue>();
        NameValue nv = new NameValue();
        nv.setKey("pegasus.test");
        nv.setValue("true");
        vdsProps.add(nv);
        opts.setVDSProperties(vdsProps);
        opts.setProperty("app.mode=dev");
        opts.addToNonStandardJavaOptions("mx1024m");

        String jvmOptions = opts.toJVMOptions();

        assertThat(jvmOptions, containsString("-Dpegasus.test=true"));
        assertThat(jvmOptions, containsString("-Dapp.mode=dev"));
        assertThat(jvmOptions, containsString("-Xmx1024m"));
    }

    @Test
    public void testToOptionsUsesOutputDirectoryForTransformationsDirCurrentBehavior() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSanitizePath(false);
        opts.setSubmitDirectory("/submit");
        opts.setOutputDirectory("/outputs");
        opts.setTransformationsDirectory("/transformations");

        String options = opts.toOptions();

        assertThat(options, containsString("--transformations-dir /outputs"));
        assertThat(options, not(containsString("--transformations-dir /transformations")));
    }

    @Test
    public void testGetCompleteOptionsAppendsDaxPath() {
        PlannerOptions opts = new PlannerOptions();
        opts.setSanitizePath(false);
        opts.setSubmitDirectory("/submit");
        opts.setDAX("/workflows/test.dax");

        String completeOptions = opts.getCompleteOptions();

        assertThat(completeOptions, containsString("--dir /submit"));
        assertThat(completeOptions, endsWith("/workflows/test.dax"));
    }

    @Test
    public void testCloneRetainsForwardOptionsReferenceButClearsVdsProperties() {
        PlannerOptions opts = new PlannerOptions();
        opts.addToForwardOptions("verbose=true");
        NameValue nv = new NameValue();
        nv.setKey("pegasus.test");
        nv.setValue("true");
        opts.setVDSProperties(Collections.singletonList(nv));

        PlannerOptions clone = (PlannerOptions) opts.clone();
        clone.addToForwardOptions("debug=false");

        assertThat(clone.getVDSProperties(), nullValue());
        assertThat(opts.getForwardOptions(), hasSize(2));
        assertThat(clone.getForwardOptions(), sameInstance(opts.getForwardOptions()));
    }
}
