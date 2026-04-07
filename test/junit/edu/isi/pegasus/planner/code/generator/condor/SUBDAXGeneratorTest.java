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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.namespace.Dagman;
import java.lang.reflect.Field;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Tests for the SUBDAXGenerator class constants and structure. */
public class SUBDAXGeneratorTest {

    private static final class TestableSUBDAXGenerator extends SUBDAXGenerator {
        String basename(String prefix, String suffix) {
            return getBasename(prefix, suffix);
        }

        String cacheFile(PlannerOptions options, String label, String index) {
            return getCacheFile(options, label, index);
        }

        String cacheFileName(PlannerOptions options, String label, String index) {
            return getCacheFileName(options, label, index);
        }

        String workflowFileName(PlannerOptions options, String label, String index, String suffix) {
            return getWorkflowFileName(options, label, index, suffix);
        }

        String workflowBasenamePrefix(PlannerOptions options, String label, String index) {
            return getWorkflowFileBasenamePrefix(options, label, index);
        }

        boolean useForPlanner(ADag dag) {
            return computeUseForPlannerFlag(dag);
        }
    }

    @Test
    public void testDefaultSubdaxCategoryKey() {
        assertThat(SUBDAXGenerator.DEFAULT_SUBDAX_CATEGORY_KEY, is("subwf"));
    }

    @Test
    public void testGenerateSubdagKeyword() {
        assertThat(SUBDAXGenerator.GENERATE_SUBDAG_KEYWORD, is(false));
    }

    @Test
    public void testCplannerLogicalName() {
        assertThat(SUBDAXGenerator.CPLANNER_LOGICAL_NAME, is("pegasus-plan"));
    }

    @Test
    public void testCondorDagmanNamespace() {
        assertThat(SUBDAXGenerator.CONDOR_DAGMAN_NAMESPACE, is("condor"));
    }

    @Test
    public void testCondorDagmanLogicalName() {
        assertThat(SUBDAXGenerator.CONDOR_DAGMAN_LOGICAL_NAME, is("dagman"));
    }

    @Test
    public void testNamespace() {
        assertThat(SUBDAXGenerator.NAMESPACE, is("pegasus"));
    }

    @Test
    public void testRetryLogicalName() {
        assertThat(SUBDAXGenerator.RETRY_LOGICAL_NAME, is("pegasus-plan"));
    }

    @Test
    public void testDagmanKnobsConstantMappings() {
        assertThat(
                SUBDAXGenerator.DAGMAN_KNOBS,
                arrayContaining(
                        new String[] {Dagman.MAXPRE_KEY, " -MaxPre "},
                        new String[] {Dagman.MAXPOST_KEY, " -MaxPost "},
                        new String[] {Dagman.MAXJOBS_KEY, " -MaxJobs "},
                        new String[] {Dagman.MAXIDLE_KEY, " -MaxIdle "}));
    }

    @Test
    public void testParseIntHandlesValidAndInvalidValues() {
        assertThat(SUBDAXGenerator.parseInt("42"), is(42));
        assertThat(SUBDAXGenerator.parseInt("-7"), is(-7));
        assertThat(SUBDAXGenerator.parseInt("abc"), is(-1));
        assertThat(SUBDAXGenerator.parseInt(null), is(-1));
    }

    @Test
    public void testConstructDAGManKnobsIncludesOnlyPositiveValues() {
        SUBDAXGenerator generator = new SUBDAXGenerator();
        Job job = new Job();
        job.dagmanVariables.construct(Dagman.MAXPRE_KEY, "3");
        job.dagmanVariables.construct(Dagman.MAXPOST_KEY, "0");
        job.dagmanVariables.construct(Dagman.MAXJOBS_KEY, "-1");
        job.dagmanVariables.construct(Dagman.MAXIDLE_KEY, "8");

        assertThat(generator.constructDAGManKnobs(job), is(" -MaxPre 3 -MaxIdle 8"));
    }

    @Test
    public void testWorkflowBasenamePrefixUsesBasenameOptionWhenPresent() {
        TestableSUBDAXGenerator generator = new TestableSUBDAXGenerator();
        PlannerOptions options = new PlannerOptions();
        options.setBasenamePrefix("inner-flow");

        assertThat(generator.workflowBasenamePrefix(options, "label", "7"), is("inner-flow"));
        assertThat(
                generator.workflowFileName(options, "label", "7", ".cache"),
                is("inner-flow.cache"));
    }

    @Test
    public void testWorkflowBasenamePrefixFallsBackToLabelAndIndex() {
        TestableSUBDAXGenerator generator = new TestableSUBDAXGenerator();
        PlannerOptions options = new PlannerOptions();

        assertThat(generator.workflowBasenamePrefix(options, "label", "7"), is("label-7"));
        assertThat(generator.cacheFileName(options, "label", "7"), is("label-7.cache"));
    }

    @Test
    public void testGetCacheFileUsesSubmitDirectoryAndWorkflowFileName() {
        TestableSUBDAXGenerator generator = new TestableSUBDAXGenerator();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(
                Path.of("build", "subdax-generator-test").toAbsolutePath().toString());
        options.setBasenamePrefix("child");

        String path = generator.cacheFile(options, "ignored", "0");

        assertThat(path.endsWith("child.cache"), is(true));
        assertThat(Path.of(path).isAbsolute(), is(true));
        assertThat(path.contains("subdax-generator-test"), is(true));
    }

    @Test
    public void testComputeUseForPlannerFlagUsesDaxVersionThresholdAndSuffixStripping() {
        TestableSUBDAXGenerator generator = new TestableSUBDAXGenerator();

        ADag oldDag = new ADag();
        oldDag.setDAXVersion("5.0.3");
        assertThat(generator.useForPlanner(oldDag), is(false));

        ADag thresholdDag = new ADag();
        thresholdDag.setDAXVersion("5.0.4rc1");
        assertThat(generator.useForPlanner(thresholdDag), is(true));

        ADag nullVersionDag = new ADag();
        assertThat(generator.useForPlanner(nullVersionDag), is(false));
    }

    @Test
    public void testCacheFileSuffixConstant() throws Exception {
        Field field = SUBDAXGenerator.class.getDeclaredField("CACHE_FILE_SUFFIX");
        field.setAccessible(true);

        assertThat(field.get(null), is(".cache"));
    }
}
