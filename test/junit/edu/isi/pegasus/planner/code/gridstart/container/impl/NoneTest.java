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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.transfer.SLS;
import edu.isi.pegasus.planner.transfer.sls.SLSFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for the None container shell wrapper class. */
public class NoneTest {

    private static class TestNone extends None {

        TestNone() {
            this.mSLSFactory =
                    new SLSFactory() {
                        @Override
                        public SLS loadInstance(Job job) {
                            return new SLS() {
                                @Override
                                public void initialize(PegasusBag bag) {}

                                @Override
                                public boolean doesCondorModifications() {
                                    return false;
                                }

                                @Override
                                public String invocationString(Job job, File slsFile) {
                                    return "";
                                }

                                @Override
                                public boolean needsSLSInputTransfers(Job job) {
                                    return false;
                                }

                                @Override
                                public boolean needsSLSOutputTransfers(Job job) {
                                    return false;
                                }

                                @Override
                                public String getSLSInputLFN(Job job) {
                                    return "input.sls";
                                }

                                @Override
                                public String getSLSOutputLFN(Job job) {
                                    return "output.sls";
                                }

                                @Override
                                public Collection determineSLSInputTransfers(
                                        Job job,
                                        String fileName,
                                        edu.isi.pegasus.planner.catalog.site.classes.FileServer
                                                stagingSiteServer,
                                        String stagingSiteDirectory,
                                        String workerNodeDirectory,
                                        boolean onlyContainer) {
                                    return Collections.emptyList();
                                }

                                @Override
                                public Collection determineSLSOutputTransfers(
                                        Job job,
                                        String fileName,
                                        edu.isi.pegasus.planner.catalog.site.classes.FileServer
                                                stagingSiteServer,
                                        String stagingSiteDirectory,
                                        String workerNodeDirectory) {
                                    return Collections.emptyList();
                                }

                                @Override
                                public boolean modifyJobForWorkerNodeExecution(
                                        Job job,
                                        String stagingSiteURLPrefix,
                                        String stagingSitedirectory,
                                        String workerNodeDirectory) {
                                    return false;
                                }

                                @Override
                                public String getDescription() {
                                    return "stub";
                                }
                            };
                        }
                    };
            this.mDoIntegrityChecking = false;
        }

        @Override
        protected StringBuilder inputFilesToPegasusLite(Job job) {
            return new StringBuilder("stage-in\n");
        }

        @Override
        protected StringBuilder outputFilesToPegasusLite(Job job) {
            return new StringBuilder("stage-out\n");
        }

        @Override
        protected StringBuilder enableForIntegrity(Job job, String prefix) {
            return new StringBuilder("integrity\n");
        }

        @Override
        protected StringBuffer slurpInFile(String directory, String file) throws IOException {
            return new StringBuffer("echo clustered\n");
        }
    }

    @Test
    public void testNoneExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(None.class), is(true));
    }

    @Test
    public void testNoneImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(None.class), is(true));
    }

    @Test
    public void testNoneClassExists() {
        assertThat(None.class, notNullValue());
    }

    @Test
    public void testDescribe() {
        assertThat(new None().describe(), is("No container wrapping"));
    }

    @Test
    public void testWrapForJobIncludesExpectedSectionsAndCommand() {
        TestNone wrapper = new TestNone();
        Job job = new Job();
        job.setJobType(Job.CLEANUP_JOB);
        job.setRemoteExecutable("/bin/echo");
        job.setArguments("hello world");

        String result = wrapper.wrap(job);

        assertThat(result, containsString("pegasus_lite_section_start stage_in"));
        assertThat(result, containsString("pegasus_lite_section_start task_execute"));
        assertThat(result, containsString("[Pegasus Lite] Executing the user task"));
        assertThat(result, containsString("/bin/echo hello world"));
        assertThat(result, containsString("job_ec=$?"));
        assertThat(result, containsString("pegasus_lite_section_start stage_out"));
        assertThat(result, containsString("set +e"));
        assertThat(result, containsString("set -e"));
    }

    @Test
    public void testWrapForAggregatedJobEmbedsStdInAndResetsInput() {
        TestNone wrapper = new TestNone();
        wrapper.mSubmitDir = "/tmp";

        AggregatedJob job = new AggregatedJob();
        job.setJobType(Job.CLEANUP_JOB);
        job.setRemoteExecutable("/bin/bash");
        job.setArguments("-s");
        job.setStdIn("cluster.in");
        job.setName("agg");
        job.condorVariables.construct("input", "cluster.in");

        String result = wrapper.wrap(job);

        assertThat(result, containsString("[Pegasus Lite] Executing the user's clustered task"));
        assertThat(result, containsString("/bin/bash -s << EOF"));
        assertThat(result, containsString("echo clustered"));
        assertThat(result, containsString("EOF"));
        assertThat(job.getStdIn(), is(""));
        assertThat(job.condorVariables.get("input"), nullValue());
    }
}
