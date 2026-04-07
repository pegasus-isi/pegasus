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
package edu.isi.pegasus.planner.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.cluster.aggregator.MPIExec;
import edu.isi.pegasus.planner.cluster.aggregator.SeqExec;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JobAggregator interface. Exercises known constants and verifies that concrete
 * implementations conform to the interface.
 */
public class JobAggregatorTest {

    @Test
    public void testVersionConstantNotNull() {
        assertThat(JobAggregator.VERSION, notNullValue());
    }

    @Test
    public void testVersionConstantNotEmpty() {
        assertThat(JobAggregator.VERSION.isEmpty(), is(false));
    }

    @Test
    public void testSeqExecImplementsJobAggregator() {
        SeqExec seqExec = new SeqExec();
        assertThat(seqExec, instanceOf(JobAggregator.class));
    }

    @Test
    public void testMPIExecImplementsJobAggregator() {
        MPIExec mpiExec = new MPIExec();
        assertThat(mpiExec, instanceOf(JobAggregator.class));
    }

    @Test
    public void testSeqExecCanBeInstantiated() {
        assertDoesNotThrow(SeqExec::new);
    }

    @Test
    public void testMPIExecCanBeInstantiated() {
        assertDoesNotThrow(MPIExec::new);
    }

    @Test
    public void testSeqExecAndMPIExecAreDifferentClasses() {
        assertThat(SeqExec.class, not(equalTo(MPIExec.class)));
    }

    @Test
    public void testJobAggregatorIsInterface() {
        assertThat(JobAggregator.class.isInterface(), is(true));
    }

    @Test
    public void testVersionConstantMatchesExpectedValue() {
        assertThat(JobAggregator.VERSION, is("1.5"));
    }

    @Test
    public void testJobAggregatorDeclaresExpectedMethods() {
        List<String> methodNames =
                Arrays.asList(
                        "initialize",
                        "constructAbstractAggregatedJob",
                        "makeAbstractAggregatedJobConcrete",
                        "topologicalOrderingRequired",
                        "setAbortOnFirstJobFailure",
                        "abortOnFristJobFailure",
                        "entryNotInTC",
                        "getClusterExecutableLFN",
                        "getClusterExecutableBasename");

        for (String methodName : methodNames) {
            assertThat(
                    Arrays.stream(JobAggregator.class.getDeclaredMethods())
                            .map(Method::getName)
                            .anyMatch(methodName::equals),
                    is(true));
        }
    }
}
