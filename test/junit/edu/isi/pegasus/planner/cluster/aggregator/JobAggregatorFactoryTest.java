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
package edu.isi.pegasus.planner.cluster.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the JobAggregatorFactory class constants and structure. */
public class JobAggregatorFactoryTest {

    public static class TestAggregator implements JobAggregator {
        private ADag mDag;
        private PegasusBag mBag;

        @Override
        public void initialize(ADag dag, PegasusBag bag) {
            mDag = dag;
            mBag = bag;
        }

        @Override
        public AggregatedJob constructAbstractAggregatedJob(List jobs, String name, String id) {
            return null;
        }

        @Override
        public void makeAbstractAggregatedJobConcrete(AggregatedJob job) {}

        @Override
        public boolean topologicalOrderingRequired() {
            return false;
        }

        @Override
        public void setAbortOnFirstJobFailure(boolean fail) {}

        @Override
        public boolean abortOnFristJobFailure() {
            return false;
        }

        @Override
        public boolean entryNotInTC(String site) {
            return false;
        }

        @Override
        public String getClusterExecutableLFN() {
            return "test";
        }

        @Override
        public String getClusterExecutableBasename() {
            return "test";
        }

        public ADag getDag() {
            return mDag;
        }

        public PegasusBag getBag() {
            return mBag;
        }
    }

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                JobAggregatorFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.cluster.aggregator"));
    }

    @Test
    public void testSeqExecClassConstant() {
        assertThat(JobAggregatorFactory.SEQ_EXEC_CLASS, is("SeqExec"));
    }

    @Test
    public void testMPIExecClassConstant() {
        assertThat(JobAggregatorFactory.MPI_EXEC_CLASS, is("MPIExec"));
    }

    @Test
    public void testAWSBatchShortnameConstant() {
        assertThat(JobAggregatorFactory.AWS_BATCH_SHORTNAME, is("aws-batch"));
    }

    @Test
    public void testAWSBatchImplementingClassConstant() {
        assertThat(
                JobAggregatorFactory.AWS_BATCH_IMPLEMENTING_CLASS,
                is(AWSBatch.class.getCanonicalName()));
    }

    @Test
    public void testFactoryClassIsPublic() {
        int modifiers = JobAggregatorFactory.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testLoadInstanceMethodExists() throws NoSuchMethodException {
        // verify the static loadInstance(String, ADag, PegasusBag) method exists
        assertThat(
                JobAggregatorFactory.class.getMethod(
                        "loadInstance",
                        String.class,
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.PegasusBag.class),
                notNullValue());
    }

    @Test
    public void testLoadInstanceWithNullPropertiesInBagThrows() {
        PegasusBag bag = new PegasusBag();

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () -> JobAggregatorFactory.loadInstance("SeqExec", new ADag(), bag));

        assertThat(exception.getMessage().contains("Invalid properties passed"), is(true));
    }

    @Test
    public void testLoadInstanceWithNullClassnameThrows() {
        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () -> JobAggregatorFactory.loadInstance(null, new ADag(), bag()));

        assertThat(exception.getMessage().contains("Invalid class specified to load"), is(true));
    }

    @Test
    public void testLoadInstanceWithUnknownClassWrapsFailure() {
        JobAggregatorFactoryException exception =
                assertThrows(
                        JobAggregatorFactoryException.class,
                        () ->
                                JobAggregatorFactory.loadInstance(
                                        "does.not.Exist", new ADag(), bag()));

        assertThat(exception.getMessage().contains("Instantiating JobAggregator"), is(true));
    }

    @Test
    public void testLoadInstanceLoadsExplicitClassAndInitializesIt() {
        ADag dag = new ADag();
        PegasusBag bag = bag();

        JobAggregator aggregator =
                JobAggregatorFactory.loadInstance(TestAggregator.class.getName(), dag, bag);

        assertThat(aggregator, instanceOf(TestAggregator.class));
        assertThat(((TestAggregator) aggregator).getDag(), sameInstance(dag));
        assertThat(((TestAggregator) aggregator).getBag(), sameInstance(bag));
    }

    private PegasusBag bag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        return bag;
    }
}
