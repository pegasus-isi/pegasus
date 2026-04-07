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
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the JobAggregatorInstanceFactory class structure. */
public class JobAggregatorInstanceFactoryTest {

    private static final class TestAggregator implements JobAggregator {
        @Override
        public void initialize(ADag dag, PegasusBag bag) {}

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
    }

    @Test
    public void testDefaultConstructor() {
        JobAggregatorInstanceFactory factory = new JobAggregatorInstanceFactory();
        assertThat(factory, notNullValue());
    }

    @Test
    public void testFactoryIsPublicClass() {
        int modifiers = JobAggregatorInstanceFactory.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testInitializeMethodExists() throws NoSuchMethodException {
        assertThat(
                JobAggregatorInstanceFactory.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.PegasusBag.class),
                notNullValue());
    }

    @Test
    public void testLoadInstanceMethodExists() throws NoSuchMethodException {
        assertThat(
                JobAggregatorInstanceFactory.class.getMethod(
                        "loadInstance", edu.isi.pegasus.planner.classes.Job.class),
                notNullValue());
    }

    @Test
    public void testLoadInstanceThrowsWhenNotInitialized() {
        JobAggregatorInstanceFactory factory = new JobAggregatorInstanceFactory();
        edu.isi.pegasus.planner.classes.Job job = new edu.isi.pegasus.planner.classes.Job();
        assertThrows(JobAggregatorFactoryException.class, () -> factory.loadInstance(job));
    }

    @Test
    public void testLoadInstanceUsesJobAggregatorProfileFromJob() throws Exception {
        JobAggregatorInstanceFactory factory = initializedFactoryWithCachedAggregator("seqexec");
        Job job = new Job();
        job.vdsNS.construct(Pegasus.JOB_AGGREGATOR_KEY, "SeqExec");

        assertThat(factory.loadInstance(job), sameInstance(cachedAggregator(factory, "seqexec")));
    }

    @Test
    public void testLoadInstanceFallsBackToDeprecatedCollapserProfile() throws Exception {
        JobAggregatorInstanceFactory factory = initializedFactoryWithCachedAggregator("seqexec");
        Job job = new Job();
        job.vdsNS.construct(Pegasus.COLLAPSER_KEY, "SeqExec");

        assertThat(factory.loadInstance(job), sameInstance(cachedAggregator(factory, "seqexec")));
    }

    @Test
    public void testLoadInstanceFallsBackToPropertiesDefaultAggregator() throws Exception {
        JobAggregatorInstanceFactory factory = initializedFactoryWithCachedAggregator("seqexec");
        Job job = new Job();

        assertThat(factory.loadInstance(job), sameInstance(cachedAggregator(factory, "seqexec")));
    }

    @Test
    public void testLoadInstanceMarksUsesPMCWhenMPIExecIsSelected() throws Exception {
        JobAggregatorInstanceFactory factory = initializedFactoryWithCachedAggregator("mpiexec");
        PegasusBag bag = (PegasusBag) ReflectionTestUtils.getField(factory, "mBag");
        Job job = new Job();
        job.vdsNS.construct(Pegasus.JOB_AGGREGATOR_KEY, "MPIExec");

        factory.loadInstance(job);

        assertThat(bag.get(PegasusBag.USES_PMC), is(Boolean.TRUE));
    }

    private JobAggregatorInstanceFactory initializedFactoryWithCachedAggregator(String key)
            throws Exception {
        JobAggregatorInstanceFactory factory = new JobAggregatorInstanceFactory();
        PegasusBag bag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty("pegasus.clusterer.job.aggregator", "SeqExec");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

        ReflectionTestUtils.setField(factory, "mBag", bag);
        ReflectionTestUtils.setField(factory, "mProps", properties);
        ReflectionTestUtils.setField(factory, "mDAG", new ADag());
        ReflectionTestUtils.setField(factory, "mInitialized", Boolean.TRUE);

        Map<String, JobAggregator> cache = new HashMap<String, JobAggregator>();
        cache.put(key, new TestAggregator());
        ReflectionTestUtils.setField(factory, "mImplementingClassTable", cache);
        return factory;
    }

    private Object cachedAggregator(JobAggregatorInstanceFactory factory, String key)
            throws Exception {
        return ((Map) ReflectionTestUtils.getField(factory, "mImplementingClassTable")).get(key);
    }
}
