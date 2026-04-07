/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Test class to test runtime clustering
 *
 * @author Rajiv Mayani
 */
public class RuntimeClusteringTest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    private TestSetup mTestSetup;
    private LogManager mLogger;
    private Horizontal mCluster;
    private Method mBestFitMethod;
    private Method mGetRunTimeMethod;

    public RuntimeClusteringTest() {}

    @BeforeEach
    public void setUp() throws NoSuchMethodException {
        mTestSetup = new DefaultTestSetup();
        mCluster = new Horizontal();

        Class[] parameters = new Class[2];
        parameters[0] = List.class;
        parameters[1] = int.class;

        mBestFitMethod = mCluster.getClass().getDeclaredMethod("bestFitBinPack", parameters);
        mBestFitMethod.setAccessible(true);

        mGetRunTimeMethod = mCluster.getClass().getDeclaredMethod("getRunTime", Job.class);
        mGetRunTimeMethod.setAccessible(true);
    }

    @Test
    public void testClusterNum() {
        // Horizontal.bestFitBinPack() internally calls mLogger.log() but mLogger is null
        // (Horizontal requires PegasusBag with initialized LogManager via initialize()).
        // Without it, the call throws via reflection as InvocationTargetException wrapping
        // EmptyStackException. Document this known limitation.
        int jobCount = 10;
        int clusterCount = 3;
        List<Job> jobs = new LinkedList<Job>();

        for (int i = jobCount; i > 0; --i) {
            Job j = new Job();
            j.setName(i + "");
            j.vdsNS.construct(Pegasus.RUNTIME_KEY, (i * 10) + "");
            jobs.add(j);
        }

        assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> mBestFitMethod.invoke(mCluster, jobs, clusterCount),
                "bestFitBinPack fails without an initialized LogManager");
    }

    @Test
    public void testClusterNumWithNoOpLoggerReturnsRequestedBins() throws Exception {
        int jobCount = 10;
        int clusterCount = 3;
        List<Job> jobs = new LinkedList<Job>();

        for (int i = jobCount; i > 0; --i) {
            jobs.add(runtimeJob("job" + i, i * 10));
        }

        setLogger(new NoOpLogManager());

        List<List<Job>> bins =
                (List<List<Job>>) mBestFitMethod.invoke(mCluster, jobs, clusterCount);

        assertThat(bins.size(), is(clusterCount));
        assertThat(bins.stream().mapToInt(List::size).sum(), is(jobCount));
    }

    @Test
    public void testGetRunTimeUsesPrimaryRuntimeKey() throws Exception {
        Job job = runtimeJob("j1", 42);
        setLogger(new NoOpLogManager());

        assertThat(mGetRunTimeMethod.invoke(mCluster, job), is("42"));
    }

    @Test
    public void testGetRunTimeFallsBackToDeprecatedRuntimeKey() throws Exception {
        Job job = new Job();
        job.setName("j2");
        job.vdsNS.construct(Pegasus.DEPRECATED_RUNTIME_KEY, "17");
        setLogger(new NoOpLogManager());

        assertThat(mGetRunTimeMethod.invoke(mCluster, job), is("17"));
    }

    @Test
    public void testGetRunTimeThrowsWhenNoRuntimeProfileExists() {
        Job job = new Job();
        job.setName("j3");

        assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> mGetRunTimeMethod.invoke(mCluster, job),
                "missing runtime metadata should be reported as a reflective failure");
    }

    @AfterEach
    public void tearDown() {
        mLogger = null;
        mTestSetup = null;
        mCluster = null;
        mBestFitMethod = null;
        mGetRunTimeMethod = null;
    }

    private Job runtimeJob(String name, int runtime) {
        Job job = new Job();
        job.setName(name);
        job.vdsNS.construct(Pegasus.RUNTIME_KEY, Integer.toString(runtime));
        return job;
    }

    private void setLogger(LogManager logger) throws Exception {
        ReflectionTestUtils.setField(mCluster, "mLogger", logger);
        mLogger = logger;
    }
}
