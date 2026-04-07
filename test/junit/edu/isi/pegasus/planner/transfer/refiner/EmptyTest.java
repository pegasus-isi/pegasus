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
package edu.isi.pegasus.planner.transfer.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.transfer.MultipleFTPerXFERJobRefiner;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class EmptyTest {

    @Test
    public void testEmptyExtendsMultipleFTRefinerAndDescription() {
        assertThat(Empty.class.getSuperclass(), sameInstance(MultipleFTPerXFERJobRefiner.class));
        assertThat(Empty.DESCRIPTION, is("Empty Implementation"));
    }

    @Test
    public void testConstructorInitializesFieldsAndDescription() throws Exception {
        Empty empty = createEmpty();

        assertThat(ReflectionTestUtils.getField(empty, "mLogMsg"), nullValue());

        Object fileTable = ReflectionTestUtils.getField(empty, "mFileTable");
        assertThat(fileTable, notNullValue());
        assertThat(fileTable instanceof Map, is(true));
        assertThat(((Map<?, ?>) fileTable).isEmpty(), is(true));

        assertThat(
                ReflectionTestUtils.getField(empty, "mCreateRegistrationJobs"), is(Boolean.FALSE));

        assertThat(empty.getDescription(), is(Empty.DESCRIPTION));
    }

    @Test
    public void testAddJobAndRelationMethodsUpdateWorkflowGraph() {
        ADag dag = new ADag();
        Empty empty = createEmpty(dag);

        Job parent = new Job();
        parent.setName("parent");
        parent.setJobType(Job.COMPUTE_JOB);

        Job child = new Job();
        child.setName("child");
        child.setJobType(Job.COMPUTE_JOB);

        empty.addJob(parent);
        empty.addJob(child);
        empty.addRelation("parent", "child");
        empty.addRelation("parent", "child", "local", true);

        assertThat(dag.size(), is(2));
        assertThat(dag.toString(), containsString("JOB parent"));
        assertThat(dag.toString(), containsString("JOB child"));
        assertThat(dag.toString(), containsString("EDGE parent -> child"));
    }

    @Test
    public void testNoOpTransferMethodsCanBeInvokedSafely() {
        Empty empty = createEmpty();
        Job job = new Job();
        job.setName("compute");

        Collection<FileTransfer> files = new ArrayList<FileTransfer>();

        assertDoesNotThrow(() -> empty.addStageInXFERNodes(job, files, files));
        assertDoesNotThrow(() -> empty.addStageInXFERNodes(job, files, "prefix_", null));
        assertDoesNotThrow(() -> empty.addInterSiteTXNodes(job, files, true));
        assertDoesNotThrow(() -> empty.addStageOutXFERNodes(job, files, files, null));
        assertDoesNotThrow(() -> empty.addStageOutXFERNodes(job, files, null, false, true));
        assertDoesNotThrow(empty::done);
    }

    @Test
    public void testCreateRegistrationJobSignature() throws Exception {
        Method method =
                Empty.class.getDeclaredMethod(
                        "createRegistrationJob",
                        String.class,
                        Job.class,
                        Collection.class,
                        edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge.class);

        assertThat(method.getReturnType(), sameInstance(Job.class));
        assertThat(Modifier.isProtected(method.getModifiers()), is(true));
        assertThat(method.getParameterCount(), is(4));
    }

    private Empty createEmpty() {
        return createEmpty(new ADag());
    }

    private Empty createEmpty(ADag dag) {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setExecutionSites(Collections.singleton("local"));

        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        return new Empty(dag, bag);
    }

    private static class NoOpLogManager extends LogManager {

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {
            this.mLogFormatter = formatter;
        }

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.DEBUG_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return System.out;
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
}
