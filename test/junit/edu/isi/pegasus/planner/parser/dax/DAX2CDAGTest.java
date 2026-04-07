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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class DAX2CDAGTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testInitializeSetsExpectedDefaultsAndPrefix() throws Exception {
        TestableDAX2CDAG callback = new TestableDAX2CDAG();
        PegasusBag bag = createBag("pref-");

        callback.initialize(bag, "workflow.dax");

        assertThat("initialize should create an ADag", getField(callback, "mDag"), notNullValue());
        assertThat(
                "initialize should create the job-id map",
                getField(callback, "mJobMap"),
                notNullValue());
        assertThat(
                "initialize should create a replica store",
                getField(callback, "mReplicaStore"),
                notNullValue());
        assertThat(
                "initialize should create a transformation store",
                getField(callback, "mTransformationStore"),
                notNullValue());
        assertThat(
                "initialize should create a site store",
                getField(callback, "mSiteStore"),
                notNullValue());
        assertThat(
                "initialize should create the compound-transformation map",
                getField(callback, "mCompoundTransformations"),
                notNullValue());
        assertThat(
                "initialize should create notifications",
                getField(callback, "mNotifications"),
                notNullValue());
        assertThat(
                "initialize should start with mDone=false",
                getBooleanField(callback, "mDone"),
                is(false));
        assertThat(
                "PlannerOptions prefix should be captured",
                getField(callback, "mJobPrefix"),
                is("pref-"));
    }

    @Test
    public void testGetConstructedObjectRequiresCbDoneFirst() {
        TestableDAX2CDAG callback = new TestableDAX2CDAG();
        callback.initialize(createBag("pref-"), "workflow.dax");

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        callback::getConstructedObject,
                        "Constructed object should not be accessible before cbDone");

        assertThat(
                "Guard message should match the current behavior",
                exception.getMessage(),
                is("Method called before the abstract dag  for the partition was fully generated"));
    }

    @Test
    public void testCbDocumentAndCbDonePopulateConstructedADag() {
        TestableDAX2CDAG callback = new TestableDAX2CDAG();
        callback.initialize(createBag(null), "workflow.dax");

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("version", "3.6");
        attributes.put("count", "2");
        attributes.put("index", "1");
        attributes.put("name", "example-wf");

        callback.cbDocument(attributes);
        callback.cbDone();

        ADag dag = (ADag) callback.getConstructedObject();

        assertThat("DAX version should come from cbDocument", dag.getDAXVersion(), is("3.6"));
        assertThat("Workflow count should come from cbDocument", dag.getCount(), is("2"));
        assertThat("Workflow index should come from cbDocument", dag.getIndex(), is("1"));
        assertThat("Workflow label should come from cbDocument", dag.getLabel(), is("example-wf"));
        assertThat(
                "cbDone/getConstructedObject should attach the replica store",
                dag.getReplicaStore(),
                notNullValue());
        assertThat(
                "cbDone/getConstructedObject should attach the transformation store",
                dag.getTransformationStore(),
                notNullValue());
        assertThat(
                "cbDone/getConstructedObject should attach the site store",
                dag.getSiteStore(),
                notNullValue());
    }

    @Test
    public void testConstructJobIDAppliesPrefixAndDagmanCompliance() {
        TestableDAX2CDAG callback = new TestableDAX2CDAG();
        callback.initialize(createBag("pref-"), "workflow.dax");

        Job job = new Job();
        job.setTransformation(null, "my.tx+name", null);
        job.setLogicalID("ID0001");

        assertThat(
                "constructJobID should prepend the prefix and sanitize . and + characters",
                callback.exposedConstructJobID(job),
                is("pref-my_tx_name_ID0001"));
    }

    private PegasusBag createBag(String prefix) {
        PegasusBag bag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty("pegasus.parser.dax.data.dependencies", "false");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        if (prefix != null) {
            PlannerOptions options = new PlannerOptions();
            options.setJobnamePrefix(prefix);
            bag.add(PegasusBag.PLANNER_OPTIONS, options);
        }
        return bag;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private boolean getBooleanField(Object target, String name) throws Exception {
        return ((Boolean) getField(target, name)).booleanValue();
    }

    private static final class TestableDAX2CDAG extends DAX2CDAG {
        String exposedConstructJobID(Job job) {
            return constructJobID(job);
        }
    }

    private static final class NoOpLogManager extends LogManager {

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
            return null;
        }

        @Override
        public void log(String message, int level) {}

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void logEventStart(String name, String entityName, String entityID) {}

        @Override
        public void logEventStart(String name, String entityName, String entityID, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
