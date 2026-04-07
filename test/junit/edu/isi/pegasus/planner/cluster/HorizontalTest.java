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

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Horizontal clusterer class. */
public class HorizontalTest {

    private static final class TestHorizontal extends Horizontal {
        String renderAttribute(String key, String value) {
            StringBuffer buffer = new StringBuffer();
            appendAttribute(buffer, key, value);
            return buffer.toString();
        }
    }

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

    private Horizontal mHorizontal;

    @BeforeEach
    public void setUp() {
        mHorizontal = new TestHorizontal();
    }

    @Test
    public void testInstantiation() {
        assertThat(mHorizontal, notNullValue());
    }

    @Test
    public void testImplementsClusterer() {
        assertThat(mHorizontal, instanceOf(Clusterer.class));
    }

    @Test
    public void testDescriptionNotNull() {
        assertThat(mHorizontal.description(), notNullValue());
    }

    @Test
    public void testDescriptionNotEmpty() {
        assertThat(mHorizontal.description().isEmpty(), is(false));
    }

    @Test
    public void testDefaultConstructorDoesNotThrow() {
        assertDoesNotThrow(Horizontal::new);
    }

    @Test
    public void testIsNotSameInstanceAsVertical() {
        Vertical v = new Vertical();
        assertThat(mHorizontal.getClass(), not(equalTo(v.getClass())));
    }

    @Test
    public void testDescriptionDiffersFromVertical() {
        Vertical v = new Vertical();
        // Descriptions should differ between implementations
        assertThat(mHorizontal.description(), not(equalTo(v.description())));
    }

    @Test
    public void testDescriptionMatchesDeclaredConstant() {
        assertThat(mHorizontal.description(), is(Horizontal.DESCRIPTION));
    }

    @Test
    public void testDefaultCollapseFactorMatchesExpectedValue() {
        assertThat(Horizontal.DEFAULT_COLLAPSE_FACTOR, is(1));
    }

    @Test
    public void testGetWorkflowIsNullBeforeInitialization() {
        assertThat(mHorizontal.getWorkflow(), nullValue());
    }

    @Test
    public void testParentsIsNoOp() {
        assertDoesNotThrow(
                () -> mHorizontal.parents("partition", java.util.Collections.singletonList("p1")),
                "parents() should be a no-op for Horizontal");
    }

    @Test
    public void testAppendAttributeFormatsXmlAttribute() {
        TestHorizontal horizontal = new TestHorizontal();

        assertThat(horizontal.renderAttribute("key", "value"), is("key=\"value\" "));
    }

    @Test
    public void testConstructMapParsesAndTrimsEntries() throws Exception {
        Map result =
                (Map)
                        invokePrivate(
                                mHorizontal,
                                "constructMap",
                                new Class<?>[] {String.class},
                                new Object[] {" alpha = 2 , beta=3 , invalid , gamma = 4 "});

        assertThat(result.size(), is(3));
        assertThat(result.get("alpha"), is("2"));
        assertThat(result.get("beta"), is("3"));
        assertThat(result.get("gamma"), is("4"));
    }

    @Test
    public void testConstructMapReturnsEmptyMapForNullInput() throws Exception {
        Map result =
                (Map)
                        invokePrivate(
                                mHorizontal,
                                "constructMap",
                                new Class<?>[] {String.class},
                                new Object[] {null});

        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void testGetClusteredDAGReturnsScheduledDagWhenNoReplacementsExist() throws Exception {
        ADag dag = new ADag();
        ReflectionTestUtils.setField(mHorizontal, "mScheduledDAG", dag);
        ReflectionTestUtils.setField(mHorizontal, "mReplacementTable", new HashMap());
        ReflectionTestUtils.setField(mHorizontal, "mLogger", new NoOpLogManager());

        assertThat(mHorizontal.getClusteredDAG(), sameInstance(dag));
    }

    private Object invokePrivate(Object target, String name, Class<?>[] types, Object[] args)
            throws Exception {
        Method method = target.getClass().getSuperclass().getDeclaredMethod(name, types);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}
