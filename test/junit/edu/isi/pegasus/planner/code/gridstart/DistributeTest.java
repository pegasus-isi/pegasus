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
package edu.isi.pegasus.planner.code.gridstart;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.GridStart;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Distribute GridStart implementation. */
public class DistributeTest {

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

    private static final class TestDistribute extends Distribute {
        TransformationCatalogEntry lookupEntry(String site) {
            return getTransformationCatalogEntry(site);
        }

        String distributeArguments(Job job) {
            return getDistributeArguments(job);
        }

        void setLogger(LogManager logger) {
            mLogger = logger;
        }

        void setTransformationCatalog(TransformationCatalog tc) {
            try {
                java.lang.reflect.Field field = Distribute.class.getDeclaredField("mTCHandle");
                field.setAccessible(true);
                field.set(this, tc);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private TransformationCatalog tcReturning(
            List<TransformationCatalogEntry> entries, boolean throwOnLookup) {
        return (TransformationCatalog)
                Proxy.newProxyInstance(
                        TransformationCatalog.class.getClassLoader(),
                        new Class<?>[] {TransformationCatalog.class},
                        (proxy, method, args) -> {
                            if ("lookup".equals(method.getName())
                                    && method.getParameterCount() == 5) {
                                if (throwOnLookup) {
                                    throw new Exception("lookup failed");
                                }
                                return entries;
                            }
                            if ("close".equals(method.getName())) {
                                return null;
                            }
                            if ("connect".equals(method.getName())) {
                                return true;
                            }
                            if ("isClosed".equals(method.getName())) {
                                return false;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    @Test
    public void testDistributeImplementsGridStart() {
        assertThat(GridStart.class.isAssignableFrom(Distribute.class), is(true));
    }

    @Test
    public void testClassNameConstant() {
        assertThat(Distribute.CLASSNAME, is("Distribute"));
    }

    @Test
    public void testShortNameConstant() {
        assertThat(Distribute.SHORT_NAME, is("distribute"));
    }

    @Test
    public void testTransformationConstants() {
        assertThat(Distribute.TRANSFORMATION_NAMESPACE, is("hubzero"));
        assertThat(Distribute.TRANSFORMATION_NAME, is("distribute"));
        assertThat(Distribute.TRANSFORMATION_VERSION, nullValue());
        assertThat(Distribute.EXECUTABLE_BASENAME, is("distribute"));
    }

    @Test
    public void testGetTransformationCatalogEntryReturnsFirstLookupResult() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry.setPhysicalTransformation("/bin/distribute");

        TestDistribute distribute = new TestDistribute();
        distribute.setLogger(new NoOpLogManager());
        distribute.setTransformationCatalog(tcReturning(Collections.singletonList(entry), false));

        TransformationCatalogEntry result = distribute.lookupEntry("local");

        assertThat(result, sameInstance(entry));
    }

    @Test
    public void testGetTransformationCatalogEntryReturnsNullOnLookupFailure() {
        TestDistribute distribute = new TestDistribute();
        distribute.setLogger(new NoOpLogManager());
        distribute.setTransformationCatalog(tcReturning(null, true));

        assertThat(distribute.lookupEntry("local"), nullValue());
    }

    @Test
    public void testGetDistributeArgumentsCurrentlyReturnsEmptyString() {
        TestDistribute distribute = new TestDistribute();

        assertThat(distribute.distributeArguments(new Job()), is(""));
    }

    @Test
    public void testCapabilityAndDescriptionMethods() {
        Distribute distribute = new Distribute();

        assertThat(distribute.canSetXBit(), is(false));
        assertThat(distribute.getVDSKeyValue(), is("Distribute"));
        assertThat(distribute.shortDescribe(), is("distribute"));
    }

    @Test
    public void testUnsupportedWorkerNodeMethodsThrow() {
        Distribute distribute = new Distribute();

        assertThrows(
                UnsupportedOperationException.class,
                () -> distribute.useFullPathToGridStarts(true));
        assertThrows(
                UnsupportedOperationException.class,
                () -> distribute.getWorkerNodeDirectory(new Job()));
    }
}
