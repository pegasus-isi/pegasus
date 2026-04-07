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
package edu.isi.pegasus.planner.catalog.replica.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class MRCTest {

    @Test
    public void testConstructorInitializesCatalogListAndLogger() {
        MRC mrc = new MRC();

        assertThat(mrc.mRCList, is(notNullValue()));
        assertThat(mrc.mRCList.isEmpty(), is(true));
        assertThat(mrc.mLogger, is(notNullValue()));
    }

    @Test
    public void testGetNameAndGetKeyHelpers() {
        MRC mrc = new MRC();

        assertThat(mrc.getName("alpha.url"), is("alpha"));
        assertThat(mrc.getName("alpha"), is("alpha"));

        assertThat(mrc.getKey("alpha", "alpha"), is(MRC.TYPE_KEY));
        assertThat(mrc.getKey("alpha.url", "alpha"), is("url"));
        assertThat(mrc.getKey("beta.url", "alpha"), is(nullValue()));
        assertThat(mrc.getKey("alphaBeta.url", "alpha"), is(nullValue()));
    }

    @Test
    public void testProtectedConnectRejectsCatalogWithoutType() {
        MRC mrc = new MRC();
        mrc.mLogger = new NoOpLogManager();

        assertThat(mrc.connect("missingType", new Properties()), is(false));
        assertThat(mrc.mRCList.isEmpty(), is(true));
    }

    @Test
    public void testClearDelegatesButCurrentlyReturnsZero() {
        MRC mrc = new MRC();
        AtomicInteger clearCalls = new AtomicInteger();
        mrc.mRCList.add(replicaCatalogProxy(clearCalls, null, 3));
        mrc.mRCList.add(replicaCatalogProxy(clearCalls, null, 4));

        assertThat(mrc.clear(), is(0));
        assertThat(clearCalls.get(), is(2));
    }

    @Test
    public void testCloseDelegatesToAllReplicaCatalogs() {
        MRC mrc = new MRC();
        AtomicInteger closeCalls = new AtomicInteger();
        mrc.mRCList.add(replicaCatalogProxy(null, closeCalls, 0));
        mrc.mRCList.add(replicaCatalogProxy(null, closeCalls, 0));

        mrc.close();

        assertThat(closeCalls.get(), is(2));
    }

    private static ReplicaCatalog replicaCatalogProxy(
            AtomicInteger clearCalls, AtomicInteger closeCalls, int clearResult) {
        return (ReplicaCatalog)
                Proxy.newProxyInstance(
                        MRCTest.class.getClassLoader(),
                        new Class<?>[] {ReplicaCatalog.class},
                        (proxy, method, args) -> {
                            String name = method.getName();
                            if (name.equals("clear")) {
                                if (clearCalls != null) {
                                    clearCalls.incrementAndGet();
                                }
                                return clearResult;
                            }
                            if (name.equals("close")) {
                                if (closeCalls != null) {
                                    closeCalls.incrementAndGet();
                                }
                                return null;
                            }
                            if (method.getReturnType().equals(boolean.class)) {
                                return false;
                            }
                            if (method.getReturnType().equals(int.class)) {
                                return 0;
                            }
                            return null;
                        });
    }

    private static final class NoOpLogManager extends LogManager {
        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

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
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
