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
package edu.isi.pegasus.planner.partitioner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class SingleLookTest {

    @Test
    public void testSingleLookExtendsDaxWriterAndInitializesFields() throws Exception {
        SingleLook writer = new SingleLook("workflow.dax", "/tmp/pdax");

        assertThat(SingleLook.class.getSuperclass(), is(DAXWriter.class));
        assertThat(writer.mDaxFile, is("workflow.dax"));
        assertThat(writer.mPDAXDirectory, is("/tmp/pdax"));
        assertThat((Boolean) ReflectionTestUtils.getField(writer, "mDAXInMemory"), is(false));
        assertThat(ReflectionTestUtils.getField(writer, "mJobMap"), is(nullValue()));
    }

    @Test
    public void testWritePartitionDaxThrowsWhenJobInformationIsMissing() throws Exception {
        SingleLook writer = new SingleLook("workflow.dax", "/tmp/pdax");
        writer.mLogger = new NoOpLogManager();
        ReflectionTestUtils.setField(writer, "mDAXInMemory", true);
        ReflectionTestUtils.setField(writer, "mJobMap", new java.util.HashMap());

        Partition partition = new Partition(Arrays.asList(new GraphNode("jobA")), "p1");
        partition.setIndex(3);

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> writer.writePartitionDax(partition, 3));

        assertThat(
                exception.getMessage(),
                is("Unable to find information about jobjobAwhile constructing partition"));
    }

    @Test
    public void testWritePartitionDaxMethodSignatureReturnsBoolean() throws Exception {
        Method method =
                SingleLook.class.getDeclaredMethod("writePartitionDax", Partition.class, int.class);

        assertThat(method.getReturnType(), is(boolean.class));
    }

    @Test
    public void testPrivateMyCallBackHandlerExistsAndImplementsCallback() throws Exception {
        Class<?> callbackClass = null;
        for (Class<?> candidate : SingleLook.class.getDeclaredClasses()) {
            if (candidate.getSimpleName().equals("MyCallBackHandler")) {
                callbackClass = candidate;
                break;
            }
        }

        assertThat(callbackClass, is(notNullValue()));
        assertThat(callbackClass.getInterfaces()[0], is(org.griphyn.vdl.euryale.Callback.class));

        Constructor<?> constructor = callbackClass.getDeclaredConstructor(SingleLook.class);
        assertThat(constructor, is(notNullValue()));

        assertThat(
                callbackClass.getDeclaredMethod("cb_document", java.util.Map.class),
                is(notNullValue()));
        assertThat(
                callbackClass.getDeclaredMethod("cb_filename", org.griphyn.vdl.dax.Filename.class),
                is(notNullValue()));
        assertThat(
                callbackClass.getDeclaredMethod("cb_job", org.griphyn.vdl.dax.Job.class),
                is(notNullValue()));
        assertThat(
                callbackClass.getDeclaredMethod("cb_parents", String.class, java.util.List.class),
                is(notNullValue()));
        assertThat(callbackClass.getDeclaredMethod("cb_done"), is(notNullValue()));
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
