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

import edu.isi.pegasus.common.logging.LogManager;
import java.io.File;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class DAXWriterTest {

    @Test
    public void testDAXWriterIsAbstract() {
        assertThat(Modifier.isAbstract(DAXWriter.class.getModifiers()), is(true));
    }

    private static final class StubDAXWriter extends DAXWriter {

        StubDAXWriter() {
            super();
        }

        StubDAXWriter(String daxFile, String directory) {
            super(daxFile, directory);
        }

        @Override
        public boolean writePartitionDax(Partition partition, int index) {
            return true;
        }
    }

    @Test
    public void testConstants() {
        assertThat(DAXWriter.PARTITION_PREFIX, is("partition_"));
        assertThat(DAXWriter.PACKAGE_NAME, is("edu.isi.pegasus.planner.partitioner"));
    }

    @Test
    public void testGetPDAXFilenameVariants() {
        assertThat(DAXWriter.getPDAXFilename("workflow", 3), is("workflow_3.dax"));
        assertThat(DAXWriter.getPDAXFilename("workflow", 3, true), is("partition_workflow_3.dax"));
        assertThat(DAXWriter.getPDAXFilename(null, 2, true), is("partition_test_2.dax"));
    }

    @Test
    public void testSetPartitionNameAddsPrefixAndDefaultsNullName() {
        StubDAXWriter writer = new StubDAXWriter();
        writer.setPartitionName("mydax");
        assertThat(writer.getPartitionName(), is("partition_mydax"));

        writer.setPartitionName(null);
        assertThat(writer.getPartitionName(), is("partition_test"));
    }

    @Test
    public void testConstructorsInitializeExpectedFields() throws Exception {
        StubDAXWriter defaultWriter = new StubDAXWriter();
        StubDAXWriter configuredWriter = new StubDAXWriter("workflow.dax", "/tmp/out");

        Object defaultDaxFile = ReflectionTestUtils.getField(defaultWriter, "mDaxFile");
        Object defaultDirectory = ReflectionTestUtils.getField(defaultWriter, "mPDAXDirectory");
        Object defaultLogger = ReflectionTestUtils.getField(defaultWriter, "mLogger");

        assertThat(defaultDaxFile, is(nullValue()));
        assertThat(defaultDirectory, is(nullValue()));
        assertThat(defaultLogger, is(notNullValue()));
        assertThat(defaultLogger instanceof LogManager, is(true));

        assertThat(ReflectionTestUtils.getField(configuredWriter, "mDaxFile"), is("workflow.dax"));
        assertThat(
                ReflectionTestUtils.getField(configuredWriter, "mPDAXDirectory"), is("/tmp/out"));
        assertThat(ReflectionTestUtils.getField(configuredWriter, "mLogger"), is(notNullValue()));
    }

    @Test
    public void testInitializeWriteHandleWritesFileAndCloseClearsHandle() throws Exception {
        File tempDir = Files.createTempDirectory("daxwriter-test").toFile();
        StubDAXWriter writer = new StubDAXWriter("workflow.dax", tempDir.getAbsolutePath());
        writer.setPartitionName("workflow");
        writer.initializeWriteHandle(5);
        writer.writeln("hello");
        writer.close();

        File expected = new File(tempDir, "partition_workflow_5.dax");
        assertThat(expected.isFile(), is(true));
        assertThat(Files.readString(expected.toPath()).contains("hello"), is(true));

        assertThat(ReflectionTestUtils.getField(writer, "mWriteHandle"), is(nullValue()));
    }
}
