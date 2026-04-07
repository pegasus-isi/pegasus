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

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class WriterCallbackTest {

    private static final class ExposedWriterCallback extends WriterCallback {
        String relation(String child, String parent) {
            return partitionRelation2XML(child, parent);
        }

        String relation(String child, java.util.List parents) {
            return partitionRelation2XML(child, parents);
        }

        String relation(String child, java.util.Set parents) {
            return partitionRelation2XML(child, parents);
        }

        PDAXWriter pdax(String daxFile, String name, String directory) {
            return getHandletoPDAXWriter(daxFile, name, directory);
        }
    }

    private static final class StubDAXWriter extends DAXWriter {
        boolean closed;

        StubDAXWriter() {
            super();
        }

        @Override
        public boolean writePartitionDax(Partition partition, int index) {
            return true;
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }
    }

    @Test
    public void testPartitionRelationSingleParentXML() {
        ExposedWriterCallback callback = new ExposedWriterCallback();

        String xml = callback.relation("ID2", "ID1");

        assertThat(xml.contains("<child ref=\"ID2\">"), is(true));
        assertThat(xml.contains("<parent ref=\"ID1\"/>"), is(true));
        assertThat(xml.contains("</child>"), is(true));
    }

    @Test
    public void testPartitionRelationListParentsXML() {
        ExposedWriterCallback callback = new ExposedWriterCallback();

        String xml = callback.relation("ID3", Arrays.asList("ID1", "ID2"));

        assertThat(xml.contains("<parent ref=\"ID1\"/>"), is(true));
        assertThat(xml.contains("<parent ref=\"ID2\"/>"), is(true));
    }

    @Test
    public void testPartitionRelationSetParentsXML() {
        ExposedWriterCallback callback = new ExposedWriterCallback();

        String xml =
                callback.relation("ID4", new LinkedHashSet<>(Arrays.asList("ID1", "ID2", "ID3")));

        assertThat(xml.contains("<parent ref=\"ID1\"/>"), is(true));
        assertThat(xml.contains("<parent ref=\"ID2\"/>"), is(true));
        assertThat(xml.contains("<parent ref=\"ID3\"/>"), is(true));
    }

    @Test
    public void testGetHandleToPDAXWriterSetsExpectedPdaxPath() throws Exception {
        ExposedWriterCallback callback = new ExposedWriterCallback();
        File directory = Files.createTempDirectory("writer-callback").toFile();

        PDAXWriter writer =
                callback.pdax("/tmp/workflow.dax", "workflow", directory.getAbsolutePath());

        assertThat(writer, is(notNullValue()));
        assertThat(callback.getPDAX(), is(new File(directory, "workflow.pdax").toString()));
        writer.close();
    }

    @Test
    public void testCbDoneResetsStateAndClosesWriters() throws Exception {
        ExposedWriterCallback callback = new ExposedWriterCallback();
        File pdaxFile = Files.createTempFile("writer-callback", ".pdax").toFile();
        callback.mPDAXWriter = new PDAXWriter("workflow", pdaxFile.getAbsolutePath());
        StubDAXWriter daxWriter = new StubDAXWriter();
        callback.mDAXWriter = daxWriter;
        callback.mPartitioningStarted = true;

        callback.cbDone();

        assertThat(callback.mPartitioningStarted, is(false));
        assertThat(daxWriter.closed, is(true));
    }
}
