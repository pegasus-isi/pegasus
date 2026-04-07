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
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class PDAXWriterTest {

    @Test
    public void testConstants() {
        assertThat(PDAXWriter.XML_VERSION, is("2.0"));
        assertThat(PDAXWriter.XML_NAMESPACE, is("https://pegasus.isi.edu/schema"));
    }

    @Test
    public void testConstructorStoresNameAndFilename() throws Exception {
        File file = Files.createTempFile("pdax-writer", ".xml").toFile();
        PDAXWriter writer = new PDAXWriter("workflow", file.getAbsolutePath());

        assertThat(ReflectionTestUtils.getField(writer, "mName"), is("workflow"));
        assertThat(ReflectionTestUtils.getField(writer, "mFileName"), is(file.getAbsolutePath()));
        writer.close();
    }

    @Test
    public void testWriteHeaderEmitsExpectedPdaxPreamble() throws Exception {
        File file = Files.createTempFile("pdax-writer", ".xml").toFile();
        PDAXWriter writer = new PDAXWriter("workflow", file.getAbsolutePath());

        writer.writeHeader();
        writer.close();

        String contents = Files.readString(file.toPath());
        assertThat(contents.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"), is(true));
        assertThat(
                contents.contains("<pdag xmlns=\"https://pegasus.isi.edu/schema/PDAX\""), is(true));
        assertThat(contents.contains("name=\"workflow\""), is(true));
        assertThat(contents.contains("version=\"2.0\""), is(true));
    }

    @Test
    public void testWriteAndWritelnFollowedByClosePersistContent() throws Exception {
        File file = Files.createTempFile("pdax-writer", ".xml").toFile();
        PDAXWriter writer = new PDAXWriter("workflow", file.getAbsolutePath());

        writer.write("alpha");
        writer.writeln("beta");
        writer.close();

        String contents = Files.readString(file.toPath());
        assertThat(contents.contains("alphabeta"), is(true));
        assertThat(contents.contains("</pdag>"), is(true));
    }
}
