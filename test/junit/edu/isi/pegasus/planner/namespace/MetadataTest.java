/**
 * Copyright 2007-2020 University Of Southern California
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
package edu.isi.pegasus.planner.namespace;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/** @author vahi */
public class MetadataTest {

    private TestSetup mTestSetup;

    public MetadataTest() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterClass
    public static void tearDownClass() {}

    @After
    public void tearDown() {}

    @Test
    public void serializationWithNoMetadata() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        // metadata serialization can only be tested by being enclosed in a ReplicaLocation
        // object as we don't have writeStartObject in the serializer implementatio of Metadata
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test");

        String expected = "---\n" + "lfn: \"test\"\n";

        String actual = mapper.writeValueAsString(rl);

        assertEquals(expected, actual);
    }

    @Test
    public void serializationWithOnlyMetadata() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        // metadata serialization can only be tested by being enclosed in a ReplicaLocation
        // object as we don't have writeStartObject in the serializer implementatio of Metadata
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test");

        Metadata m = new Metadata();
        rl.addMetadata("user", "vahi");
        rl.addMetadata("year", "2020");

        String expected =
                "---\n"
                        + "lfn: \"test\"\n"
                        + "metadata:\n"
                        + "  year: \"2020\"\n"
                        + "  user: \"vahi\"\n";

        String actual = mapper.writeValueAsString(rl);

        assertEquals(expected, actual);
    }

    @Test
    public void serializationWithOnlyChecksum() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        // metadata serialization can only be tested by being enclosed in a ReplicaLocation
        // object as we don't have writeStartObject in the serializer implementatio of Metadata
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test");

        Metadata m = new Metadata();
        rl.addMetadata(Metadata.CHECKSUM_TYPE_KEY, "sha256");
        rl.addMetadata(Metadata.CHECKSUM_VALUE_KEY, "sdasdsadas2020");

        String expected =
                "---\n" + "lfn: \"test\"\n" + "checksum:\n" + "  sha256: \"sdasdsadas2020\"\n";

        String actual = mapper.writeValueAsString(rl);
        // System.out.println(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void serializationWithBothChecksumAndMetadata() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        // metadata serialization can only be tested by being enclosed in a ReplicaLocation
        // object as we don't have writeStartObject in the serializer implementatio of Metadata
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("test");

        Metadata m = new Metadata();
        rl.addMetadata("user", "vahi");
        rl.addMetadata("year", "2020");
        rl.addMetadata(Metadata.CHECKSUM_TYPE_KEY, "sha256");
        rl.addMetadata(Metadata.CHECKSUM_VALUE_KEY, "sdasdsadas2020");

        String expected =
                "---\n"
                        + "lfn: \"test\"\n"
                        + "checksum:\n"
                        + "  sha256: \"sdasdsadas2020\"\n"
                        + "metadata:\n"
                        + "  year: \"2020\"\n"
                        + "  user: \"vahi\"\n";

        String actual = mapper.writeValueAsString(rl);
        // System.out.println(actual);
        assertEquals(expected, actual);
    }
}
