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
package edu.isi.pegasus.planner.classes;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.dax.PFN;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class ReplicaLocationTest {

    private TestSetup mTestSetup;

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterAll
    public static void tearDownClass() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void serializerReplicaLocation() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f.a");
        rl.addMetadata("foo", "bar");
        rl.addPFN(new PFN("file:///scratch/f.a").setSite("local"));
        String expected =
                "---\n"
                        + "lfn: \"f.a\"\n"
                        + "pfns:\n"
                        + " -\n"
                        + "  pfn: \"file:///scratch/f.a\"\n"
                        + "  site: \"local\"\n"
                        + "metadata:\n"
                        + "  foo: \"bar\"\n";
        assertEquals(expected, mapper.writeValueAsString(rl));
    }
}
