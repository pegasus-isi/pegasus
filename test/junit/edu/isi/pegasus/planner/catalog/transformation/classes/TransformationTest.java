/*
 *
 *   Copyright 2007-2020 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import static org.junit.Assert.*;

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container.MountPoint;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import java.util.List;
import static org.hamcrest.CoreMatchers.hasItem;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * @author vahi
 */
public class TransformationTest {

    private TestSetup mTestSetup;

    public TransformationTest() {
    }

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testTransformationDeserialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test
                = "      namespace: \"example\"\n"
                + "      name: \"keg\"\n"
                + "      version: \"1.0\"\n"
                + "      profiles:\n"
                + "          env:\n"
                + "              APP_HOME: \"/tmp/myscratch\"\n"
                + "              JAVA_HOME: \"/opt/java/1.6\"\n"
                + "          pegasus:\n"
                + "              clusters.num: \"1\"\n"
                + "      hooks:\n"
                + "          shell:\n"
                + "             - _on: start\n"
                + "               cmd: /bin/date\n"
                + "             - _on: end\n"
                + "               cmd: /bin/echo \"Finished\"\n"
                + "      \n"
                + "      requires:\n"
                + "          - anotherTr\n"
                + "\n"
                + "      sites:\n"
                + "        - name: \"isi\"\n"
                + "          type: \"installed\"\n"
                + "          pfn: \"/path/to/keg\"\n"
                + "          arch: \"x86\"\n"
                + "          os.type: \"linux\"\n"
                + "          os.release: \"fc\"\n"
                + "          os.version: \"1.0\"\n"
                + "          profiles:\n"
                + "            env:\n"
                + "                Hello: World\n"
                + "                JAVA_HOME: /bin/java.1.6\n"
                + "            condor:\n"
                + "                FOO: bar\n"
                + "          container: centos-pegasus";

        Transformation tx = mapper.readValue(test, Transformation.class);
        System.err.println(tx.getTransformationCatalogEntries());
        assertNotNull(tx);
    }
}
