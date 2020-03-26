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

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/** @author Karan Vahi */
public class TransformationTest {

    private TestSetup mTestSetup;

    public TransformationTest() {}

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
    public void testBaseTransformation() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "namespace: \"example\"\n" + "name: \"keg\"\n" + "version: \"1.0\"\n";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        assertEquals(expected, actual);
    }

    @Test
    public void testBaseTransformationWithProfiles() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    APP_HOME: \"/tmp/myscratch\"\n"
                        + "    JAVA_HOME: \"/opt/java/1.6\"\n"
                        + "  pegasus:\n"
                        + "    clusters.num: \"1\"\n";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        expected.addProfile(new Profile("env", "APP_HOME", "/tmp/myscratch"));
        expected.addProfile(new Profile("env", "JAVA_HOME", "/opt/java/1.6"));
        expected.addProfile(new Profile("pegasus", "clusters.num", "1"));
        assertEquals(expected, actual);
    }
    
    @Test
    public void testBaseTransformationWithProfilesandMetadData() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "metadata:\n"
                        + "  user: \"karan\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    APP_HOME: \"/tmp/myscratch\"\n"
                        + "    JAVA_HOME: \"/opt/java/1.6\"\n"
                        + "  pegasus:\n"
                        + "    clusters.num: \"1\"\n";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        expected.addProfile(new Profile("env", "APP_HOME", "/tmp/myscratch"));
        expected.addProfile(new Profile("env", "JAVA_HOME", "/opt/java/1.6"));
        expected.addProfile(new Profile("pegasus", "clusters.num", "1"));
        expected.addProfile(new Profile("metadata", "user", "karan"));
        assertEquals(expected, actual);
    }

    @Test
    public void testBaseTransformationWithHooks() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "hooks:\n"
                        + "  shell:\n"
                        + "    - _on: start\n"
                        + "      cmd: /bin/date\n"
                        + "    - _on: end\n"
                        + "      cmd: /bin/echo \"Finished\"\n";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        expected.addNotification(new Invoke(Invoke.WHEN.start, "/bin/date"));
        expected.addNotification(new Invoke(Invoke.WHEN.end, "/bin/echo \"Finished\""));
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testTransformation() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: \"/opt/java/1.6\"\n"
                        + "sites:\n"
                        + "  - name: \"isi\"\n"
                        + "    type: \"installed\"\n"
                        + "    pfn: \"/path/to/keg\"\n"
                        + "    arch: \"x86\"\n"
                        + "    os.type: \"linux\"\n"
                        + "    os.release: \"fc\"\n"
                        + "    os.version: \"1.0\"\n";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        expected.setResourceId("isi");
        expected.setType(TCType.INSTALLED);
        expected.setPhysicalTransformation("/path/to/keg");
        expected.addProfile(new Profile("env", "JAVA_HOME", "/opt/java/1.6"));
        SysInfo sys = new SysInfo();
        sys.setArchitecture(SysInfo.Architecture.x86);
        sys.setOS(SysInfo.OS.linux);
        sys.setOSRelease("fc");
        sys.setOSVersion("1.0");
        expected.setSysInfo(sys);
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testProfileOverloading() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: \"/opt/java/1.6\"\n"
                        + "sites:\n"
                        + "  - name: \"isi\"\n"
                        + "    profiles:\n"
                        + "      env:\n"
                        + "        JAVA_HOME: \"/opt/java/1.7\"\n";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        expected.setResourceId("isi");
        expected.setType(TCType.INSTALLED);
        expected.addProfile(new Profile("env", "JAVA_HOME", "/opt/java/1.7"));
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testContainerReference() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "sites:\n"
                        + "  - name: \"isi\"\n"
                        + "    pfn: \"/path/to/keg\"\n"
                        + "    container: centos-pegasus";

        Transformation tx = mapper.readValue(test, Transformation.class);
        assertNotNull(tx);
        assertEquals(1, tx.getTransformationCatalogEntries().size());
        TransformationCatalogEntry actual = tx.getTransformationCatalogEntries().get(0);
        TransformationCatalogEntry expected =
                new TransformationCatalogEntry("example", "keg", "1.0");
        expected.setResourceId("isi");
        expected.setType(TCType.INSTALLED);
        expected.setPhysicalTransformation("/path/to/keg");
        expected.setContainer(new Container("centos-pegasus"));
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testCompleteTransformationDeserialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "      namespace: \"example\"\n"
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
                        //                        + "      requires:\n"
                        //                        + "          - anotherTr\n"
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
        // System.err.println(tx.getTransformationCatalogEntries());
        assertNotNull(tx);
    }
}
