/*
 * Copyright 2007-20120 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class TransformationCatalogEntryTest {

    public TransformationCatalogEntryTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {}

    @After
    public void tearDown() {}

    @Test
    public void serializeTransformationCatalogEntry() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        TransformationCatalogEntry entry = new TransformationCatalogEntry("example", "keg", "1.0");
        entry.addProfile(
                new Profile(Profiles.NAMESPACES.env.toString(), "JAVA_HOME", "/opt/java/1.6"));
        entry.setPhysicalTransformation("/usr/bin/keg");
        entry.setResourceId("isi");
        SysInfo sysinfo = new SysInfo();
        sysinfo.setArchitecture(SysInfo.Architecture.x86);
        sysinfo.setOS(SysInfo.OS.linux);
        sysinfo.setOSRelease("rhel");
        sysinfo.setOSVersion("7.0");
        entry.setSysInfo(sysinfo);

        String expected =
                "---\n"
                        + "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "sites:\n"
                        + " -\n"
                        + "  name: \"isi\"\n"
                        + "  type: \"installed\"\n"
                        + "  pfn: \"/usr/bin/keg\"\n"
                        + "  bypass: false\n"
                        + "  arch: \"x86\"\n"
                        + "  os.type: \"linux\"\n"
                        + "  os.release: \"rhel\"\n"
                        + "  os.version: \"7.0\"\n"
                        + "  profiles:\n"
                        + "    env:\n"
                        + "      JAVA_HOME: \"/opt/java/1.6\"\n"
                        + "";
        String actual = mapper.writeValueAsString(entry);
        // System.err.println(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void serializeBaseEntryWithProfilesAndHooks() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        TransformationCatalogEntry entry = new TransformationCatalogEntry("example", "keg", "1.0");
        entry.addProfile(
                new Profile(Profiles.NAMESPACES.env.toString(), "JAVA_HOME", "/opt/java/1.6"));

        entry.addNotification(new Invoke(Invoke.WHEN.end, "echo Test"));

        String expected =
                "---\n"
                        + "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "hooks:\n"
                        + "  shell:\n"
                        + "   -\n"
                        + "    _on: \"end\"\n"
                        + "    cmd: \"echo Test\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: \"/opt/java/1.6\"\n"
                        + "";
        String actual = mapper.writeValueAsString(entry);
        // System.err.println(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void serializeBaseEntryWithMetadataAndChecksum() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        TransformationCatalogEntry entry = new TransformationCatalogEntry("example", "keg", "1.0");
        entry.addProfile(new Profile(Profiles.NAMESPACES.metadata.toString(), "user", "vahi"));
        entry.addProfile(
                new Profile(
                        Profiles.NAMESPACES.metadata.toString(),
                        Metadata.CHECKSUM_TYPE_KEY,
                        "sha256"));
        entry.addProfile(
                new Profile(
                        Profiles.NAMESPACES.metadata.toString(),
                        Metadata.CHECKSUM_VALUE_KEY,
                        "dsadsadsa093232"));

        String expected =
                "---\n"
                        + "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "checksum:\n"
                        + "  sha256: \"dsadsadsa093232\"\n"
                        + "metadata:\n"
                        + "  user: \"vahi\"\n";
        String actual = mapper.writeValueAsString(entry);
        // System.err.println(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void serializeEntryWithContainerReference() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        TransformationCatalogEntry entry = new TransformationCatalogEntry("example", "keg", "1.0");
        entry.setPhysicalTransformation("/usr/bin/keg");
        entry.setResourceId("isi");
        entry.setContainer(new Container("centos-pegasus"));

        String expected =
                "---\n"
                        + "namespace: \"example\"\n"
                        + "name: \"keg\"\n"
                        + "version: \"1.0\"\n"
                        + "sites:\n"
                        + " -\n"
                        + "  name: \"isi\"\n"
                        + "  type: \"installed\"\n"
                        + "  pfn: \"/usr/bin/keg\"\n"
                        + "  bypass: false\n"
                        + "  arch: \"x86_64\"\n"
                        + "  os.type: \"linux\"\n"
                        + "  container: \"centos-pegasus\"\n";
        String actual = mapper.writeValueAsString(entry);
        // System.err.println(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void envProfileFromContainerWithNoExistingProfile() {
        TransformationCatalogEntry e = new TransformationCatalogEntry();
        Container c = new Container();
        String key = "PEGASUS_HOME";
        String containerValue = "/usr/bin/";
        c.addProfile(new Profile("env", key, containerValue));
        e.incorporateContainerProfiles(c);
        // nothing should be in the tc
        assertNull(e.getAllProfiles());
        assertTrue(c.getAllProfiles().get(Profiles.NAMESPACES.env).containsKey(key));
        assertTrue(c.getAllProfiles().get(Profiles.NAMESPACES.env).get(key).equals(containerValue));
    }

    @Test
    public void envProfileFromContainerWithTCExistingProfile() {
        String key = "PEGASUS_HOME";
        String containerValue = "/usr/bin/";
        String tcValue = "/shared/pegasus-5.0";

        TransformationCatalogEntry e = new TransformationCatalogEntry();
        e.addProfile(new Profile("env", key, tcValue));
        Container c = new Container();
        c.addProfile(new Profile("env", key, containerValue));
        e.incorporateContainerProfiles(c);
        assertNotNull(e.getAllProfiles());
        assertTrue(e.getAllProfiles().get(Profiles.NAMESPACES.env).isEmpty());
        assertTrue(c.getAllProfiles().get(Profiles.NAMESPACES.env).containsKey(key));
        assertTrue(c.getAllProfiles().get(Profiles.NAMESPACES.env).get(key).equals(tcValue));
    }

    @Test
    public void containerArgsFromContainerWithNoExistingProfile() {
        String key = "container.arguments";
        String containerValue = "--shm-size 256";
        TransformationCatalogEntry e = new TransformationCatalogEntry();
        Container c = new Container();
        c.addProfile(new Profile("pegasus", key, containerValue));
        e.incorporateContainerProfiles(c);

        assertNotNull(e.getAllProfiles());
        assertTrue(e.getAllProfiles().get(Profiles.NAMESPACES.pegasus).containsKey(key));
        assertTrue(
                e.getAllProfiles()
                        .get(Profiles.NAMESPACES.pegasus)
                        .get(key)
                        .equals(containerValue));

        assertTrue(c.getAllProfiles().get(Profiles.NAMESPACES.pegasus).containsKey(key));
        assertTrue(
                c.getAllProfiles()
                        .get(Profiles.NAMESPACES.pegasus)
                        .get(key)
                        .equals(containerValue));
    }

    @Test
    public void containerArgsFromContainerWithExistingProfile() {
        String key = "container.arguments";
        String containerValue = "--shm-size 256";
        String tcValue = "--shm-size 64";
        TransformationCatalogEntry e = new TransformationCatalogEntry();
        e.addProfile(new Profile("pegasus", key, tcValue));
        Container c = new Container();

        c.addProfile(new Profile("pegasus", key, containerValue));
        e.incorporateContainerProfiles(c);

        assertNotNull(e.getAllProfiles());
        assertTrue(e.getAllProfiles().get(Profiles.NAMESPACES.pegasus).containsKey(key));
        assertTrue(e.getAllProfiles().get(Profiles.NAMESPACES.pegasus).get(key).equals(tcValue));

        assertTrue(c.getAllProfiles().get(Profiles.NAMESPACES.pegasus).containsKey(key));
        assertTrue(
                c.getAllProfiles()
                        .get(Profiles.NAMESPACES.pegasus)
                        .get(key)
                        .equals(containerValue));
    }
}
