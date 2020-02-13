/**
 *  Copyright 2007-2020 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test cases for the YAML parser for Transformation Catalog
 * @author Karan Vahi
 */
public class TransformationCatalogYAMLParserTest {
    
    
    private TestSetup mTestSetup;
    
    private LogManager mLogger;
    
    
    public TransformationCatalogYAMLParserTest() {
        
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory( this.getClass() );
        
        mLogger = mTestSetup.loadLogger( PegasusProperties.nonSingletonInstance() );

        mLogger.setLevel(LogManager.ERROR_MESSAGE_LEVEL);
        mLogger.logEventStart("test.edu.isi.pegasus.planner.parser.TransformationCatalogYamlParser", "setup", "0");
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
    
    @Test
    public void testSingleProfileParsing(){
        String input = 
            "APP_HOME: \"/tmp/myscratch\"\n";
        
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        List<Profile> profiles = parser.createProfiles("env", n);
        assertEquals("Number of profiles", 1, profiles.size());
        Profile expected = new Profile("env", "APP_HOME", "/tmp/myscratch");
        Profile p = profiles.get(0);
        assertProfile( expected, p );
    }
    
    @Test
    public void testMultipleProfileParsing(){
        String input = 
            "APP_HOME: \"/tmp/myscratch\"\n"+
            "JAVA_HOME: \"/opt/java/1.6\"\n";
        
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        List<Profile> profiles = parser.createProfiles("env", n);
        assertEquals("Number of profiles", 2, profiles.size());
        
        Profile expected = new Profile("env", "APP_HOME", "/tmp/myscratch");
        Profile p = profiles.get(0);
        assertProfile( expected, p );
        
        p = profiles.get(1);
        expected = new Profile("env", "JAVA_HOME", "/opt/java/1.6");
        assertProfile( expected, p );
        
    }
                
    @Test
    public void testProfileWithDifferentNamespaceParsing(){
        String input = 
"    env:\n" +
"        APP_HOME: \"/tmp/myscratch\"\n" +
"        JAVA_HOME: \"/opt/java/1.6\"\n" +
"    pegasus:\n" +
"        clusters.num: \"1\"";
        
        
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        Profiles profiles = parser.createProfiles(n);
        
        assertEquals("Number of Env Profiles", 2, profiles.getProfiles(Profiles.NAMESPACES.env).size() );
        assertEquals("Number of Pegasus Profiles", 1, profiles.getProfiles(Profiles.NAMESPACES.pegasus).size() );
        
        Profile expected = new Profile("pegasus", "clusters.num", "1");
        Profile p = profiles.getProfiles(Profiles.NAMESPACES.pegasus).get(0);
        assertProfile( expected, p );
    }
    
    @Test
    public void testTransformationWithoutSiteData(){
        String input = 
"      namespace: \"example\"\n" +
"      name: \"keg\"\n" +
"      version: \"1.0\"\n" +
"      profiles:\n" +
"          env:\n" +
"              APP_HOME: \"/tmp/myscratch\"\n" +
"              JAVA_HOME: \"/opt/java/1.6\"\n" +
"          pegasus:\n" +
"              clusters.num: \"1\"";
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        List<TransformationCatalogEntry> entries = parser.createTransformationCatalogEntry(n);
        assertEquals("Number of TC Entries", 1 , entries.size() );
        
        TransformationCatalogEntry expected = new TransformationCatalogEntry("example", "keg", "1.0");
        expected.addProfile(new Profile("env", "APP_HOME", "/tmp/myscratch"));
        expected.addProfile(new Profile("env", "JAVA_HOME", "/opt/java/1.6"));
        expected.addProfile(new Profile("pegasus", "clusters.num", "1"));
        TransformationCatalogEntry actual = entries.get(0);
        assertEquals("Mismatch in TC Entry", expected.toString(), actual.toString() );
    }
    
    @Test
    public void testTransformation(){
        String input = 
"      namespace: \"example\"\n" +
"      name: \"keg\"\n" +
"      version: \"1.0\"\n" +
"      profiles:\n" +
"          pegasus:\n" +
"              clusters.num: \"1\"\n" +
"\n" +
"      requires:\n" +
"          - anotherTr\n" +
"\n" +
"      sites:\n" +
"        - name: \"isi\"\n" +
"          type: \"installed\"\n" +
"          pfn: \"/path/to/keg\"\n" +
"          arch: \"x86_64\"\n" +
"          os.type: \"linux\"\n" +
"          os.release: \"fc\"\n" +
"          os.version: \"1.0\"\n" +
"          profiles:\n" +
"            env:\n" +
"                Hello: World\n" +
"                JAVA_HOME: /bin/java.1.6\n" +
"            condor:\n" +
"                FOO: bar\n" +
"          container: centos-pegasus";
                
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        List<TransformationCatalogEntry> entries = parser.createTransformationCatalogEntry(n);
        assertEquals("Number of TC Entries", 1 , entries.size() );
        
        TransformationCatalogEntry expected = new TransformationCatalogEntry("example", "keg", "1.0");
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.linux);
        s.setOSRelease("fc");
        s.setOSVersion("1.0");
        expected.setResourceId("isi");
        expected.setPhysicalTransformation("/path/to/keg");
        expected.setSysInfo(s);
        expected.addProfile(new Profile("pegasus", "clusters.num", "1"));
        expected.addProfile(new Profile("env", "Hello", "World"));
        expected.addProfile(new Profile("env", "JAVA_HOME", "/bin/java.1.6"));
        expected.addProfile(new Profile("condor", "FOO", "bar"));
        expected.setContainer( new Container("centos-pegasus"));
        TransformationCatalogEntry actual = entries.get(0);
        assertEquals("Mismatch in TC Entry", expected.toString(), actual.toString() );
    }
    
    
    @Test
    public void testContainer(){
        String input = 
"      name: centos-pegasus\n" +
"      type: docker\n" +
"      image: docker:///rynge/montage:latest\n" +
"      mounts: \n" +
"        - /Volumes/Work/lfs1:/shared-data/:ro\n" +
"      profiles:\n" +
"        env:\n" +
"            JAVA_HOME: /opt/java/1.6";
        
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        Container c = parser.createContainer(n);
        Container expected = new Container("centos-pegasus");
        expected.setType(Container.TYPE.docker);
        expected.setImageURL("docker:///rynge/montage:latest");
        expected.addProfile(new Profile("env", "JAVA_HOME", "/opt/java/1.6" ));
        expected.addMountPoint("/Volumes/Work/lfs1:/shared-data/:ro");
        
        assertEquals(expected.toString(), c.toString());
        
    }
    
    @Test
    public void testHooks(){
        String input = 
" shell:\n" +
"    - _on: start\n" +
"      cmd: /bin/date\n" +
"    - _on: end\n" +
"      cmd: /bin/echo \"Finished\"";
        JsonNode n = this.getJsonNode(input);
        TransformationCatalogYAMLParser parser = new TransformationCatalogYAMLParser(mLogger);
        Notifications actual = parser.createNotifications(n);
        Notifications expected = new Notifications();
        expected.add(new Invoke(Invoke.WHEN.start,"/bin/date"));
        expected.add(new Invoke(Invoke.WHEN.end,"/bin/echo \"Finished\"" ));
        assertEquals(expected.toString(),actual.toString());
    }
    
    private JsonNode getJsonNode( String input ){
        JsonNode root = null;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        Reader r = new StringReader(input);
        try {
            root = mapper.readTree(r);
        }
        catch (Exception e) {
            throw new ScannerException("Error in loading the yaml file " + r, e);
        }
        return root;
    }

    private void assertProfile(Profile expected, Profile actual) {
        assertEquals("Namespace mismatch", expected.getProfileNamespace(), actual.getProfileNamespace());
        assertEquals("Key mismatch", expected.getProfileKey(), actual.getProfileKey());
        assertEquals("Value mismatch", expected.getProfileValue(), actual.getProfileValue());
    }
    
    private void assertNotification(Invoke expected, Invoke actual) {
        assertEquals("Condition mismatch", expected.getWhen(), actual.getWhen());
        assertEquals("Command mismatch", expected.getWhat(), actual.getWhat());
    }
}
