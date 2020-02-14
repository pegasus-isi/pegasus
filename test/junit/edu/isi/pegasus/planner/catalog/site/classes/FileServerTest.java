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
package edu.isi.pegasus.planner.catalog.site.classes;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.util.PegasusURL;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Karan Vahi
 */
public class FileServerTest {
    
    public FileServerTest() {
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
    public void testFileServerSerialization()throws IOException{
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        PegasusURL url = new PegasusURL("http://test.isi.edu/shared/data/scratch");
        fs.setURLPrefix(url.getURLPrefix());
                    fs.setProtocol(url.getProtocol());
                    fs.setMountPoint(url.getPath());
        
        
        String expected = "---\n"
                + "operation: \"get\"\n"
                + "url: \"http://test.isi.edu/shared/data/scratch\"\n";
        String actual = mapper.writeValueAsString(fs); 
        System.err.println(actual);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testFileServerDeserialization() throws IOException{
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
       
        String test = 
                "operation: all\n" +
                "url: file:///tmp/workflows/scratch";

        FileServer fs = mapper.readValue(test, FileServer.class);
        
        assertNotNull(fs);
        assertEquals(FileServer.OPERATION.all, fs.getSupportedOperation());
        assertEquals("file:///tmp/workflows/scratch", fs.getURL());
    }
}
