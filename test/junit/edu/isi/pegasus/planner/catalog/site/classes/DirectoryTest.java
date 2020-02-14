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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.util.PegasusURL;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class DirectoryTest {

    public DirectoryTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {}

    @After
    public void tearDown() {}

    @Test
    public void testDirectorySerialization() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(
                new InternalMountPoint("/mount/workflows/scratch", "122GB", "1GB"));
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        PegasusURL url = new PegasusURL("/tmp/workflows/scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);

        String expected =
                "---\n"
                        + "type: \"sharedScratch\"\n"
                        + "path: \"/mount/workflows/scratch\"\n"
                        + "freeSize: \"1GB\"\n"
                        + "totalSize: \"122GB\"\n"
                        + "fileServers:\n"
                        + " -\n"
                        + "  operation: \"get\"\n"
                        + "  url: \"file:///tmp/workflows/scratch\"\n";
        String actual = mapper.writeValueAsString(dir);
        // System.err.println(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void testDirectoryDeserialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "  type: sharedScratch\n"
                        + "  path: /mount/workflows/scratch\n"
                        + "  freeSize: 1GB\n"
                        + "  totalSize: 122GB\n"
                        + "  fileServers:\n"
                        + "    - operation: all\n"
                        + "      url: file:///tmp/workflows/scratch\n";

        Directory dir = mapper.readValue(test, Directory.class);
        assertNotNull(dir);
        assertEquals(Directory.TYPE.shared_scratch, dir.getType());
        assertEquals(
                new InternalMountPoint("/mount/workflows/scratch", "122GB", "1GB").toString(),
                dir.getInternalMountPoint().toString());

        List<FileServer> expectedFS = new LinkedList();
        FileServer fs = new FileServer();
        PegasusURL url = new PegasusURL("file:///tmp/workflows/scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        expectedFS.add(fs);

        List<FileServer> actualFS = dir.getFileServers(FileServerType.OPERATION.all);
        assertThat(actualFS, is(expectedFS));
    }
}
