/*
 * Copyright 2007-2020 University Of Southern California
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.Architecture;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.OS;
import edu.isi.pegasus.planner.classes.Profile;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class SiteCatalogEntryTest {

    public SiteCatalogEntryTest() {}

    @Test
    public void testSiteCatalogEntrySerialization() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        SiteCatalogEntry entry = new SiteCatalogEntry("condor-pool");
        entry.setArchitecture(Architecture.amd64);
        entry.setOS(OS.linux);

        GridGateway gw = new GridGateway();
        gw.setArchitecture(SysInfo.Architecture.x86_64);
        gw.setIdleNodes(3);
        gw.setTotalNodes(40);
        gw.setContact("smarty.isi.edu/jobmanager-pbs");
        gw.setJobType(GridGateway.JOB_TYPE.compute);
        gw.setScheduler(GridGateway.SCHEDULER_TYPE.pbs);
        entry.addGridGateway(gw);

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

        entry.addDirectory(dir);

        String expected =
                "---\n"
                        + "name: \"condor-pool\"\n"
                        + "arch: \"amd64\"\n"
                        + "os.type: \"linux\"\n"
                        + "directories:\n"
                        + " -\n"
                        + "  type: \"sharedScratch\"\n"
                        + "  path: \"/mount/workflows/scratch\"\n"
                        + "  freeSize: \"1GB\"\n"
                        + "  totalSize: \"122GB\"\n"
                        + "  sharedFileSystem: false\n"
                        + "  fileServers:\n"
                        + "   -\n"
                        + "    operation: \"get\"\n"
                        + "    url: \"file:///tmp/workflows/scratch\"\n"
                        + "grids:\n"
                        + " -\n"
                        + "  type: \"gt5\"\n"
                        + "  contact: \"smarty.isi.edu/jobmanager-pbs\"\n"
                        + "  scheduler: \"pbs\"\n"
                        + "  jobtype: \"compute\"\n"
                        + "  idleNodes: 3\n"
                        + "  totalNodes: 40\n";
        String actual = mapper.writeValueAsString(entry);
        assertThat(actual, is(expected));
    }

    @Test
    public void testSiteCatalogEntryDeserialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "name: condor_pool\n"
                        + "arch: x86_64\n"
                        + "os.type: linux\n"
                        + "grids:\n"
                        + "  - type: gt5\n"
                        + "    contact: smarty.isi.edu/jobmanager-fork\n"
                        + "    scheduler: pbs\n"
                        + "    jobtype: auxillary\n"
                        + "  - type: gt5\n"
                        + "    contact: smarty.isi.edu/jobmanager-pbs\n"
                        + "    scheduler: pbs\n"
                        + "    jobtype: compute\n"
                        + "directories:\n"
                        + "  - type: sharedScratch\n"
                        + "    path: /lustre\n"
                        + "    fileServers:\n"
                        + "      - operation: get\n"
                        + "        url: gsiftp://smarty.isi.edu/lustre\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    PATH: /usr/bin:/bin\n"
                        + "  pegasus:\n"
                        + "    clusters.num: 1\n"
                        + "  x-ext: true";

        SiteCatalogEntry entry = mapper.readValue(test, SiteCatalogEntry.class);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.getSiteHandle(), is("condor_pool"));
        assertThat(entry.getArchitecture(), is(Architecture.x86_64));
        assertThat(entry.getSysInfo().getOS(), is(OS.linux));

        testGridGateway(
                entry,
                GridGateway.JOB_TYPE.compute,
                GridGateway.SCHEDULER_TYPE.pbs,
                "smarty.isi.edu/jobmanager-pbs");
        testGridGateway(
                entry,
                GridGateway.JOB_TYPE.auxillary,
                GridGateway.SCHEDULER_TYPE.pbs,
                "smarty.isi.edu/jobmanager-fork");

        Directory directory = entry.getDirectory(Directory.TYPE.shared_scratch);
        testDirectory(directory, Directory.TYPE.shared_scratch, "/lustre");

        List<FileServer> expectedFS = new LinkedList();
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        PegasusURL url = new PegasusURL("gsiftp://smarty.isi.edu/lustre");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        expectedFS.add(fs);

        List<FileServer> actualFS = directory.getFileServers(FileServerType.OPERATION.get);
        assertThat(actualFS, is(expectedFS));

        testProfile(entry, "env", "PATH", "/usr/bin:/bin");
    }

    @Test
    public void testAARCH64InSiteCatalog() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "name: chameleon\n" + "arch: aarch64\n" + "os.type: linux\n";
        SiteCatalogEntry entry = mapper.readValue(test, SiteCatalogEntry.class);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.getSiteHandle(), is("chameleon"));
        assertThat(entry.getArchitecture(), is(Architecture.aarch64));
        assertThat(entry.getSysInfo().getOS(), is(OS.linux));
    }

    private void testGridGateway(
            SiteCatalogEntry entry,
            GridGateway.JOB_TYPE jobType,
            GridGateway.SCHEDULER_TYPE schedulerType,
            String contact) {

        GridGateway gw = entry.getGridGateway(jobType);
        assertThat(gw, is(notNullValue()));
        assertThat(gw.getScheduler(), is(schedulerType));
        assertThat(gw.getContact(), is(contact));
    }

    protected void testDirectory(Directory directory, Directory.TYPE type, String path) {
        assertThat(directory, is(notNullValue()));
        assertThat(directory.getType(), is(type));
        InternalMountPoint mp = directory.getInternalMountPoint();
        assertThat(mp, is(notNullValue()));
        assertThat(mp.getMountPoint(), is(path));
    }

    protected void testProfile(SiteCatalogEntry entry, String namespace, String key, String value) {
        List<Profile> pProfs = entry.getProfiles().getProfiles(namespace);
        assertThat(pProfs, is(notNullValue()));
        assertThat(pProfs.size(), is(1));
        Profile style = pProfs.get(0);
        assertThat(style.getProfileKey(), is(key));
        assertThat(style.getProfileValue(), is(value));
    }
}
