/*
 * Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.code.generator.condor.style;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import org.junit.Before;
import org.junit.Test;

/**
 * To test the Condor style class for condor code generator.
 *
 * @author vahi
 */
public class CondorTest {
    private Condor cs = null;

    private static final String REQUEST_CPUS_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_CPUS_KEY;

    private static final String REQUEST_GPUS_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_GPUS_KEY;

    private static final String REQUEST_MEMORY_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_MEMORY_KEY;

    private static final String REQUEST_DISK_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_DISK_KEY;
    private DefaultTestSetup mTestSetup;
    private LogManager mLogger;

    public CondorTest() {}

    @Before
    public void setUp() throws CondorStyleException {
        mTestSetup = new DefaultTestSetup();
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        mLogger = mTestSetup.loadLogger(props);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.pegasus.code.generator.style.Condor", "setup", "0");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.SITE_STORE, this.constructTestSiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        cs = new Condor();
        cs.initialize(bag, new CredentialHandlerFactory());
    }

    @Test
    public void testPegasusProfileCores() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.CORES_KEY, "5");
        testForKey(j, REQUEST_CPUS_KEY, "5");
    }

    @Test
    public void testPegasusProfileGPUS() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.GPUS_KEY, "6");
        testForKey(j, REQUEST_GPUS_KEY, "6");
    }

    @Test
    public void testPegasusProfileCoresAndCondorKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.CORES_KEY, "5");
        j.condorVariables.checkKeyInNS(REQUEST_CPUS_KEY, "6");
        testForKey(j, REQUEST_CPUS_KEY, "6");
    }

    @Test
    public void testPegasusProfileGPUSAndCondorKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.GPUS_KEY, "5");
        j.condorVariables.checkKeyInNS(REQUEST_GPUS_KEY, "6");
        testForKey(j, REQUEST_GPUS_KEY, "6");
    }

    @Test
    public void testPegasusProfileMemory() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, "5");
        testForKey(j, REQUEST_MEMORY_KEY, "5");
    }

    @Test
    public void testPegasusProfileMemoryAndCondorKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, "5");
        j.condorVariables.checkKeyInNS(REQUEST_MEMORY_KEY, "6");
        testForKey(j, REQUEST_MEMORY_KEY, "6");
    }

    @Test
    public void testPegasusProfileDiskspaceAndCondorKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.DISKSPACE_KEY, "5");
        testForKey(j, REQUEST_DISK_KEY, Long.toString(5 * 1024));
    }

    private void testForKey(Job j, String key, String expectedValue) throws CondorStyleException {
        cs.apply(j);
        assertTrue(j.condorVariables.containsKey(key));
        assertEquals(expectedValue, j.condorVariables.get(key));
    }

    private SiteStore constructTestSiteStore() {
        SiteStore store = new SiteStore();

        SiteCatalogEntry computeSite = new SiteCatalogEntry("compute");
        computeSite.setArchitecture(SysInfo.Architecture.x86_64);
        computeSite.setOS(SysInfo.OS.linux);
        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(
                new InternalMountPoint("/internal/workflows/compute/shared-scratch"));
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        PegasusURL url =
                new PegasusURL("gsiftp://compute.isi.edu/workflows/compute/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        computeSite.addDirectory(dir);
        store.addEntry(computeSite);

        // add a default local site
        SiteCatalogEntry localSite = new SiteCatalogEntry("local");
        localSite.setArchitecture(SysInfo.Architecture.x86_64);
        localSite.setOS(SysInfo.OS.linux);
        dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(
                new InternalMountPoint("/internal/workflows/local/shared-scratch"));
        fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.all);
        url = new PegasusURL("gsiftp://local.isi.edu/workflows/local/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        localSite.addDirectory(dir);
        store.addEntry(localSite);

        return store;
    }
}
