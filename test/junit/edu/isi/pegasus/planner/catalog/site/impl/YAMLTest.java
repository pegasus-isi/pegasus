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
package edu.isi.pegasus.planner.catalog.site.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.Architecture;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.OS;

import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.EnvSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

/**
 *
 * A Test class to test the Site catalog implementation un YAML
 *
 * @author Mukund Murrali
 */
public class YAMLTest {

    /**
     * The properties used for this test.
     */
    private static final String PROPERTIES_BASENAME = "properties";

    private static final String EXPANDED_SITE = "bamboo";
    private static final String EXPANDED_ARCH = "x86_64";
    private static final String EXPANDED_OS = "linux";
    private static final String EXPANDED_DIRECTORY_TYPE = Directory.YAML_TYPE.sharedScratch.toString();
    private static final String EXPANDED_INTERNAL_MOUNT_POINT = "/bamboo/scratch";
    private static final String EXPANDED_EXTERNAL_MOUNT_POINT = "gsiftp://cartman.isi.edu/bamboo/scratch";
    private static final String EXPANDED_PEGASUS_HOME = "/usr/bin";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;
    private SiteCatalog mCatalog;

    @BeforeClass
    public static void setUpClass() {
        Map<String, String> testEnvVariables = new HashMap();
        testEnvVariables.put("SITE", EXPANDED_SITE);
        testEnvVariables.put("DIRECTORY_TYPE", EXPANDED_DIRECTORY_TYPE);
        testEnvVariables.put("INTERNAL_MOUNT_POINT", EXPANDED_INTERNAL_MOUNT_POINT);
        testEnvVariables.put("EXTERNAL_MOUNT_POINT", EXPANDED_EXTERNAL_MOUNT_POINT);
        testEnvVariables.put("ARCH", EXPANDED_ARCH);
        testEnvVariables.put("OS", EXPANDED_OS);
        testEnvVariables.put("PEGASUS_HOME", EXPANDED_PEGASUS_HOME);
        EnvSetup.setEnvironmentVariables(testEnvVariables);
    }

    @AfterClass
    public static void tearDownClass() {
    }

    public YAMLTest() {

    }

    /**
     * Setup the logger and properties that all test functions require
     */
    @Before
    public final void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());

        //set some properties required to set up the test
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY,
                "YAML");
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.yml").getAbsolutePath());

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.site.impl.YAML", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        //mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
        //load the site catalog backend
        mCatalog = SiteFactory.loadInstance(mProps);
        List l = new LinkedList();
        l.add("*");
        mCatalog.load(l);
        mLogger.logEventCompletion();
    }

    @Test
    public void testWholeCount() throws Exception {
        mLogger.logEventStart("test.catalog.site.impl.YAML", "whole-count-test", Integer.toString(mTestNumber++));
        Set<String> entries = mCatalog.list();
        assertEquals("Expected total number of entries", 7, entries.size());
        SiteCatalogEntry entry = mCatalog.lookup("osg");
        assertNotNull(entry);
        mLogger.logEventCompletion();
    }

    @Test
    public void testOSGEntry() throws Exception {
        mLogger.logEventStart("test.catalog.site.impl.YAML", "osg-entry", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("osg");
        assertNotNull(entry);

        assertEquals("osg", entry.getSiteHandle());
        assertEquals(Architecture.x86, entry.getArchitecture());
        assertEquals(OS.linux, entry.getOS());

        Directory directory = entry.getDirectory(Directory.TYPE.local_scratch);
        testDirectory(directory,
                Directory.TYPE.local_scratch,
                "/tmp"
        );

        testFileServer(directory.getFileServers(FileServerType.OPERATION.all),
                FileServerType.OPERATION.all,
                "file:///tmp");

        testProfile(entry, "pegasus", Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);
        testProfile(entry, "condor", Condor.UNIVERSE_KEY, "vanilla");

        mLogger.logEventCompletion();

    }

    @Test
    public void testSRMStagingSiteWithDifferentURLS() throws Exception {
        mLogger.logEventStart("test.catalog.site.impl.YAML", "unl", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("unl");
        assertNotNull(entry);

        assertEquals("unl", entry.getSiteHandle());
        assertEquals(Architecture.x86, entry.getArchitecture());
        assertEquals(OS.linux, entry.getOS());

        Directory directory = entry.getDirectory(Directory.TYPE.shared_scratch);
        testDirectory(directory,
                Directory.TYPE.shared_scratch,
                "/internal-mnt/panfs/CMS/data/engage/scratch"
        );

        testFileServer(directory.getFileServers(FileServerType.OPERATION.get),
                FileServerType.OPERATION.get,
                "http://ff-se.unl.edu:8443/panfs/panasas/CMS/data/engage/scratch");

        testFileServer(directory.getFileServers(FileServerType.OPERATION.put),
                FileServerType.OPERATION.put,
                "srm://ff-se.unl.edu:8443/panfs/panasas/CMS/data/engage/scratch");

        mLogger.logEventCompletion();
    }

    @Test
    public void testSharedFSSite() throws Exception {
        mLogger.logEventStart("test.catalog.site.impl.YAML", "sharedfs-site", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("isi");
        assertNotNull(entry);

        assertEquals("isi", entry.getSiteHandle());
        assertEquals(Architecture.x86_64, entry.getArchitecture());
        assertEquals(OS.linux, entry.getOS());

        Directory directory = entry.getDirectory(Directory.TYPE.shared_scratch);
        testDirectory(directory,
                Directory.TYPE.shared_scratch,
                "/nfs/scratch01"
        );

        testFileServer(directory.getFileServers(FileServerType.OPERATION.get),
                FileServerType.OPERATION.get,
                "http://skynet-data.isi.edu/nfs/scratch01");

        testFileServer(directory.getFileServers(FileServerType.OPERATION.put),
                FileServerType.OPERATION.put,
                "gsiftp://skynet-data.isi.edu/scratch01");

        testGridGateway(entry, GridGateway.JOB_TYPE.compute, GridGateway.SCHEDULER_TYPE.pbs, "smarty.isi.edu/jobmanager-pbs");
        testGridGateway(entry, GridGateway.JOB_TYPE.auxillary, GridGateway.SCHEDULER_TYPE.pbs, "smarty.isi.edu/jobmanager-fork");

        mLogger.logEventCompletion();
    }

    @Test
    public void testExpandedSite() throws Exception {
        mLogger.logEventStart("test.catalog.site.impl.YAML", "expanded-site", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup(EXPANDED_SITE);
        assertNotNull(entry);

        assertEquals(EXPANDED_SITE, entry.getSiteHandle());
        assertEquals(EXPANDED_ARCH, entry.getArchitecture().toString());
        assertEquals(EXPANDED_OS, entry.getOS().toString());

        Directory directory = entry.getDirectory(Directory.TYPE.value(EXPANDED_DIRECTORY_TYPE));
        testDirectory(directory,
                Directory.TYPE.value(EXPANDED_DIRECTORY_TYPE),
                EXPANDED_INTERNAL_MOUNT_POINT
        );

        testFileServer(directory.getFileServers(FileServerType.OPERATION.all),
                FileServerType.OPERATION.all,
                EXPANDED_EXTERNAL_MOUNT_POINT);

        testProfile(entry, "env", "PEGASUS_HOME", EXPANDED_PEGASUS_HOME);
        mLogger.logEventCompletion();
    }

    @Test
    public void testMetadata() {
        mLogger.logEventStart("test.catalog.site.impl.YAML", "metadata", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("ec2");
        assertNotNull(entry);

        testProfile(entry, "metadata", "resource-type", "cloud");
        mLogger.logEventCompletion();
    }

    private void testGridGateway(SiteCatalogEntry entry, GridGateway.JOB_TYPE jobType, GridGateway.SCHEDULER_TYPE schedulerType, String contact) {

        GridGateway gw = entry.getGridGateway(jobType);
        assertNotNull(gw);
        assertEquals(schedulerType, gw.getScheduler());
        assertEquals(contact, gw.getContact());
    }

    private void testFileServer(List<FileServer> servers, FileServerType.OPERATION operation, String url) {
        assertNotNull(servers);
        assertEquals(1, servers.size());
        FileServer fs = servers.get(0);
        assertEquals(operation, fs.getSupportedOperation());
        assertEquals(url, fs.getURL());
    }

    protected void testDirectory(Directory directory, Directory.TYPE type, String path) {
        assertNotNull(directory);
        assertEquals(type, directory.getType());
        InternalMountPoint mp = directory.getInternalMountPoint();
        assertNotNull(mp);
        assertEquals(path, mp.getMountPoint());
    }

    protected void testProfile(SiteCatalogEntry entry, String namespace, String key, String value) {
        List<Profile> pProfs = entry.getProfiles().getProfiles(namespace);
        assertNotNull(pProfs);
        assertEquals(1, pProfs.size());
        Profile style = pProfs.get(0);
        assertEquals(style.getProfileKey(), key);
        assertEquals(style.getProfileValue(), value);
    }

    @Test
    public void testInvalidYAMLFile() {
        PegasusProperties mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());

        //set some properties required to set up the test
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY,
                "YAML");
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites_invalid.yml").getAbsolutePath());

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.site.impl.YAML", "setup", "0");
        PegasusBag mBag = new PegasusBag();
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        //mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
        //load the site catalog backend
        SiteCatalog mCatalog = SiteFactory.loadInstance(mProps);
        List l = new LinkedList();
        l.add("*");
        try {
            mCatalog.load(l);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("line 4: Problem in the line :4, column:7 with tag os:\"linux\"^"));
        }
    }

    @Test
    public void testInvalidSiteYAMLFormat() {
        PegasusProperties mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());

        //set some properties required to set up the test
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY,
                "YAML");
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites_invalid_fromat.yml").getAbsolutePath());

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.site.impl.YAML", "setup", "0");
        PegasusBag mBag = new PegasusBag();
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        //mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
        //load the site catalog backend
        SiteCatalog mCatalog = SiteFactory.loadInstance(mProps);
        List l = new LinkedList();
        l.add("*");
        try {
            mCatalog.load(l);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Error 1:{Unknown fields [\"aa\"] present in"));
        }
    }

    @Test
    public void testEmptySiteYAMLFormat() {
        PegasusProperties mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());

        //set some properties required to set up the test
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY,
                "YAML");
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites_empty.yml").getAbsolutePath());

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.site.impl.YAML", "setup", "0");
        PegasusBag mBag = new PegasusBag();
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        //mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
        //load the site catalog backend
        SiteCatalog mCatalog = SiteFactory.loadInstance(mProps);
        List l = new LinkedList();
        l.add("*");
        try {
            mCatalog.load(l);
        } catch (RuntimeException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        assertTrue(true);
    }

}
