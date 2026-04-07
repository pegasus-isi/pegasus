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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

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
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A Test class to test the Site catalog implementation
 *
 * @author Karan Vahi
 */
public class XMLTest {
    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "properties";

    private static final String EXPANDED_SITE = "bamboo";
    private static final String EXPANDED_ARCH = "x86_64";
    private static final String EXPANDED_OS = "linux";
    private static final String EXPANDED_DIRECTORY_TYPE = Directory.TYPE.shared_scratch.toString();
    private static final String EXPANDED_INTERNAL_MOUNT_POINT = "/bamboo/scratch";
    private static final String EXPANDED_EXTERNAL_MOUNT_POINT =
            "gsiftp://cartman.isi.edu/bamboo/scratch";
    private static final String EXPANDED_PEGASUS_HOME = "/usr/bin";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;
    private SiteCatalog mCatalog;
    private File mExpandedCatalogFile;

    public XMLTest() {}

    /** Setup the logger and properties that all test functions require */
    @BeforeEach
    public final void setUp() throws IOException {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
        mProps.setProperty(
                "pegasus.home.schemadir",
                Paths.get("share", "pegasus", "schema").toAbsolutePath().toString());
        mExpandedCatalogFile =
                expandCatalogFile(new File(mTestSetup.getInputDirectory(), "sites.xml4"), ".xml4");

        // set some properties required to set up the test
        mProps.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY, "XML");
        mProps.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                mExpandedCatalogFile.getAbsolutePath());

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.site.impl.XML", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        // mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );

        // load the site catalog backend
        mCatalog = SiteFactory.loadInstance(mBag);
        List l = new LinkedList();
        l.add("*");
        mCatalog.load(l);
        mLogger.logEventCompletion();
    }

    @AfterEach
    public void tearDown() {
        if (mExpandedCatalogFile != null) {
            mExpandedCatalogFile.delete();
        }
    }

    @Test
    public void testWholeCount() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.impl.XML", "whole-count-test", Integer.toString(mTestNumber++));
        Set<String> entries = mCatalog.list();
        assertThat("Expected total number of entries", entries.size(), is(7));
        SiteCatalogEntry entry = mCatalog.lookup("osg");
        assertThat(entry, is(notNullValue()));
        mLogger.logEventCompletion();
    }

    @Test
    public void testOSGEntry() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.impl.XML", "osg-entry", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("osg");
        assertThat(entry, is(notNullValue()));

        assertThat(entry.getSiteHandle(), is("osg"));
        assertThat(entry.getArchitecture(), is(Architecture.x86));
        assertThat(entry.getOS(), is(OS.linux));

        Directory directory = entry.getDirectory(Directory.TYPE.local_scratch);
        testDirectory(directory, Directory.TYPE.local_scratch, "/tmp");

        testFileServer(
                directory.getFileServers(FileServerType.OPERATION.all),
                FileServerType.OPERATION.all,
                "file:///tmp");

        testProfile(entry, "pegasus", Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);
        testProfile(entry, "condor", Condor.UNIVERSE_KEY, "vanilla");

        mLogger.logEventCompletion();
    }

    @Test
    public void testSRMStagingSiteWithDifferentURLS() throws Exception {
        mLogger.logEventStart("test.catalog.site.impl.XML", "unl", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("unl");
        assertThat(entry, is(notNullValue()));

        assertThat(entry.getSiteHandle(), is("unl"));
        assertThat(entry.getArchitecture(), is(Architecture.x86));
        assertThat(entry.getOS(), is(OS.linux));

        Directory directory = entry.getDirectory(Directory.TYPE.shared_scratch);
        testDirectory(
                directory,
                Directory.TYPE.shared_scratch,
                "/internal-mnt/panfs/CMS/data/engage/scratch");

        testFileServer(
                directory.getFileServers(FileServerType.OPERATION.get),
                FileServerType.OPERATION.get,
                "http://ff-se.unl.edu:8443/panfs/panasas/CMS/data/engage/scratch");

        testFileServer(
                directory.getFileServers(FileServerType.OPERATION.put),
                FileServerType.OPERATION.put,
                "srm://ff-se.unl.edu:8443/panfs/panasas/CMS/data/engage/scratch");

        mLogger.logEventCompletion();
    }

    @Test
    public void testSharedFSSite() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.impl.XML", "sharedfs-site", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("isi");
        assertThat(entry, is(notNullValue()));

        assertThat(entry.getSiteHandle(), is("isi"));
        assertThat(entry.getArchitecture(), is(Architecture.x86_64));
        assertThat(entry.getOS(), is(OS.linux));

        Directory directory = entry.getDirectory(Directory.TYPE.shared_scratch);
        testDirectory(directory, Directory.TYPE.shared_scratch, "/nfs/scratch01");

        testFileServer(
                directory.getFileServers(FileServerType.OPERATION.get),
                FileServerType.OPERATION.get,
                "http://skynet-data.isi.edu/nfs/scratch01");

        testFileServer(
                directory.getFileServers(FileServerType.OPERATION.put),
                FileServerType.OPERATION.put,
                "gsiftp://skynet-data.isi.edu/scratch01");

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

        mLogger.logEventCompletion();
    }

    @Test
    public void testExpandedSite() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.impl.XML", "expanded-site", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup(EXPANDED_SITE);
        assertThat(entry, is(notNullValue()));

        assertThat(entry.getSiteHandle(), is(EXPANDED_SITE));
        assertThat(entry.getArchitecture().toString(), is(EXPANDED_ARCH));
        assertThat(entry.getOS().toString(), is(EXPANDED_OS));

        Directory directory = entry.getDirectory(Directory.TYPE.value(EXPANDED_DIRECTORY_TYPE));
        testDirectory(
                directory,
                Directory.TYPE.value(EXPANDED_DIRECTORY_TYPE),
                EXPANDED_INTERNAL_MOUNT_POINT);

        testFileServer(
                directory.getFileServers(FileServerType.OPERATION.all),
                FileServerType.OPERATION.all,
                EXPANDED_EXTERNAL_MOUNT_POINT);

        testProfile(entry, "env", "PEGASUS_HOME", EXPANDED_PEGASUS_HOME);
        mLogger.logEventCompletion();
    }

    @Test
    public void testMetadata() {
        mLogger.logEventStart(
                "test.catalog.site.impl.XML", "metadata", Integer.toString(mTestNumber++));
        SiteCatalogEntry entry = mCatalog.lookup("ec2");
        assertThat(entry, is(notNullValue()));

        testProfile(entry, "metadata", "resource-type", "cloud");
        mLogger.logEventCompletion();
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

    private void testFileServer(
            List<FileServer> servers, FileServerType.OPERATION operation, String url) {
        assertThat(servers, is(notNullValue()));
        assertThat(servers.size(), is(1));
        FileServer fs = servers.get(0);
        assertThat(fs.getSupportedOperation(), is(operation));
        assertThat(fs.getURL(), is(url));
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

    private File expandCatalogFile(File source, String suffix) throws IOException {
        String expanded =
                Files.readString(source.toPath(), StandardCharsets.UTF_8)
                        .replace("${SITE}", EXPANDED_SITE)
                        .replace("${DIRECTORY_TYPE}", EXPANDED_DIRECTORY_TYPE)
                        .replace("${INTERNAL_MOUNT_POINT}", EXPANDED_INTERNAL_MOUNT_POINT)
                        .replace("${EXTERNAL_MOUNT_POINT}", EXPANDED_EXTERNAL_MOUNT_POINT)
                        .replace("${ARCH}", EXPANDED_ARCH)
                        .replace("${OS}", EXPANDED_OS)
                        .replace("${PEGASUS_HOME}", EXPANDED_PEGASUS_HOME);

        File expandedFile = File.createTempFile("sites-expanded", suffix);
        Files.writeString(expandedFile.toPath(), expanded, StandardCharsets.UTF_8);
        return expandedFile;
    }
}
