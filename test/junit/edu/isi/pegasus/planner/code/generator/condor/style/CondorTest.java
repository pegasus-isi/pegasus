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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.credential.impl.PegasusCredentials;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
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
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * To test the Condor style class for condor code generator.
 *
 * @author vahi
 */
public class CondorTest {
    private Condor mCS = null;

    private static final String REQUEST_CPUS_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_CPUS_KEY;

    private static final String REQUEST_GPUS_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_GPUS_KEY;

    private static final String REQUEST_MEMORY_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_MEMORY_KEY;

    private static final String REQUEST_DISK_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_DISK_KEY;

    private static int mTestNumber = 1;

    private DefaultTestSetup mTestSetup;
    private LogManager mLogger;
    private PegasusProperties mProps;
    private PegasusBag mBag;

    // the workflow submit dir associated with the job
    private static final String TEST_WF_SUBMIT_DIR = ".";

    public CondorTest() {}

    @BeforeEach
    public void setUp() throws CondorStyleException {
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
        // System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        mBag = new PegasusBag();
        mProps = PegasusProperties.nonSingletonInstance();
        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.pegasus.code.generator.style.Condor", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);
        mBag.add(PegasusBag.SITE_STORE, this.constructTestSiteStore());
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mCS = new Condor();
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

    @Test
    public void testForHTTPCredentialForRemoteExecWithEmptyCredFile()
            throws CondorStyleException, IOException {
        File credFile = this.createEmptyCredFile();

        try {
            mProps.setProperty(
                    Profiles.NAMESPACES.pegasus
                            + "."
                            + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                    credFile.toString());

            // nothing extra is added into the env or condor variables
            ENV expectedENV = this.defaultENV();
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                    this.defaultCondorVariablesForRemoteExec();
            this.testForCredentialForRemoteExec(
                    expectedENV,
                    expectedCondorVariables,
                    credFile,
                    "staging",
                    "http://pegasus.isi.edu/data/f.in",
                    "testForHTTPCredentialForRemoteExecWithEmptyCredFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testForHTTPCredentialForLocalExecWithEmptyCredFile()
            throws CondorStyleException, IOException {
        File credFile = this.createEmptyCredFile();

        try {
            mProps.setProperty(
                    Profiles.NAMESPACES.pegasus
                            + "."
                            + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                    credFile.toString());

            // nothing extra is added into the env or condor variables
            // as http credential is not specified
            ENV expectedENV = this.defaultENV();
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                    this.defaultCondorVariablesForLocalExec();
            this.testForCredentialForLocalExec(
                    expectedENV,
                    expectedCondorVariables,
                    credFile,
                    "staging",
                    "http://pegasus.isi.edu/data/f.in",
                    "testForHTTPCredentialForLocalExecWithEmptyCredFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testForHTTPCredentialForRemoteExecWithEntryNotInCredFile()
            throws CondorStyleException, IOException {
        File credFile = new File(mTestSetup.getInputDirectory(), "credentials.conf");

        mProps.setProperty(
                Profiles.NAMESPACES.pegasus
                        + "."
                        + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                credFile.toString());

        // nothing extra is added into the env or condor variables
        ENV expectedENV = this.defaultENV();
        edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                this.defaultCondorVariablesForRemoteExec();
        this.testForCredentialForRemoteExec(
                expectedENV,
                expectedCondorVariables,
                credFile,
                "staging",
                "http://pegasus.isi.edu/data/f.in",
                "testForHTTPCredentialForRemoteExecWithEntryNotInCredFile");
    }

    @Test
    public void testForHTTPCredentialForRemoteExecWithEntryInCredFile()
            throws CondorStyleException, IOException {
        File credFile = new File(mTestSetup.getInputDirectory(), "credentials.conf");

        mProps.setProperty(
                Profiles.NAMESPACES.pegasus
                        + "."
                        + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                credFile.toString());

        // the credential file is associated in the env with basename
        // and local path in the condor transfer_input_files key
        ENV expectedENV = this.defaultENV();
        expectedENV.construct(PegasusCredentials.CREDENTIALS_FILE, credFile.getName());
        edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                this.defaultCondorVariablesWithFileTX(credFile.getPath());
        this.testForCredentialForRemoteExec(
                expectedENV,
                expectedCondorVariables,
                credFile,
                "staging",
                "http://download.pegasus.isi.edu/data/f.in",
                "testForHTTPCredentialForRemoteExecWithEntryNotInCredFile");
    }

    @Test
    public void testForS3CredentialForRemoteExecWithEmptyCredFile()
            throws CondorStyleException, IOException {
        File credFile = this.createEmptyCredFile();

        try {
            mProps.setProperty(
                    Profiles.NAMESPACES.pegasus
                            + "."
                            + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                    credFile.toString());

            // the credential file is associated in the env with basename
            // and local path in the condor transfer_input_files key
            ENV expectedENV = this.defaultENV();
            expectedENV.construct(PegasusCredentials.CREDENTIALS_FILE, credFile.getName());
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                    this.defaultCondorVariablesWithFileTX(credFile.getPath());

            this.testForCredentialForRemoteExec(
                    expectedENV,
                    expectedCondorVariables,
                    credFile,
                    "staging",
                    "s3://vahi@pegasus/f.in",
                    "testForS3CredentialForRemoteExecWithEmptyCredFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testForS3CredentialForLocalExecWithEmptyCredFile()
            throws CondorStyleException, IOException {
        File credFile = this.createEmptyCredFile();

        try {
            mProps.setProperty(
                    Profiles.NAMESPACES.pegasus
                            + "."
                            + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                    credFile.toString());

            // the credential file is associated in the env with full path
            // and nothing in condor varialbes
            ENV expectedENV = this.defaultENV();
            expectedENV.construct(PegasusCredentials.CREDENTIALS_FILE, credFile.getPath());
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                    this.defaultCondorVariablesForLocalExec();

            this.testForCredentialForLocalExec(
                    expectedENV,
                    expectedCondorVariables,
                    credFile,
                    "staging",
                    "s3://vahi@pegasus/f.in",
                    "testForS3CredentialForRemoteExecWithEmptyCredFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testForS3CredentialForRemoteExecWithEntryNotInCredFile()
            throws CondorStyleException, IOException {
        File credFile = new File(mTestSetup.getInputDirectory(), "credentials.conf");

        mProps.setProperty(
                Profiles.NAMESPACES.pegasus
                        + "."
                        + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                credFile.toString());

        // the credential file is associated in the env with basename
        // and local path in the condor transfer_input_files key
        ENV expectedENV = this.defaultENV();
        expectedENV.construct(PegasusCredentials.CREDENTIALS_FILE, credFile.getName());
        edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                this.defaultCondorVariablesWithFileTX(credFile.getPath());
        this.testForCredentialForRemoteExec(
                expectedENV,
                expectedCondorVariables,
                credFile,
                "staging",
                "s3://vahi@pegasus/f.in",
                "testForS3CredentialForRemoteExecWithEntryNotInCredFile");
    }

    @Test
    public void testForS3CredentialForLocalExecWithEntryNotInCredFile()
            throws CondorStyleException, IOException {
        File credFile = new File(mTestSetup.getInputDirectory(), "credentials.conf");

        mProps.setProperty(
                Profiles.NAMESPACES.pegasus
                        + "."
                        + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                credFile.toString());

        // the credential file is associated in the env with basename
        // and local path in the condor transfer_input_files key
        ENV expectedENV = this.defaultENV();
        expectedENV.construct(PegasusCredentials.CREDENTIALS_FILE, credFile.getName());
        edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables =
                this.defaultCondorVariablesWithFileTX(credFile.getPath());
        this.testForCredentialForRemoteExec(
                expectedENV,
                expectedCondorVariables,
                credFile,
                "staging",
                "s3://vahi@pegasus/f.in",
                "testForS3CredentialForLocalExecWithEntryNotInCredFile");
    }

    private void testForKey(Job j, String key, String expectedValue) throws CondorStyleException {
        mCS.apply(j);
        assertTrue(j.condorVariables.containsKey(key));
        assertEquals(expectedValue, j.condorVariables.get(key));
    }

    private void testForCredentialForRemoteExec(
            ENV expectedEnv,
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables,
            File credFile,
            String stagingSite,
            String url,
            String testName)
            throws CondorStyleException, IOException {

        this.testForCredential(
                expectedEnv,
                expectedCondorVariables,
                credFile,
                "compute",
                stagingSite,
                url,
                testName);
    }

    private void testForCredentialForLocalExec(
            ENV expectedEnv,
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables,
            File credFile,
            String stagingSite,
            String url,
            String testName)
            throws CondorStyleException, IOException {

        expectedEnv.construct(Condor.PEGASUS_WF_SUBMIT_DIR_KEY, TEST_WF_SUBMIT_DIR);

        this.testForCredential(
                expectedEnv,
                expectedCondorVariables,
                credFile,
                "local",
                stagingSite,
                url,
                testName);
    }

    private void testForCredential(
            ENV expectedEnv,
            edu.isi.pegasus.planner.namespace.Condor expectedCondorVariables,
            File credFile,
            String executionSite,
            String stagingSite,
            String url,
            String testName)
            throws CondorStyleException, IOException {

        // git only tracks x bits on files. So 600 permission
        // does not get preserved at check in .
        // https://unix.stackexchange.com/questions/560448/permission-changed-from-600-to-664-after-git-push-pull
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(credFile.toPath(), perms);

        CredentialHandlerFactory credFactory = new CredentialHandlerFactory();
        credFactory.initialize(mBag);
        mCS = new Condor();
        mCS.initialize(mBag, credFactory);

        Job j = new Job();
        j.condorVariables.construct(
                edu.isi.pegasus.planner.namespace.Condor.WF_SUBMIT_DIR_KEY, TEST_WF_SUBMIT_DIR);
        j.setTXName("pegasus-keg");
        j.setName("pegasus-keg");
        j.setRemoteExecutable("/bin/remote/exec");
        j.setSiteHandle(executionSite);
        j.addCredentialType(stagingSite, url);
        mCS.apply(j);

        mLogger.logEventStart(
                "test.pegasus.code.generator.style.Condor",
                testName,
                Integer.toString(mTestNumber++));

        // System.err.println(j);

        assertEquals(
                expectedEnv.size(),
                j.envVariables.size(),
                "Num of Environment Variables in " + j.envVariables);
        for (Object key : expectedEnv.keySet()) {
            assertEquals(
                    expectedEnv.get(key),
                    j.envVariables.get(key),
                    "Value for key " + key + " in " + j.envVariables);
        }
        // System.err.println(j.condorVariables);
        assertEquals(
                expectedCondorVariables.size(),
                j.condorVariables.size(),
                "Num of Condor Variables in " + j.condorVariables);
        for (Object key : expectedCondorVariables.keySet()) {
            assertEquals(
                    expectedCondorVariables.get(key),
                    j.condorVariables.get(key),
                    "Value for key " + key + " in " + j.condorVariables);
        }

        mLogger.logEventCompletion(0);
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

    private ENV defaultENV() {
        ENV env = new ENV();
        env.construct("LANG", Job.DEFAULT_ENV_LANG_VALUE);
        return env;
    }

    private edu.isi.pegasus.planner.namespace.Condor defaultCondorVariablesForRemoteExec() {
        edu.isi.pegasus.planner.namespace.Condor condorVar =
                new edu.isi.pegasus.planner.namespace.Condor();
        condorVar.construct("+WantIOProxy", "True");
        condorVar.construct("universe", "vanilla");
        condorVar.construct(
                edu.isi.pegasus.planner.namespace.Condor.WF_SUBMIT_DIR_KEY, TEST_WF_SUBMIT_DIR);
        return condorVar;
    }

    private edu.isi.pegasus.planner.namespace.Condor defaultCondorVariablesForLocalExec() {
        edu.isi.pegasus.planner.namespace.Condor condorVar =
                new edu.isi.pegasus.planner.namespace.Condor();
        condorVar.construct("universe", "local");
        condorVar.construct(
                edu.isi.pegasus.planner.namespace.Condor.WF_SUBMIT_DIR_KEY, TEST_WF_SUBMIT_DIR);
        return condorVar;
    }

    private edu.isi.pegasus.planner.namespace.Condor defaultCondorVariablesWithFileTX(String file) {
        edu.isi.pegasus.planner.namespace.Condor condorVar =
                this.defaultCondorVariablesForRemoteExec();
        condorVar.construct(Condor.EMPTY_TRANSFER_OUTPUT_KEY, "\"\"");
        condorVar.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, "ON_EXIT");
        condorVar.construct(
                edu.isi.pegasus.planner.namespace.Condor.SHOULD_TRANSFER_FILES_KEY, "YES");
        condorVar.construct(edu.isi.pegasus.planner.namespace.Condor.TRANSFER_IP_FILES_KEY, file);
        condorVar.construct(edu.isi.pegasus.planner.namespace.Condor.ENCRYPT_IP_FILES_KEY, file);
        return condorVar;
    }

    private File createEmptyCredFile() throws IOException {

        File credFile =
                File.createTempFile(
                        "pegasus.", ".credentials.conf", new File(mTestSetup.getInputDirectory()));
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(credFile.toPath(), perms);
        return credFile;
    }
}
