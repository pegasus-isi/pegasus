/*
 * Copyright 2007-2024 University Of Southern California
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
 * limit
 */
package edu.isi.pegasus.common.credential.impl;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class PegasusCredentialsTest {

    private PegasusBag mBag;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;
    private PegasusProperties mProps;
    private static String CREDENTIALS_TEST_FILE = "credentials.conf";

    public PegasusCredentialsTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        mProps = PegasusProperties.nonSingletonInstance();
        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.pegasus.common.credentials.PegasusCredentials", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.SITE_STORE, new SiteStore());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);
        mLogger.logEventCompletion();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testVerifyForHTTPCredsWithNonExistantFile() throws IOException {
        File credFile = new File("/does/not/exist/credfile");
        try {
            this.testVerify(
                    credFile,
                    null,
                    CredentialHandler.TYPE.http,
                    "testVerifyForHTTPCredsWithNonExistantFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testVerifyForHTTPCredsWithExistingFile() throws IOException {
        File credFile =
                File.createTempFile(
                        "pegasus.", ".credentials.conf", new File(mTestSetup.getInputDirectory()));
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(credFile.toPath(), perms);
        try {
            this.testVerify(
                    credFile,
                    null,
                    CredentialHandler.TYPE.http,
                    "testVerifyForHTTPCredsWithExistingFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testVerifyForHTTPCredsWithExistingFileAndIncorrectPermissions() throws IOException {
        // credentials file exists. so need to check for permissions
        File credFile =
                File.createTempFile(
                        "pegasus.", ".credentials.conf", new File(mTestSetup.getInputDirectory()));
        Exception expected =
                new RuntimeException("Unable to verify credential of type http for job");
        try {
            this.testVerify(
                    credFile,
                    expected,
                    CredentialHandler.TYPE.http,
                    "testVerifyForHTTPCredsWithExistingFileIncorrectPerm");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testVerifyForS3CredsWithNonExistantFile() throws IOException {
        File credFile = new File("/does/not/exist/credfile");
        Exception expected =
                new RuntimeException("Unable to verify credential of type credentials for job");
        try {
            this.testVerify(
                    credFile,
                    expected,
                    CredentialHandler.TYPE.credentials,
                    "testVerifyForS3CredsWithNonExistantFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testVerifyForS3CredsWithExistingFile() throws IOException {
        File credFile =
                File.createTempFile(
                        "pegasus.", ".credentials.conf", new File(mTestSetup.getInputDirectory()));
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(credFile.toPath(), perms);
        try {
            this.testVerify(
                    credFile,
                    null,
                    CredentialHandler.TYPE.credentials,
                    "testVerifyForS3CredsWithExistingFile");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testVerifyForS3CredsWithExistingFileAndIncorrectPermissions() throws IOException {

        File credFile =
                File.createTempFile(
                        "pegasus.", ".credentials.conf", new File(mTestSetup.getInputDirectory()));
        Exception expected =
                new RuntimeException("Unable to verify credential of type credentials for job");
        try {
            this.testVerify(
                    credFile,
                    expected,
                    CredentialHandler.TYPE.credentials,
                    "testVerifyForS3CredsWithExistingFileIncorrectPerm");
        } finally {
            credFile.delete();
        }
    }

    @Test
    public void testForExistingEndpointInCredFile() throws IOException {

        File credFile = new File(mTestSetup.getInputDirectory(), CREDENTIALS_TEST_FILE);

        this.testForEndpointInCredentialFile(
                credFile,
                CredentialHandler.TYPE.http,
                "http://download.pegasus.isi.edu",
                true,
                "testForExistingEndpointInCredFile");
    }

    @Test
    public void testForNonExistingEndpointInCredFile() throws IOException {

        File credFile = new File(mTestSetup.getInputDirectory(), CREDENTIALS_TEST_FILE);

        this.testForEndpointInCredentialFile(
                credFile,
                CredentialHandler.TYPE.http,
                "http://not.existant.endpoint",
                false,
                "testForNonExistingEndpointInCredFile");
    }

    /**
     * @param credFile
     * @param expectedException
     * @param type
     * @param testName
     * @throws IOException
     */
    private void testVerify(
            File credFile,
            Exception expectedException,
            CredentialHandler.TYPE type,
            String testName)
            throws IOException {
        mLogger.logEventStart(
                "test.pegasus.common.credentials.PegasusCredentials",
                testName,
                Integer.toString(mTestNumber++));

        mProps.setProperty(
                Profiles.NAMESPACES.pegasus
                        + "."
                        + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                credFile.toString());
        CredentialHandlerFactory factory = new CredentialHandlerFactory();
        factory.initialize(mBag);
        if (expectedException == null) {
            CredentialHandler credentials =
                    factory.loadInstance(CredentialHandler.TYPE.credentials);
            credentials.verify(new Job(), type, credFile.getAbsolutePath());
        } else {
            Exception exception =
                    assertThrows(
                            expectedException.getClass(),
                            () -> {
                                CredentialHandler credentials = factory.loadInstance(type);
                                credentials.verify(new Job(), type, credFile.getAbsolutePath());
                            });
            assertTrue(
                    exception.getMessage().contains(expectedException.getMessage()),
                    "EXCEPTION MESSAGE "
                            + exception.getMessage()
                            + " DOES NOT CONTAIN "
                            + expectedException.getMessage());
        }
        mLogger.logEventCompletion();
    }

    /**
     * Tests whether there is a credential for an particular endpoint in a credential file
     *
     * @param credFile
     * @param type
     * @param endPoint
     * @param exists
     * @param testName
     * @throws IOException
     */
    private void testForEndpointInCredentialFile(
            File credFile,
            CredentialHandler.TYPE type,
            String endPoint,
            boolean exists,
            String testName)
            throws IOException {
        mLogger.logEventStart(
                "test.pegasus.common.credentials.PegasusCredentials",
                testName,
                Integer.toString(mTestNumber++));

        // git only tracks x bits on files. So 600 permission
        // does not get preserved at check in .
        // https://unix.stackexchange.com/questions/560448/permission-changed-from-600-to-664-after-git-push-pull
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(credFile.toPath(), perms);

        mProps.setProperty(
                Profiles.NAMESPACES.pegasus
                        + "."
                        + PegasusCredentials.CREDENTIALS_FILE.toLowerCase(),
                credFile.toString());
        CredentialHandlerFactory factory = new CredentialHandlerFactory();
        factory.initialize(mBag);

        CredentialHandler credentials = factory.loadInstance(CredentialHandler.TYPE.credentials);
        credentials.verify(new Job(), type, credFile.getAbsolutePath());

        assertEquals(
                exists,
                credentials.hasCredential(type, credFile.getAbsolutePath(), endPoint),
                endPoint + " existence in cred file");

        mLogger.logEventCompletion();
    }
}
