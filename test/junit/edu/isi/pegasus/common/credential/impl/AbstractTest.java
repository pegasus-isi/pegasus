/**
 * Copyright 2007-2013 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.common.credential.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class AbstractTest {

    @Test
    public void testInitializeSetsBagBackedFieldsAndVerificationCache() throws Exception {
        TestCredentialHandler handler = new TestCredentialHandler();
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        SiteStore siteStore = new SiteStore();
        NoOpLogManager logger = new NoOpLogManager();

        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.SITE_STORE, siteStore);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);

        handler.initialize(bag);

        assertThat(ReflectionTestUtils.getField(handler, "mProps"), is(sameInstance(props)));
        assertThat(
                ReflectionTestUtils.getField(handler, "mSiteStore"), is(sameInstance(siteStore)));
        assertThat(ReflectionTestUtils.getField(handler, "mLogger"), is(sameInstance(logger)));
        assertThat(
                ReflectionTestUtils.getField(handler, "mVerifiedCredentials"), is(notNullValue()));
    }

    @Test
    public void testGetSiteNameForEnvironmentKeyReplacesDashes() {
        TestCredentialHandler handler = new TestCredentialHandler();

        assertThat(handler.getSiteNameForEnvironmentKey("stampede2-login"), is("stampede2_login"));
        assertThat(handler.getSiteNameForEnvironmentKey("local"), is("local"));
    }

    @Test
    public void testHasCredentialThrowsUnsupportedOperationExceptionMessage() {
        TestCredentialHandler handler = new TestCredentialHandler();

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                handler.hasCredential(
                                        CredentialHandler.TYPE.x509, "/tmp/cred", "endpoint"),
                        "base implementation should reject hasCredential lookups");

        assertThat(
                exception.getMessage(),
                containsString("does not support hasCredential(String,String,String) function"));
    }

    @Test
    public void testVerifyCachesValid0600Credential() throws Exception {
        TestCredentialHandler handler = new TestCredentialHandler();
        handler.initialize(new PegasusBag());
        Path credential = Files.createTempFile("credential", ".txt");
        Files.setPosixFilePermissions(
                credential, PosixFilePermissions.fromString(Abstract.POSIX_600_PERMISSIONS_STRING));

        handler.verify(null, CredentialHandler.TYPE.x509, credential.toString());
        handler.verify(null, CredentialHandler.TYPE.x509, credential.toString());

        Object verified = ReflectionTestUtils.getField(handler, "mVerifiedCredentials");
        assertThat(((java.util.Set) verified).contains(credential), is(true));
        assertThat(((java.util.Set) verified).size(), is(1));
    }

    @Test
    public void testVerifyRejectsMissingCredentialAndIncludesJobName() throws Exception {
        TestCredentialHandler handler = new TestCredentialHandler();
        handler.initialize(new PegasusBag());
        Job job = new Job();
        job.setName("stage-in");

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                handler.verify(
                                        job,
                                        CredentialHandler.TYPE.ssh,
                                        "/tmp/definitely-missing-credential-" + System.nanoTime()),
                        "verify should reject missing credentials");

        assertThat(
                exception.getMessage(), containsString("Unable to verify credential of type ssh"));
        assertThat(exception.getMessage(), containsString("for job stage-in"));
        assertThat(
                exception.getMessage(),
                containsString("exists and has following permissions rw-------."));
    }

    private static final class TestCredentialHandler extends Abstract {
        @Override
        public String getPath(String site) {
            return "/tmp/" + site;
        }

        @Override
        public String getProfileKey() {
            return "profile";
        }

        @Override
        public String getEnvironmentVariable(String site) {
            return "ENV_" + getSiteNameForEnvironmentKey(site);
        }

        @Override
        public String getDescription() {
            return "TestCredentialHandler";
        }

        @Override
        public String getBaseName(String site) {
            return site + ".cred";
        }
    }

    private static final class NoOpLogManager extends LogManager {
        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.INFO_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
