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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.PrintStream;
import java.util.Properties;

/**
 * @author Rajiv Mayani
 */
public class GoogleADCTest {

    @Test
    public void testInitializeUsesLocalSitePegasusProfileForLocalCredentialPath() throws Exception {
        GoogleADC credential = new GoogleADC();
        PegasusBag bag = createBagWithLocalSite("/site/google/local.json", null);

        credential.initialize(bag);

        assertThat(
                ReflectionTestUtils.getField(credential, "mLocalCredentialPath"),
                is("/site/google/local.json"));
        assertThat(credential.getPath(), is("/site/google/local.json"));
    }

    @Test
    public void testGetPathPrefersSiteSpecificPegasusProfileOverLocalFallback() {
        GoogleADC credential = new GoogleADC();
        SiteStore store = new SiteStore();

        SiteCatalogEntry local = new SiteCatalogEntry("local");
        local.getProfiles()
                .addProfile(
                        Profiles.NAMESPACES.pegasus,
                        "google_application_credentials",
                        "/site/google/local.json");
        store.addEntry(local);

        SiteCatalogEntry remote = new SiteCatalogEntry("gce");
        remote.getProfiles()
                .addProfile(
                        Profiles.NAMESPACES.pegasus,
                        "google_application_credentials",
                        "/site/google/remote.json");
        store.addEntry(remote);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getPath("gce"), is("/site/google/remote.json"));
        assertThat(credential.getPath("unknown-site"), is("/site/google/local.json"));
    }

    @Test
    public void testGetLocalPathFallsBackToLocalEnvProfileWhenPegasusProfileMissing() {
        GoogleADC credential = new GoogleADC();
        PegasusBag bag = createBagWithLocalSite(null, "/site/google/from-env.json");

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is("/site/google/from-env.json"));
    }

    @Test
    public void testGetLocalPathReturnsNullWhenNoProfilesOrEnvironmentAreAvailable() {
        GoogleADC credential = new GoogleADC();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is(nullValue()));
    }

    @Test
    public void testAccessorMethodsExposeCurrentConstantsAndFormatting() {
        GoogleADC credential = new GoogleADC();
        GoogleADC baseNameCredential = new GoogleADC();
        baseNameCredential.initialize(
                createBagWithLocalSite("/tmp/google/credential.json", null));

        assertThat(credential.getProfileKey(), is("GOOGLE_APPLICATION_CREDENTIALS"));
        assertThat(
                credential.getEnvironmentVariable("google-batch"),
                is("GOOGLE_APPLICATION_CREDENTIALS_google_batch"));
        assertThat(
                credential.getDescription(),
                is("Google Application Credentials File Handler"));
        assertThat(baseNameCredential.getBaseName("missing-site"), is("credential.json"));
    }

    private PegasusBag createBagWithLocalSite(
            String localPegasusCredential, String localEnvCredential) {
        SiteStore store = new SiteStore();
        SiteCatalogEntry local = new SiteCatalogEntry("local");
        if (localPegasusCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.pegasus,
                            "google_application_credentials",
                            localPegasusCredential);
        }
        if (localEnvCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.env,
                            GoogleADC.GOOGLE_APPLICATION_CREDENTIALS_FILE_VARIABLE,
                            localEnvCredential);
        }
        store.addEntry(local);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
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
