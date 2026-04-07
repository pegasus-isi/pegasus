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
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class GoogleP12Test {

    @Test
    public void testInitializeUsesLocalSitePegasusProfileForLocalCredentialPath() throws Exception {
        GoogleP12 credential = new GoogleP12();
        PegasusBag bag = createBagWithLocalSite("/site/google/local.p12", null);

        credential.initialize(bag);

        assertThat(
                ReflectionTestUtils.getField(credential, "mLocalCredentialPath"),
                is("/site/google/local.p12"));
        assertThat(credential.getPath(), is("/site/google/local.p12"));
    }

    @Test
    public void testGetPathPrefersSiteSpecificPegasusProfileOverLocalFallback() {
        GoogleP12 credential = new GoogleP12();
        SiteStore store = new SiteStore();

        SiteCatalogEntry local = new SiteCatalogEntry("local");
        local.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "google_pkcs12", "/site/google/local.p12");
        store.addEntry(local);

        SiteCatalogEntry remote = new SiteCatalogEntry("gce");
        remote.getProfiles()
                .addProfile(
                        Profiles.NAMESPACES.pegasus, "google_pkcs12", "/site/google/remote.p12");
        store.addEntry(remote);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getPath("gce"), is("/site/google/remote.p12"));
        assertThat(credential.getPath("unknown-site"), is("/site/google/local.p12"));
    }

    @Test
    public void testGetLocalPathFallsBackToLocalEnvProfileWhenPegasusProfileMissing() {
        GoogleP12 credential = new GoogleP12();
        PegasusBag bag = createBagWithLocalSite(null, "/site/google/from-env.p12");

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is("/site/google/from-env.p12"));
    }

    @Test
    public void testGetLocalPathReturnsNullWhenNoProfilesOrEnvironmentAreAvailable() {
        GoogleP12 credential = new GoogleP12();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is(nullValue()));
    }

    @Test
    public void testAccessorMethodsExposeCurrentConstantsAndFormatting() {
        GoogleP12 credential = new GoogleP12();
        GoogleP12 baseNameCredential = new GoogleP12();
        baseNameCredential.initialize(createBagWithLocalSite("/tmp/google/credential.p12", null));

        assertThat(credential.getProfileKey(), is("GOOGLE_PKCS12"));
        assertThat(
                credential.getEnvironmentVariable("google-batch"),
                is("GOOGLE_PKCS12_google_batch"));
        assertThat(credential.getDescription(), is("Google PKCS12 File Credential Handler"));
        assertThat(baseNameCredential.getBaseName("missing-site"), is("credential.p12"));
    }

    private PegasusBag createBagWithLocalSite(
            String localPegasusCredential, String localEnvCredential) {
        SiteStore store = new SiteStore();
        SiteCatalogEntry local = new SiteCatalogEntry("local");
        if (localPegasusCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.pegasus, "google_pkcs12", localPegasusCredential);
        }
        if (localEnvCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.env,
                            GoogleP12.GOOGLEP12_FILE_VARIABLE,
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
