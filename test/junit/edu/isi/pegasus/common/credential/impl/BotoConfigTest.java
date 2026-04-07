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
public class BotoConfigTest {

    @Test
    public void testInitializeUsesLocalSitePegasusProfileForLocalCredentialPath() throws Exception {
        BotoConfig credential = new BotoConfig();
        PegasusBag bag = createBagWithLocalSite("/site/boto/local.cfg", null);

        credential.initialize(bag);

        assertThat(
                ReflectionTestUtils.getField(credential, "mLocalCredentialPath"),
                is("/site/boto/local.cfg"));
        assertThat(credential.getPath(), is("/site/boto/local.cfg"));
    }

    @Test
    public void testGetPathPrefersSiteSpecificPegasusProfileOverLocalFallback() {
        BotoConfig credential = new BotoConfig();
        SiteStore store = new SiteStore();

        SiteCatalogEntry local = new SiteCatalogEntry("local");
        local.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "boto_config", "/site/boto/local.cfg");
        store.addEntry(local);

        SiteCatalogEntry remote = new SiteCatalogEntry("aws-batch");
        remote.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "boto_config", "/site/boto/remote.cfg");
        store.addEntry(remote);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getPath("aws-batch"), is("/site/boto/remote.cfg"));
        assertThat(credential.getPath("unknown-site"), is("/site/boto/local.cfg"));
    }

    @Test
    public void testGetLocalPathFallsBackToLocalEnvProfileWhenPegasusProfileMissing() {
        BotoConfig credential = new BotoConfig();
        PegasusBag bag = createBagWithLocalSite(null, "/site/boto/from-env.cfg");

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is("/site/boto/from-env.cfg"));
    }

    @Test
    public void testGetLocalPathReturnsNullWhenNoProfilesOrEnvironmentAreAvailable() {
        BotoConfig credential = new BotoConfig();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is(nullValue()));
    }

    @Test
    public void testAccessorMethodsExposeCurrentConstantsAndFormatting() {
        BotoConfig credential = new BotoConfig();
        BotoConfig baseNameCredential = new BotoConfig();
        baseNameCredential.initialize(createBagWithLocalSite("/tmp/boto/boto.cfg", null));

        assertThat(credential.getProfileKey(), is("BOTO_CONFIG"));
        assertThat(credential.getEnvironmentVariable("aws-batch"), is("BOTO_CONFIG_aws_batch"));
        assertThat(credential.getDescription(), is("Boto Config File Credential Handler"));
        assertThat(baseNameCredential.getBaseName("missing-site"), is("boto.cfg"));
    }

    private PegasusBag createBagWithLocalSite(
            String localPegasusCredential, String localEnvCredential) {
        SiteStore store = new SiteStore();
        SiteCatalogEntry local = new SiteCatalogEntry("local");
        if (localPegasusCredential != null) {
            local.getProfiles()
                    .addProfile(Profiles.NAMESPACES.pegasus, "boto_config", localPegasusCredential);
        }
        if (localEnvCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.env,
                            BotoConfig.BOTO_CONFIG_FILE_VARIABLE,
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
