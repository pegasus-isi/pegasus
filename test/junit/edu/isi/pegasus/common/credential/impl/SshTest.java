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
import static org.hamcrest.Matchers.*;
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
public class SshTest {

    @Test
    public void testInitializeUsesLocalSitePegasusProfileForLocalCredentialPath() throws Exception {
        Ssh credential = new Ssh();
        PegasusBag bag = createBagWithLocalSite("/site/ssh/local.key", null);

        credential.initialize(bag);

        assertThat(getField(credential, "mLocalCredentialPath"), is("/site/ssh/local.key"));
        assertThat(credential.getPath(), is("/site/ssh/local.key"));
    }

    @Test
    public void testGetPathPrefersSiteSpecificPegasusProfileOverLocalFallback() {
        Ssh credential = new Ssh();
        SiteStore store = new SiteStore();

        SiteCatalogEntry local = new SiteCatalogEntry("local");
        local.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "ssh_private_key", "/site/ssh/local.key");
        store.addEntry(local);

        SiteCatalogEntry remote = new SiteCatalogEntry("stampede2");
        remote.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "ssh_private_key", "/site/ssh/remote.key");
        store.addEntry(remote);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getPath("stampede2"), is("/site/ssh/remote.key"));
        assertThat(credential.getPath("unknown-site"), is("/site/ssh/local.key"));
    }

    @Test
    public void testGetLocalPathFallsBackToLocalEnvProfileWhenPegasusProfileMissing() {
        Ssh credential = new Ssh();
        PegasusBag bag = createBagWithLocalSite(null, "/site/ssh/from-env.key");

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is("/site/ssh/from-env.key"));
    }

    @Test
    public void testGetLocalPathReturnsNullWhenNoProfilesOrEnvironmentAreAvailable() {
        Ssh credential = new Ssh();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is(nullValue()));
    }

    @Test
    public void testAccessorMethodsExposeCurrentConstantsAndFormatting() {
        Ssh credential = new Ssh();
        Ssh baseNameCredential = new Ssh();
        baseNameCredential.initialize(createBagWithLocalSite("/tmp/ssh/id_rsa", null));

        assertThat(credential.getProfileKey(), is("SSH_PRIVATE_KEY"));
        assertThat(
                credential.getEnvironmentVariable("submit-host"),
                is("SSH_PRIVATE_KEY_submit_host"));
        assertThat(credential.getDescription(), is("SSH private key Credential Handler"));
        assertThat(baseNameCredential.getBaseName("missing-site"), is("id_rsa"));
    }

    private PegasusBag createBagWithLocalSite(
            String localPegasusCredential, String localEnvCredential) {
        SiteStore store = new SiteStore();
        SiteCatalogEntry local = new SiteCatalogEntry("local");
        if (localPegasusCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.pegasus, "ssh_private_key", localPegasusCredential);
        }
        if (localEnvCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.env,
                            Ssh.SSH_PRIVATE_KEY_VARIABLE,
                            localEnvCredential);
        }
        store.addEntry(local);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
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
