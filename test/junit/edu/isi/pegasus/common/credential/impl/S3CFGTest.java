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
import java.io.File;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class S3CFGTest {

    @Test
    public void testInitializeUsesLocalSitePegasusProfileForLocalCredentialPath() throws Exception {
        S3CFG credential = new S3CFG();
        PegasusBag bag = createBagWithLocalSite("/site/s3cfg/local.cfg", null);

        credential.initialize(bag);

        assertThat(getField(credential, "mLocalCredentialPath"), is("/site/s3cfg/local.cfg"));
        assertThat(credential.getPath(), is("/site/s3cfg/local.cfg"));
    }

    @Test
    public void testGetPathPrefersSiteSpecificPegasusProfileOverLocalFallback() {
        S3CFG credential = new S3CFG();
        SiteStore store = new SiteStore();

        SiteCatalogEntry local = new SiteCatalogEntry("local");
        local.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "s3cfg", "/site/s3cfg/local.cfg");
        store.addEntry(local);

        SiteCatalogEntry remote = new SiteCatalogEntry("aws-batch");
        remote.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "s3cfg", "/site/s3cfg/remote.cfg");
        store.addEntry(remote);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        assertThat(credential.getPath("aws-batch"), is("/site/s3cfg/remote.cfg"));
        assertThat(credential.getPath("unknown-site"), is("/site/s3cfg/local.cfg"));
    }

    @Test
    public void testGetLocalPathFallsBackToLocalEnvProfileWhenPegasusProfileMissing() {
        S3CFG credential = new S3CFG();
        PegasusBag bag = createBagWithLocalSite(null, "/site/s3cfg/from-env.cfg");

        credential.initialize(bag);

        assertThat(credential.getLocalPath(), is("/site/s3cfg/from-env.cfg"));
    }

    @Test
    public void testGetLocalPathFallsBackToCurrentHomeBasedDefault() {
        S3CFG credential = new S3CFG();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        credential.initialize(bag);

        String home = System.getenv("HOME");
        String expected = home + "/.pegasus/s3cfg";
        if (!new File(expected).isFile()) {
            expected = home + "/.s3cfg";
        }

        assertThat(credential.getLocalPath(), is(expected));
    }

    @Test
    public void testAccessorMethodsExposeCurrentConstantsAndFormatting() {
        S3CFG credential = new S3CFG();
        S3CFG baseNameCredential = new S3CFG();
        baseNameCredential.initialize(createBagWithLocalSite("/tmp/s3cfg/s3cfg", null));

        assertThat(credential.getProfileKey(), is("S3CFG"));
        assertThat(credential.getEnvironmentVariable("aws-batch"), is("S3CFG_aws_batch"));
        assertThat(credential.getDescription(), is("S3 Conf File Credential Handler"));
        assertThat(baseNameCredential.getBaseName("missing-site"), is("s3cfg"));
    }

    private PegasusBag createBagWithLocalSite(
            String localPegasusCredential, String localEnvCredential) {
        SiteStore store = new SiteStore();
        SiteCatalogEntry local = new SiteCatalogEntry("local");
        if (localPegasusCredential != null) {
            local.getProfiles()
                    .addProfile(Profiles.NAMESPACES.pegasus, "s3cfg", localPegasusCredential);
        }
        if (localEnvCredential != null) {
            local.getProfiles()
                    .addProfile(
                            Profiles.NAMESPACES.env, S3CFG.S3CFG_FILE_VARIABLE, localEnvCredential);
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
