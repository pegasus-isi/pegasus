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
import org.globus.common.CoGProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class ProxyTest {

    @Test
    public void testInitializeUsesLocalSitePegasusProfileForLocalProxyPath() throws Exception {
        Proxy proxy = new Proxy();
        PegasusBag bag = createBagWithLocalSite("/site/local-proxy", null, null);

        proxy.initialize(bag);

        assertThat(getField(proxy, "mLocalProxyPath"), is("/site/local-proxy"));
        assertThat(proxy.getPath(), is("/site/local-proxy"));
    }

    @Test
    public void testGetPathPrefersSiteSpecificPegasusProfileOverLocalFallback() {
        Proxy proxy = new Proxy();
        SiteStore store = new SiteStore();

        SiteCatalogEntry local = new SiteCatalogEntry("local");
        local.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "x509_user_proxy", "/site/local-proxy");
        store.addEntry(local);

        SiteCatalogEntry remote = new SiteCatalogEntry("stampede2");
        remote.getProfiles()
                .addProfile(Profiles.NAMESPACES.pegasus, "x509_user_proxy", "/remote/site-proxy");
        store.addEntry(remote);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        proxy.initialize(bag);

        assertThat(proxy.getPath("stampede2"), is("/remote/site-proxy"));
        assertThat(proxy.getPath("unknown-site"), is("/site/local-proxy"));
    }

    @Test
    public void testGetLocalPathFallsBackToDefaultCogProxyWhenNoProfilesArePresent() {
        Proxy proxy = new Proxy();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        proxy.initialize(bag);
        assertThat(proxy.getLocalPath(), is(CoGProperties.getDefault().getProxyFile()));
    }

    @Test
    public void testGetLocalPathFallsBackToLocalEnvProfileWhenPegasusProfileMissing() {
        Proxy proxy = new Proxy();
        PegasusBag bag = createBagWithLocalSite(null, "/site/env-proxy", null);

        proxy.initialize(bag);

        assertThat(proxy.getLocalPath(), is("/site/env-proxy"));
    }

    @Test
    public void testAccessorMethodsExposeCurrentConstantsAndFormatting() {
        Proxy proxy = new Proxy();
        Proxy baseNameProxy = new Proxy();
        baseNameProxy.initialize(createBagWithLocalSite("/tmp/security/proxy.pem", null, null));

        assertThat(proxy.getProfileKey(), is("X509_USER_PROXY"));
        assertThat(
                proxy.getEnvironmentVariable("stampede2-login"),
                is("X509_USER_PROXY_stampede2_login"));
        assertThat(proxy.getDescription(), is("X509 Proxy Handler"));
        assertThat(baseNameProxy.getBaseName("missing-site"), is("proxy.pem"));
    }

    private PegasusBag createBagWithLocalSite(
            String localPegasusProxy, String localEnvProxy, String propertyProxy) {
        SiteStore store = new SiteStore();
        SiteCatalogEntry local = new SiteCatalogEntry("local");
        if (localPegasusProxy != null) {
            local.getProfiles()
                    .addProfile(Profiles.NAMESPACES.pegasus, "x509_user_proxy", localPegasusProxy);
        }
        if (localEnvProxy != null) {
            local.getProfiles()
                    .addProfile(Profiles.NAMESPACES.env, Proxy.X509_USER_PROXY_KEY, localEnvProxy);
        }
        store.addEntry(local);

        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        if (propertyProxy != null) {
            props.setProperty("pegasus.profile.pegasus.x509_user_proxy", propertyProxy);
        }

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
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
