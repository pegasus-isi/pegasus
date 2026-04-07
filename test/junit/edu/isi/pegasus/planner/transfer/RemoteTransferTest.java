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
package edu.isi.pegasus.planner.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.common.PegasusProperties;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class RemoteTransferTest {

    @Test
    public void testConstantsAndPropertyTableMapping() throws Exception {
        assertThat(RemoteTransfer.ALL_SITES, is("*"));
        assertThat(
                RemoteTransfer.ALL_TRANSFERS_REMOTE_PROPERTY,
                is("pegasus.transfer.*.remote.sites"));
        assertThat(
                RemoteTransfer.STAGE_IN_TRANSFERS_REMOTE_PROPERTY,
                is("pegasus.transfer.stagein.remote.sites"));
        assertThat(
                RemoteTransfer.INTER_TRANSFERS_REMOTE_PROPERTY,
                is("pegasus.transfer.inter.remote.sites"));
        assertThat(
                RemoteTransfer.STAGE_OUT_TRANSFERS_REMOTE_PROPERTY,
                is("pegasus.transfer.stageout.remote.sites"));

        Method propertyTableMethod = RemoteTransfer.class.getDeclaredMethod("propertyTable");
        propertyTableMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<Integer, String> propertyTable =
                (Map<Integer, String>) propertyTableMethod.invoke(null);

        assertThat(propertyTable.size(), is(4));
        assertThat(
                propertyTable.get(Integer.valueOf(0x1)),
                is(RemoteTransfer.STAGE_IN_TRANSFERS_REMOTE_PROPERTY));
        assertThat(
                propertyTable.get(Integer.valueOf(0x2)),
                is(RemoteTransfer.INTER_TRANSFERS_REMOTE_PROPERTY));
        assertThat(
                propertyTable.get(Integer.valueOf(0x4)),
                is(RemoteTransfer.STAGE_OUT_TRANSFERS_REMOTE_PROPERTY));
        assertThat(
                propertyTable.get(Integer.valueOf(0x7)),
                is(RemoteTransfer.ALL_TRANSFERS_REMOTE_PROPERTY));
    }

    @Test
    public void testGetThirdPartySitesParsesCsvAndNull() throws Exception {
        RemoteTransfer remoteTransfer =
                new RemoteTransfer(PegasusProperties.nonSingletonInstance());

        Method method = RemoteTransfer.class.getDeclaredMethod("getThirdPartySites", String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> empty = (Set<String>) method.invoke(remoteTransfer, new Object[] {null});
        assertThat(empty.isEmpty(), is(true));

        @SuppressWarnings("unchecked")
        Set<String> sites = (Set<String>) method.invoke(remoteTransfer, "alpha,beta,alpha");
        assertThat(sites.size(), is(2));
        assertThat(sites, hasItems("alpha", "beta"));
    }

    @Test
    public void testBuildStateAppliesSiteSpecificRemoteFlags() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty(RemoteTransfer.STAGE_IN_TRANSFERS_REMOTE_PROPERTY, "siteA");
        properties.setProperty(RemoteTransfer.INTER_TRANSFERS_REMOTE_PROPERTY, "siteB");
        properties.setProperty(RemoteTransfer.STAGE_OUT_TRANSFERS_REMOTE_PROPERTY, "siteC");

        RemoteTransfer remoteTransfer = new RemoteTransfer(properties);
        remoteTransfer.buildState();

        assertThat(remoteTransfer.stageInOnRemoteSite("siteA"), is(true));
        assertThat(remoteTransfer.interOnRemoteSite("siteA"), is(false));
        assertThat(remoteTransfer.stageOutOnRemoteSite("siteA"), is(false));

        assertThat(remoteTransfer.stageInOnRemoteSite("siteB"), is(false));
        assertThat(remoteTransfer.interOnRemoteSite("siteB"), is(true));
        assertThat(remoteTransfer.stageOutOnRemoteSite("siteB"), is(false));

        assertThat(remoteTransfer.stageInOnRemoteSite("siteC"), is(false));
        assertThat(remoteTransfer.interOnRemoteSite("siteC"), is(false));
        assertThat(remoteTransfer.stageOutOnRemoteSite("siteC"), is(true));
    }

    @Test
    public void testBuildStateAppliesWildcardToKnownAndUnknownSites() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty(RemoteTransfer.STAGE_IN_TRANSFERS_REMOTE_PROPERTY, "siteA");
        properties.setProperty(RemoteTransfer.ALL_TRANSFERS_REMOTE_PROPERTY, "*");

        RemoteTransfer remoteTransfer = new RemoteTransfer(properties);
        remoteTransfer.buildState();

        assertThat(remoteTransfer.stageInOnRemoteSite("siteA"), is(true));
        assertThat(remoteTransfer.interOnRemoteSite("siteA"), is(true));
        assertThat(remoteTransfer.stageOutOnRemoteSite("siteA"), is(true));

        assertThat(remoteTransfer.stageInOnRemoteSite("siteZ"), is(true));
        assertThat(remoteTransfer.interOnRemoteSite("siteZ"), is(true));
        assertThat(remoteTransfer.stageOutOnRemoteSite("siteZ"), is(true));
    }
}
