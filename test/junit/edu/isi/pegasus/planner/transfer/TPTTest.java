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

import edu.isi.pegasus.planner.common.PegasusProperties;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TPTTest {

    @Test
    public void testConstantsAndPropertyTableMapping() throws Exception {
        assertThat(TPT.ALL_SITES, equalTo("*"));
        assertThat(TPT.ALL_TPT_PROPERTY, equalTo("pegasus.transfer.*.thirdparty.sites"));
        assertThat(TPT.STAGE_IN_TPT_PROPERTY, equalTo("pegasus.transfer.stagein.thirdparty.sites"));
        assertThat(TPT.INTER_TPT_PROPERTY, equalTo("pegasus.transfer.inter.thirdparty.sites"));
        assertThat(
                TPT.STAGE_OUT_TPT_PROPERTY, equalTo("pegasus.transfer.stageout.thirdparty.sites"));
        assertThat(TPT.ALL_TPT_REMOTE_PROPERTY, equalTo("pegasus.transfer.*.thirdparty.remote"));
        assertThat(
                TPT.STAGE_IN_TPT_REMOTE_PROPERTY,
                equalTo("pegasus.transfer.stagein.thirdparty.remote"));
        assertThat(
                TPT.INTER_TPT_REMOTE_PROPERTY, equalTo("pegasus.transfer.inter.thirdparty.remote"));
        assertThat(
                TPT.STAGE_OUT_TPT_REMOTE_PROPERTY,
                equalTo("pegasus.transfer.stageout.thirdparty.remote"));

        Map first = invokePropertyTable();
        Map second = invokePropertyTable();

        assertThat(first, sameInstance(second));
        assertThat(first.get(Integer.valueOf(0x1)), equalTo(TPT.STAGE_IN_TPT_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x2)), equalTo(TPT.INTER_TPT_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x4)), equalTo(TPT.STAGE_OUT_TPT_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x7)), equalTo(TPT.ALL_TPT_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x8)), equalTo(TPT.STAGE_IN_TPT_REMOTE_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x10)), equalTo(TPT.INTER_TPT_REMOTE_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x20)), equalTo(TPT.STAGE_OUT_TPT_REMOTE_PROPERTY));
        assertThat(first.get(Integer.valueOf(0x38)), equalTo(TPT.ALL_TPT_REMOTE_PROPERTY));
    }

    @Test
    public void testPrivateGetThirdPartySitesHandlesNullAndDeduplicates() throws Exception {
        TPT tpt = new TPT(PegasusProperties.nonSingletonInstance());

        Set nullSites = invokeGetThirdPartySites(tpt, null);
        Set parsedSites = invokeGetThirdPartySites(tpt, "siteA,siteB,siteA");

        assertThat(nullSites.isEmpty(), is(true));
        assertThat(parsedSites.size(), equalTo(2));
        assertThat(parsedSites.contains("siteA"), is(true));
        assertThat(parsedSites.contains("siteB"), is(true));
    }

    @Test
    public void testBuildStateAppliesWildcardStateToKnownSitesAndFallbackLookups() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(TPT.STAGE_IN_TPT_PROPERTY, "siteA");
        props.setProperty(TPT.ALL_TPT_REMOTE_PROPERTY, "*");

        TPT tpt = new TPT(props);
        tpt.buildState();

        assertThat(tpt.stageInThirdParty("siteA"), is(true));
        assertThat(tpt.stageInThirdPartyRemote("siteA"), is(true));
        assertThat(tpt.interThirdPartyRemote("siteA"), is(true));
        assertThat(tpt.stageOutThirdPartyRemote("siteA"), is(true));

        assertThat(tpt.stageInThirdParty("unknown"), is(false));
        assertThat(tpt.stageInThirdPartyRemote("unknown"), is(true));
        assertThat(tpt.interThirdPartyRemote("unknown"), is(true));
        assertThat(tpt.stageOutThirdPartyRemote("unknown"), is(true));
    }

    @Test
    public void testBuildStateRespectsSiteSpecificThirdPartyFlags() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(TPT.STAGE_IN_TPT_PROPERTY, "siteA");
        props.setProperty(TPT.INTER_TPT_PROPERTY, "siteB");
        props.setProperty(TPT.STAGE_OUT_TPT_PROPERTY, "siteC");
        props.setProperty(TPT.STAGE_OUT_TPT_REMOTE_PROPERTY, "siteC");

        TPT tpt = new TPT(props);
        tpt.buildState();

        assertThat(tpt.stageInThirdParty("siteA"), is(true));
        assertThat(tpt.interThirdParty("siteA"), is(false));
        assertThat(tpt.stageOutThirdParty("siteA"), is(false));

        assertThat(tpt.interThirdParty("siteB"), is(true));
        assertThat(tpt.stageInThirdParty("siteB"), is(false));

        assertThat(tpt.stageOutThirdParty("siteC"), is(true));
        assertThat(tpt.stageOutThirdPartyRemote("siteC"), is(true));
        assertThat(tpt.stageInThirdPartyRemote("siteC"), is(false));
    }

    private Map invokePropertyTable() throws Exception {
        Method method = TPT.class.getDeclaredMethod("propertyTable");
        method.setAccessible(true);
        return (Map) method.invoke(null);
    }

    private Set invokeGetThirdPartySites(TPT tpt, String value) throws Exception {
        Method method = TPT.class.getDeclaredMethod("getThirdPartySites", String.class);
        method.setAccessible(true);
        return (Set) method.invoke(tpt, value);
    }
}
