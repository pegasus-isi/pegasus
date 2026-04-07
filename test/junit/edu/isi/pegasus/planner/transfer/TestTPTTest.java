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

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.common.PegasusProperties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TestTPTTest {

    @Test
    public void buildStateDefaultsToFalseForUnknownSite() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        TPT tpt = new TPT(properties);

        tpt.buildState();

        assertThat(tpt.stageInThirdParty("X"), is(false));
        assertThat(tpt.interThirdParty("X"), is(false));
        assertThat(tpt.stageOutThirdParty("X"), is(false));
    }

    @Test
    public void buildStateRespectsGlobalThirdPartyProperties() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty(TPT.ALL_TPT_PROPERTY, "*");
        TPT tpt = new TPT(properties);

        tpt.buildState();

        assertThat(tpt.stageInThirdParty("X"), is(true));
        assertThat(tpt.interThirdParty("X"), is(true));
        assertThat(tpt.stageOutThirdParty("X"), is(true));
    }

    @Test
    public void testPegasusURLExtractionMatchesHarnessExample() {
        PegasusURL url = new PegasusURL("file:///gpfs-wan/karan.txt");

        assertThat(url.getHost(), equalTo(""));
        assertThat(url.getPath(), equalTo("/gpfs-wan/karan.txt"));
    }
}
