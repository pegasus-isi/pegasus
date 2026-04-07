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
package edu.isi.pegasus.planner.selector.site;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.SiteSelector;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the SiteSelectorFactory class. */
public class SiteSelectorFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertThat(
                SiteSelectorFactory.DEFAULT_PACKAGE_NAME,
                equalTo("edu.isi.pegasus.planner.selector.site"));
    }

    @Test
    public void testDefaultSiteSelector() {
        assertThat(SiteSelectorFactory.DEFAULT_SITE_SELECTOR, equalTo("Random"));
    }

    @Test
    public void testDefaultPackageNameNotNull() {
        assertThat(SiteSelectorFactory.DEFAULT_PACKAGE_NAME, notNullValue());
    }

    @Test
    public void testDefaultSiteSelectorNotNull() {
        assertThat(SiteSelectorFactory.DEFAULT_SITE_SELECTOR, notNullValue());
    }

    @Test
    public void testFactoryClassExists() {
        assertThat(SiteSelectorFactory.class, notNullValue());
    }

    @Test
    public void testLoadInstanceRejectsMissingProperties() {
        PegasusBag bag = new PegasusBag();

        SiteSelectorFactoryException exception =
                assertThrows(
                        SiteSelectorFactoryException.class,
                        () -> SiteSelectorFactory.loadInstance(bag));

        assertThat(exception.getMessage(), equalTo("Instantiating SiteSelector "));
        assertThat(exception.getClassname(), nullValue());
        assertThat(exception.getCause(), notNullValue());
        assertThat(exception.getCause().getMessage(), containsString("Invalid properties passed"));
    }

    @Test
    public void testLoadInstanceWithExplicitClassInitializesSelector() throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.site", TrackingSiteSelector.class.getName());
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        SiteSelector selector = SiteSelectorFactory.loadInstance(bag);

        assertThat(selector, instanceOf(TrackingSiteSelector.class));
        assertThat(((TrackingSiteSelector) selector).initializedBag, sameInstance(bag));
    }

    @Test
    public void testLoadInstanceDefaultsWhenPropertyIsBlank() throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.site", " ");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        SiteSelector selector = SiteSelectorFactory.loadInstance(bag);

        assertThat(selector, instanceOf(Random.class));
    }

    public static class TrackingSiteSelector implements SiteSelector {
        PegasusBag initializedBag;

        @Override
        public void initialize(PegasusBag bag) {
            initializedBag = bag;
        }

        @Override
        public void mapWorkflow(ADag workflow, List sites) {}

        @Override
        public String description() {
            return "tracking";
        }
    }
}
