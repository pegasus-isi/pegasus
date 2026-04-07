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
package edu.isi.pegasus.planner.selector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the SiteSelector interface constants. */
public class SiteSelectorTest {

    @Test
    public void testVersionConstant() {
        assertThat(SiteSelector.VERSION, notNullValue());
        assertThat(SiteSelector.VERSION.isEmpty(), is(false));
    }

    @Test
    public void testVersionFormat() {
        assertThat(SiteSelector.VERSION.matches("\\d+\\.\\d+"), is(true));
    }

    @Test
    public void testSiteNotFoundConstantNotNull() {
        assertThat(SiteSelector.SITE_NOT_FOUND, notNullValue());
    }

    @Test
    public void testSiteNotFoundValue() {
        assertThat(SiteSelector.SITE_NOT_FOUND, is("NONE"));
    }

    @Test
    public void testSiteNotFoundNotEmpty() {
        assertThat(SiteSelector.SITE_NOT_FOUND.isEmpty(), is(false));
    }

    @Test
    public void testSiteSelectorIsInterface() {
        assertThat(SiteSelector.class.isInterface(), is(true));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                (Object)
                        SiteSelector.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        SiteSelector.class
                                .getMethod(
                                        "mapWorkflow",
                                        edu.isi.pegasus.planner.classes.ADag.class,
                                        java.util.List.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object) SiteSelector.class.getMethod("description").getReturnType(),
                is((Object) String.class));
    }

    @Test
    public void testSiteSelectorDeclaresExpectedMethods() {
        assertThat(SiteSelector.class.getDeclaredMethods().length, is(3));
    }
}
