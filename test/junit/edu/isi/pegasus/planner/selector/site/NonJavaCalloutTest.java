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
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.selector.SiteSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the NonJavaCallout site selector. */
public class NonJavaCalloutTest {

    private NonJavaCallout mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new NonJavaCallout();
    }

    @Test
    public void testInstantiation() {
        assertThat(mSelector, notNullValue());
    }

    @Test
    public void testDescription() {
        String desc = mSelector.description();
        assertThat(desc, not(isEmptyOrNullString()));
    }

    @Test
    public void testImplementsSiteSelector() {
        assertThat(mSelector, instanceOf(SiteSelector.class));
    }

    @Test
    public void testExtendsAbstract() {
        assertThat(mSelector, instanceOf(Abstract.class));
    }

    @Test
    public void testDescriptionIsString() {
        String desc = mSelector.description();
        assertThat(desc, instanceOf(String.class));
    }

    @Test
    public void testConstants() {
        assertThat(NonJavaCallout.PREFIX_TEMPORARY_FILE, is("pegasus"));
        assertThat(NonJavaCallout.SUFFIX_TEMPORARY_FILE, nullValue());
        assertThat(NonJavaCallout.PREFIX_PROPERTIES, is("pegasus.selector.site.env."));
        assertThat(NonJavaCallout.SOLUTION_PREFIX, is("SOLUTION:"));
        assertThat(NonJavaCallout.VERSION, is("2.0"));
        assertThat(NonJavaCallout.KEEP_NEVER, is(0));
        assertThat(NonJavaCallout.KEEP_ONERROR, is(1));
        assertThat(NonJavaCallout.KEEP_ALWAYS, is(2));
    }

    @Test
    public void testConstructorInitializesDefaultTimeoutAndKeepState() throws Exception {
        assertThat((Integer) ReflectionTestUtils.getField(mSelector, "mTimeout"), is(60));
        assertThat(
                (Integer) ReflectionTestUtils.getField(mSelector, "mKeepTMP"),
                is(NonJavaCallout.KEEP_ONERROR));
    }

    @Test
    public void testPrivateHelperMethodsExist() throws Exception {
        assertThat(
                NonJavaCallout.class
                        .getDeclaredMethod(
                                "prepareInputFile",
                                edu.isi.pegasus.planner.classes.Job.class,
                                java.util.List.class)
                        .getReturnType(),
                is(java.io.File.class));
        assertThat(
                NonJavaCallout.class.getDeclaredMethod("loadEnvironmentVariables").getReturnType(),
                is(Void.TYPE));
        assertThat(
                NonJavaCallout.class.getDeclaredMethod("getEnvArrFromMap").getReturnType(),
                is(String[].class));
        assertThat(
                NonJavaCallout.class
                        .getDeclaredMethod("getKeepTMPValue", String.class)
                        .getReturnType(),
                is(Integer.TYPE));
    }
}
