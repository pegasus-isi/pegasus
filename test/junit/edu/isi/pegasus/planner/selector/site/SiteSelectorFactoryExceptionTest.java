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

import org.junit.jupiter.api.Test;

/** Tests for the SiteSelectorFactoryException. */
public class SiteSelectorFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertThat(
                "Site Selector",
                SiteSelectorFactoryException.DEFAULT_NAME,
                equalTo("Site Selector"));
    }

    @Test
    public void testConstructorWithMessage() {
        SiteSelectorFactoryException ex = new SiteSelectorFactoryException("test error");
        assertThat(ex.getMessage(), equalTo("test error"));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        SiteSelectorFactoryException ex =
                new SiteSelectorFactoryException("test error", "TestSiteSelector");
        assertThat(ex.getMessage(), notNullValue());
        assertThat(ex.getClassname(), equalTo("TestSiteSelector"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        SiteSelectorFactoryException ex = new SiteSelectorFactoryException("test error", cause);
        assertThat(ex.getCause(), sameInstance(cause));
        assertThat(ex.getClassname(), equalTo(SiteSelectorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithAllParams() {
        Throwable cause = new RuntimeException("root cause");
        SiteSelectorFactoryException ex =
                new SiteSelectorFactoryException("test error", "TestClass", cause);
        assertThat(ex.getClassname(), equalTo("TestClass"));
        assertThat(ex.getCause(), sameInstance(cause));
    }

    @Test
    public void testIsFactoryException() {
        SiteSelectorFactoryException ex = new SiteSelectorFactoryException("test");
        assertThat(ex, instanceOf(edu.isi.pegasus.common.util.FactoryException.class));
    }

    @Test
    public void testDefaultClassnameSet() {
        SiteSelectorFactoryException ex = new SiteSelectorFactoryException("test");
        assertThat(ex.getClassname(), equalTo(SiteSelectorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndNullCauseUsesDefaultClassname() {
        SiteSelectorFactoryException ex =
                new SiteSelectorFactoryException("test error", (Throwable) null);

        assertThat(ex.getMessage(), equalTo("test error"));
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), equalTo(SiteSelectorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndNullClassnamePreservesNull() {
        SiteSelectorFactoryException ex =
                new SiteSelectorFactoryException("test error", (String) null);

        assertThat(ex.getMessage(), equalTo("test error"));
        assertThat(ex.getClassname(), nullValue());
    }

    @Test
    public void testConstructorWithNullClassnameAndNullCausePreservesNullClassname() {
        SiteSelectorFactoryException ex =
                new SiteSelectorFactoryException("test error", null, null);

        assertThat(ex.getMessage(), equalTo("test error"));
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), nullValue());
    }
}
