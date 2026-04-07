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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class DAXParserFactoryExceptionTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testDefaultNameConstant() {
        assertThat(
                "DEFAULT_NAME should match the documented fallback classname",
                DAXParserFactoryException.DEFAULT_NAME,
                is("DAX Callback"));
    }

    @Test
    public void testMessageOnlyConstructorUsesDefaultClassname() throws Exception {
        DAXParserFactoryException exception = new DAXParserFactoryException("message");

        assertThat("Message should be preserved", exception.getMessage(), is("message"));
        assertThat("Cause should be unset", exception.getCause(), nullValue());
        assertThat(
                "Message-only constructor should use the default classname",
                getClassname(exception),
                is(DAXParserFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndCauseConstructorUsesDefaultClassname() throws Exception {
        IllegalStateException cause = new IllegalStateException("boom");
        DAXParserFactoryException exception = new DAXParserFactoryException("message", cause);

        assertThat("Message should be preserved", exception.getMessage(), is("message"));
        assertThat("Cause should be preserved", exception.getCause(), sameInstance(cause));
        assertThat(
                "Message-and-cause constructor should use the default classname",
                getClassname(exception),
                is(DAXParserFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testExplicitClassnameAndCauseConstructorPreservesValues() throws Exception {
        RuntimeException cause = new RuntimeException("boom");
        DAXParserFactoryException exception =
                new DAXParserFactoryException("message", "custom", cause);

        assertThat("Message should be preserved", exception.getMessage(), is("message"));
        assertThat("Cause should be preserved", exception.getCause(), sameInstance(cause));
        assertThat("Explicit classname should be preserved", getClassname(exception), is("custom"));
    }

    private String getClassname(DAXParserFactoryException exception) throws Exception {
        return (String) ReflectionTestUtils.getField(exception, "mClassname");
    }
}
