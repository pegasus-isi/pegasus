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
package edu.isi.pegasus.planner.transfer.sls;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SLSFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertThat(SLSFactoryException.DEFAULT_NAME, is("Second Level Staging Implementor"));
    }

    @Test
    public void testMessageOnlyConstructorUsesDefaultClassname() {
        SLSFactoryException exception = new SLSFactoryException("message");

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getClassname(), is(SLSFactoryException.DEFAULT_NAME));
        assertThat(exception.getCause(), nullValue());
    }

    @Test
    public void testMessageAndCauseConstructorUsesDefaultClassname() {
        RuntimeException cause = new RuntimeException("root cause");

        SLSFactoryException exception = new SLSFactoryException("message", cause);

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getClassname(), is(SLSFactoryException.DEFAULT_NAME));
        assertThat(exception.getCause(), sameInstance(cause));
    }

    @Test
    public void testExplicitClassnameAndCauseConstructorPreservesValues() {
        IllegalStateException cause = new IllegalStateException("broken");

        SLSFactoryException exception =
                new SLSFactoryException("message", "custom.class.Name", cause);

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getClassname(), is("custom.class.Name"));
        assertThat(exception.getCause(), sameInstance(cause));
    }
}
