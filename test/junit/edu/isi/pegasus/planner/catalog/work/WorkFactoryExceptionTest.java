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
package edu.isi.pegasus.planner.catalog.work;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class WorkFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertThat(WorkFactoryException.DEFAULT_NAME, equalTo("Work Catalog"));
    }

    @Test
    public void testMessageOnlyConstructorUsesDefaultClassname() throws Exception {
        WorkFactoryException exception = new WorkFactoryException("message");

        assertThat(exception.getMessage(), is("message"));
        assertThat(getClassname(exception), equalTo(WorkFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndCauseConstructorUsesDefaultClassname() throws Exception {
        Throwable cause = new IllegalStateException("boom");
        WorkFactoryException exception = new WorkFactoryException("message", cause);

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getCause(), is(sameInstance(cause)));
        assertThat(getClassname(exception), equalTo(WorkFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testExplicitClassnameAndCauseConstructorPreservesValues() throws Exception {
        Throwable cause = new IllegalArgumentException("bad");
        WorkFactoryException exception =
                new WorkFactoryException("message", "custom-work-module", cause);

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getCause(), is(sameInstance(cause)));
        assertThat(getClassname(exception), equalTo("custom-work-module"));
    }

    private static String getClassname(WorkFactoryException exception) {
        return (String) ReflectionTestUtils.getField(exception, "mClassname");
    }
}
