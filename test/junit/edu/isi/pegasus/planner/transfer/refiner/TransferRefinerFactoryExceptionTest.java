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
package edu.isi.pegasus.planner.transfer.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class TransferRefinerFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertThat(TransferRefinerFactoryException.DEFAULT_NAME, is("Transfer Refiner"));
    }

    @Test
    public void testMessageOnlyConstructorUsesDefaultClassname() throws Exception {
        TransferRefinerFactoryException exception = new TransferRefinerFactoryException("message");

        assertThat(exception.getMessage(), is("message"));
        assertThat(getClassname(exception), is(TransferRefinerFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndCauseConstructorUsesDefaultClassname() throws Exception {
        Throwable cause = new IllegalStateException("boom");
        TransferRefinerFactoryException exception =
                new TransferRefinerFactoryException("message", cause);

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getCause(), sameInstance(cause));
        assertThat(getClassname(exception), is(TransferRefinerFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testExplicitClassnameAndCauseConstructorPreservesValues() throws Exception {
        Throwable cause = new IllegalArgumentException("bad");
        TransferRefinerFactoryException exception =
                new TransferRefinerFactoryException("message", "custom-refiner", cause);

        assertThat(exception.getMessage(), is("message"));
        assertThat(exception.getCause(), sameInstance(cause));
        assertThat(getClassname(exception), is("custom-refiner"));
    }

    private static String getClassname(TransferRefinerFactoryException exception) throws Exception {
        return (String) ReflectionTestUtils.getField(exception, "mClassname");
    }
}
