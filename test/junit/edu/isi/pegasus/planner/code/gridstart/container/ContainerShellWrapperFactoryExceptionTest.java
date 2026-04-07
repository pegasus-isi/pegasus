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
package edu.isi.pegasus.planner.code.gridstart.container;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for the ContainerShellWrapperFactoryException class. */
public class ContainerShellWrapperFactoryExceptionTest {

    @Test
    public void testExceptionExtendsFactoryException() {
        assertThat(
                FactoryException.class.isAssignableFrom(
                        ContainerShellWrapperFactoryException.class),
                is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(
                ContainerShellWrapperFactoryException.DEFAULT_NAME, is("Container Shell Wrapper"));
    }

    @Test
    public void testConstructorWithMessage() {
        ContainerShellWrapperFactoryException ex =
                new ContainerShellWrapperFactoryException("test message");
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("test message"));
    }

    @Test
    public void testExceptionIsThrowable() {
        assertThat(
                Throwable.class.isAssignableFrom(ContainerShellWrapperFactoryException.class),
                is(true));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                ContainerShellWrapperFactoryException.class,
                () -> {
                    throw new ContainerShellWrapperFactoryException("test");
                });
    }

    @Test
    public void testConstructorWithMessageSetsDefaultClassname() {
        ContainerShellWrapperFactoryException ex =
                new ContainerShellWrapperFactoryException("test message");

        assertThat(ex.getClassname(), is(ContainerShellWrapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndClassnamePreservesExplicitClassname() {
        ContainerShellWrapperFactoryException ex =
                new ContainerShellWrapperFactoryException("test message", "WrapperClass");

        assertThat(ex.getClassname(), is("WrapperClass"));
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testConstructorWithMessageAndNullCauseUsesDefaultClassname() {
        ContainerShellWrapperFactoryException ex =
                new ContainerShellWrapperFactoryException("test message", (Throwable) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), is(ContainerShellWrapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageClassnameAndNullCausePreservesClassname() {
        ContainerShellWrapperFactoryException ex =
                new ContainerShellWrapperFactoryException("test message", "WrapperClass", null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), is("WrapperClass"));
    }
}
