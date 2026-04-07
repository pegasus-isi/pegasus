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
package edu.isi.pegasus.planner.code;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for GridStartFactoryException */
public class GridStartFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertThat(GridStartFactoryException.DEFAULT_NAME, is("GridStart"));
    }

    @Test
    public void testSingleMessageConstructor() {
        GridStartFactoryException e = new GridStartFactoryException("gridstart failed");
        assertThat(e.getMessage(), is("gridstart failed"));
    }

    @Test
    public void testMessageAndClassnameConstructor() {
        GridStartFactoryException e = new GridStartFactoryException("failed", "Kickstart");
        assertThat(e.getMessage(), is("failed"));
    }

    @Test
    public void testMessageAndCauseConstructor() {
        Throwable cause = new RuntimeException("underlying cause");
        GridStartFactoryException e = new GridStartFactoryException("wrapped", cause);
        assertThat(e.getMessage(), is("wrapped"));
        assertThat(e.getCause(), sameInstance(cause));
    }

    @Test
    public void testMessageClassnameAndCauseConstructor() {
        Throwable cause = new RuntimeException("root");
        GridStartFactoryException e = new GridStartFactoryException("msg", "NoGridStart", cause);
        assertThat(e.getMessage(), is("msg"));
        assertThat(e.getCause(), sameInstance(cause));
    }

    @Test
    public void testExtendsFactoryException() {
        assertThat(
                FactoryException.class.isAssignableFrom(GridStartFactoryException.class), is(true));
    }

    @Test
    public void testIsCatchableAsFactoryException() {
        FactoryException caught = null;
        try {
            throw new GridStartFactoryException("test gridstart");
        } catch (FactoryException ex) {
            caught = ex;
        }
        assertThat(caught, notNullValue());
        assertThat(caught.getMessage(), is("test gridstart"));
    }

    @Test
    public void testSingleMessageConstructorSetsDefaultClassname() {
        GridStartFactoryException e = new GridStartFactoryException("gridstart failed");

        assertThat(e.getClassname(), is(GridStartFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndClassnameConstructorSetsExplicitClassname() {
        GridStartFactoryException e = new GridStartFactoryException("failed", "Kickstart");

        assertThat(e.getClassname(), is("Kickstart"));
    }

    @Test
    public void testMessageAndCauseConstructorAllowsNullCause() {
        GridStartFactoryException e = new GridStartFactoryException("wrapped", (Throwable) null);

        assertThat(e.getMessage(), is("wrapped"));
        assertThat(e.getCause(), nullValue());
        assertThat(e.getClassname(), is(GridStartFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageClassnameAndCauseConstructorAllowsNullCause() {
        GridStartFactoryException e = new GridStartFactoryException("msg", "NoGridStart", null);

        assertThat(e.getMessage(), is("msg"));
        assertThat(e.getClassname(), is("NoGridStart"));
        assertThat(e.getCause(), nullValue());
    }

    @Test
    public void testMessageAndClassnameConstructorAllowsNullClassname() {
        GridStartFactoryException e = new GridStartFactoryException("failed", (String) null);

        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getClassname(), nullValue());
    }
}
