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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for GridStartFactoryException */
public class GridStartFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertEquals("GridStart", GridStartFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testSingleMessageConstructor() {
        GridStartFactoryException e = new GridStartFactoryException("gridstart failed");
        assertEquals("gridstart failed", e.getMessage());
    }

    @Test
    public void testMessageAndClassnameConstructor() {
        GridStartFactoryException e = new GridStartFactoryException("failed", "Kickstart");
        assertEquals("failed", e.getMessage());
    }

    @Test
    public void testMessageAndCauseConstructor() {
        Throwable cause = new RuntimeException("underlying cause");
        GridStartFactoryException e = new GridStartFactoryException("wrapped", cause);
        assertEquals("wrapped", e.getMessage());
        assertSame(cause, e.getCause());
    }

    @Test
    public void testMessageClassnameAndCauseConstructor() {
        Throwable cause = new RuntimeException("root");
        GridStartFactoryException e = new GridStartFactoryException("msg", "NoGridStart", cause);
        assertEquals("msg", e.getMessage());
        assertSame(cause, e.getCause());
    }

    @Test
    public void testExtendsFactoryException() {
        assertTrue(FactoryException.class.isAssignableFrom(GridStartFactoryException.class));
    }

    @Test
    public void testIsCatchableAsFactoryException() {
        FactoryException caught = null;
        try {
            throw new GridStartFactoryException("test gridstart");
        } catch (FactoryException ex) {
            caught = ex;
        }
        assertNotNull(caught);
        assertEquals("test gridstart", caught.getMessage());
    }
}
