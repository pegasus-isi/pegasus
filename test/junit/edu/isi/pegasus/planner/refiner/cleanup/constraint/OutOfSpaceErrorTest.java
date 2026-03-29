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
package edu.isi.pegasus.planner.refiner.cleanup.constraint;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link OutOfSpaceError}. */
public class OutOfSpaceErrorTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testIsAnError() {
        OutOfSpaceError e = new OutOfSpaceError("no space left");
        assertTrue(e instanceof Error);
    }

    @Test
    public void testMessageIsPreserved() {
        String msg = "disk full on site cluster-A";
        OutOfSpaceError e = new OutOfSpaceError(msg);
        assertEquals(msg, e.getMessage());
    }

    @Test
    public void testCanBeThrownAndCaught() {
        assertThrows(
                OutOfSpaceError.class,
                () -> {
                    throw new OutOfSpaceError("test throw");
                });
    }

    @Test
    public void testIsSubtypeOfError() {
        OutOfSpaceError e = new OutOfSpaceError("test");
        assertInstanceOf(Error.class, e);
    }

    @Test
    public void testEmptyMessage() {
        OutOfSpaceError e = new OutOfSpaceError("");
        assertEquals("", e.getMessage());
    }

    @Test
    public void testExtendsError() {
        // OutOfSpaceError extends Error (not RuntimeException)
        OutOfSpaceError e = new OutOfSpaceError("test");
        assertTrue(e instanceof Error);
        assertFalse(e.getClass().getSuperclass().equals(RuntimeException.class));
    }
}
