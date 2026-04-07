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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for {@link OutOfSpaceError}. */
public class OutOfSpaceErrorTest {

    @Test
    public void testIsAnError() {
        OutOfSpaceError e = new OutOfSpaceError("no space left");
        assertThat(e, instanceOf(Error.class));
    }

    @Test
    public void testMessageIsPreserved() {
        String msg = "disk full on site cluster-A";
        OutOfSpaceError e = new OutOfSpaceError(msg);
        assertThat(e.getMessage(), is(msg));
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
        assertThat(e, instanceOf(Error.class));
    }

    @Test
    public void testEmptyMessage() {
        OutOfSpaceError e = new OutOfSpaceError("");
        assertThat(e.getMessage(), is(""));
    }

    @Test
    public void testExtendsError() {
        // OutOfSpaceError extends Error (not RuntimeException)
        OutOfSpaceError e = new OutOfSpaceError("test");
        assertThat(e, instanceOf(Error.class));
        assertThat(e.getClass().getSuperclass().equals(RuntimeException.class), is(false));
    }

    @Test
    public void testNullMessageIsAllowed() {
        OutOfSpaceError e = new OutOfSpaceError(null);
        assertThat(e.getMessage(), nullValue());
    }

    @Test
    public void testHasSingleStringConstructor() throws Exception {
        assertThat(OutOfSpaceError.class.getConstructor(String.class), notNullValue());
        assertThat(OutOfSpaceError.class.getDeclaredConstructors().length, is(1));
    }

    @Test
    public void testDeclaresNoAdditionalMethods() {
        assertThat(OutOfSpaceError.class.getDeclaredMethods().length, is(0));
    }
}
