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
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** @author Rajiv Mayani */
public class ProfileParserExceptionTest {

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testIsCheckedException() {
        ProfileParserException ex = new ProfileParserException("test", 1);
        assertThat(ex, is(instanceOf(Exception.class)));
        assertThat(ex.getClass().getSuperclass(), is(Exception.class));
    }

    @Test
    public void testIsNotRuntimeException() {
        assertThat(
                RuntimeException.class.isAssignableFrom(ProfileParserException.class), is(false));
    }

    // -----------------------------------------------------------------------
    // Constructor(String, int)
    // -----------------------------------------------------------------------

    @Test
    public void testConstructor_messageAndPosition() {
        ProfileParserException ex = new ProfileParserException("illegal char", 5);
        assertThat(ex.getMessage(), is("illegal char"));
        assertThat(ex.getPosition(), is(5));
    }

    @Test
    public void testConstructor_twoArg_nullMessage() {
        ProfileParserException ex = new ProfileParserException(null, 2);
        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getPosition(), is(2));
    }

    // -----------------------------------------------------------------------
    // Constructor(String, int, Throwable)
    // -----------------------------------------------------------------------

    @Test
    public void testConstructor_messagePositionAndCause() {
        Throwable cause = new RuntimeException("root");
        ProfileParserException ex = new ProfileParserException("parse error", 10, cause);
        assertThat(ex.getMessage(), is("parse error"));
        assertThat(ex.getPosition(), is(10));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructor_threeArg_nullCause() {
        ProfileParserException ex = new ProfileParserException("msg", 4, null);
        assertThat(ex.getMessage(), is("msg"));
        assertThat(ex.getPosition(), is(4));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testConstructor_threeArg_nullMessage() {
        Throwable cause = new IllegalStateException("cause");
        ProfileParserException ex = new ProfileParserException(null, 1, cause);
        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructor_threeArg_causeIsChained() {
        RuntimeException root = new RuntimeException("root");
        RuntimeException mid = new RuntimeException("mid", root);
        ProfileParserException ex = new ProfileParserException("top", 0, mid);
        assertThat(ex.getCause(), is(mid));
        assertThat(ex.getCause().getCause(), is(root));
    }

    // -----------------------------------------------------------------------
    // getPosition() — boundary and representative values
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 42, 100, Integer.MAX_VALUE})
    public void testGetPosition_positivePositions(int position) {
        ProfileParserException ex = new ProfileParserException("msg", position);
        assertThat(ex.getPosition(), is(position));
    }

    @Test
    public void testGetPosition_negativePosition() {
        // No guard in the implementation — negative values are stored as-is
        ProfileParserException ex = new ProfileParserException("negative", -1);
        assertThat(ex.getPosition(), is(-1));
    }

    @Test
    public void testGetPosition_isIndependentOfMessage() {
        ProfileParserException ex1 = new ProfileParserException("msg-a", 3);
        ProfileParserException ex2 = new ProfileParserException("msg-b", 99);
        assertThat(ex1.getPosition(), is(3));
        assertThat(ex2.getPosition(), is(99));
    }

    @Test
    public void testGetPosition_threeArgConstructorPreservesPosition() {
        Throwable cause = new RuntimeException("c");
        ProfileParserException ex = new ProfileParserException("msg", 77, cause);
        assertThat(ex.getPosition(), is(77));
    }

    // -----------------------------------------------------------------------
    // getMessage() — stored verbatim
    // -----------------------------------------------------------------------

    @Test
    public void testGetMessage_emptyString() {
        ProfileParserException ex = new ProfileParserException("", 0);
        assertThat(ex.getMessage(), is(""));
    }

    @Test
    public void testGetMessage_withSpecialCharacters() {
        String msg = "unexpected token '<' at position 5";
        ProfileParserException ex = new ProfileParserException(msg, 5);
        assertThat(ex.getMessage(), is(msg));
    }

    @Test
    public void testThreeArgConstructorPreservesExtremeNegativePosition() {
        ProfileParserException ex =
                new ProfileParserException("msg", Integer.MIN_VALUE, new RuntimeException("c"));
        assertThat(ex.getPosition(), is(Integer.MIN_VALUE));
    }

    @Test
    public void testDeclaresPrivateIntPositionField() throws Exception {
        Field field = ProfileParserException.class.getDeclaredField("m_position");
        assertThat(field.getType(), is(int.class));
        assertThat(Modifier.isPrivate(field.getModifiers()), is(true));
    }

    @Test
    public void testTwoArgConstructorLeavesCauseUnset() {
        ProfileParserException ex = new ProfileParserException("msg", 8);
        assertThat(ex.getCause(), is(nullValue()));
    }
}
