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
package edu.isi.pegasus.planner.parser.tokens;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for {@link OpenParanthesis} token. */
public class OpenParanthesisTest {

    @Test
    public void testCanBeInstantiated() {
        OpenParanthesis op = new OpenParanthesis();
        assertThat(op, is(notNullValue()));
    }

    @Test
    public void testImplementsToken() {
        OpenParanthesis op = new OpenParanthesis();
        assertThat(op, instanceOf(Token.class));
    }

    @Test
    public void testIsDistinctFromCloseParanthesis() {
        OpenParanthesis op = new OpenParanthesis();
        CloseParanthesis cp = new CloseParanthesis();
        assertNotEquals(op.getClass(), cp.getClass());
    }

    @Test
    public void testMultipleInstances() {
        OpenParanthesis op1 = new OpenParanthesis();
        OpenParanthesis op2 = new OpenParanthesis();
        assertNotSame(op1, op2);
    }

    @Test
    public void testDeclaresNoFields() {
        assertThat(OpenParanthesis.class.getDeclaredFields().length, is(0));
    }

    @Test
    public void testDeclaresOnlyDefaultConstructor() {
        assertThat(OpenParanthesis.class.getDeclaredConstructors().length, is(1));
        assertThat(OpenParanthesis.class.getDeclaredConstructors()[0].getParameterCount(), is(0));
    }

    @Test
    public void testDeclaresNoAdditionalMethods() {
        assertThat(OpenParanthesis.class.getDeclaredMethods().length, is(0));
    }
}
