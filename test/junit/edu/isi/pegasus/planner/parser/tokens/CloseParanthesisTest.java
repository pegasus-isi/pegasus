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

/** Tests for {@link CloseParanthesis} token. */
public class CloseParanthesisTest {

    @Test
    public void testCanBeInstantiated() {
        CloseParanthesis cp = new CloseParanthesis();
        assertThat(cp, is(notNullValue()));
    }

    @Test
    public void testImplementsToken() {
        CloseParanthesis cp = new CloseParanthesis();
        assertThat(cp, instanceOf(Token.class));
    }

    @Test
    public void testIsDistinctFromOpenParanthesis() {
        CloseParanthesis cp = new CloseParanthesis();
        OpenParanthesis op = new OpenParanthesis();
        assertNotEquals(cp.getClass(), op.getClass());
    }

    @Test
    public void testMultipleInstances() {
        CloseParanthesis cp1 = new CloseParanthesis();
        CloseParanthesis cp2 = new CloseParanthesis();
        assertNotSame(cp1, cp2);
    }

    @Test
    public void testDeclaresNoFields() {
        assertThat(CloseParanthesis.class.getDeclaredFields().length, is(0));
    }

    @Test
    public void testDeclaresOnlyDefaultConstructor() {
        assertThat(CloseParanthesis.class.getDeclaredConstructors().length, is(1));
        assertThat(CloseParanthesis.class.getDeclaredConstructors()[0].getParameterCount(), is(0));
    }

    @Test
    public void testDeclaresNoAdditionalMethods() {
        assertThat(CloseParanthesis.class.getDeclaredMethods().length, is(0));
    }
}
