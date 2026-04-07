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
package edu.isi.pegasus.planner.code.gridstart;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusExitCodeEncodeTest {

    @Test
    public void testDefaultConstructorAndEncodeDecodeRoundTrip() {
        PegasusExitCodeEncode encoder = new PegasusExitCodeEncode();

        assertThat(encoder.encode("Error + Message"), is("Error+\\++Message"));
        assertThat(encoder.decode("Error+\\++Message"), is("Error + Message"));
        assertThat(encoder.decode("Error+++Message"), is("Error   Message"));
        assertThat(encoder.encode(null), nullValue());
        assertThat(encoder.decode(null), nullValue());
    }

    @Test
    public void testCustomConstructorEnsuresEscapeCharacterIsEscapable() throws Exception {
        PegasusExitCodeEncode encoder = new PegasusExitCodeEncode("%", '!');

        Field escape = PegasusExitCodeEncode.class.getDeclaredField("mEscape");
        escape.setAccessible(true);
        Field escapable = PegasusExitCodeEncode.class.getDeclaredField("mEscapable");
        escapable.setAccessible(true);

        assertThat(escape.getChar(encoder), is('!'));
        assertThat(escapable.get(encoder), is("%!"));
    }

    @Test
    public void testEncodeRejectsUnsupportedWhitespaceAndNonPrintableCharacters() {
        PegasusExitCodeEncode encoder = new PegasusExitCodeEncode();

        IllegalArgumentException whitespace =
                assertThrows(
                        IllegalArgumentException.class, () -> encoder.encode("Error\tMessage"));
        assertThat(whitespace.getMessage(), containsString("Invalid whitespace character"));

        IllegalArgumentException nonPrintable =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> encoder.encode("Error Message \u0007"));
        assertThat(nonPrintable.getMessage(), containsString("Invalid non printing character"));
    }

    @Test
    public void testDecodePreservesTrailingAndUnknownEscapes() {
        PegasusExitCodeEncode encoder = new PegasusExitCodeEncode();

        assertThat(encoder.decode("Message\\"), is("Message\\"));
        assertThat(encoder.decode("Error\\x"), is("Error\\x"));
    }

    @Test
    public void testAsciiPrintableAndMethodSignatures() throws Exception {
        PegasusExitCodeEncode encoder = new PegasusExitCodeEncode();

        assertThat(encoder.isAsciiPrintable('a'), is(true));
        assertThat(encoder.isAsciiPrintable('~'), is(true));
        assertThat(encoder.isAsciiPrintable('\n'), is(false));
        assertThat(encoder.isAsciiPrintable((char) 163), is(false));

        assertMethod("encode", String.class, String.class);
        assertMethod("decode", String.class, String.class);
        assertMethod("isAsciiPrintable", boolean.class, char.class);
        assertMethod("test", void.class, String.class);
        assertMethod("main", void.class, String[].class);

        assertField("mEscape", char.class, Modifier.PRIVATE);
        assertField("mEscapable", String.class, Modifier.PRIVATE);
        assertField("mEncodeable", char.class, Modifier.PRIVATE);
        assertField("mEncode", char.class, Modifier.PRIVATE);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = PegasusExitCodeEncode.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = PegasusExitCodeEncode.class.getMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
    }
}
