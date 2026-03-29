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
package edu.isi.pegasus.planner.common;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonDeserializer;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for PegasusJsonDeserializer via reflection.
 *
 * @author Rajiv Mayani
 */
public class PegasusJsonDeserializerTest {

    @Test
    public void testPegasusJsonDeserializerIsAbstract() {
        assertTrue(
                Modifier.isAbstract(PegasusJsonDeserializer.class.getModifiers()),
                "PegasusJsonDeserializer should be abstract");
    }

    @Test
    public void testPegasusJsonDeserializerExtendsJsonDeserializer() {
        assertTrue(
                JsonDeserializer.class.isAssignableFrom(PegasusJsonDeserializer.class),
                "PegasusJsonDeserializer should extend Jackson's JsonDeserializer");
    }

    @Test
    public void testHasComplainForIllegalKeyMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complainForIllegalKey",
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertNotNull(m, "PegasusJsonDeserializer should have complainForIllegalKey method");
    }

    @Test
    public void testHasComplainForUnsupportedKeyMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complainForUnsupportedKey",
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertNotNull(m, "PegasusJsonDeserializer should have complainForUnsupportedKey method");
    }

    @Test
    public void testHasComplainMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complain",
                        String.class,
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertNotNull(m, "PegasusJsonDeserializer should have complain method");
    }

    @Test
    public void testComplainForIllegalKeyIsPublic() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complainForIllegalKey",
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertTrue(Modifier.isPublic(m.getModifiers()), "complainForIllegalKey should be public");
    }

    @Test
    public void testHasCreateChecksumMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "createChecksum",
                        com.fasterxml.jackson.databind.JsonNode.class,
                        String.class);
        assertNotNull(m, "PegasusJsonDeserializer should have createChecksum method");
    }

    @Test
    public void testIsGenericClass() {
        assertTrue(
                PegasusJsonDeserializer.class.getTypeParameters().length > 0,
                "PegasusJsonDeserializer should be a generic class with type parameters");
    }
}
