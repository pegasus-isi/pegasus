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

import com.fasterxml.jackson.databind.JsonSerializer;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for PegasusJsonSerializer via reflection.
 *
 * @author Rajiv Mayani
 */
public class PegasusJsonSerializerTest {

    @Test
    public void testPegasusJsonSerializerIsAbstract() {
        assertTrue(
                Modifier.isAbstract(PegasusJsonSerializer.class.getModifiers()),
                "PegasusJsonSerializer should be abstract");
    }

    @Test
    public void testPegasusJsonSerializerExtendsJsonSerializer() {
        assertTrue(
                JsonSerializer.class.isAssignableFrom(PegasusJsonSerializer.class),
                "PegasusJsonSerializer should extend Jackson's JsonSerializer");
    }

    @Test
    public void testHasWriteStringFieldMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeStringField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        String.class);
        assertNotNull(
                m, "PegasusJsonSerializer should have writeStringField(gen, key, String) method");
    }

    @Test
    public void testHasWriteStringFieldWithObjectMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeStringField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        Object.class);
        assertNotNull(
                m, "PegasusJsonSerializer should have writeStringField(gen, key, Object) method");
    }

    @Test
    public void testHasWriteArrayMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeArray",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        Collection.class);
        assertNotNull(m, "PegasusJsonSerializer should have writeArray method");
    }

    @Test
    public void testHasWriteNumberFieldMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeNumberField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        int.class);
        assertNotNull(m, "PegasusJsonSerializer should have writeNumberField method");
    }

    @Test
    public void testWriteStringFieldIsPublic() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeStringField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        String.class);
        assertTrue(Modifier.isPublic(m.getModifiers()), "writeStringField should be public");
    }

    @Test
    public void testIsGenericClass() {
        assertTrue(
                PegasusJsonSerializer.class.getTypeParameters().length > 0,
                "PegasusJsonSerializer should be a generic class with type parameters");
    }
}
