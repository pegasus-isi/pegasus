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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for PegasusJsonSerializer via reflection.
 *
 * @author Rajiv Mayani
 */
public class PegasusJsonSerializerTest {

    private static class TestSerializer extends PegasusJsonSerializer<Object> {
        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("value", value == null ? "null" : value.toString());
            gen.writeEndObject();
        }
    }

    private JsonNode writeNode(SerializerAction action) throws Exception {
        StringWriter writer = new StringWriter();
        JsonGenerator gen = new ObjectMapper().getFactory().createGenerator(writer);
        gen.writeStartObject();
        action.accept(gen);
        gen.writeEndObject();
        gen.close();
        return new ObjectMapper().readTree(writer.toString());
    }

    @FunctionalInterface
    private interface SerializerAction {
        void accept(JsonGenerator gen) throws Exception;
    }

    @Test
    public void testPegasusJsonSerializerIsAbstract() {
        assertThat(Modifier.isAbstract(PegasusJsonSerializer.class.getModifiers()), is(true));
    }

    @Test
    public void testPegasusJsonSerializerExtendsJsonSerializer() {
        assertThat(JsonSerializer.class.isAssignableFrom(PegasusJsonSerializer.class), is(true));
    }

    @Test
    public void testHasWriteStringFieldMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeStringField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        String.class);
        assertThat(m, notNullValue());
    }

    @Test
    public void testHasWriteStringFieldWithObjectMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeStringField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        Object.class);
        assertThat(m, notNullValue());
    }

    @Test
    public void testHasWriteArrayMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeArray",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        Collection.class);
        assertThat(m, notNullValue());
    }

    @Test
    public void testHasWriteNumberFieldMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeNumberField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        int.class);
        assertThat(m, notNullValue());
    }

    @Test
    public void testWriteStringFieldIsPublic() throws NoSuchMethodException {
        Method m =
                PegasusJsonSerializer.class.getMethod(
                        "writeStringField",
                        com.fasterxml.jackson.core.JsonGenerator.class,
                        String.class,
                        String.class);
        assertThat(Modifier.isPublic(m.getModifiers()), is(true));
    }

    @Test
    public void testIsGenericClass() {
        assertThat(PegasusJsonSerializer.class.getTypeParameters().length > 0, is(true));
    }

    @Test
    public void testWriteStringFieldWithStringSkipsNullAndEmptyValues() throws Exception {
        TestSerializer serializer = new TestSerializer();

        JsonNode node =
                writeNode(
                        gen -> {
                            serializer.writeStringField(gen, "nullValue", (String) null);
                            serializer.writeStringField(gen, "emptyValue", "");
                            serializer.writeStringField(gen, "actual", "value");
                        });

        assertThat(node.has("nullValue"), is(false));
        assertThat(node.has("emptyValue"), is(false));
        assertThat(node.get("actual").asText(), is("value"));
    }

    @Test
    public void testWriteStringFieldWithObjectUsesToString() throws Exception {
        TestSerializer serializer = new TestSerializer();

        JsonNode node =
                writeNode(gen -> serializer.writeStringField(gen, "count", Integer.valueOf(7)));

        assertThat(node.get("count").asText(), is("7"));
    }

    @Test
    public void testWriteArraySkipsNullAndEmptyCollections() throws Exception {
        TestSerializer serializer = new TestSerializer();

        JsonNode node =
                writeNode(
                        gen -> {
                            serializer.writeArray(gen, "nullItems", null);
                            serializer.writeArray(gen, "emptyItems", Collections.emptyList());
                            serializer.writeArray(gen, "items", Arrays.asList("a", "b"));
                        });

        assertThat(node.has("nullItems"), is(false));
        assertThat(node.has("emptyItems"), is(false));
        assertThat(node.get("items").size(), is(2));
        assertThat(node.get("items").get(0).asText(), is("a"));
    }

    @Test
    public void testWriteNumberFieldSkipsSentinelMinusOne() throws Exception {
        TestSerializer serializer = new TestSerializer();

        JsonNode node =
                writeNode(
                        gen -> {
                            serializer.writeNumberField(gen, "missing", -1);
                            serializer.writeNumberField(gen, "present", 42);
                        });

        assertThat(node.has("missing"), is(false));
        assertThat(node.get("present").asInt(), is(42));
    }
}
