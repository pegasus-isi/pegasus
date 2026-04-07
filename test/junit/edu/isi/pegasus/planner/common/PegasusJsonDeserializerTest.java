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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for PegasusJsonDeserializer via reflection.
 *
 * @author Rajiv Mayani
 */
public class PegasusJsonDeserializerTest {

    private static class TestDeserializer extends PegasusJsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return null;
        }

        @Override
        public RuntimeException getException(String message) {
            return new RuntimeException(message);
        }

        List<Profile> profiles(String namespace, JsonNode node) {
            return createProfiles(namespace, node);
        }

        List<Profile> metadata(JsonNode node) {
            return createMetadata(node);
        }
    }

    @Test
    public void testPegasusJsonDeserializerIsAbstract() {
        assertThat(Modifier.isAbstract(PegasusJsonDeserializer.class.getModifiers()), is(true));
    }

    @Test
    public void testPegasusJsonDeserializerExtendsJsonDeserializer() {
        assertThat(
                JsonDeserializer.class.isAssignableFrom(PegasusJsonDeserializer.class), is(true));
    }

    @Test
    public void testHasComplainForIllegalKeyMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complainForIllegalKey",
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertThat(m, notNullValue());
    }

    @Test
    public void testHasComplainForUnsupportedKeyMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complainForUnsupportedKey",
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertThat(m, notNullValue());
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
        assertThat(m, notNullValue());
    }

    @Test
    public void testComplainForIllegalKeyIsPublic() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "complainForIllegalKey",
                        String.class,
                        String.class,
                        com.fasterxml.jackson.databind.JsonNode.class);
        assertThat(Modifier.isPublic(m.getModifiers()), is(true));
    }

    @Test
    public void testHasCreateChecksumMethod() throws NoSuchMethodException {
        Method m =
                PegasusJsonDeserializer.class.getMethod(
                        "createChecksum",
                        com.fasterxml.jackson.databind.JsonNode.class,
                        String.class);
        assertThat(m, notNullValue());
    }

    @Test
    public void testIsGenericClass() {
        assertThat(PegasusJsonDeserializer.class.getTypeParameters().length > 0, is(true));
    }

    @Test
    public void testComplainForIllegalKeyUsesConfiguredException() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"bad\":1}");

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> deserializer.complainForIllegalKey("replica", "bad", node));

        assertThat(
                e.getMessage(),
                allOf(
                        containsString("Illegal key bad for element replica"),
                        containsString("{\"bad\":1}")));
    }

    @Test
    public void testComplainForUnsupportedKeyUsesConfiguredException() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"bad\":1}");

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> deserializer.complainForUnsupportedKey("checksum", "bad", node));

        assertThat(e.getMessage(), containsString("Unsupported key bad for element checksum"));
    }

    @Test
    public void testCreateChecksumParsesSupportedSha256Key() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"sha256\":\"abc123\"}");

        Metadata metadata = deserializer.createChecksum(node, "checksum");

        assertThat(metadata.get(Metadata.CHECKSUM_TYPE_KEY), is("sha256"));
        assertThat(metadata.get(Metadata.CHECKSUM_VALUE_KEY), is("abc123"));
    }

    @Test
    public void testCreateChecksumRejectsIllegalKey() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"md5\":\"abc123\"}");

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> deserializer.createChecksum(node, "checksum"));

        assertThat(e.getMessage(), containsString("Illegal key md5"));
    }

    @Test
    public void testCreateChecksumRejectsNonObjectNode() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node = new ObjectMapper().readTree("\"abc123\"");

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> deserializer.createChecksum(node, "checksum"));

        assertThat(e.getMessage(), containsString("Checksum needs to be object node"));
    }

    @Test
    public void testCreateProfilesRejectsInvalidNamespace() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"k\":\"v\"}");

        RuntimeException e =
                assertThrows(RuntimeException.class, () -> deserializer.profiles("bogus", node));

        assertThat(e.getMessage(), containsString("Invalid namespace specified bogus"));
    }

    @Test
    public void testCreateMetadataBuildsMetadataProfiles() throws Exception {
        TestDeserializer deserializer = new TestDeserializer();
        JsonNode node =
                new ObjectMapper()
                        .readTree("{\"checksum.type\":\"sha256\",\"checksum.value\":\"abc\"}");

        List<Profile> profiles = deserializer.metadata(node);

        assertThat(profiles, hasSize(2));
        assertThat(profiles.get(0).getProfileNamespace(), is("metadata"));
        assertThat(profiles.get(0).getProfileKey(), is("checksum.type"));
        assertThat(profiles.get(0).getProfileValue(), is("sha256"));
    }
}
