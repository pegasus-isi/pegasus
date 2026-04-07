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
package edu.isi.pegasus.planner.catalog.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the CatalogEntryJsonDeserializer abstract class. */
public class CatalogEntryJsonDeserializerTest {

    // -----------------------------------------------------------------------
    // Named concrete subclass — avoids anonymous-class boilerplate and exposes
    // protected inherited methods for testing.
    // -----------------------------------------------------------------------
    private static class ConcreteDeserializer extends CatalogEntryJsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser jp, DeserializationContext dc) throws IOException {
            return null;
        }

        List<Profile> exposedCreateProfiles(String namespace, JsonNode node) {
            return createProfiles(namespace, node);
        }

        List<Profile> exposedCreateMetadata(JsonNode node) {
            return createMetadata(node);
        }
    }

    private ConcreteDeserializer deserializer;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        deserializer = new ConcreteDeserializer();
    }

    // -----------------------------------------------------------------------
    // getException — return type, message, identity
    // -----------------------------------------------------------------------

    @Test
    public void testGetExceptionReturnsCatalogException() {
        RuntimeException ex = deserializer.getException("test error");
        assertThat(ex, instanceOf(CatalogException.class));
    }

    @Test
    public void testGetExceptionReturnedIsExactlyCatalogException() {
        RuntimeException ex = deserializer.getException("msg");
        assertThat(ex.getClass(), is(CatalogException.class));
    }

    @Test
    public void testGetExceptionIsRuntimeException() {
        assertThat(deserializer.getException("msg"), instanceOf(RuntimeException.class));
    }

    @Test
    public void testGetExceptionMessage() {
        String msg = "something went wrong";
        assertThat(deserializer.getException(msg).getMessage(), is(msg));
    }

    @Test
    public void testGetExceptionNonNull() {
        assertThat(deserializer.getException("error"), is(notNullValue()));
    }

    @Test
    public void testGetExceptionEmptyMessagePreserved() {
        assertThat(deserializer.getException("").getMessage(), is(""));
    }

    @Test
    public void testGetExceptionNullMessagePreserved() {
        assertThat(deserializer.getException(null).getMessage(), is(nullValue()));
    }

    @Test
    public void testGetExceptionReturnsDifferentInstanceEachCall() {
        RuntimeException a = deserializer.getException("msg");
        RuntimeException b = deserializer.getException("msg");
        assertNotSame(a, b);
    }

    // -----------------------------------------------------------------------
    // complainForIllegalKey — delegates to complain → getException
    // Validates that the CatalogException wiring flows through inherited methods.
    // -----------------------------------------------------------------------

    @Test
    public void testComplainForIllegalKey_throwsCatalogException() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        assertThrows(
                CatalogException.class,
                () -> deserializer.complainForIllegalKey("element", "badKey", node));
    }

    @Test
    public void testComplainForIllegalKey_messageContainsIllegalKey() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complainForIllegalKey("element", "badKey", node));
        assertThat(ex.getMessage(), containsString("Illegal key"));
    }

    @Test
    public void testComplainForIllegalKey_messageContainsKeyName() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complainForIllegalKey("myElement", "myKey", node));
        assertThat(ex.getMessage(), containsString("myKey"));
    }

    @Test
    public void testComplainForIllegalKey_messageContainsElementName() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complainForIllegalKey("myElement", "myKey", node));
        assertThat(ex.getMessage(), containsString("myElement"));
    }

    // -----------------------------------------------------------------------
    // complainForUnsupportedKey
    // -----------------------------------------------------------------------

    @Test
    public void testComplainForUnsupportedKey_throwsCatalogException() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        assertThrows(
                CatalogException.class,
                () -> deserializer.complainForUnsupportedKey("element", "unsupKey", node));
    }

    @Test
    public void testComplainForUnsupportedKey_messageContainsUnsupportedKey() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complainForUnsupportedKey("el", "unsupKey", node));
        assertThat(ex.getMessage(), containsString("Unsupported key"));
    }

    @Test
    public void testComplainForUnsupportedKey_messageContainsKeyName() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complainForUnsupportedKey("el", "theKey", node));
        assertThat(ex.getMessage(), containsString("theKey"));
    }

    // -----------------------------------------------------------------------
    // complain — message format
    // -----------------------------------------------------------------------

    @Test
    public void testComplain_throwsCatalogException() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        assertThrows(
                CatalogException.class, () -> deserializer.complain("PREFIX", "elem", "k", node));
    }

    @Test
    public void testComplain_messageContainsPrefix() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complain("MYPREFIX", "elem", "k", node));
        assertThat(ex.getMessage(), containsString("MYPREFIX"));
    }

    @Test
    public void testComplain_messageContainsKeyAndElement() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complain("PRE", "myElem", "myKey", node));
        assertThat(ex.getMessage(), containsString("myKey"));
        assertThat(ex.getMessage(), containsString("myElem"));
    }

    @Test
    public void testComplain_messageContainsForElement() {
        JsonNode node = JsonNodeFactory.instance.objectNode();
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.complain("PRE", "elem", "key", node));
        assertThat(ex.getMessage(), containsString("for element"));
    }

    // -----------------------------------------------------------------------
    // createChecksum — public method, called with sha256 or invalid inputs
    // -----------------------------------------------------------------------

    @Test
    public void testCreateChecksum_sha256_returnsNonNullMetadata() throws Exception {
        JsonNode node = MAPPER.readTree("{\"sha256\": \"abc123def456\"}");
        Metadata m = deserializer.createChecksum(node, "file");
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testCreateChecksum_sha256_setsChecksumTypeKey() throws Exception {
        JsonNode node = MAPPER.readTree("{\"sha256\": \"abc123def456\"}");
        Metadata m = deserializer.createChecksum(node, "file");
        assertThat(m.get(Metadata.CHECKSUM_TYPE_KEY), is("sha256"));
    }

    @Test
    public void testCreateChecksum_sha256_setsChecksumValueKey() throws Exception {
        JsonNode node = MAPPER.readTree("{\"sha256\": \"abc123def456\"}");
        Metadata m = deserializer.createChecksum(node, "file");
        assertThat(m.get(Metadata.CHECKSUM_VALUE_KEY), is("abc123def456"));
    }

    @Test
    public void testCreateChecksum_nonObjectNode_throwsCatalogException() throws Exception {
        JsonNode textNode = MAPPER.readTree("\"not-an-object\"");
        assertThrows(CatalogException.class, () -> deserializer.createChecksum(textNode, "file"));
    }

    @Test
    public void testCreateChecksum_nonObjectNode_messageContainsKeyword() throws Exception {
        JsonNode textNode = MAPPER.readTree("\"not-an-object\"");
        CatalogException ex =
                assertThrows(
                        CatalogException.class,
                        () -> deserializer.createChecksum(textNode, "myFile"));
        assertThat(ex.getMessage(), containsString("object node"));
    }

    @Test
    public void testCreateChecksum_unknownKey_throwsCatalogException() throws Exception {
        JsonNode node = MAPPER.readTree("{\"bogusAlgorithm\": \"abc123\"}");
        assertThrows(CatalogException.class, () -> deserializer.createChecksum(node, "file"));
    }

    // -----------------------------------------------------------------------
    // createProfiles (protected, exposed via ConcreteDeserializer)
    // -----------------------------------------------------------------------

    @Test
    public void testCreateProfiles_validNamespace_returnsNonEmptyList() throws Exception {
        JsonNode node = MAPPER.readTree("{\"universe\": \"vanilla\", \"request_cpus\": \"1\"}");
        List<Profile> profiles = deserializer.exposedCreateProfiles("condor", node);
        assertThat(profiles, is(notNullValue()));
        assertThat(profiles.size(), is(2));
    }

    @Test
    public void testCreateProfiles_validNamespace_profilesHaveCorrectNamespace() throws Exception {
        JsonNode node = MAPPER.readTree("{\"universe\": \"vanilla\"}");
        List<Profile> profiles = deserializer.exposedCreateProfiles("condor", node);
        assertThat(profiles.get(0).getProfileNamespace(), is("condor"));
    }

    @Test
    public void testCreateProfiles_validNamespace_profileKeyAndValuePreserved() throws Exception {
        JsonNode node = MAPPER.readTree("{\"universe\": \"vanilla\"}");
        List<Profile> profiles = deserializer.exposedCreateProfiles("condor", node);
        assertThat(profiles.get(0).getProfileKey(), is("universe"));
        assertThat(profiles.get(0).getProfileValue(), is("vanilla"));
    }

    @Test
    public void testCreateProfiles_emptyNode_returnsEmptyList() throws Exception {
        JsonNode node = MAPPER.readTree("{}");
        List<Profile> profiles = deserializer.exposedCreateProfiles("condor", node);
        assertThat(profiles, is(notNullValue()));
        assertThat(profiles.isEmpty(), is(true));
    }

    @Test
    public void testCreateProfiles_invalidNamespace_throwsCatalogException() throws Exception {
        JsonNode node = MAPPER.readTree("{\"key\": \"value\"}");
        assertThrows(
                CatalogException.class,
                () -> deserializer.exposedCreateProfiles("bogusNamespace", node));
    }

    @Test
    public void testCreateProfiles_nullNamespace_throwsCatalogException() throws Exception {
        JsonNode node = MAPPER.readTree("{\"key\": \"value\"}");
        assertThrows(CatalogException.class, () -> deserializer.exposedCreateProfiles(null, node));
    }

    // -----------------------------------------------------------------------
    // createMetadata (delegates to createProfiles("metadata", node))
    // -----------------------------------------------------------------------

    @Test
    public void testCreateMetadata_returnsProfilesList() throws Exception {
        JsonNode node = MAPPER.readTree("{\"APP_HOME\": \"/tmp/scratch\"}");
        List<Profile> profiles = deserializer.exposedCreateMetadata(node);
        assertThat(profiles, is(notNullValue()));
        assertThat(profiles.size(), is(1));
    }

    @Test
    public void testCreateMetadata_profilesAreInMetadataNamespace() throws Exception {
        JsonNode node = MAPPER.readTree("{\"APP_HOME\": \"/tmp/scratch\"}");
        List<Profile> profiles = deserializer.exposedCreateMetadata(node);
        assertThat(profiles.get(0).getProfileNamespace(), is("metadata"));
    }

    @Test
    public void testCreateMetadata_emptyNode_returnsEmptyList() throws Exception {
        JsonNode node = MAPPER.readTree("{}");
        List<Profile> profiles = deserializer.exposedCreateMetadata(node);
        assertThat(profiles, is(notNullValue()));
        assertThat(profiles.isEmpty(), is(true));
    }

    @Test
    public void testCatalogEntryJsonDeserializerIsAbstract() {
        assertThat(
                Modifier.isAbstract(CatalogEntryJsonDeserializer.class.getModifiers()), is(true));
    }

    @Test
    public void testCatalogEntryJsonDeserializerExtendsPegasusJsonDeserializer() {
        assertThat(
                CatalogEntryJsonDeserializer.class.getSuperclass(),
                is(edu.isi.pegasus.planner.common.PegasusJsonDeserializer.class));
    }

    @Test
    public void testCatalogEntryJsonDeserializerDeclaresOnlyGetExceptionMethod() {
        Method[] methods = CatalogEntryJsonDeserializer.class.getDeclaredMethods();

        assertThat(methods.length, is(1));
        assertThat(methods[0].getName(), is("getException"));
    }
}
