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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for SiteDataJsonDeserializer. */
public class SiteDataJsonDeserializerTest {

    private static class DummyDeserializer extends SiteDataJsonDeserializer<Object> {
        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return null;
        }
    }

    @Test
    public void testGetExceptionReturnsSiteCatalogException() {
        DummyDeserializer deserializer = new DummyDeserializer();

        RuntimeException exception = deserializer.getException("bad site data");

        assertThat(exception, is(instanceOf(SiteCatalogException.class)));
        assertThat(exception.getMessage(), equalTo("bad site data"));
    }

    @Test
    public void testGetExceptionAllowsNullMessage() {
        DummyDeserializer deserializer = new DummyDeserializer();

        RuntimeException exception = deserializer.getException(null);

        assertThat(exception, is(instanceOf(SiteCatalogException.class)));
        assertThat(exception.getMessage(), nullValue());
    }

    @Test
    public void testGetExceptionReturnsFreshInstanceEachTime() {
        DummyDeserializer deserializer = new DummyDeserializer();

        RuntimeException first = deserializer.getException("bad site data");
        RuntimeException second = deserializer.getException("bad site data");

        assertNotSame(first, second);
    }

    @Test
    public void testComplainForIllegalKeyThrowsSiteCatalogException() throws IOException {
        DummyDeserializer deserializer = new DummyDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"bad\":1}");

        SiteCatalogException exception =
                assertThrows(
                        SiteCatalogException.class,
                        () -> deserializer.complainForIllegalKey("sites", "bad", node));

        assertThat(exception.getMessage(), containsString("Illegal key bad for element sites"));
        assertThat(exception.getMessage(), containsString("{\"bad\":1}"));
    }

    @Test
    public void testComplainForUnsupportedKeyThrowsSiteCatalogException() throws IOException {
        DummyDeserializer deserializer = new DummyDeserializer();
        JsonNode node = new ObjectMapper().readTree("{\"bad\":1}");

        SiteCatalogException exception =
                assertThrows(
                        SiteCatalogException.class,
                        () -> deserializer.complainForUnsupportedKey("sites", "bad", node));

        assertThat(exception.getMessage(), containsString("Unsupported key bad for element sites"));
        assertThat(exception.getMessage(), containsString("{\"bad\":1}"));
    }
}
