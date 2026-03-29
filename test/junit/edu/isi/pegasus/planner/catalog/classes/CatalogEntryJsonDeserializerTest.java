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

import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.common.PegasusJsonDeserializer;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the CatalogEntryJsonDeserializer abstract class. */
public class CatalogEntryJsonDeserializerTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassIsAbstract() {
        assertTrue(
                Modifier.isAbstract(CatalogEntryJsonDeserializer.class.getModifiers()),
                "CatalogEntryJsonDeserializer should be abstract");
    }

    @Test
    public void testExtendsJsonDeserializer() {
        assertTrue(
                PegasusJsonDeserializer.class.isAssignableFrom(CatalogEntryJsonDeserializer.class),
                "CatalogEntryJsonDeserializer should extend PegasusJsonDeserializer");
    }

    @Test
    public void testGetExceptionReturnsCatalogException() {
        // Create an anonymous subclass to test getException()
        CatalogEntryJsonDeserializer<?> deserializer =
                new CatalogEntryJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };
        RuntimeException ex = deserializer.getException("test error");
        assertThat(ex, instanceOf(CatalogException.class));
    }

    @Test
    public void testGetExceptionMessage() {
        CatalogEntryJsonDeserializer<?> deserializer =
                new CatalogEntryJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };
        String msg = "something went wrong";
        RuntimeException ex = deserializer.getException(msg);
        assertEquals(msg, ex.getMessage());
    }

    @Test
    public void testGetExceptionNonNull() {
        CatalogEntryJsonDeserializer<?> deserializer =
                new CatalogEntryJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };
        assertNotNull(deserializer.getException("error"));
    }
}
