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
package edu.isi.pegasus.planner.catalog.replica.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogJsonDeserializerTest {

    @Test
    public void testClassIsAbstract() {
        assertThat(
                Modifier.isAbstract(ReplicaCatalogJsonDeserializer.class.getModifiers()), is(true));
    }

    @Test
    public void testExtendsCatalogEntryJsonDeserializer() {
        assertThat(
                CatalogEntryJsonDeserializer.class.isAssignableFrom(
                        ReplicaCatalogJsonDeserializer.class),
                is(true));
    }

    @Test
    public void testGetExceptionReturnsReplicaCatalogException() {
        ReplicaCatalogJsonDeserializer<?> deserializer =
                new ReplicaCatalogJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };
        RuntimeException ex = deserializer.getException("test error");
        assertThat(ex, instanceOf(ReplicaCatalogException.class));
    }

    @Test
    public void testGetExceptionMessage() {
        ReplicaCatalogJsonDeserializer<?> deserializer =
                new ReplicaCatalogJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };
        String msg = "replica catalog error";
        RuntimeException ex = deserializer.getException(msg);
        assertThat(ex.getMessage(), is(msg));
    }

    @Test
    public void testGetExceptionAllowsNullMessage() {
        ReplicaCatalogJsonDeserializer<?> deserializer =
                new ReplicaCatalogJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };

        RuntimeException ex = deserializer.getException(null);

        assertThat(ex, instanceOf(ReplicaCatalogException.class));
        assertThat(ex.getMessage(), is(nullValue()));
    }

    @Test
    public void testGetExceptionReturnsFreshExceptionEachTime() {
        ReplicaCatalogJsonDeserializer<?> deserializer =
                new ReplicaCatalogJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };

        RuntimeException first = deserializer.getException("same message");
        RuntimeException second = deserializer.getException("same message");

        assertNotSame(first, second);
        assertThat(second.getMessage(), is(first.getMessage()));
    }

    @Test
    public void testGetExceptionReturnsRuntimeExceptionSubtype() {
        ReplicaCatalogJsonDeserializer<?> deserializer =
                new ReplicaCatalogJsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(
                            com.fasterxml.jackson.core.JsonParser jp,
                            com.fasterxml.jackson.databind.DeserializationContext dc)
                            throws java.io.IOException {
                        return null;
                    }
                };

        RuntimeException ex = deserializer.getException("boom");

        assertThat(ex, instanceOf(RuntimeException.class));
        assertThat(ex, instanceOf(edu.isi.pegasus.planner.catalog.CatalogException.class));
    }

    @Test
    public void testClassInCorrectPackage() {
        assertThat(
                ReplicaCatalogJsonDeserializer.class.getPackage().getName(),
                is("edu.isi.pegasus.planner.catalog.replica.classes"));
    }
}
