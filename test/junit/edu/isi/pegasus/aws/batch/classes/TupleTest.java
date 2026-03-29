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
package edu.isi.pegasus.aws.batch.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TupleTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testConstructorSetsFields() {
        Tuple<String, Integer> t = new Tuple<>("hello", 42);
        assertThat(t.x, is("hello"));
        assertThat(t.y, is(42));
    }

    @Test
    public void testGetKeyAndValue() {
        Tuple<String, String> t = new Tuple<>("key", "value");
        assertThat(t.getKey(), is("key"));
        assertThat(t.getValue(), is("value"));
    }

    @Test
    public void testToStringFormat() {
        Tuple<String, Integer> t = new Tuple<>("alpha", 7);
        assertThat(t.toString(), is("alpha , 7"));
    }

    @Test
    public void testToStringWithNullX() {
        Tuple<String, String> t = new Tuple<>(null, "val");
        assertThat(t.toString(), is("null , val"));
    }

    @Test
    public void testToStringWithNullY() {
        Tuple<String, String> t = new Tuple<>("key", null);
        assertThat(t.toString(), is("key , null"));
    }

    @Test
    public void testNullValues() {
        Tuple<String, String> t = new Tuple<>(null, null);
        assertNull(t.getKey());
        assertNull(t.getValue());
    }

    @Test
    public void testIntegerTuple() {
        Tuple<Integer, Integer> t = new Tuple<>(1, 2);
        assertThat(t.getKey(), is(1));
        assertThat(t.getValue(), is(2));
        assertThat(t.toString(), is("1 , 2"));
    }
}
