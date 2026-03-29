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
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SeparatorTest {

    // ---- combine(ns, name, version) tests ----

    @Test
    public void testCombine3_allComponents() {
        assertThat(Separator.combine("ns", "name", "1.0"), is("ns::name:1.0"));
    }

    @Test
    public void testCombine3_noNamespace() {
        assertThat(Separator.combine(null, "name", "1.0"), is("name:1.0"));
    }

    @Test
    public void testCombine3_noVersion() {
        assertThat(Separator.combine("ns", "name", null), is("ns::name"));
    }

    @Test
    public void testCombine3_nameOnly() {
        assertThat(Separator.combine(null, "name", null), is("name"));
    }

    @Test
    public void testCombine3_emptyNameThrows() {
        assertThrows(NullPointerException.class, () -> Separator.combine("ns", null, "1.0"));
    }

    @Test
    public void testCombine3_emptyNamespaceString() {
        // empty string namespace is treated as no namespace
        assertThat(Separator.combine("", "name", "1.0"), is("name:1.0"));
    }

    // ---- combine(ns, name, min, max) tests ----

    @Test
    public void testCombine4_allComponents() {
        assertThat(Separator.combine("ns", "name", "1.0", "2.0"), is("ns::name:1.0,2.0"));
    }

    @Test
    public void testCombine4_onlyMin() {
        assertThat(Separator.combine("ns", "name", "1.0", null), is("ns::name:1.0,"));
    }

    @Test
    public void testCombine4_onlyMax() {
        assertThat(Separator.combine("ns", "name", null, "2.0"), is("ns::name:,2.0"));
    }

    @Test
    public void testCombine4_noVersionRange() {
        assertThat(Separator.combine("ns", "name", null, null), is("ns::name"));
    }

    // ---- split() tests ----

    @Test
    public void testSplit_nameOnly() {
        String[] parts = Separator.split("name");
        assertThat(parts.length, is(3));
        assertNull(parts[0]);
        assertThat(parts[1], is("name"));
        assertNull(parts[2]);
    }

    @Test
    public void testSplit_nameAndVersion() {
        String[] parts = Separator.split("name:1.0");
        assertThat(parts[0], nullValue());
        assertThat(parts[1], is("name"));
        assertThat(parts[2], is("1.0"));
    }

    @Test
    public void testSplit_nsAndName() {
        String[] parts = Separator.split("test::me");
        assertThat(parts[0], is("test"));
        assertThat(parts[1], is("me"));
        assertNull(parts[2]);
    }

    @Test
    public void testSplit_nsNameVersion() {
        String[] parts = Separator.split("ns::name:1.0");
        assertThat(parts[0], is("ns"));
        assertThat(parts[1], is("name"));
        assertThat(parts[2], is("1.0"));
    }

    @Test
    public void testSplit_rangeFormat() {
        String[] parts = Separator.split("ns::name:1.0,2.0");
        assertThat(parts.length, is(4));
        assertThat(parts[0], is("ns"));
        assertThat(parts[1], is("name"));
        assertThat(parts[2], is("1.0"));
        assertThat(parts[3], is("2.0"));
    }

    @Test
    public void testSplit_illegalInput() {
        assertThrows(IllegalArgumentException.class, () -> Separator.split(":::,"));
    }
}
