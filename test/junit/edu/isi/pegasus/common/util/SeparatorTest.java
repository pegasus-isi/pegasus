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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** @author Rajiv Mayani */
public class SeparatorTest {

    @Test
    public void testSeparatorConstants() {
        assertThat(Separator.NAMESPACE, is("::"));
        assertThat(Separator.NAME, is(":"));
        assertThat(Separator.VERSION, is(","));
        assertThat(Separator.DEFAULT, is("default"));
    }

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

    @Test
    public void testCombine4_emptyNamespaceStringIsTreatedAsNoNamespace() {
        assertThat(Separator.combine("", "name", "1.0", "2.0"), is("name:1.0,2.0"));
    }

    @Test
    public void testCombine4_emptyNameThrows() {
        assertThrows(NullPointerException.class, () -> Separator.combine("ns", "", "1.0", "2.0"));
    }

    // ---- split() tests ----

    @Test
    public void testSplit_nameOnly() {
        String[] parts = Separator.split("name");
        assertThat(parts.length, is(3));
        assertThat(parts[0], is(nullValue()));
        assertThat(parts[1], is("name"));
        assertThat(parts[2], is(nullValue()));
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
        assertThat(parts[2], is(nullValue()));
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

    @Test
    public void testSplit_nameWithVersionRange() {
        String[] x = Separator.split("test::me:a,b");
        assertThat(x.length, is(4));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("a"));
        assertThat(x[3], is("b"));
    }

    @Test
    public void testSplit_namespaceNameWithExplicitEmptyVersion() {
        String[] x = Separator.split("ns::name:");
        assertThat(x.length, is(3));
        assertThat(x[0], is("ns"));
        assertThat(x[1], is("name"));
        assertThat(x[2], is(nullValue()));
    }

    @Test
    public void testSplit_minVersionOnly() {
        String[] x = Separator.split("me:a,");
        assertThat(x.length, is(4));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("a"));
        assertThat(x[3], is(nullValue()));
    }

    @Test
    public void testSplit_maxVersionOnlyIsIllegal() {
        assertThrows(IllegalArgumentException.class, () -> Separator.split("me:,b"));
    }

    @Test
    public void testSplit_illegalTripleColon() {
        assertThrows(IllegalArgumentException.class, () -> Separator.split("illegal:::too"));
    }

    @Test
    public void testSplit_illegalEmptyRange() {
        assertThrows(IllegalArgumentException.class, () -> Separator.split("il::legal:,"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "test",
                "test::me",
                "test:me",
                "test::me:too",
                "test::me:a,b",
                "test::me:,b",
                "test::me:a,",
                "me:a,b",
                "me:a,",
            })
    public void testSeparatorHarnessValidSplitInputsDoNotThrow(String input) {
        assertDoesNotThrow(() -> Separator.split(input));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "illegal:::too",
                "il::legal:,",
                "me:,b",
                "illegal:,",
                ":::,",
            })
    public void testSeparatorHarnessInvalidSplitInputsThrow(String input) {
        assertThrows(IllegalArgumentException.class, () -> Separator.split(input));
    }

    @Test
    public void testSplitShowFormattingFromDemoHarness() {
        String printed = renderSplitShow("test::me:a,b");
        assertThat(printed, containsString("test::me:a,b => ["));
        assertThat(printed, containsString("0:\"test\""));
        assertThat(printed, containsString("1:\"me\""));
        assertThat(printed, containsString("2:\"a\""));
        assertThat(printed, containsString("3:\"b\""));
    }

    @Test
    public void testSplitFQDI_nameOnly() {
        String[] x = Separator.splitFQDI("me");
        assertThat(x.length, is(3));
        assertThat(x[0], nullValue());
        assertThat(x[1], is("me"));
        assertThat(x[2], nullValue());
    }

    @Test
    public void testSplitFQDI_emptyNamespace() {
        String[] x = Separator.splitFQDI("::me");
        assertThat(x.length, is(3));
        assertThat(x[0], is(""));
        assertThat(x[1], is("me"));
        assertThat(x[2], nullValue());
    }

    @Test
    public void testSplitFQDI_emptyVersion() {
        String[] x = Separator.splitFQDI("::me:");
        assertThat(x.length, is(3));
        assertThat(x[0], is(""));
        assertThat(x[1], is("me"));
        assertThat(x[2], is(""));
    }

    @Test
    public void testSplitFQDI_nsNameVersion() {
        String[] x = Separator.splitFQDI("test::me:too");
        assertThat(x.length, is(3));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("too"));
    }

    @Test
    public void testSplitFQDI_emptyNamespaceVersion() {
        String[] x = Separator.splitFQDI("::me:too");
        assertThat(x.length, is(3));
        assertThat(x[0], is(""));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("too"));
    }

    @Test
    public void testSplitFQDI_nameAndEmptyVersion() {
        String[] x = Separator.splitFQDI("me:");
        assertThat(x.length, is(3));
        assertThat(x[0], nullValue());
        assertThat(x[1], is("me"));
        assertThat(x[2], is(""));
    }

    @Test
    public void testSplitFQDI_nameAndVersionNoNamespace() {
        String[] x = Separator.splitFQDI("me:too");
        assertThat(x.length, is(3));
        assertThat(x[0], nullValue());
        assertThat(x[1], is("me"));
        assertThat(x[2], is("too"));
    }

    @Test
    public void testSplitFQDI_nsAndNameNoVersion() {
        String[] x = Separator.splitFQDI("test::me");
        assertThat(x.length, is(3));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], nullValue());
    }

    @Test
    public void testSplitFQDI_nsNameAndEmptyVersion() {
        String[] x = Separator.splitFQDI("test::me:");
        assertThat(x.length, is(3));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], is(""));
    }

    @Test
    public void testSplitFQDI_emptyNamespaceVersionTrailingColonThrows() {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI("::me::"));
    }

    @Test
    public void testSplitFQDI_versionWithTrailingColonThrows() {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI("::me:too:"));
    }

    @Test
    public void testSplitFQDI_tripleColonWithNamespaceThrows() {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI("test:::"));
    }

    @Test
    public void testSplitFQDI_tripleColonWithVersionThrows() {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI(":::too"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "me",
                "::me",
                "::me:",
                "me:",
                "me:too",
                "test::me",
                "test::me:",
                "::me:too",
                "test::me:too",
            })
    public void testSeparator2HarnessValidSplitFQDIInputsDoNotThrow(String input) {
        assertDoesNotThrow(() -> Separator.splitFQDI(input));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                ":::",
                "test:::",
                ":::too",
                "::me::",
                "::me:too:",
            })
    public void testSeparator2HarnessInvalidSplitFQDIInputsThrow(String input) {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI(input));
    }

    @Test
    public void testSplitFQDIShowFormattingFromDemoHarness() {
        String printed = renderSplitFQDIShow("test::me:too");
        assertThat(printed, containsString("\"test::me:too\""));
        assertThat(printed, containsString("0:\"test\""));
        assertThat(printed, containsString("1:\"me\""));
        assertThat(printed, containsString("2:\"too\""));
    }

    private String renderSplitShow(String what) {
        StringBuilder sb = new StringBuilder();
        sb.append(what).append(" => [");
        try {
            String[] x = Separator.split(what);
            for (int i = 0; i < x.length; ++i) {
                sb.append(i).append(':');
                sb.append(x[i] == null ? "null" : "\"" + x[i] + "\"");
                if (i < x.length - 1) {
                    sb.append(", ");
                }
            }
        } catch (IllegalArgumentException iae) {
            sb.append(iae.getMessage());
        }
        sb.append(']');
        return sb.toString();
    }

    private String renderSplitFQDIShow(String what) {
        StringBuilder sb = new StringBuilder();
        appendAligned(sb, what == null ? "(null)" : ("\"" + what + "\""), 16);
        sb.append(" => [");
        try {
            String[] x = Separator.splitFQDI(what);
            for (int i = 0; i < x.length; ++i) {
                sb.append(i).append(':');
                appendAligned(sb, x[i] == null ? "(null)" : ("\"" + x[i] + "\""), 8);
                if (i < x.length - 1) {
                    sb.append(", ");
                }
            }
        } catch (IllegalArgumentException iae) {
            sb.append(iae.getMessage());
        }
        sb.append(']');
        return sb.toString();
    }

    private void appendAligned(StringBuilder sb, String value, int len) {
        sb.append(value);
        for (int i = value.length(); i < len; ++i) {
            sb.append(' ');
        }
    }
}
