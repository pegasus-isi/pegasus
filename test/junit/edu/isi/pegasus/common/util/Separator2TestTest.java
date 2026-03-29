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

/** Additional Separator.splitFQDI() tests covering the demo cases from Separator2Test in src/ */
public class Separator2TestTest {

    // -----------------------------------------------------------------------
    // Cases from Separator2Test.main() — already tested
    // -----------------------------------------------------------------------

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
        // "::me" — explicit empty namespace, no version
        String[] x = Separator.splitFQDI("::me");
        assertThat(x.length, is(3));
        assertThat(x[0], is(""));
        assertThat(x[1], is("me"));
        assertThat(x[2], nullValue());
    }

    @Test
    public void testSplitFQDI_emptyVersion() {
        // "::me:" — explicit empty namespace and explicit empty version
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
    public void testSplitFQDI_illegalInput() {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI(":::"));
    }

    @Test
    public void testSplitFQDI_emptyNamespaceVersion() {
        String[] x = Separator.splitFQDI("::me:too");
        assertThat(x.length, is(3));
        assertThat(x[0], is(""));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("too"));
    }

    // -----------------------------------------------------------------------
    // Remaining cases from Separator2Test.main() — not previously covered
    // -----------------------------------------------------------------------

    @Test
    public void testSplitFQDI_nameAndEmptyVersion() {
        // "me:" — no namespace (null), name="me", explicit empty version
        String[] x = Separator.splitFQDI("me:");
        assertThat(x.length, is(3));
        assertThat(x[0], nullValue());
        assertThat(x[1], is("me"));
        assertThat(x[2], is(""));
    }

    @Test
    public void testSplitFQDI_nameAndVersion_noNamespace() {
        // "me:too" — no namespace (null), name="me", version="too"
        String[] x = Separator.splitFQDI("me:too");
        assertThat(x.length, is(3));
        assertThat(x[0], nullValue());
        assertThat(x[1], is("me"));
        assertThat(x[2], is("too"));
    }

    @Test
    public void testSplitFQDI_nsAndName_noVersion() {
        // "test::me" — namespace="test", name="me", no version (null)
        String[] x = Separator.splitFQDI("test::me");
        assertThat(x.length, is(3));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], nullValue());
    }

    @Test
    public void testSplitFQDI_nsNameAndEmptyVersion() {
        // "test::me:" — namespace="test", name="me", explicit empty version
        String[] x = Separator.splitFQDI("test::me:");
        assertThat(x.length, is(3));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], is(""));
    }

    @Test
    public void testSplitFQDI_emptyNamespaceVersionTrailingColon_throws() {
        // "::me::" — double-colon after version → error state 9
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI("::me::"));
    }

    @Test
    public void testSplitFQDI_versionWithTrailingColon_throws() {
        // "::me:too:" — trailing colon after version → error state 9
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI("::me:too:"));
    }

    @Test
    public void testSplitFQDI_tripleColonWithNamespace_throws() {
        // "test:::" — empty name (result[1] empty) → IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI("test:::"));
    }

    @Test
    public void testSplitFQDI_tripleColonWithVersion_throws() {
        // ":::too" — empty name → IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI(":::too"));
    }

    // -----------------------------------------------------------------------
    // Additional edge cases
    // -----------------------------------------------------------------------

    @Test
    public void testSplitFQDI_nameWithDigits() {
        String[] x = Separator.splitFQDI("ns::job123:1.0");
        assertThat(x[0], is("ns"));
        assertThat(x[1], is("job123"));
        assertThat(x[2], is("1.0"));
    }

    @Test
    public void testSplitFQDI_versionWithDots() {
        String[] x = Separator.splitFQDI("condor::pegasus:4.9.2");
        assertThat(x[0], is("condor"));
        assertThat(x[1], is("pegasus"));
        assertThat(x[2], is("4.9.2"));
    }

    @Test
    public void testSplitFQDI_longNamespace() {
        String[] x = Separator.splitFQDI("edu.isi.pegasus::transformation:1.0");
        assertThat(x[0], is("edu.isi.pegasus"));
        assertThat(x[1], is("transformation"));
        assertThat(x[2], is("1.0"));
    }

    @Test
    public void testSplitFQDI_emptyVersionWithNullNamespace() {
        // "name:" — no namespace (null), name="name", empty version
        String[] x = Separator.splitFQDI("name:");
        assertThat(x[0], nullValue());
        assertThat(x[1], is("name"));
        assertThat(x[2], is(""));
    }

    // -----------------------------------------------------------------------
    // Parameterised: all error inputs from Separator2Test.main() must throw
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            strings = {
                ":::", // three colons — always illegal
                "test:::", // empty name with trailing colon
                ":::too", // empty name
                "::me::", // extra colon after version slot
                "::me:too:", // trailing colon after version
            })
    public void testSplitFQDI_invalidInputs_allThrow(String input) {
        assertThrows(IllegalArgumentException.class, () -> Separator.splitFQDI(input));
    }

    // -----------------------------------------------------------------------
    // Parameterised: all valid inputs from Separator2Test.main() must not throw
    // -----------------------------------------------------------------------

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
    public void testSplitFQDI_validInputs_doNotThrow(String input) {
        assertDoesNotThrow(() -> Separator.splitFQDI(input));
    }
}
