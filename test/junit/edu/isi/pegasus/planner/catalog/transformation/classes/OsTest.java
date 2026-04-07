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
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the Os enumerated type class. */
public class OsTest {

    @Test
    public void testPredefinedConstantsHaveCorrectValues() {
        assertThat(Os.LINUX.getValue(), equalTo("LINUX"));
        assertThat(Os.SUNOS.getValue(), equalTo("SUNOS"));
        assertThat(Os.AIX.getValue(), equalTo("AIX"));
        assertThat(Os.WINDOWS.getValue(), equalTo("WINDOWS"));
    }

    @Test
    public void testFromValueReturnsKnownConstant() {
        assertThat(Os.fromValue("LINUX"), is(Os.LINUX));
        assertThat(Os.fromValue("SUNOS"), is(Os.SUNOS));
        assertThat(Os.fromValue("AIX"), is(Os.AIX));
        assertThat(Os.fromValue("WINDOWS"), is(Os.WINDOWS));
    }

    @Test
    public void testFromValueIsCaseInsensitive() {
        assertThat(Os.fromValue("linux"), is(Os.LINUX));
        assertThat(Os.fromValue("Windows"), is(Os.WINDOWS));
        assertThat(Os.fromValue("aix"), is(Os.AIX));
    }

    @Test
    public void testFromStringDelegatesToFromValue() {
        assertThat(Os.fromString("LINUX"), is(Os.LINUX));
        assertThat(Os.fromString("sunos"), is(Os.SUNOS));
    }

    @Test
    public void testFromValueThrowsForUnknownOs() {
        assertThrows(IllegalStateException.class, () -> Os.fromValue("UNKNOWN_OS"));
    }

    @Test
    public void testToStringReturnsValue() {
        assertThat(Os.LINUX.toString(), equalTo("LINUX"));
        assertThat(Os.WINDOWS.toString(), equalTo("WINDOWS"));
    }

    @Test
    public void testEqualsSameInstance() {
        assertThat(Os.LINUX.equals(Os.LINUX), is(true));
    }

    @Test
    public void testEqualsDifferentInstances() {
        assertThat(Os.LINUX.equals(Os.WINDOWS), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForNull() {
        assertThat(Os.LINUX.equals(null), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForDifferentObjectType() {
        assertThat(Os.LINUX.equals("LINUX"), is(false));
    }

    @Test
    public void testHashCodeConsistentWithToString() {
        assertThat(Os.LINUX.hashCode(), is(Os.LINUX.toString().hashCode()));
    }

    @Test
    public void testErrorMessageIsNonNull() {
        assertThat(Os.err, is(org.hamcrest.Matchers.not(org.hamcrest.Matchers.emptyString())));
    }

    @Test
    public void testErrorMessageMentionsSupportedOperatingSystems() {
        assertThat(Os.err, containsString("LINUX"));
        assertThat(Os.err, containsString("SUNOS"));
        assertThat(Os.err, containsString("AIX"));
        assertThat(Os.err, containsString("WINDOWS"));
    }

    @Test
    public void testFromValueReturnsCanonicalSingletonInstance() {
        Os fromValue = Os.fromValue("windows");
        Os fromString = Os.fromString("WINDOWS");

        assertThat(fromValue, is(Os.WINDOWS));
        assertThat(fromString, is(fromValue));
    }
}
