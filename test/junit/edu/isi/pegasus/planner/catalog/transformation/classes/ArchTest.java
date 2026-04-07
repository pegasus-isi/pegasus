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

/** Tests for the Arch enumerated type class. */
public class ArchTest {

    @Test
    public void testPredefinedConstantsHaveCorrectValues() {
        assertThat(Arch.INTEL32.getValue(), equalTo("INTEL32"));
        assertThat(Arch.INTEL64.getValue(), equalTo("INTEL64"));
        assertThat(Arch.AMD64.getValue(), equalTo("AMD64"));
        assertThat(Arch.SPARCV7.getValue(), equalTo("SPARCV7"));
        assertThat(Arch.SPARCV9.getValue(), equalTo("SPARCV9"));
    }

    @Test
    public void testFromValueReturnsKnownConstant() {
        assertThat(Arch.fromValue("INTEL32"), is(Arch.INTEL32));
        assertThat(Arch.fromValue("INTEL64"), is(Arch.INTEL64));
        assertThat(Arch.fromValue("AMD64"), is(Arch.AMD64));
        assertThat(Arch.fromValue("SPARCV7"), is(Arch.SPARCV7));
        assertThat(Arch.fromValue("SPARCV9"), is(Arch.SPARCV9));
    }

    @Test
    public void testFromValueIsCaseInsensitive() {
        assertThat(Arch.fromValue("intel32"), is(Arch.INTEL32));
        assertThat(Arch.fromValue("amd64"), is(Arch.AMD64));
        assertThat(Arch.fromValue("InTeL64"), is(Arch.INTEL64));
    }

    @Test
    public void testFromStringDelegatesToFromValue() {
        assertThat(Arch.fromString("INTEL32"), is(Arch.INTEL32));
        assertThat(Arch.fromString("sparcv9"), is(Arch.SPARCV9));
    }

    @Test
    public void testFromValueThrowsForUnknownArchitecture() {
        assertThrows(IllegalStateException.class, () -> Arch.fromValue("UNKNOWN_ARCH"));
    }

    @Test
    public void testToStringReturnsValue() {
        assertThat(Arch.INTEL32.toString(), equalTo("INTEL32"));
        assertThat(Arch.AMD64.toString(), equalTo("AMD64"));
    }

    @Test
    public void testEqualsSameInstance() {
        assertThat(Arch.INTEL32.equals(Arch.INTEL32), is(true));
    }

    @Test
    public void testEqualsDifferentInstances() {
        assertThat(Arch.INTEL32.equals(Arch.INTEL64), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForNull() {
        assertThat(Arch.INTEL32.equals(null), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForDifferentObjectType() {
        assertThat(Arch.INTEL32.equals("INTEL32"), is(false));
    }

    @Test
    public void testHashCodeConsistentWithToString() {
        assertThat(Arch.INTEL32.hashCode(), is(Arch.INTEL32.toString().hashCode()));
    }

    @Test
    public void testErrorMessageIsNonNull() {
        assertThat(Arch.err, is(org.hamcrest.Matchers.not(org.hamcrest.Matchers.emptyString())));
    }

    @Test
    public void testErrorMessageMentionsSupportedArchitectures() {
        assertThat(Arch.err, containsString("INTEL32"));
        assertThat(Arch.err, containsString("INTEL64"));
        assertThat(Arch.err, containsString("AMD64"));
        assertThat(Arch.err, containsString("SPARCV7"));
        assertThat(Arch.err, containsString("SPARCV9"));
    }

    @Test
    public void testFromValueReturnsCanonicalSingletonInstance() {
        Arch fromValue = Arch.fromValue("intel64");
        Arch fromString = Arch.fromString("INTEL64");

        assertThat(fromValue, is(Arch.INTEL64));
        assertThat(fromString, is(fromValue));
    }
}
