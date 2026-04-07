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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;

/** Tests for the TCType enum. */
public class TCTypeTest {

    @Test
    public void testEnumHasThreeValues() {
        assertThat(TCType.values().length, is(3));
    }

    @Test
    public void testStaticBinaryExists() {
        assertThat(TCType.STATIC_BINARY, is(notNullValue()));
    }

    @Test
    public void testInstalledExists() {
        assertThat(TCType.INSTALLED, is(notNullValue()));
    }

    @Test
    public void testStageableExists() {
        assertThat(TCType.STAGEABLE, is(notNullValue()));
    }

    @Test
    public void testValueOfInstalledReturnsCorrectConstant() {
        assertThat(TCType.valueOf("INSTALLED"), is(TCType.INSTALLED));
    }

    @Test
    public void testValueOfStageableReturnsCorrectConstant() {
        assertThat(TCType.valueOf("STAGEABLE"), is(TCType.STAGEABLE));
    }

    @Test
    public void testValueOfStaticBinaryReturnsCorrectConstant() {
        assertThat(TCType.valueOf("STATIC_BINARY"), is(TCType.STATIC_BINARY));
    }

    @Test
    public void testToStringMatchesName() {
        assertThat(TCType.INSTALLED.name(), is("INSTALLED"));
        assertThat(TCType.STAGEABLE.name(), is("STAGEABLE"));
        assertThat(TCType.STATIC_BINARY.name(), is("STATIC_BINARY"));
    }

    @Test
    public void testOrdinalOrdering() {
        assertThat(TCType.STATIC_BINARY.ordinal() < TCType.INSTALLED.ordinal(), is(true));
        assertThat(TCType.INSTALLED.ordinal() < TCType.STAGEABLE.ordinal(), is(true));
    }

    @Test
    public void testValueOfInvalidThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> TCType.valueOf("NONEXISTENT"));
    }

    @Test
    public void testTypeIsEnum() {
        assertThat(TCType.class.isEnum(), is(true));
    }

    @Test
    public void testValuesAreDeclaredInExpectedOrder() {
        assertThat(
                Arrays.asList(TCType.values()),
                is(Arrays.asList(TCType.STATIC_BINARY, TCType.INSTALLED, TCType.STAGEABLE)));
    }

    @Test
    public void testValueOfIsCaseSensitive() {
        assertThrows(IllegalArgumentException.class, () -> TCType.valueOf("installed"));
    }

    @Test
    public void testEnumSetContainsAllDeclaredConstants() {
        assertThat(
                EnumSet.allOf(TCType.class),
                is(EnumSet.of(TCType.STATIC_BINARY, TCType.INSTALLED, TCType.STAGEABLE)));
    }
}
