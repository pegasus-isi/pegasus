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
package edu.isi.pegasus.planner.dax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Tests for the Patterns utility class. */
public class PatternsTest {

    @Test
    public void testIsVersionValidSimple() {
        assertThat(Patterns.isVersionValid("1"), is(true));
    }

    @Test
    public void testIsVersionValidTwoPart() {
        assertThat(Patterns.isVersionValid("1.0"), is(true));
    }

    @Test
    public void testIsVersionValidThreePart() {
        assertThat(Patterns.isVersionValid("1.2.3"), is(true));
    }

    @Test
    public void testIsVersionValidRejectsAlpha() {
        assertThat(Patterns.isVersionValid("1.0a"), is(false));
    }

    @Test
    public void testIsVersionValidRejectsEmpty() {
        assertThat(Patterns.isVersionValid(""), is(false));
    }

    @Test
    public void testIsVersionValidRejectsDash() {
        assertThat(Patterns.isVersionValid("1-0"), is(false));
    }

    @Test
    public void testIsVersionValidRejectsFourPart() {
        assertThat(Patterns.isVersionValid("1.2.3.4"), is(false));
    }

    @Test
    public void testIsNodeIdValidSimple() {
        assertThat(Patterns.isNodeIdValid("job1"), is(true));
    }

    @Test
    public void testIsNodeIdValidWithUnderscore() {
        assertThat(Patterns.isNodeIdValid("my_job"), is(true));
    }

    @Test
    public void testIsNodeIdValidWithDash() {
        assertThat(Patterns.isNodeIdValid("my-job"), is(true));
    }

    @Test
    public void testIsNodeIdValidStartsWithLetter() {
        assertThat(Patterns.isNodeIdValid("A_job_1"), is(true));
    }

    @Test
    public void testIsNodeIdValidRejectsStartWithDash() {
        assertThat(Patterns.isNodeIdValid("-job"), is(false));
    }

    @Test
    public void testIsNodeIdValidRejectsEmpty() {
        assertThat(Patterns.isNodeIdValid(""), is(false));
    }

    @Test
    public void testIsNodeIdValidRejectsSpace() {
        assertThat(Patterns.isNodeIdValid("my job"), is(false));
    }

    @Test
    public void testIsNodeIdValidSingleChar() {
        assertThat(Patterns.isNodeIdValid("A"), is(true));
    }

    @Test
    public void testIsValidUsesProvidedPatternDirectly() {
        Pattern lowerCasePattern = Pattern.compile("^[a-z]{3}$");

        assertThat(Patterns.isValid(lowerCasePattern, "abc"), is(true));
        assertThat(Patterns.isValid(lowerCasePattern, "ab1"), is(false));
    }

    @Test
    public void testIsNodeIdValidAllowsLeadingDigit() {
        assertThat(Patterns.isNodeIdValid("1_job"), is(true));
    }

    @Test
    public void testIsNodeIdValidRejectsNonAsciiPunctuation() {
        assertThat(Patterns.isNodeIdValid("job.name"), is(false));
    }

    @Test
    public void testIsVersionValidNullCurrentlyThrowsNullPointerException() {
        assertThrows(
                NullPointerException.class,
                () -> Patterns.isVersionValid(null),
                "Null version input currently throws via Pattern.matcher");
    }
}
