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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the Patterns utility class. */
public class PatternsTest {

    @Test
    public void testIsVersionValidSimple() {
        assertTrue(Patterns.isVersionValid("1"), "Single integer should be a valid version");
    }

    @Test
    public void testIsVersionValidTwoPart() {
        assertTrue(Patterns.isVersionValid("1.0"), "Two-part version should be valid");
    }

    @Test
    public void testIsVersionValidThreePart() {
        assertTrue(Patterns.isVersionValid("1.2.3"), "Three-part version should be valid");
    }

    @Test
    public void testIsVersionValidRejectsAlpha() {
        assertFalse(Patterns.isVersionValid("1.0a"), "Version with alpha suffix should be invalid");
    }

    @Test
    public void testIsVersionValidRejectsEmpty() {
        assertFalse(Patterns.isVersionValid(""), "Empty string should be invalid version");
    }

    @Test
    public void testIsVersionValidRejectsDash() {
        assertFalse(
                Patterns.isVersionValid("1-0"), "Version with dash separator should be invalid");
    }

    @Test
    public void testIsVersionValidRejectsFourPart() {
        assertFalse(Patterns.isVersionValid("1.2.3.4"), "Four-part version should be invalid");
    }

    @Test
    public void testIsNodeIdValidSimple() {
        assertTrue(Patterns.isNodeIdValid("job1"), "Simple alphanumeric node ID should be valid");
    }

    @Test
    public void testIsNodeIdValidWithUnderscore() {
        assertTrue(Patterns.isNodeIdValid("my_job"), "Node ID with underscore should be valid");
    }

    @Test
    public void testIsNodeIdValidWithDash() {
        assertTrue(Patterns.isNodeIdValid("my-job"), "Node ID with dash should be valid");
    }

    @Test
    public void testIsNodeIdValidStartsWithLetter() {
        assertTrue(
                Patterns.isNodeIdValid("A_job_1"), "Node ID starting with letter should be valid");
    }

    @Test
    public void testIsNodeIdValidRejectsStartWithDash() {
        assertFalse(Patterns.isNodeIdValid("-job"), "Node ID starting with dash should be invalid");
    }

    @Test
    public void testIsNodeIdValidRejectsEmpty() {
        assertFalse(Patterns.isNodeIdValid(""), "Empty string should be invalid node ID");
    }

    @Test
    public void testIsNodeIdValidRejectsSpace() {
        assertFalse(Patterns.isNodeIdValid("my job"), "Node ID with space should be invalid");
    }

    @Test
    public void testIsNodeIdValidSingleChar() {
        assertTrue(Patterns.isNodeIdValid("A"), "Single character node ID should be valid");
    }
}
