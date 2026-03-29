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
package edu.isi.pegasus.planner.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for RunDirectoryFilenameFilter. */
public class RunDirectoryFilenameFilterTest {

    private RunDirectoryFilenameFilter mFilter;
    private File mDummyDir;

    @BeforeEach
    public void setUp() {
        mFilter = new RunDirectoryFilenameFilter();
        mDummyDir = new File("/tmp");
    }

    @Test
    public void testAcceptsValidRunDirectory() {
        assertTrue(mFilter.accept(mDummyDir, "run0001"), "Filter should accept 'run0001'");
    }

    @Test
    public void testAcceptsRunWithMaxDigits() {
        assertTrue(mFilter.accept(mDummyDir, "run9999"), "Filter should accept 'run9999'");
    }

    @Test
    public void testRejectsDirectoryWithoutPrefix() {
        assertFalse(
                mFilter.accept(mDummyDir, "0001"), "Filter should reject '0001' (no 'run' prefix)");
    }

    @Test
    public void testRejectsDirectoryWithWrongPrefix() {
        assertFalse(mFilter.accept(mDummyDir, "dir0001"), "Filter should reject 'dir0001'");
    }

    @Test
    public void testRejectsRunWithTooFewDigits() {
        assertFalse(
                mFilter.accept(mDummyDir, "run001"),
                "Filter should reject 'run001' (only 3 digits)");
    }

    @Test
    public void testRejectsRunWithTooManyDigits() {
        assertFalse(
                mFilter.accept(mDummyDir, "run00001"),
                "Filter should reject 'run00001' (5 digits)");
    }

    @Test
    public void testRejectsRunWithLetters() {
        assertFalse(mFilter.accept(mDummyDir, "run000a"), "Filter should reject 'run000a'");
    }

    @Test
    public void testSubmitDirectoryPrefixConstant() {
        assertEquals(
                "run",
                RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX,
                "Submit directory prefix should be 'run'");
    }
}
