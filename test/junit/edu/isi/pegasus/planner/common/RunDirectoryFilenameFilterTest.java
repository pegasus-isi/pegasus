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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
        assertThat(mFilter.accept(mDummyDir, "run0001"), is(true));
    }

    @Test
    public void testAcceptsRunWithMaxDigits() {
        assertThat(mFilter.accept(mDummyDir, "run9999"), is(true));
    }

    @Test
    public void testRejectsDirectoryWithoutPrefix() {
        assertThat(mFilter.accept(mDummyDir, "0001"), is(false));
    }

    @Test
    public void testRejectsDirectoryWithWrongPrefix() {
        assertThat(mFilter.accept(mDummyDir, "dir0001"), is(false));
    }

    @Test
    public void testRejectsRunWithTooFewDigits() {
        assertThat(mFilter.accept(mDummyDir, "run001"), is(false));
    }

    @Test
    public void testRejectsRunWithTooManyDigits() {
        assertThat(mFilter.accept(mDummyDir, "run00001"), is(false));
    }

    @Test
    public void testRejectsRunWithLetters() {
        assertThat(mFilter.accept(mDummyDir, "run000a"), is(false));
    }

    @Test
    public void testSubmitDirectoryPrefixConstant() {
        assertThat(RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX, is("run"));
    }

    @Test
    public void testAcceptIgnoresDirectoryArgument() {
        assertThat(mFilter.accept(null, "run1234"), is(true));
    }

    @Test
    public void testRejectsUppercasePrefix() {
        assertThat(mFilter.accept(mDummyDir, "RUN1234"), is(false));
    }

    @Test
    public void testRejectsFilenameWithTrailingCharacters() {
        assertThat(mFilter.accept(mDummyDir, "run1234.tmp"), is(false));
    }

    @Test
    public void testRejectsFilenameContainingPathSeparator() {
        assertThat(mFilter.accept(mDummyDir, "run1234/subdir"), is(false));
    }
}
