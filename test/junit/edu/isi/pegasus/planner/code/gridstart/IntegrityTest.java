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
package edu.isi.pegasus.planner.code.gridstart;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Integrity class. */
public class IntegrityTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testIntegrityCheckToolBasename() {
        assertEquals("pegasus-integrity", Integrity.PEGASUS_INTEGRITY_CHECK_TOOL_BASENAME);
    }

    @Test
    public void testStdinFileDescriptor() {
        assertEquals("stdin", Integrity.STDIN_FILE_DESCRIPTOR);
    }

    @Test
    public void testIntegrityClassIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Integrity.class.getModifiers()));
    }

    @Test
    public void testIntegrityClassIsNotInterface() {
        assertFalse(Integrity.class.isInterface());
    }

    @Test
    public void testIntegrityCanBeInstantiated() {
        Integrity integrity = new Integrity();
        assertNotNull(integrity);
    }

    @Test
    public void testIntegrityCheckToolBasenameNotNull() {
        assertNotNull(Integrity.PEGASUS_INTEGRITY_CHECK_TOOL_BASENAME);
    }
}
