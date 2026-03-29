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
package edu.isi.pegasus.planner.refiner.cleanup.constraint;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link Utilities}. */
public class UtilitiesTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testGetFileSizeWhenSizeSet() {
        PegasusFile pf = new PegasusFile("myfile.txt");
        pf.setSize(2048.0);
        long size = Utilities.getFileSize(pf);
        assertEquals(2048L, size);
    }

    @Test
    public void testGetFileSizeDefaultWhenNoSize() {
        PegasusFile pf = new PegasusFile("unknown.txt");
        // size is -1 by default so falls back to DEFAULT_FILE_SIZE (10 MB)
        long size = Utilities.getFileSize(pf);
        assertEquals(10485760L, size);
    }

    @Test
    public void testGetFileSizeWhenSizeIsZero() {
        PegasusFile pf = new PegasusFile("empty.txt");
        pf.setSize(0.0);
        long size = Utilities.getFileSize(pf);
        assertEquals(0L, size);
    }

    @Test
    public void testGetFileSizeLargeFile() {
        PegasusFile pf = new PegasusFile("big.bin");
        pf.setSize(1073741824.0); // 1 GB
        long size = Utilities.getFileSize(pf);
        assertEquals(1073741824L, size);
    }

    @Test
    public void testSizesMapInitiallyNull() {
        // Accessing getFileSize with sizes=null (or undefined files) should use fallback
        PegasusFile pf = new PegasusFile("not-in-csv.dat");
        long size = Utilities.getFileSize(pf);
        // Should be either the file's own size or the DEFAULT_FILE_SIZE
        assertTrue(size > 0);
    }
}
