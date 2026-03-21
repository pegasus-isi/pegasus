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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** @author vahi */
public class UnitConverterTest {

    public UnitConverterTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @ParameterizedTest
    @CsvSource(
            value = {
                "1 K, 1",
                "1 KB, 1",
                "1024 KB, 1",
                "1, 1",
                "1MB, 1",
                "1024 M, 1024",
                "1G, 1024",
                "1 GB, 1024",
                "1gb, 1024",
                "1TB, 1048576",
                "1PB, 1073741824",
                "test $(memory), -1"
            },
            nullValues = {"null"})
    public void testToMB(String value, long expected) {
        assertEquals(expected, UnitConverter.toMB(value));
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "1 K, 1024", // because of ceiling
                "1 KB, 1024", // because of ceiling
                "1024 KB, 1024",
                "1, 1024",
                "1MB, 1024",
                "1024 M, 1048576",
                "1G, 1048576",
                "1 GB, 1048576",
                "1gb, 1048576",
                "1TB, 1073741824",
                "1PB, 1099511627776",
                "test $(memory), -1"
            },
            nullValues = {"null"})
    public void testToKB(String value, long expected) {
        assertEquals(expected, UnitConverter.toKB(value));
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
