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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import org.junit.jupiter.api.Test;

/** Unit tests for VariableExpansionReader. */
public class VariableExpansionReaderTest {

    @Test
    public void testReadSimpleLineWithoutVariables() throws IOException {
        String content = "hello world";
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(content));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        br.close();
        assertEquals("hello world", line, "Line without variables should pass through unchanged");
    }

    @Test
    public void testReadEmptyContent() throws IOException {
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(""));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        br.close();
        // readLine() returns null at EOF (empty input has no lines)
        assertNull(line, "Read from empty string should return null (EOF)");
    }

    @Test
    public void testReadMultipleLines() throws IOException {
        String content = "line1\nline2\nline3";
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(content));
        BufferedReader br = new BufferedReader(reader);
        assertEquals("line1", br.readLine(), "First line should be 'line1'");
        assertEquals("line2", br.readLine(), "Second line should be 'line2'");
        assertEquals("line3", br.readLine(), "Third line should be 'line3'");
        br.close();
    }

    @Test
    public void testReaderCanBeClosed() throws IOException {
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader("test"));
        assertDoesNotThrow(reader::close, "close() should not throw an exception");
    }

    @Test
    public void testReadReturnsNegativeOneAtEndOfStream() throws IOException {
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(""));
        char[] buf = new char[10];
        // Read beyond end: first read may consume the empty line+\n, second should be -1
        reader.read(buf, 0, 1); // consumes the \n from empty line
        int result = reader.read(buf, 0, 1);
        reader.close();
        assertEquals(-1, result, "Reading at end of stream should return -1");
    }

    @Test
    public void testReadWithSystemPropertyVariable() {
        // VariableExpansionReader pre-reads its input in the constructor.
        // VariableExpander throws RuntimeException for variables not registered via
        // PegasusProperties
        // (system properties alone are not used). The exception is thrown during construction.
        String propName = "test.peg.var.expansion";
        System.setProperty(propName, "expanded_value");
        try {
            String content = "${" + propName + "}";
            assertThrows(
                    RuntimeException.class,
                    () -> new VariableExpansionReader(new StringReader(content)),
                    "VariableExpansionReader constructor throws for unknown variables");
        } finally {
            System.clearProperty(propName);
        }
    }

    @Test
    public void testReadContentWithSpecialCharacters() throws IOException {
        String content = "path=/usr/local/bin:special#chars";
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(content));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        br.close();
        assertEquals(
                content, line, "Special characters in non-variable content should be preserved");
    }
}
