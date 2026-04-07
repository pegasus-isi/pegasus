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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

/** Unit tests for VariableExpansionReader. */
public class VariableExpansionReaderTest {

    private static final class TrackingReader extends StringReader {
        private boolean mClosed;

        TrackingReader(String s) {
            super(s);
        }

        @Override
        public void close() {
            mClosed = true;
            super.close();
        }

        boolean isClosed() {
            return mClosed;
        }
    }

    @Test
    public void testReadSimpleLineWithoutVariables() throws IOException {
        String content = "hello world";
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(content));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        br.close();
        assertThat(line, is("hello world"));
    }

    @Test
    public void testReadEmptyContent() throws IOException {
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(""));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        br.close();
        // readLine() returns null at EOF (empty input has no lines)
        assertThat(line, nullValue());
    }

    @Test
    public void testReadMultipleLines() throws IOException {
        String content = "line1\nline2\nline3";
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(content));
        BufferedReader br = new BufferedReader(reader);
        assertThat(br.readLine(), is("line1"));
        assertThat(br.readLine(), is("line2"));
        assertThat(br.readLine(), is("line3"));
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
        assertThat(result, is(-1));
    }

    @Test
    @SetSystemProperty(key = "test.peg.var.expansion", value = "expanded_value")
    public void testReadWithSystemPropertyVariable() {
        // VariableExpansionReader pre-reads its input in the constructor.
        // VariableExpander throws RuntimeException for variables not registered via
        // PegasusProperties
        // (system properties alone are not used). The exception is thrown during construction.
        String propName = "test.peg.var.expansion";
        try {
            String content = "${" + propName + "}";
            assertThrows(
                    RuntimeException.class,
                    () -> new VariableExpansionReader(new StringReader(content)),
                    "VariableExpansionReader constructor throws for unknown variables");
        } finally {
        }
    }

    @Test
    public void testReadContentWithSpecialCharacters() throws IOException {
        String content = "path=/usr/local/bin:special#chars";
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader(content));
        BufferedReader br = new BufferedReader(reader);
        String line = br.readLine();
        br.close();
        assertThat(line, is(content));
    }

    @Test
    public void testReadAddsNewlinesBackAcrossMultipleLines() throws IOException {
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader("a\nbb"));
        char[] buf = new char[4];

        int read = reader.read(buf, 0, buf.length);
        reader.close();

        assertThat(read, is(4));
        assertThat(new String(buf), is("a\nbb"));
    }

    @Test
    public void testReadUsesDestinationOffset() throws IOException {
        VariableExpansionReader reader = new VariableExpansionReader(new StringReader("xy"));
        char[] buf = new char[] {'_', '_', '_', '_', '_'};

        int read = reader.read(buf, 1, 3);
        reader.close();

        assertThat(read, is(3));
        assertThat(buf, is(new char[] {'_', 'x', 'y', '\n', '_'}));
    }

    @Test
    public void testCloseClosesUnderlyingReader() throws IOException {
        TrackingReader tracking = new TrackingReader("content");
        VariableExpansionReader reader = new VariableExpansionReader(tracking);

        reader.close();

        assertThat(tracking.isClosed(), is(true));
    }
}
