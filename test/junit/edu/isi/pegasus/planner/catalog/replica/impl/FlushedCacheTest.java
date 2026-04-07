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
package edu.isi.pegasus.planner.catalog.replica.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.Escape;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class FlushedCacheTest {

    @Test
    public void testConnectAndCloseUpdateClosedState() throws IOException {
        Path file = Files.createTempFile("flushed-cache", ".rc");
        FlushedCache cache = new FlushedCache();

        assertThat(cache.isClosed(), is(true));
        assertThat(cache.connect(file.toString()), is(true));
        assertThat(cache.isClosed(), is(false));

        cache.close();

        assertThat(cache.isClosed(), is(true));
    }

    @Test
    public void testConnectPropertiesRequiresFileProperty() {
        FlushedCache cache = new FlushedCache();

        assertThat(cache.connect(new Properties()), is(false));
    }

    @Test
    public void testQuoteOnlyEscapesWhenNecessary() {
        FlushedCache cache = new FlushedCache();
        Escape escape = new Escape("\"\\", '\\');

        assertThat(cache.quote(escape, "simple"), equalTo("simple"));
        assertThat(cache.quote(escape, "needs space"), equalTo("\"needs space\""));
        assertThat(cache.quote(escape, "a=b"), equalTo("\"a=b\""));
    }

    @Test
    public void testInsertWritesSimpleFileStyleEntry() throws IOException {
        Path file = Files.createTempFile("flushed-cache-insert", ".rc");
        FlushedCache cache = new FlushedCache();
        assertThat(cache.connect(file.toString()), is(true));

        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("file:///tmp/data.txt", "local");
        entry.addAttribute("site", "local");

        assertThat(cache.insert("lfn", entry), is(1));
        cache.close();

        String contents = Files.readString(file, StandardCharsets.UTF_8);
        assertThat(contents, containsString("lfn file:///tmp/data.txt"));
        assertThat(contents, containsString("site=\"local\""));
    }

    @Test
    public void testLookupMethodsRemainUnsupported() {
        FlushedCache cache = new FlushedCache();

        UnsupportedOperationException exception =
                assertThrows(UnsupportedOperationException.class, () -> cache.lookup("lfn"));

        assertThat(exception.getMessage(), is("Method not implemented"));
    }
}
