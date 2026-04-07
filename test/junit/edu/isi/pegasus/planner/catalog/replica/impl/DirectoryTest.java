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
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DirectoryTest {

    @Test
    public void testConstructorDefaults() {
        Directory directory = new Directory();

        assertThat(directory.isClosed(), is(true));
        assertThat(directory.mSiteHandle, is(Directory.DEFAULT_SITE_HANDLE));
        assertThat(directory.mURLPrefix, is(Directory.DEFAULT_URL_PREFIX));
    }

    @Test
    public void testConnectStringBuildsDeepLfnsByDefault() throws IOException {
        Path root = Files.createTempDirectory("directory-rc-deep");
        Path nested = Files.createDirectories(root.resolve("input"));
        Path file = Files.write(nested.resolve("f.a"), "data".getBytes());

        Directory directory = new Directory();

        assertThat(directory.connect(root.toString()), is(true));
        assertThat(directory.isClosed(), is(false));
        assertThat(
                directory.lookup(
                        "input" + java.io.File.separator + "f.a", Directory.DEFAULT_SITE_HANDLE),
                is("file://" + file.toFile().getAbsolutePath()));
    }

    @Test
    public void testConnectPropertiesSupportsFlatLfnsAndOverridesSiteAndUrlPrefix()
            throws IOException {
        Path root = Files.createTempDirectory("directory-rc-flat");
        Path nested = Files.createDirectories(root.resolve("nested"));
        Path file = Files.write(nested.resolve("flat.txt"), "data".getBytes());

        Properties props = new Properties();
        props.setProperty(Directory.DIRECTORY_PROPERTY_KEY, root.toString());
        props.setProperty(Directory.FLAT_LFN_PROPERTY_KEY, "true");
        props.setProperty(Directory.SITE_PROPERTY_KEY, "condorpool");
        props.setProperty(Directory.URL_PRFIX_PROPERTY_KEY, "gsiftp://example");

        Directory directory = new Directory();

        assertThat(directory.connect(props), is(true));
        assertThat(
                directory.lookup("flat.txt", "condorpool"),
                is("gsiftp://example" + file.toFile().getAbsolutePath()));

        Collection entries = directory.lookup("flat.txt");
        assertThat(entries.size(), is(1));
        ReplicaCatalogEntry entry = (ReplicaCatalogEntry) entries.iterator().next();
        assertThat(entry.getResourceHandle(), is("condorpool"));
    }

    @Test
    public void testConnectReturnsFalseForMissingDirectoryProperty() {
        Directory directory = new Directory();

        assertThat(directory.connect(new Properties()), is(false));
        assertThat(directory.isClosed(), is(true));
    }

    @Test
    public void testCloseClearsCatalogAndMarksItClosed() throws IOException {
        Path root = Files.createTempDirectory("directory-rc-close");
        Files.write(root.resolve("a.txt"), "data".getBytes());

        Directory directory = new Directory();
        assertThat(directory.connect(root.toString()), is(true));

        directory.close();

        assertThat(directory.isClosed(), is(true));
    }
}
