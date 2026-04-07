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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class MetaTest {

    @Test
    public void testConstructorDefaultsToClosedAndWritable() throws Exception {
        Meta meta = new Meta();

        assertThat(meta.isClosed(), is(true));
        assertThat((Boolean) ReflectionTestUtils.getField(meta, "mQuote"), is(false));
        assertThat((Boolean) ReflectionTestUtils.getField(meta, "m_readonly"), is(false));
    }

    @Test
    public void testConnectStringRejectsNullFilename() {
        Meta meta = new Meta();

        assertThat(meta.connect((String) null), is(false));
        assertThat(meta.isClosed(), is(true));
    }

    @Test
    public void testConnectPropertiesParsesQuoteAndReadonlyEvenWithoutFile() throws Exception {
        Meta meta = new Meta();
        Properties props = new Properties();
        props.setProperty("quote", "true");
        props.setProperty(YAML.READ_ONLY_KEY, "true");

        assertThat(meta.connect(props), is(false));
        assertThat((Boolean) ReflectionTestUtils.getField(meta, "mQuote"), is(true));
        assertThat((Boolean) ReflectionTestUtils.getField(meta, "m_readonly"), is(true));
    }

    @Test
    public void testLookupAndLookupNoAttributesUseInjectedInMemoryMap() throws Exception {
        Meta meta = new Meta();
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("file:///tmp/data.txt", "local");
        ReplicaLocation location =
                new ReplicaLocation("lfn", java.util.Collections.singletonList(entry));
        location.addMetadata("checksum.type", "sha256");

        Map<String, ReplicaLocation> map = new LinkedHashMap<String, ReplicaLocation>();
        map.put("lfn", location);
        ReflectionTestUtils.setField(meta, "mLFN", map);

        assertThat(meta.lookup("lfn", "local"), equalTo("file:///tmp/data.txt"));

        Collection<ReplicaCatalogEntry> result = meta.lookup("lfn");
        assertThat(result.size(), is(1));
        ReplicaCatalogEntry lookedUp = result.iterator().next();
        assertThat(lookedUp.getAttribute("checksum.type"), is("sha256"));

        Set<String> pfns = meta.lookupNoAttributes("lfn");
        assertThat(pfns, is(java.util.Collections.singleton("file:///tmp/data.txt")));
    }

    @Test
    public void testCloseClearsReadonlyCatalog() throws Exception {
        Meta meta = new Meta();
        ReflectionTestUtils.setField(meta, "mFilename", "/tmp/meta.yml");
        ReflectionTestUtils.setField(meta, "mLFN", new LinkedHashMap<String, ReplicaLocation>());
        ReflectionTestUtils.setField(meta, "m_readonly", true);

        meta.close();

        assertThat(meta.isClosed(), is(true));
        assertThat(ReflectionTestUtils.getField(meta, "mFilename"), nullValue());
    }
}
