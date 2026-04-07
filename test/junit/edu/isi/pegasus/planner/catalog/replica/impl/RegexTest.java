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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.Escape;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class RegexTest {

    @Test
    public void testConstructorDefaults() throws Exception {
        Regex regex = new Regex();

        assertThat(regex.isClosed(), is(true));
        assertThat((Boolean) getField(regex, "m_quote"), is(false));
        assertThat((Boolean) getField(regex, "m_readonly"), is(false));
        assertThat((Boolean) getField(regex, "mDoVariableExpansion"), is(true));
    }

    @Test
    public void testConnectPropertiesParsesFlagsWithoutFile() throws Exception {
        Regex regex = new Regex();
        Properties props = new Properties();
        props.setProperty("quote", "true");
        props.setProperty(ReplicaCatalog.READ_ONLY_KEY, "true");
        props.setProperty(ReplicaCatalog.VARIABLE_EXPANSION_KEY, "false");

        assertThat(regex.connect(props), is(false));
        assertThat((Boolean) getField(regex, "m_quote"), is(true));
        assertThat((Boolean) getField(regex, "m_readonly"), is(true));
        assertThat((Boolean) getField(regex, "mDoVariableExpansion"), is(false));
    }

    @Test
    public void testParseStoresRegularEntryAndAttributes() throws Exception {
        Regex regex = new Regex();
        setField(
                regex,
                "m_lfn",
                new java.util.LinkedHashMap<String, Collection<ReplicaCatalogEntry>>());
        setField(
                regex,
                "m_lfn_regex",
                new java.util.LinkedHashMap<String, Collection<ReplicaCatalogEntry>>());
        setField(regex, "m_lfn_pattern", new java.util.LinkedHashMap<String, Pattern>());

        assertThat(regex.parse("lfn file:///tmp/data.txt pool=\"local\"", 1), is(true));

        Collection<ReplicaCatalogEntry> result = regex.lookup("lfn");
        assertThat(result.size(), is(1));
        ReplicaCatalogEntry entry = result.iterator().next();
        assertThat(entry.getPFN(), is("file:///tmp/data.txt"));
        assertThat(entry.getResourceHandle(), is("local"));
    }

    @Test
    public void testLookupExpandsRegexPfns() throws Exception {
        Regex regex = new Regex();
        setField(
                regex,
                "m_lfn",
                new java.util.LinkedHashMap<String, Collection<ReplicaCatalogEntry>>());
        setField(
                regex,
                "m_lfn_regex",
                new java.util.LinkedHashMap<String, Collection<ReplicaCatalogEntry>>());
        setField(regex, "m_lfn_pattern", new java.util.LinkedHashMap<String, Pattern>());

        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("/data/[1].txt", "local");
        entry.addAttribute(Regex.REGEX_KEY, "true");
        regex.insert("f.(.*)", entry);

        assertThat(regex.lookup("f.beta", "local"), is("/data/beta.txt"));
    }

    @Test
    public void testQuoteAndReadonlyCloseCurrentBehavior() throws Exception {
        Regex regex = new Regex();
        Escape escape = new Escape("\"\\", '\\');

        assertThat(regex.quote(escape, "simple"), is("simple"));
        assertThat(regex.quote(escape, "a=b"), is("\"a=b\""));

        setField(regex, "m_filename", "/tmp/regex.rc");
        setField(
                regex,
                "m_lfn",
                new java.util.LinkedHashMap<String, Collection<ReplicaCatalogEntry>>());
        setField(
                regex,
                "m_lfn_regex",
                new java.util.LinkedHashMap<String, Collection<ReplicaCatalogEntry>>());
        setField(regex, "m_lfn_pattern", new java.util.LinkedHashMap<String, Pattern>());
        setField(regex, "m_readonly", true);

        regex.close();

        assertThat(regex.isClosed(), is(true));
        assertThat(getField(regex, "m_filename"), is(nullValue()));
    }

    private static Object getField(Object instance, String fieldName) throws Exception {
        return ReflectionTestUtils.getField(instance, fieldName);
    }

    private static void setField(Object instance, String fieldName, Object value) throws Exception {
        ReflectionTestUtils.setField(instance, fieldName, value);
    }
}
