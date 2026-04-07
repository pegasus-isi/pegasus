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

import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class YAMLTest {

    @Test
    public void testConstructorDefaults() throws Exception {
        YAML yaml = new YAML();

        assertThat(yaml.isClosed(), is(true));
        assertThat((Boolean) getField(yaml, "mQuote"), is(false));
        assertThat((Boolean) getField(yaml, "m_readonly"), is(false));
        assertThat(getField(yaml, "mVersion"), is(YAML.DEFAULT_REPLICA_CATALOG_VERSION));
        assertThat(
                ((File) getField(yaml, "SCHEMA_FILE")).getName().endsWith("rc-5.0.yml"), is(true));
    }

    @Test
    public void testConnectStringRejectsNullFilename() {
        YAML yaml = new YAML();

        assertThat(yaml.connect((String) null), is(false));
        assertThat(yaml.isClosed(), is(true));
    }

    @Test
    public void testConnectPropertiesParsesFlagsAndDocumentSizeWithoutFile() throws Exception {
        YAML yaml = new YAML();
        Properties props = new Properties();
        props.setProperty("quote", "true");
        props.setProperty(ReplicaCatalog.READ_ONLY_KEY, "true");
        props.setProperty(ReplicaCatalog.PARSER_DOCUMENT_SIZE_PROPERTY_KEY, "7");

        assertThat(yaml.connect(props), is(false));
        assertThat((Boolean) getField(yaml, "mQuote"), is(true));
        assertThat((Boolean) getField(yaml, "m_readonly"), is(true));
        assertThat(getField(yaml, "mMAXParsedDocSize"), is(7));
    }

    @Test
    public void testLookupWithHandleExpandsRegexPfns() throws Exception {
        YAML yaml = new YAML();
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("/data/[1].txt", "local");
        ReplicaLocation regexLocation =
                new ReplicaLocation("f.(.*)", java.util.Collections.singletonList(entry), false);
        regexLocation.addMetadata("owner", "pegasus");

        setField(yaml, "mLFN", new LinkedHashMap<String, ReplicaLocation>());
        Map<String, ReplicaLocation> regexMap = new LinkedHashMap<String, ReplicaLocation>();
        regexMap.put("f.(.*)", regexLocation);
        setField(yaml, "mLFNRegex", regexMap);
        Map<String, Pattern> patternMap = new LinkedHashMap<String, Pattern>();
        patternMap.put("f.(.*)", Pattern.compile("f.(.*)"));
        setField(yaml, "mLFNPattern", patternMap);

        assertThat(yaml.lookup("f.beta", "local"), is("/data/beta.txt"));
        ReplicaCatalogEntry lookedUp = yaml.lookup("f.beta").iterator().next();
        assertThat(lookedUp.getAttribute("owner"), is("pegasus"));
    }

    @Test
    public void testCloseClearsReadonlyState() throws Exception {
        YAML yaml = new YAML();
        setField(yaml, "mFilename", "/tmp/replicas.yml");
        setField(yaml, "mLFN", new LinkedHashMap<String, ReplicaLocation>());
        setField(yaml, "mLFNRegex", new LinkedHashMap<String, ReplicaLocation>());
        setField(yaml, "mLFNPattern", new LinkedHashMap<String, Pattern>());
        setField(yaml, "m_readonly", true);

        yaml.close();

        assertThat(yaml.isClosed(), is(true));
        assertThat(getField(yaml, "mFilename"), is(nullValue()));
    }

    private static Object getField(Object instance, String fieldName) throws Exception {
        return ReflectionTestUtils.getField(instance, fieldName);
    }

    private static void setField(Object instance, String fieldName, Object value) throws Exception {
        ReflectionTestUtils.setField(instance, fieldName, value);
    }
}
