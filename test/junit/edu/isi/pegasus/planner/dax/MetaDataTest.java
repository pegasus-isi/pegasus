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
package edu.isi.pegasus.planner.dax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.isi.pegasus.common.util.XMLWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the MetaData class. */
public class MetaDataTest {

    private MetaData mMetaData;

    @BeforeEach
    public void setUp() {
        mMetaData = new MetaData("checksum.type", "sha256");
    }

    @Test
    public void testInstantiation() {
        assertThat(mMetaData, notNullValue());
    }

    @Test
    public void testGetKey() {
        assertThat(mMetaData.getKey(), is("checksum.type"));
    }

    @Test
    public void testGetValue() {
        assertThat(mMetaData.getValue(), is("sha256"));
    }

    @Test
    public void testSetValue() {
        mMetaData.setValue("md5");
        assertThat(mMetaData.getValue(), is("md5"));
    }

    @Test
    public void testSetValueReturnsSelf() {
        MetaData result = mMetaData.setValue("md5");
        assertThat(result, sameInstance(mMetaData));
    }

    @Test
    public void testClone() {
        MetaData clone = mMetaData.clone();
        assertThat(clone, not(sameInstance(mMetaData)));
        assertThat(clone.getKey(), is(mMetaData.getKey()));
        assertThat(clone.getValue(), is(mMetaData.getValue()));
    }

    @Test
    public void testCopyConstructorSwapsKeyAndType() {
        MetaData copy = new MetaData(mMetaData);
        assertThat(copy.getKey(), is(mMetaData.getKey()));
        assertThat(copy.getValue(), is(mMetaData.getValue()));
    }

    @Test
    public void testCopyConstructorIndependence() {
        MetaData copy = new MetaData(mMetaData);
        copy.setValue("different");
        assertThat(mMetaData.getValue(), is("sha256"));
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mMetaData.toXML(writer);
        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("metadata"),
                        containsString("checksum.type"),
                        containsString("sha256")));
    }

    @Test
    public void testXMLSerializationWithIndent() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mMetaData.toXML(writer, 2);
        String result = sw.toString();
        assertThat(result, containsString("metadata"));
    }

    @Test
    public void testSetValueAllowsNull() {
        mMetaData.setValue(null);

        assertThat(mMetaData.getValue(), nullValue());
    }

    @Test
    public void testClonePreservesNullValue() {
        mMetaData.setValue(null);

        MetaData clone = mMetaData.clone();

        assertThat(clone.getKey(), is(mMetaData.getKey()));
        assertThat(clone.getValue(), nullValue());
    }

    @Test
    public void testXMLSerializationContainsKeyAttributeAndNoTypeAttribute() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        mMetaData.toXML(writer, 1);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<metadata"),
                        containsString("key=\"checksum.type\""),
                        containsString(">sha256<")));
        assertThat(result.contains("type="), is(false));
    }

    @Test
    public void testJsonSerializationUsesKeyAsFieldName() throws Exception {
        String json = new ObjectMapper().writeValueAsString(mMetaData);

        assertThat(json, is("{\"checksum.type\":\"sha256\"}"));
    }
}
