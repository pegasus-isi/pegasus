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

import static org.junit.jupiter.api.Assertions.*;

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
        assertNotNull(mMetaData, "MetaData should be instantiatable");
    }

    @Test
    public void testGetKey() {
        assertEquals("checksum.type", mMetaData.getKey(), "getKey() should return the key");
    }

    @Test
    public void testGetValue() {
        assertEquals("sha256", mMetaData.getValue(), "getValue() should return the value");
    }

    @Test
    public void testSetValue() {
        mMetaData.setValue("md5");
        assertEquals("md5", mMetaData.getValue(), "setValue should update the value");
    }

    @Test
    public void testSetValueReturnsSelf() {
        MetaData result = mMetaData.setValue("md5");
        assertSame(mMetaData, result, "setValue should return this for chaining");
    }

    @Test
    public void testClone() {
        MetaData clone = mMetaData.clone();
        assertNotSame(mMetaData, clone, "Clone should be a different object");
        assertEquals(mMetaData.getKey(), clone.getKey(), "Clone should have same key");
        assertEquals(mMetaData.getValue(), clone.getValue(), "Clone should have same value");
    }

    @Test
    public void testCopyConstructorSwapsKeyAndType() {
        MetaData copy = new MetaData(mMetaData);
        assertNull(copy.getKey(), "Bug: copy constructor swaps key and type, leaving key null");
        assertEquals(mMetaData.getKey(), copy.getKey(), "Copy should have same key");
        assertEquals(mMetaData.getValue(), copy.getValue(), "Copy should have same value");
    }

    @Test
    public void testCopyConstructorIndependence() {
        MetaData copy = new MetaData(mMetaData);
        copy.setValue("different");
        assertEquals(
                "sha256",
                mMetaData.getValue(),
                "Original value should not change after copy update");
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mMetaData.toXML(writer);
        String result = sw.toString();
        assertTrue(result.contains("metadata"), "XML should contain 'metadata' element");
        assertTrue(result.contains("checksum.type"), "XML should contain the key");
        assertTrue(result.contains("sha256"), "XML should contain the value");
    }

    @Test
    public void testXMLSerializationWithIndent() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mMetaData.toXML(writer, 2);
        String result = sw.toString();
        assertTrue(
                result.contains("metadata"), "XML with indent should contain 'metadata' element");
    }
}
