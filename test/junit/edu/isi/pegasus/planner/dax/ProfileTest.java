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

/** Tests for the Profile class. */
public class ProfileTest {

    private Profile mProfile;

    @BeforeEach
    public void setUp() {
        mProfile = new Profile("pegasus", "maxwalltime", "3600");
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mProfile, "Profile should be instantiatable");
    }

    @Test
    public void testGetNameSpace() {
        assertEquals("pegasus", mProfile.getNameSpace(), "getNameSpace() should return namespace");
    }

    @Test
    public void testGetKey() {
        assertEquals("maxwalltime", mProfile.getKey(), "getKey() should return the key");
    }

    @Test
    public void testGetValue() {
        assertEquals("3600", mProfile.getValue(), "getValue() should return the value");
    }

    @Test
    public void testConstructorWithNamespaceEnum() {
        Profile p = new Profile(Profile.NAMESPACE.condor, "universe", "vanilla");
        assertEquals("condor", p.getNameSpace(), "Namespace enum should be converted to string");
        assertEquals("universe", p.getKey(), "Key should be set from constructor");
        assertEquals("vanilla", p.getValue(), "Value should be set from constructor");
    }

    @Test
    public void testConstructorWithTwoArgs() {
        Profile p = new Profile("env", "HOME");
        assertEquals("env", p.getNameSpace(), "Two-arg constructor should set namespace");
        assertEquals("HOME", p.getKey(), "Two-arg constructor should set key");
        assertNull(p.getValue(), "Two-arg constructor should have null value");
    }

    @Test
    public void testSetValue() {
        mProfile.setValue("7200");
        assertEquals("7200", mProfile.getValue(), "setValue should update the value");
    }

    @Test
    public void testSetValueReturnsSelf() {
        Profile result = mProfile.setValue("7200");
        assertSame(mProfile, result, "setValue should return this for chaining");
    }

    @Test
    public void testClone() {
        Profile clone = mProfile.clone();
        assertNotSame(mProfile, clone, "Clone should be a different object");
        assertEquals(
                mProfile.getNameSpace(), clone.getNameSpace(), "Clone should have same namespace");
        assertEquals(mProfile.getKey(), clone.getKey(), "Clone should have same key");
        assertEquals(mProfile.getValue(), clone.getValue(), "Clone should have same value");
    }

    @Test
    public void testCopyConstructor() {
        Profile copy = new Profile(mProfile);
        assertEquals(
                mProfile.getNameSpace(), copy.getNameSpace(), "Copy should have same namespace");
        assertEquals(mProfile.getKey(), copy.getKey(), "Copy should have same key");
        assertEquals(mProfile.getValue(), copy.getValue(), "Copy should have same value");
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mProfile.toXML(writer);
        String result = sw.toString();
        assertTrue(result.contains("profile"), "XML should contain 'profile' element");
        assertTrue(result.contains("pegasus"), "XML should contain the namespace");
        assertTrue(result.contains("maxwalltime"), "XML should contain the key");
        assertTrue(result.contains("3600"), "XML should contain the value");
    }

    @Test
    public void testXMLSerializationWithIndent() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mProfile.toXML(writer, 1);
        String result = sw.toString();
        assertTrue(result.contains("profile"), "XML with indent should contain 'profile' element");
    }

    @Test
    public void testNamespaceEnumValues() {
        assertNotNull(Profile.NAMESPACE.condor);
        assertNotNull(Profile.NAMESPACE.pegasus);
        assertNotNull(Profile.NAMESPACE.dagman);
        assertNotNull(Profile.NAMESPACE.globus);
        assertNotNull(Profile.NAMESPACE.env);
        assertNotNull(Profile.NAMESPACE.hints);
        assertNotNull(Profile.NAMESPACE.selector);
    }
}
