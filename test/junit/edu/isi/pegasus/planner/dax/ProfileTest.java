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
        assertThat(mProfile, notNullValue());
    }

    @Test
    public void testGetNameSpace() {
        assertThat(mProfile.getNameSpace(), is("pegasus"));
    }

    @Test
    public void testGetKey() {
        assertThat(mProfile.getKey(), is("maxwalltime"));
    }

    @Test
    public void testGetValue() {
        assertThat(mProfile.getValue(), is("3600"));
    }

    @Test
    public void testConstructorWithNamespaceEnum() {
        Profile p = new Profile(Profile.NAMESPACE.condor, "universe", "vanilla");
        assertThat(p.getNameSpace(), is("condor"));
        assertThat(p.getKey(), is("universe"));
        assertThat(p.getValue(), is("vanilla"));
    }

    @Test
    public void testConstructorWithTwoArgs() {
        Profile p = new Profile("env", "HOME");
        assertThat(p.getNameSpace(), is("env"));
        assertThat(p.getKey(), is("HOME"));
        assertThat(p.getValue(), nullValue());
    }

    @Test
    public void testSetValue() {
        mProfile.setValue("7200");
        assertThat(mProfile.getValue(), is("7200"));
    }

    @Test
    public void testSetValueReturnsSelf() {
        Profile result = mProfile.setValue("7200");
        assertThat(result, sameInstance(mProfile));
    }

    @Test
    public void testClone() {
        Profile clone = mProfile.clone();
        assertThat(clone, not(sameInstance(mProfile)));
        assertThat(clone.getNameSpace(), is(mProfile.getNameSpace()));
        assertThat(clone.getKey(), is(mProfile.getKey()));
        assertThat(clone.getValue(), is(mProfile.getValue()));
    }

    @Test
    public void testCopyConstructor() {
        Profile copy = new Profile(mProfile);
        assertThat(copy.getNameSpace(), is(mProfile.getNameSpace()));
        assertThat(copy.getKey(), is(mProfile.getKey()));
        assertThat(copy.getValue(), is(mProfile.getValue()));
    }

    @Test
    public void testCloneIsIndependentAfterMutation() {
        Profile clone = mProfile.clone();

        mProfile.setValue("9999");

        assertThat(clone.getValue(), is("3600"));
    }

    @Test
    public void testCopyConstructorIsIndependentAfterMutation() {
        Profile copy = new Profile(mProfile);

        mProfile.setValue("9999");

        assertThat(copy.getValue(), is("3600"));
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mProfile.toXML(writer);
        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("profile"),
                        containsString("pegasus"),
                        containsString("maxwalltime"),
                        containsString("3600")));
    }

    @Test
    public void testXMLSerializationWithIndent() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mProfile.toXML(writer, 1);
        String result = sw.toString();
        assertThat(result, containsString("profile"));
    }

    @Test
    public void testXMLSerializationLowercasesUppercaseNamespace() {
        Profile profile = new Profile(Profile.NAMESPACE.PEGASUS, "style", "condor");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        profile.toXML(writer);

        String result = sw.toString();
        assertThat(result, containsString("namespace=\"pegasus\""));
    }

    @Test
    public void testNamespaceEnumValues() {
        assertThat(Profile.NAMESPACE.condor, notNullValue());
        assertThat(Profile.NAMESPACE.pegasus, notNullValue());
        assertThat(Profile.NAMESPACE.dagman, notNullValue());
        assertThat(Profile.NAMESPACE.globus, notNullValue());
        assertThat(Profile.NAMESPACE.env, notNullValue());
        assertThat(Profile.NAMESPACE.hints, notNullValue());
        assertThat(Profile.NAMESPACE.selector, notNullValue());
    }

    @Test
    public void testNamespaceEnumContainsUpperAndLowercaseVariants() {
        assertThat(Profile.NAMESPACE.CONDOR, notNullValue());
        assertThat(Profile.NAMESPACE.condor, notNullValue());
        assertThat(Profile.NAMESPACE.ENV, notNullValue());
        assertThat(Profile.NAMESPACE.env, notNullValue());
    }
}
