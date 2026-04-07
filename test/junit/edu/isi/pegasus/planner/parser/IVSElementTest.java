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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.invocation.ArgEntry;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class IVSElementTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorStoresNameAndInvocationReference() {
        ArgEntry invocation = new ArgEntry(1, "value");
        IVSElement element = new IVSElement("job", invocation);

        assertThat("Constructor should store the element name", element.m_name, is("job"));
        assertThat(
                "Constructor should preserve the invocation object reference",
                element.m_obj,
                sameInstance(invocation));
    }

    @Test
    public void testConstructorCopiesNameStringInstance() {
        String originalName = new String("site");
        IVSElement element = new IVSElement(originalName, new ArgEntry());

        assertThat("Copied name should keep the same characters", element.m_name, is("site"));
        assertThat(
                "Constructor currently creates a defensive String copy for the name",
                element.m_name,
                not(sameInstance(originalName)));
    }

    @Test
    public void testConstructorAllowsNullInvocation() {
        IVSElement element = new IVSElement("profile", null);

        assertThat("Name should still be stored", element.m_name, is("profile"));
        assertThat("Invocation reference may be null", element.m_obj, nullValue());
    }

    @Test
    public void testConstructorRejectsNullName() {
        assertThrows(
                NullPointerException.class,
                () -> new IVSElement(null, new ArgEntry()),
                "Null names currently trigger the String copy constructor failure");
    }
}
