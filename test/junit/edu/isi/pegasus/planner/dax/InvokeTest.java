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

/** Tests for the Invoke class. */
public class InvokeTest {

    private Invoke mInvoke;

    @BeforeEach
    public void setUp() {
        mInvoke = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
    }

    @Test
    public void testInstantiation() {
        assertThat(mInvoke, notNullValue());
    }

    @Test
    public void testGetWhen() {
        assertThat(mInvoke.getWhen(), is("start"));
    }

    @Test
    public void testGetWhat() {
        assertThat(mInvoke.getWhat(), is("/bin/notify.sh"));
    }

    @Test
    public void testSetWhenNormalizesAtEnd() {
        mInvoke.setWhen(Invoke.WHEN.at_end);
        assertThat(mInvoke.getWhen(), is("end"));
    }

    @Test
    public void testSetWhenNormalizesOnError() {
        mInvoke.setWhen(Invoke.WHEN.on_error);
        assertThat(mInvoke.getWhen(), is("error"));
    }

    @Test
    public void testSetWhenNormalizesOnSuccess() {
        mInvoke.setWhen(Invoke.WHEN.on_success);
        assertThat(mInvoke.getWhen(), is("success"));
    }

    @Test
    public void testSetWhenNever() {
        mInvoke.setWhen(Invoke.WHEN.never);
        assertThat(mInvoke.getWhen(), is("never"));
    }

    @Test
    public void testSetWhenAll() {
        mInvoke.setWhen(Invoke.WHEN.all);
        assertThat(mInvoke.getWhen(), is("all"));
    }

    @Test
    public void testSetWhat() {
        mInvoke.setWhat("/usr/local/bin/other.sh");
        assertThat(mInvoke.getWhat(), is("/usr/local/bin/other.sh"));
    }

    @Test
    public void testClone() {
        Invoke clone = mInvoke.clone();
        assertThat(clone, not(sameInstance(mInvoke)));
        assertThat(clone.getWhen(), is(mInvoke.getWhen()));
        assertThat(clone.getWhat(), is(mInvoke.getWhat()));
    }

    @Test
    public void testCopyConstructor() {
        Invoke copy = new Invoke(mInvoke);
        assertThat(copy.getWhen(), is(mInvoke.getWhen()));
        assertThat(copy.getWhat(), is(mInvoke.getWhat()));
    }

    @Test
    public void testEqualsWithSameValues() {
        Invoke i1 = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        Invoke i2 = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        assertThat(i1.equals(i2), is(true));
    }

    @Test
    public void testEqualsWithDifferentValues() {
        Invoke i1 = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        Invoke i2 = new Invoke(Invoke.WHEN.end, "/bin/notify.sh");
        assertThat(i1.equals(i2), is(false));
    }

    @Test
    public void testToString() {
        String str = mInvoke.toString();
        assertThat(str, allOf(containsString("start"), containsString("/bin/notify.sh")));
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mInvoke.toXML(writer);
        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("invoke"),
                        containsString("start"),
                        containsString("/bin/notify.sh")));
    }

    @Test
    public void testWhenEnumValues() {
        assertThat(Invoke.WHEN.never, notNullValue());
        assertThat(Invoke.WHEN.start, notNullValue());
        assertThat(Invoke.WHEN.success, notNullValue());
        assertThat(Invoke.WHEN.error, notNullValue());
        assertThat(Invoke.WHEN.end, notNullValue());
        assertThat(Invoke.WHEN.all, notNullValue());
    }

    @Test
    public void testSingleArgumentConstructorLeavesWhatNull() {
        Invoke invoke = new Invoke(Invoke.WHEN.end);

        assertThat(invoke.getWhen(), is("end"));
        assertThat(invoke.getWhat(), nullValue());
    }

    @Test
    public void testEqualsReturnsFalseForNullAndDifferentType() {
        assertThat(mInvoke.equals(null), is(false));
        assertThat(mInvoke.equals("invoke"), is(false));
    }

    @Test
    public void testCopyConstructorPreservesNormalizedWhenValue() {
        Invoke original = new Invoke(Invoke.WHEN.on_error, "/bin/fail.sh");

        Invoke copy = new Invoke(original);

        assertThat(original.getWhen(), is("error"));
        assertThat(copy.getWhen(), is("error"));
        assertThat(copy.getWhat(), is("/bin/fail.sh"));
    }

    @Test
    public void testToXMLWritesLowercaseWhenAttribute() {
        Invoke invoke = new Invoke(Invoke.WHEN.on_success, "/bin/pass.sh");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        invoke.toXML(writer, 1);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<invoke"),
                        containsString("when=\"success\""),
                        containsString("/bin/pass.sh")));
    }
}
