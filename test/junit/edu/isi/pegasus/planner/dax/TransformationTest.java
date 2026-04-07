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
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Transformation class. */
public class TransformationTest {

    private Transformation mTransformation;

    @BeforeEach
    public void setUp() {
        mTransformation = new Transformation("pegasus", "preprocess", "1.0");
    }

    @Test
    public void testInstantiation() {
        assertThat(mTransformation, notNullValue());
    }

    @Test
    public void testGetName() {
        assertThat(mTransformation.getName(), is("preprocess"));
    }

    @Test
    public void testGetNamespace() {
        assertThat(mTransformation.getNamespace(), is("pegasus"));
    }

    @Test
    public void testGetVersion() {
        assertThat(mTransformation.getVersion(), is("1.0"));
    }

    @Test
    public void testSimpleNameConstructor() {
        Transformation t = new Transformation("my-transform");
        assertThat(t.getName(), is("my-transform"));
        assertThat(t.getNamespace(), is(""));
        assertThat(t.getVersion(), is(""));
    }

    @Test
    public void testNullNamespaceNormalizedToEmpty() {
        Transformation t = new Transformation(null, "tool", null);
        assertThat(t.getNamespace(), is(""));
        assertThat(t.getVersion(), is(""));
    }

    @Test
    public void testUsesFile() {
        File f = new File("input.txt");
        mTransformation.uses(f);
        assertThat(mTransformation.getUses().size(), is(1));
        assertThat(mTransformation.getUses().get(0), sameInstance(f));
    }

    @Test
    public void testUsesExecutable() {
        Executable e = new Executable("pegasus", "preprocess", "1.0");
        mTransformation.uses(e);
        assertThat(mTransformation.getUses().size(), is(1));
    }

    @Test
    public void testUsesMultiple() {
        mTransformation.uses(new File("a.txt"));
        mTransformation.uses(new File("b.txt"));
        assertThat(mTransformation.getUses().size(), is(2));
    }

    @Test
    public void testUsesListAppendsAllEntriesInOrder() {
        File first = new File("a.txt");
        Executable second = new Executable("pegasus", "tool", "1.0");

        mTransformation.uses(Arrays.asList(first, second));

        assertThat(mTransformation.getUses().size(), is(2));
        assertThat(mTransformation.getUses().get(0), sameInstance(first));
        assertThat(mTransformation.getUses().get(1), sameInstance(second));
    }

    @Test
    public void testInitialUsesEmpty() {
        assertThat(mTransformation.getUses().isEmpty(), is(true));
    }

    @Test
    public void testAddInvokeWhenWhat() {
        mTransformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        assertThat(mTransformation.getInvoke().isEmpty(), is(false));
        assertThat(mTransformation.getInvoke().size(), is(1));
    }

    @Test
    public void testAddInvokeObject() {
        Invoke invoke = new Invoke(Invoke.WHEN.end, "/bin/cleanup.sh");
        mTransformation.addInvoke(invoke);
        assertThat(mTransformation.getInvoke().size(), is(1));
    }

    @Test
    public void testAddInvokesClonesPassedInvokes() {
        Invoke invoke = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");

        mTransformation.addInvokes(Arrays.asList(invoke));

        assertThat(mTransformation.getInvoke().size(), is(1));
        assertThat(mTransformation.getInvoke().get(0), not(sameInstance(invoke)));
        assertThat(mTransformation.getInvoke().get(0).getWhat(), is(invoke.getWhat()));
    }

    @Test
    public void testGetNotificationIsSameAsGetInvoke() {
        mTransformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        assertThat(mTransformation.getInvoke(), is(mTransformation.getNotification()));
    }

    @Test
    public void testAddNotificationIsSameAsAddInvoke() {
        mTransformation.addNotification(Invoke.WHEN.start, "/bin/notify.sh");
        assertThat(mTransformation.getInvoke().size(), is(1));
    }

    @Test
    public void testEquals() {
        Transformation t1 = new Transformation("pegasus", "preprocess", "1.0");
        Transformation t2 = new Transformation("pegasus", "preprocess", "1.0");
        assertThat(t1.equals(t2), is(true));
    }

    @Test
    public void testNotEqualsOnName() {
        Transformation t1 = new Transformation("pegasus", "preprocess", "1.0");
        Transformation t2 = new Transformation("pegasus", "other", "1.0");
        assertThat(t1.equals(t2), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForNullAndDifferentType() {
        assertThat(mTransformation.equals(null), is(false));
        assertThat(mTransformation.equals("preprocess"), is(false));
    }

    @Test
    public void testHashCode() {
        Transformation t1 = new Transformation("pegasus", "preprocess", "1.0");
        Transformation t2 = new Transformation("pegasus", "preprocess", "1.0");
        assertThat(t1.hashCode(), is(t2.hashCode()));
    }

    @Test
    public void testToString() {
        String str = mTransformation.toString();
        assertThat(
                str,
                allOf(
                        containsString("pegasus"),
                        containsString("preprocess"),
                        containsString("1.0")));
    }

    @Test
    public void testCopyConstructor() {
        mTransformation.uses(new File("input.txt"));
        mTransformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        Transformation copy = new Transformation(mTransformation);
        assertThat(copy.getName(), is(mTransformation.getName()));
        assertThat(copy.getNamespace(), is(mTransformation.getNamespace()));
        assertThat(copy.getUses().size(), is(1));
        assertThat(copy.getInvoke().size(), is(1));
    }

    @Test
    public void testXMLSerializationWithUses() {
        mTransformation.uses(new Executable("pegasus", "preprocess", "1.0"));
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mTransformation.toXML(writer, 0);
        String result = sw.toString();
        assertThat(result, allOf(containsString("transformation"), containsString("preprocess")));
    }

    @Test
    public void testXMLSerializationForFileUseMarksExecutableFalse() {
        mTransformation.uses(new File("input.txt"));
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        mTransformation.toXML(writer, 0);

        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("name=\"input.txt\""),
                        containsString("executable=\"false\"")));
    }

    @Test
    public void testXMLSerializationOmitsEmptyNamespaceAndVersionButIncludesInvokes() {
        Transformation transformation = new Transformation("tool");
        transformation.uses(new File("input.txt"));
        transformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        transformation.toXML(writer, 0);

        String result = sw.toString();
        String transformationStartTag =
                result.substring(
                        result.indexOf("<transformation"),
                        result.indexOf(">", result.indexOf("<transformation")) + 1);
        assertThat(transformationStartTag.contains("namespace="), is(false));
        assertThat(transformationStartTag.contains("version="), is(false));
        assertThat(result, allOf(containsString("<invoke"), containsString("/bin/notify.sh")));
    }

    @Test
    public void testXMLSerializationEmptyUsesProducesNoOutput() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mTransformation.toXML(writer, 0);
        String result = sw.toString();
        assertThat(result.contains("transformation"), is(false));
    }
}
