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

/** Tests for the File class. */
public class FileTest {

    private File mFile;

    @BeforeEach
    public void setUp() {
        mFile = new File("test.txt");
    }

    @Test
    public void testInstantiation() {
        assertThat(mFile, notNullValue());
    }

    @Test
    public void testGetName() {
        assertThat(mFile.getName(), is("test.txt"));
    }

    @Test
    public void testConstructorWithLink() {
        File f = new File("output.txt", File.LINK.OUTPUT);
        assertThat(f.getLink(), is(File.LINK.OUTPUT));
    }

    @Test
    public void testDefaultRegisterIsTrue() {
        assertThat(mFile.getRegister(), is(true));
    }

    @Test
    public void testDefaultTransferIsTrue() {
        assertThat(mFile.getTransfer(), is(File.TRANSFER.TRUE));
    }

    @Test
    public void testSetRegister() {
        mFile.setRegister(false);
        assertThat(mFile.getRegister(), is(false));
    }

    @Test
    public void testSetTransfer() {
        mFile.setTransfer(File.TRANSFER.FALSE);
        assertThat(mFile.getTransfer(), is(File.TRANSFER.FALSE));
    }

    @Test
    public void testSetOptional() {
        mFile.setOptional(true);
        assertThat(mFile.getOptional(), is(true));
    }

    @Test
    public void testExtendsCatalogType() {
        assertThat(mFile, instanceOf(CatalogType.class));
    }

    @Test
    public void testCopyConstructor() {
        mFile.setRegister(false);
        File copy = new File(mFile);
        assertThat(copy.getName(), is(mFile.getName()));
        assertThat(copy.getRegister(), is(mFile.getRegister()));
    }

    @Test
    public void testLinkEnumValues() {
        assertThat(File.LINK.INPUT, notNullValue());
        assertThat(File.LINK.OUTPUT, notNullValue());
        assertThat(File.LINK.INOUT, notNullValue());
        assertThat(File.LINK.CHECKPOINT, notNullValue());
    }

    @Test
    public void testTransferEnumValues() {
        assertThat(File.TRANSFER.TRUE, notNullValue());
        assertThat(File.TRANSFER.FALSE, notNullValue());
        assertThat(File.TRANSFER.OPTIONAL, notNullValue());
    }

    @Test
    public void testSetSize() {
        mFile.setSize("1024");
        assertThat(mFile.getSize(), is("1024"));
    }

    @Test
    public void testSetExecutableAndUseForPlanningFlags() {
        mFile.setExecutable();
        mFile.setUseForPlanning();

        assertThat(mFile.getExecutable(), is(true));
        assertThat(mFile.useForPlanning(), is(true));
    }

    @Test
    public void testSetSizeWithNullDoesNotOverwriteExistingValue() {
        mFile.setSize("1024");

        mFile.setSize(null);

        assertThat(mFile.getSize(), is("1024"));
    }

    @Test
    public void testToXMLUsesElementForInputOmitsTransferAndRegisterAttributes() {
        File input = new File("input.txt", File.LINK.INPUT);
        input.setTransfer(File.TRANSFER.FALSE);
        input.setRegister(false);
        input.setOptional(true);
        input.setExecutable(true);

        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        input.toXML(writer, 0, "uses");
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<uses"),
                        containsString("name=\"input.txt\""),
                        containsString("link=\"input\""),
                        containsString("optional=\"true\""),
                        containsString("executable=\"true\"")));
        assertThat(result.contains("transfer="), is(false));
        assertThat(result.contains("register="), is(false));
    }

    @Test
    public void testCloneCurrentBehaviorResetsFlagsToDefaults() {
        mFile.setRegister(false);
        mFile.setTransfer(File.TRANSFER.OPTIONAL);
        mFile.setOptional(true);
        mFile.setExecutable(true);
        mFile.setUseForPlanning(true);
        mFile.setSize("2048");

        File cloned = mFile.clone();

        assertThat(cloned.getRegister(), is(true));
        assertThat(cloned.getTransfer(), is(File.TRANSFER.TRUE));
        assertThat(cloned.getOptional(), is(false));
        assertThat(cloned.getExecutable(), is(false));
        assertThat(cloned.useForPlanning(), is(false));
        assertThat(cloned.getSize(), is(""));
    }
}
