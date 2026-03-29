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
        assertNotNull(mFile, "File should be instantiatable");
    }

    @Test
    public void testGetName() {
        assertEquals("test.txt", mFile.getName(), "Name should match constructor argument");
    }

    @Test
    public void testConstructorWithLink() {
        File f = new File("output.txt", File.LINK.OUTPUT);
        assertEquals(File.LINK.OUTPUT, f.getLink(), "Link should be OUTPUT");
    }

    @Test
    public void testDefaultRegisterIsTrue() {
        assertTrue(mFile.getRegister(), "Default register flag should be true");
    }

    @Test
    public void testDefaultTransferIsTrue() {
        assertEquals(File.TRANSFER.TRUE, mFile.getTransfer(), "Default transfer should be TRUE");
    }

    @Test
    public void testSetRegister() {
        mFile.setRegister(false);
        assertFalse(mFile.getRegister(), "Register should be false after setRegister(false)");
    }

    @Test
    public void testSetTransfer() {
        mFile.setTransfer(File.TRANSFER.FALSE);
        assertEquals(File.TRANSFER.FALSE, mFile.getTransfer(), "Transfer should be updated");
    }

    @Test
    public void testSetOptional() {
        mFile.setOptional(true);
        assertTrue(mFile.getOptional(), "Optional should be true after setOptional(true)");
    }

    @Test
    public void testExtendsCatalogType() {
        assertInstanceOf(CatalogType.class, mFile, "File should extend CatalogType");
    }

    @Test
    public void testCopyConstructor() {
        mFile.setRegister(false);
        File copy = new File(mFile);
        assertEquals(mFile.getName(), copy.getName(), "Copy should have same name");
        assertEquals(
                mFile.getRegister(), copy.getRegister(), "Copy should have same register flag");
    }

    @Test
    public void testLinkEnumValues() {
        assertNotNull(File.LINK.INPUT);
        assertNotNull(File.LINK.OUTPUT);
        assertNotNull(File.LINK.INOUT);
        assertNotNull(File.LINK.CHECKPOINT);
    }

    @Test
    public void testTransferEnumValues() {
        assertNotNull(File.TRANSFER.TRUE);
        assertNotNull(File.TRANSFER.FALSE);
        assertNotNull(File.TRANSFER.OPTIONAL);
    }

    @Test
    public void testSetSize() {
        mFile.setSize("1024");
        assertEquals("1024", mFile.getSize(), "Size should be set correctly");
    }
}
