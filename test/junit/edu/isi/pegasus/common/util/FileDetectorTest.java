/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.isi.pegasus.common.util;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.junit.*;

/** Test class for file type detector */
public class FileDetectorTest {

    private File mTestFile = null;

    public FileDetectorTest() {}

    @Before
    public void setUp() throws IOException {}

    @Test
    public void simpleYAMLWithHeader() {
        BufferedWriter writer;
        try {
            mTestFile = File.createTempFile("pegasus", ".txt");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.write("pegasus: 5.0\n");
            writer.close();
        } catch (IOException ex) {
        }
        assertTrue(FileDetector.isTypeYAML(mTestFile));
    }

    @Test
    public void malformedYAMLWithHeader() {
        BufferedWriter writer;
        try {
            mTestFile = File.createTempFile("pegasus", ".txt");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.write("pegasus: 5.0\n");
            writer.write("     jobs: \n");
            writer.write(" ted: { name: ted, age: 32, email: ted@tedtalks.com }\n");
            writer.close();
        } catch (IOException ex) {
        }
        assertTrue(FileDetector.isTypeYAML(mTestFile));
    }

    @Test
    public void malformedYAMLWithYMLExtension() {
        BufferedWriter writer;
        try {
            // test file with yaml extension
            mTestFile = File.createTempFile("pegasus", ".yml");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.write("dummy: 5.0\n");
            writer.write("     jobs: \n");
            writer.write(" ted: { name: ted, age: 32, email: ted@tedtalks.com }\n");
            writer.close();
        } catch (IOException ex) {
        }
        assertTrue(FileDetector.isTypeYAML(mTestFile));
    }

    @Test
    public void checkForYAMLAgainstTextRC() {
        BufferedWriter writer;
        try {
            mTestFile = File.createTempFile("pegasus", ".txt");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.write("david.f.a gsiftp://hellboy.isi.edu/tmp/david.test pool=\"local\"n");
            writer.close();
        } catch (IOException ex) {
        }
        assertFalse(FileDetector.isTypeYAML(mTestFile));
    }

    @Test
    public void checkForYAMLAgainstTextTC() {
        BufferedWriter writer;
        try {
            mTestFile = File.createTempFile("pegasus", ".txt");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.write(
                    "tr vahi::analyze:1.0 {\n"
                            + "\n"
                            + "   #specify profiles that apply for all the sites for the transformation\n"
                            + "   #in each site entry the profile can be overriden\n"
                            + "   #profile env \"APP_HOME\" \"/tmp/karan\"\n"
                            + "   #profile env \"JAVA_HOME\" \"/bin/java.1.5\"\n"
                            + "  \n"
                            + "   site local {\n"
                            + "    #profile env \"me\" \"with\"\n"
                            + "    #profile condor \"more\" \"test\"\n"
                            + "    #profile env \"JAVA_HOME\" \"/bin/java.1.6\"\n"
                            + "    pfn \"/path/to/keg\"\n"
                            + "    arch  \"x86\"\n"
                            + "    os    \"linux\"\n"
                            + "    #osrelease \"fc\"\n"
                            + "    #osversion \"4\"\n"
                            + "    type \"installed\"\n"
                            + "   }\n"
                            + "}\n");
            writer.close();
        } catch (IOException ex) {
        }
        assertFalse(FileDetector.isTypeYAML(mTestFile));
    }

    @Test
    public void checkForYAMLAgainstXMLSC() {
        BufferedWriter writer;
        try {
            mTestFile = File.createTempFile("pegasus", ".txt");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.write(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                            + "<sitecatalog xmlns=\"https://pegasus.isi.edu/schema/sitecatalog\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"https://pegasus.isi.edu/schema/sitecatalog https://pegasus.isi.edu/schema/sc-4.0.xsd\" version=\"4.0\">\n"
                            + "\n"
                            + "    <site  handle=\"local\" arch=\"ppc64le\" os=\"LINUX\">\n"
                            + "        <directory type=\"shared-scratch\" path=\"/Volumes/Work/lfs1/work/pegasus-features/PM-1159/work\">\n"
                            + "            <file-server operation=\"all\" url=\"file:///Volumes/Work/lfs1/work/pegasus-features/PM-1159/work\"/>\n"
                            + "        </directory>\n"
                            + "        <directory type=\"local-storage\" path=\"/Volumes/Work/lfs1/work/pegasus-features/PM-1159/outputs\">\n"
                            + "            <file-server operation=\"all\" url=\"file:///Volumes/Work/lfs1/work/pegasus-features/PM-1159/outputs\"/>\n"
                            + "        </directory>\n"
                            + "        <profile namespace=\"pegasus\" key=\"clusters.num\">1</profile>\n"
                            + "        <profile namespace=\"env\" key=\"PEGASUS_HOME\">/usr/</profile>\n"
                            + "    </site>\n"
                            + "\n"
                            + "</sitecatalog>");
            writer.close();
        } catch (IOException ex) {
        }
        assertFalse(FileDetector.isTypeYAML(mTestFile));
    }

    @Test
    public void checkForYAMLAgainstEmptyFile() {
        BufferedWriter writer;
        try {
            mTestFile = File.createTempFile("pegasus", ".txt");
            writer = new BufferedWriter(new FileWriter(mTestFile));
            writer.close();
        } catch (IOException ex) {
        }
        assertFalse(FileDetector.isTypeYAML(mTestFile));
    }

    @After
    public void tearDown() {
        mTestFile.delete();
    }
}
