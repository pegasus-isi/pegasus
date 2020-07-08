/**
 * Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.partitioner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Currently;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * It writes out the partition graph in xml form.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PDAXWriter {

    /**
     * The version of the associated xml schema, to which the pdax files being written conform to.
     */
    public static final String XML_VERSION = "2.0";

    public static final String XML_NAMESPACE = "https://pegasus.isi.edu/schema";

    /** The write handle to the xml file being written. */
    private PrintWriter mWriteHandle;

    /** The handle to the logging object. */
    private LogManager mLogger;

    /** The name assigned to the pdax file being written. */
    private String mName;

    /** The fully qaulified path to the file being written. */
    private String mFileName;

    /**
     * The overloaded constructor.
     *
     * @param name the name that is assigned to the pdax.
     * @param fileName the path to the xml file that has to be written.
     */
    public PDAXWriter(String name, String fileName) {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mFileName = fileName;
        mName = name;

        try {
            mWriteHandle = new PrintWriter(new BufferedWriter(new FileWriter(fileName)));
        } catch (IOException e) {
            throw new RuntimeException("Unable to write to file " + fileName + " :", e);
        }
    }

    /** Writes out the opening element of the xml document. */
    public void writeHeader() {
        String name = new File(mFileName).getName();

        writeln("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writeln("<!-- generated: " + Currently.iso8601(false) + "-->");
        writeln(
                "<pdag xmlns=\""
                        + XML_NAMESPACE
                        + "/PDAX\""
                        + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                        + " xsi:schemaLocation=\""
                        + XML_NAMESPACE
                        + "/PDAX "
                        + XML_NAMESPACE
                        + "/pdax-"
                        + XML_VERSION
                        + ".xsd\" "
                        + "name=\""
                        + mName
                        + "\" "
                        + "index=\"0\" count=\"1\""
                        + " version=\""
                        + XML_VERSION
                        + "\" "
                        + ">");
    }

    /**
     * Writes out a partition to the associate XML stream.
     *
     * @param p the partition to be written to the stream.
     * @exception IOException if something fishy happens to the stream.
     */
    public void write(Partition p) throws IOException {
        p.toXML(mWriteHandle);
    }

    /**
     * Writes out to the file.
     *
     * @param st String
     */
    public void write(String st) {
        mWriteHandle.write(st);
    }

    /**
     * Writes out to the file.
     *
     * @param st String
     */
    public void writeln(String st) {
        mWriteHandle.println(st);
    }

    /** Close the xml file that is written. */
    public void close() {
        write("\n</pdag>");
        mWriteHandle.close();
    }
}
