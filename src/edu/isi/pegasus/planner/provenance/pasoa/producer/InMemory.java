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
package edu.isi.pegasus.planner.provenance.pasoa.producer;

import edu.isi.pegasus.planner.provenance.pasoa.XMLProducer;
import java.io.*;

/**
 * An implementation of the XMLProducer interface backed by a StringBuffer. It does not check for
 * any wellformedness of the XML. It is basically a data store.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class InMemory implements XMLProducer {

    /** The StringBuffer store. */
    private StringBuffer mStore;

    /** The initial size of the buffer. */
    private int mSize;

    /** The default constructor. */
    public InMemory() {
        mSize = 32;
        reset();
    }

    /**
     * The overloaded constructor.
     *
     * @param size the intial number of characters it can store.
     */
    public InMemory(int size) {
        mSize = size;
        reset();
    }

    /**
     * Adds to the internal XML representation.
     *
     * @param xml the XML fragment to be added.
     */
    public void add(String xml) {
        mStore.append(xml);
    }

    /** Clears the internal state. */
    public void clear() {
        reset();
    }

    /**
     * Returns the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @throws IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer) throws IOException {
        writer.write(mStore.toString());
    }

    /**
     * Returns the interaction assertions as a XML blob.
     *
     * @return String
     * @throws IOException if something fishy happens to the stream.
     */
    public String toXML() throws IOException {
        Writer writer = new StringWriter(mSize);
        toXML(writer);
        return writer.toString();
    }

    /** Resets the internal store. */
    private void reset() {
        mStore = new StringBuffer(mSize);
    }
}
