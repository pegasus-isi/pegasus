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
package edu.isi.pegasus.planner.provenance.pasoa;

import java.io.IOException;
import java.io.Writer;

/**
 * A PASOA specific interface to generate various assertions as XML.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface XMLProducer {

    /** Clears the internal state. */
    public void clear();

    /**
     * Adds an XML fragment to the internal XML store
     *
     * @param xml the XML fragment to be added.
     */
    public void add(String xml);

    /**
     * Returns the xml description of the object. This is used for generating the partition graph.
     * That is no longer done.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer) throws IOException;

    /**
     * Returns the interaction assertions as a XML blob.
     *
     * @return String
     * @exception IOException if something fishy happens to the stream.
     */
    public String toXML() throws IOException;
}
