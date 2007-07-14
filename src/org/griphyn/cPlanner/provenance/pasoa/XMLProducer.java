/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.provenance.pasoa;

import java.io.IOException;
import java.io.Writer;

/**
 * A PASOA specific interface to generate various assertions as XML.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface XMLProducer {

    /**
     * Clears the internal state.
     */
    public void clear();

    /**
     * Adds an XML fragment to the internal XML store
     *
     * @param xml the XML fragment to be added.
     *
     */
    public void add( String xml );



    /**
     * Returns the xml description of the object. This is used for generating
     * the partition graph. That is no longer done.
     *
     * @param writer is a Writer opened and ready for writing. This can also
     *               be a StringWriter for efficient output.
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML( Writer writer ) throws IOException ;


    /**
     * Returns the interaction assertions as a XML blob.
     *
     * @return String
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public String toXML() throws IOException;


}
