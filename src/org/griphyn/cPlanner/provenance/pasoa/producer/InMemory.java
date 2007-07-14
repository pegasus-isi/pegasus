/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.provenance.pasoa.producer;

import org.griphyn.cPlanner.provenance.pasoa.XMLProducer;

import java.io.*;

/**
 * An implementation of the XMLProducer interface backed by a StringBuffer.
 * It does not check for any wellformedness of the XML. It is basically a
 * data store.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class InMemory
    implements XMLProducer {


    /**
     * The StringBuffer store.
     */
    private StringBuffer mStore;

    /**
     * The initial size of the buffer.
     */
    private int mSize;

    /**
     * The default constructor.
     */
    public InMemory() {
        mSize = 32;
        reset();
    }

    /**
     * The overloaded constructor.
     *
     * @param size the intial number of characters it can store.
     */
    public InMemory( int size ){
        mSize = size;
        reset();
    }

    /**
     * Adds to the internal XML representation.
     *
     * @param xml the XML fragment to be added.
     *
     */
    public void add( String xml ) {
        mStore.append( xml );
    }

    /**
     * Clears the internal state.
     *
     *
     */
    public void clear() {
        reset();
    }

    /**
     * Returns the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also
     *   be a StringWriter for efficient output.
     * @throws IOException if something fishy happens to the stream.
     */
    public void toXML( Writer writer ) throws IOException {
        writer.write( mStore.toString() );
    }

    /**
     * Returns the interaction assertions as a XML blob.
     *
     * @return String
     * @throws IOException if something fishy happens to the stream.
     */
    public String toXML() throws IOException {
        Writer writer = new StringWriter( mSize );
        toXML( writer );
        return writer.toString();

    }

    /**
     * Resets the internal store.
     */
    private void reset(){
        mStore = new StringBuffer( mSize );
    }
}
