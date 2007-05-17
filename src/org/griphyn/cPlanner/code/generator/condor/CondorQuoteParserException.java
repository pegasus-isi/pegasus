/*
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
package org.griphyn.cPlanner.code.generator.condor;

/**
 * This class is used to signal errors while parsing argument strings for
 * Condor Quoting.
 *
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 * @see CondorQuoteParser
 */
public class CondorQuoteParserException extends Exception{

    /**
     * Remembers the position that cause the exception to be thrown.
     */
    private int mPosition;

    /**
     * The overloaded constructor
     *
     * @param msg       the message to be thrown in exception
     * @param position  the position at which error occured
     */
    public CondorQuoteParserException( String msg, int position ){
        super(msg);
        mPosition = position;
    }

    /**
     * The overloaded constructor
     *
     * @param msg       the message to be thrown in exception.
     * @param position  the position at which error occured.
     * @param cause     the cause of the error.
     */
    public CondorQuoteParserException( String msg, int position, Throwable cause ){
        super(msg,cause);
        mPosition = position;
    }

    /**
     * Obtains the position at which point the exception was thrown.
     * @return a column position into the string
     */
    public int getPosition(){
        return this.mPosition;
    }
}
