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

package org.griphyn.common.util;

import org.griphyn.common.util.DynamicLoader;



/**
 * The base exception class to notify of errors, while instantiating classes
 * via any of the factories. Each factory, should throw an exception that is
 * a subclass of this class.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Gaurang Mehta
 */
public class FactoryException
    // method A: no need to change interface, obsfuscated use, though
    extends java.lang.RuntimeException
    //method B: needs API small change, but makes things clear.
    //extends java.lang.Exception
    {

    /**
     * The default classname that is associated with the exception.
     */
    public static final String DEFAULT_NAME = "Object";

    /**
     * The name of the class that was trying to be instantiated when this
     * error occured, or some other signifier like module name.
     */
    protected String mClassname;

    /**
     * Converts most of the common instantiation exceptions from the class
     * loader into an error message. Use for getting sensible error messages,
     * of the causes that are associated with a FactoryException. The index
     * in the messages starts from 0.
     *
     * @return the error message
     */
    public String convertException(){
        return convertException( mClassname, this, 0 );
    }

    /**
     * Converts most of the common instantiation exceptions from the class
     * loader into an error message. Use for getting sensible error messages,
     * of the causes that are associated with a FactoryException. The index
     * in the messages starts from 0.
     *
     * @param index     the index to start from.
     *
     * @return  the error message.
     */
    public  String convertException( int index ){
        return convertException( mClassname, this , index );
    }



    /**
     * Converts most of the common instantiation exceptions from the class
     * loader into an error message. Use for getting sensible error messages,
     * of the causes that are associated with a FactoryException.
     *
     * @param classname the class that was trying to be loaded or some other
     *                  signifier.
     * @param e         the FactoryException that is thrown.
     * @param index     the index to start from.
     *
     * @return  the error message.
     */
    public static String convertException( String classname, Throwable e , int index){
        Throwable prev = null;
        StringBuffer message = new StringBuffer();

        int i = index;
        //append all the causes
        for(Throwable cause = e; cause != null ; cause  = cause.getCause()){
            message.append(" [").append(Integer.toString(++i) ).append(  "]: ");
            if ( cause instanceof FactoryException ){
                message.append( cause.getMessage() );
                classname = ((FactoryException)cause).getClassname();
            }
            else if ( prev != null && prev instanceof FactoryException ){
                //we can use the convert method
                message.append( DynamicLoader.convertExceptionToString( classname, cause ) );
            }
            else{
                //for all other exceptions just chain the message
                message.append( cause.getMessage() );
            }

            //append just one elment of stack trace for each exception
            message.append( " at " ).append( cause.getStackTrace()[0] );

            prev = cause;
        }
        return message.toString();
    }


    /**
     * Constructs a <code>FactoryException</code> with no detail
     * message. The associated classname is set to value specified by
     * <code>DEFAULT_NAME</code>.
     *
     * @param msg the detailed message.
     *
     * @see #DEFAULT_NAME
     */
    public FactoryException( String msg ) {
        super( msg );
        mClassname = this.DEFAULT_NAME;
    }

    /**
     * Constructs a <code>FactoryException</code> with the specified detailed
     * message.
     *
     * @param msg       is the detailed message.
     * @param classname the name of class that was trying to be instantiated or
     *                  some other signifier like module name.
     */
    public FactoryException(String msg, String classname) {
        super( msg );
        mClassname = classname;
    }


    /**
     * Constructs a <code>FactoryException</code> with the
     * specified detailed message and a cause. The associated classname is set
     * to value specified by <code>DEFAULT_NAME</code>.
     *
     * @param msg       is the detailed message that is to be logged.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     *
     * @see #DEFAULT_NAME
     */
    public FactoryException(String msg, Throwable cause) {
        super(msg, cause);
        mClassname = this.DEFAULT_NAME;
    }


    /**
     * Constructs a <code>FactoryException</code> with the
     * specified detailed message and a cause.
     *
     * @param msg       is the detailed message that is to be logged.
     * @param classname the name of class that was trying to be instantiated.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public FactoryException(String msg, String classname, Throwable cause) {
        super(msg, cause);
        mClassname = classname;
    }

    /**
     * Returns the name of the class that was trying to be loaded when this
     * error occured, or some other signifier like a module name.
     *
     * @return the name of the class.
     */
    public String getClassname(){
        return this.mClassname;
    }

}
