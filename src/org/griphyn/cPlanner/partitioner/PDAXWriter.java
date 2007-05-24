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
package org.griphyn.cPlanner.partitioner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.griphyn.cPlanner.common.LogManager;

/**
 * It writes out the partition graph in xml form.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PDAXWriter {

    /**
     * The version of the associated xml schema, to which the pdax files being
     * written conform to.
     */
    public static final String XML_VERSION = "2.0";

    public static final String XML_NAMESPACE="http://pegasus.isi.edu/schema";

    /**
     * The write handle to the xml file being written.
     */
    private PrintWriter mWriteHandle;


    /**
     * The handle to the logging object.
     */
    private LogManager mLogger;

    /**
     * The name assigned to the pdax file being written.
     */
    private String mName;

    /**
     * The fully qaulified path to the file being written.
     */
    private String mFileName;

    /**
     * The overloaded constructor.
     *
     * @param name      the name that is assigned to the pdax.
     * @param fileName  the path to the xml file that has to be written.
     */
    public PDAXWriter(String name, String fileName) {
        mLogger   = LogManager.getInstance();
        mFileName = fileName;
        mName     = name;

        try{
            mWriteHandle =
                new PrintWriter(new BufferedWriter(new FileWriter(fileName)));
        }
        catch(IOException e){
            throw new RuntimeException( "Unable to write to file " + fileName + " :",
                                        e);
        }
    }

    /**
     * Writes out the opening element of the xml document.
     */
    public void writeHeader(){
        String name = new File(mFileName).getName();

        writeln("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writeln("<!-- generated: " + mLogger.getTimeStamp() + "-->");
        writeln("<pdag xmlns=\""+XML_NAMESPACE+"/PDAX\"" +
                " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
                " xsi:schemaLocation=\""+XML_NAMESPACE+"/PDAX " +
                XML_NAMESPACE+"/pdax-"+XML_VERSION+".xsd\" " +
                "name=\"" + mName + "\" " + "index=\"0\" count=\"1\"" +
                " version=\"" + XML_VERSION + "\" " + ">");

    }

    /**
     * Writes out a partition to the associate XML stream.
     *
     * @param p  the partition to be written to the stream.
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public void write( Partition p ) throws IOException{
        p.toXML( mWriteHandle );
    }

    /**
     * Writes out to the file.
     * @param st String
     */
    public void write(String st){
        mWriteHandle.write(st);
    }

    /**
     * Writes out to the file.
     * @param st String
     */
    public void writeln(String st){
        mWriteHandle.println(st);
    }

    /**
     * Close the xml file that is written.
     */
    public void close(){
        write("\n</pdag>");
        mWriteHandle.close();
    }
}
