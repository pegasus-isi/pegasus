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

package org.griphyn.cPlanner.parser.dax;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * A callback that causes the parser to exit after the metadata about the DAX
 * has been parsed. This is achieved by stopping the parsing after the
 * cbDocument method.
 *
 * @author Karan Vahi
 * @version $Revision: 314 $
 */
public class DAX2Metadata implements Callback {


    /**
     * The handle to the properties object.
     */
    private PegasusProperties mProps;

    /**
     * A flag to specify whether the graph has been generated for the partition
     * or not.
     */
    private boolean mDone;

    /**
     * The metadata of the workflow.
     */
    private Map mMetadata;

    /**
     * The overloaded constructor.
     *
     * @param properties  the properties passed to the planner.
     * @param dax         the path to the DAX file.
     */
    public DAX2Metadata( PegasusProperties properties, String dax ) {
        mProps        = properties;
        mDone         = false;
    }


    /**
     * Callback when the opening tag was parsed. This contains all
     * attributes and their raw values within a map. It ends up storing
     * the attributes with the adag element in the internal memory structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        mMetadata = new HashMap();
        mMetadata.put( "count", (String)attributes.get( "count" ) );
        mMetadata.put( "index", (String)attributes.get( "index" ) );
        mMetadata.put( "name", (String)attributes.get( "name" ) );

        //call the cbDone()
        cbDone();
    }

    /**
     * Callback for the job from section 2 jobs. These jobs are completely
     * assembled, but each is passed separately.
     *
     * @param job  the <code>SubInfo</code> object storing the job information
     *             gotten from parser.
     */
    public void cbJob( SubInfo job ) {

    }

    /**
     * Callback for child and parent relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cbParents(String child, List parents) {

    }

    /**
     * Callback when the parsing of the document is done. It sets the flag
     * that the parsing has been done, that is used to determine whether the
     * ADag object has been fully generated or not.
     */
    public void cbDone() {
        mDone = true;
        throw new RuntimeException( "Parsing done" );
    }

    /**
     * Returns an ADag object corresponding to the abstract plan it has generated.
     * It throws a runtime exception if the method is called before the object
     * has been created fully.
     *
     * @return  ADag object containing the abstract plan referred in the dax.
     */
    public Object getConstructedObject(){
        if(!mDone)
            throw new RuntimeException( "Method called before the metadata was parsed" );
        return mMetadata;
    }
}
