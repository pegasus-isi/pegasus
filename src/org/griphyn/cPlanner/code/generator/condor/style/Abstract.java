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

package org.griphyn.cPlanner.code.generator.condor.style;

import org.griphyn.cPlanner.code.generator.condor.CondorStyle;
import org.griphyn.cPlanner.code.generator.condor.CondorStyleException;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;


/**
 * An abstract implementation of the CondorStyle interface.
 * Impelements the initialization method.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */

public abstract class Abstract implements CondorStyle {

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the Site Catalog.
     */
    protected PoolInfoProvider mSCHandle;


    /**
     * A handle to the logging object.
     */
    protected LogManager mLogger;

    /**
     * The default constructor.
     */
    public Abstract() {
        mLogger = LogManager.getInstance();
    }


    /**
     * Initializes the Code Style implementation.
     *
     * @param properties  the <code>PegasusProperties</code> object containing all
     *                    the properties required by Pegasus.
     * @param siteCatalog a handle to the Site Catalog being used.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void initialize( PegasusProperties properties,
                            PoolInfoProvider siteCatalog ) throws CondorStyleException{

        mProps = properties;
        mSCHandle = siteCatalog;

    }


    /**
     * Constructs an error message in case of style mismatch.
     *
     * @param job      the job object.
     * @param style    the name of the style.
     * @param universe the universe associated with the job.
     */
    protected String errorMessage( SubInfo job, String style, String universe){
        StringBuffer sb = new StringBuffer();
        sb.append( "( " ).
             append( style ).append( "," ).
             append( universe ).append( "," ).
             append( job.getSiteHandle() ).
             append( ")" ).
             append( " mismatch for job " ).append( job.getName() );

         return sb.toString();
    }

}