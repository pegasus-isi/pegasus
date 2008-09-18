/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.griphyn.cPlanner.code.generator.condor.style;

import edu.isi.pegasus.common.logging.LoggerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.code.generator.condor.CondorStyle;
import org.griphyn.cPlanner.code.generator.condor.CondorStyleException;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;



/**
 * An abstract implementation of the CondorStyle interface.
 * Impelements the initialization method.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public abstract class Abstract implements CondorStyle {

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the Site Catalog Store.
     */
    protected SiteStore mSiteStore;


    /**
     * A handle to the logging object.
     */
    protected LogManager mLogger;

    /**
     * The default constructor.
     */
    public Abstract() {
        //mLogger = LogManager.getInstance();
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
                            SiteStore siteStore ) throws CondorStyleException{

        mProps = properties;
        mSiteStore = siteStore;
        mLogger = LoggerFactory.loadSingletonInstance( properties );
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
