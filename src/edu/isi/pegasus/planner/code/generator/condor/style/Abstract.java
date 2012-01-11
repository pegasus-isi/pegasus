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


package edu.isi.pegasus.planner.code.generator.condor.style;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;

import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Iterator;



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
     * @param bag  the bag of initialization objects
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CondorStyleException{

        mProps     = bag.getPegasusProperties();
        mSiteStore = bag.getHandleToSiteStore();
        mLogger    = bag.getLogger();
    }


    /**
     * Constructs an error message in case of style mismatch.
     *
     * @param job      the job object.
     * @param style    the name of the style.
     * @param universe the universe associated with the job.
     */
    protected String errorMessage( Job job, String style, String universe){
        StringBuffer sb = new StringBuffer();
        sb.append( "( " ).
             append( style ).append( "," ).
             append( universe ).append( "," ).
             append( job.getSiteHandle() ).
             append( ")" ).
             append( " mismatch for job " ).append( job.getName() );

         return sb.toString();
    }

    /**
     * Apply a style to an AggregatedJob
     *
     * @param job  the <code>AggregatedJob</code> object containing the job.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( AggregatedJob job ) throws CondorStyleException{
        //apply style to all constituent jobs
        for( Iterator it = job.constituentJobsIterator(); it.hasNext(); ){
            Job j = (Job) it.next();
            this.apply( j );
        }
        //also apply style to the aggregated job itself
        this.apply( (Job)job );
    }

}
