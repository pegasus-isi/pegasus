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

import edu.isi.pegasus.common.credential.CredentialHandler.TYPE;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.ENV;




/**
 * Enables a job to be directly submitted to a remote CREAM CE front end
 *
 * The CREAM CE support in Condor is documented at the following link
 *
 * <pre>
 * http://research.cs.wisc.edu/htcondor/manual/v7.9/5_3Grid_Universe.html#SECTION00637000000000000000
 * </pre>
 *
 * The protocol requires an X.509 proxy for the job, so the submit description file
 * command x509userproxy will be used.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CreamCE extends Abstract{

    /**
     * The key that designates the collector associated with the job
     */
    public static final String GRID_RESOURCE_KEY =
            edu.isi.pegasus.planner.namespace.Condor.GRID_RESOURCE_KEY;

    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "CreamCE";
    
    /**
     * Handle to CondorG style to translate task requirements
     */
    private final CondorG mCondorG;

    /**
     * The default constructor.
     */
    public CreamCE() {
        super();
        mCondorG = new CondorG();
    }

    /**
     * Applies the CREAM CE style to the job.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException{
        String workdir = job.getDirectory();

        //the universe for CondorC is always grid
        job.condorVariables.construct( Condor.UNIVERSE_KEY, "grid" );
        
        //construct the grid_resource for the job
        String gridResource = constructGridResource( job );

        job.condorVariables.construct( CreamCE.GRID_RESOURCE_KEY, gridResource.toString() );
        
        //associate the proxy to be used
        //we always say a proxy is required
        job.setSubmissionCredential( TYPE.x509 );

        job.condorVariables.construct( "remote_initialdir", workdir );
        if( workdir != null ){
            //PM-961 also associate the value as an environment variable
            job.envVariables.construct( ENV.PEGASUS_SCRATCH_DIR_KEY, workdir);
        }

        applyCredentialsForRemoteExec(job);
    }

    /**
     * Constructs the grid_resource entry for the job. The grid resource is a 
     * tuple consisting of three fields.
     *
     * A CREAM grid resource specification is of the form:
     *
     * grid_resource = cream <web-services-address> <batch-system> <queue-name>
     *
     * The <batch-system> is the name of the batch system that sits behind the CREAM server,
     * into which it submits the jobs. Normal values are pbs, lsf, and condor.
     * It is picked up from the scheduler attribute for the grid gateway entry
     * in the site catalog entry for the site
     *
     * The <queue-name> identifies which queue within the batch system should be used.
     * Values for this will vary by site, with no typical values and are picked up
     * from the globus profile queue associated with the job
     * 
     * @param job  the job
     * 
     * @return the grid_resource entry
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    protected String constructGridResource( Job job ) throws CondorStyleException{
        StringBuffer gridResource = new StringBuffer();
        
        //first field is always condor
        gridResource.append( "cream" ).append( " " );
        
        //PM-962
        this.handleResourceRequirements(job);

        SiteCatalogEntry s = mSiteStore.lookup( job.getSiteHandle() );
        GridGateway g = s.selectGridGateway( job.getGridGatewayJobType() );
        String contact =  ( g == null ) ? null : g.getContact();

        if( contact == null ){
            StringBuffer error = new StringBuffer();
            error.append( "Grid Gateway not specified for site in site catalog  " ).append( job.getSiteHandle() );
            throw new CondorStyleException( error.toString() );

        }

        gridResource.append( g.getContact() ).append( " " );
        
        //the job should have the collector key associated
        GridGateway.SCHEDULER_TYPE scheduler = g.getScheduler();
        if( scheduler.equals( GridGateway.SCHEDULER_TYPE.fork ) || scheduler.equals( GridGateway.SCHEDULER_TYPE.unknown) ){
            StringBuffer error = new StringBuffer();
            error.append( "Please specify a valid scheduler with the grid gateway for site " ).
                  append( job.getSiteHandle() ).append( " and job type " ).
                  append( job.getGridGatewayJobType() );
            throw new RuntimeException( error.toString() );
        }
        gridResource.append( scheduler.toString().toLowerCase()).append( " " );

        String queue = (String) job.globusRSL.removeKey( "queue" );
        if( queue == null ){
            //we assume that collector is running at same place where remote_schedd
            //is running
            StringBuffer message = new StringBuffer();
            message.append( "Globus Profile Queue " ).append( "queue" ).
                  append( " not associated with job " ).append( job.getID() );
            mLogger.log( message.toString(), LogManager.TRACE_MESSAGE_LEVEL );
        }
        gridResource.append( queue );
        
        return gridResource.toString();
    }

    /**
     * This translates the Pegasus resource profiles to corresponding globus 
     * profiles that are used to set CREAMCE parameters
     * 
     * @param job 
     */
    protected void handleResourceRequirements( Job job ) throws CondorStyleException {
        //PM-962 we update the globus RSL keys on basis
        //of Pegasus profile keys before doing any translation
        
        mCondorG.handleResourceRequirements(job);
    }
}
