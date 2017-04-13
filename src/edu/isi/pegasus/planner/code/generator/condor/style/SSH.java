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

import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;




/**
 * Enables a job to be directly submitted to a remote PBS cluster using direct
 * ssh submission available as part of BOSCO
 *
 * The CREAM CE support in Condor is documented at the following link
 *
 * <pre>
 * http://bosco.opensciencegrid.org
 * </pre>
 *
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SSH extends GLite{

    /**
     * The key that designates the collector associated with the job
     */
    public static final String GRID_RESOURCE_KEY =
            edu.isi.pegasus.planner.namespace.Condor.GRID_RESOURCE_KEY;

    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "SSH";

    /**
     * The default constructor.
     */
    public SSH() {
        super();
    }

    /**
     * Applies the CREAM CE style to the job.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException{
        //construct the grid_resource for the job
        String gridResource = constructGridResource( job );
        job.condorVariables.construct( SSH.GRID_RESOURCE_KEY, gridResource.toString() );
       
        //glite and ssh submission for now only differ in the grid_resource
        //construction
        super.apply(job);


    }

    /**
     * Constructs the grid_resource entry for the job. The grid resource is a 
     * tuple consisting of three fields.
     *
     * A SSH grid resource specification is of the form:
     *
     * grid_resource = batch <batch-system> remote_username@batch-headnode-hostname
     *
     * The <batch-system> is the name of the batch system that we are submitting to. 
     * Normal values are pbs, lsf, and condor.
     * It is picked up from the scheduler attribute for the grid gateway entry
     * in the site catalog entry for the site
     *
     * @param job  the job
     * 
     * @return the grid_resource entry
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    protected String constructGridResource( Job job ) throws CondorStyleException{
        StringBuffer gridResource = new StringBuffer();
        
 

        SiteCatalogEntry s = mSiteStore.lookup( job.getSiteHandle() );
        GridGateway g = s.selectGridGateway( job.getGridGatewayJobType() );
        String contact =  ( g == null ) ? null : g.getContact();

        //first field is the type of grid gateway
        gridResource.append( g.getType()  ).append( " " );



        if( contact == null ){
            StringBuffer error = new StringBuffer();
            error.append( "Grid Gateway not specified for site in site catalog  " ).append( job.getSiteHandle() );
            throw new CondorStyleException( error.toString() );

        }

        //the job should have a scheduler specified
        GridGateway.SCHEDULER_TYPE scheduler = g.getScheduler();
        if( scheduler.equals( GridGateway.SCHEDULER_TYPE.fork ) || scheduler.equals( GridGateway.SCHEDULER_TYPE.unknown) ){
            StringBuffer error = new StringBuffer();
            error.append( "Please specify a valid scheduler with the grid gateway for site " ).
                  append( job.getSiteHandle() ).append( " and job type " ).
                  append( job.getGridGatewayJobType() );
            throw new RuntimeException( error.toString() );
        }
        gridResource.append( scheduler.toString().toLowerCase()).append( " " );

        gridResource.append( g.getContact() ).append( " " );
        
        
/*
        String queue = (String) job.globusRSL.removeKey( "queue" );
        if( queue == null ){
            
            StringBuffer message = new StringBuffer();
            message.append( "Globus Profile Queue " ).append( "queue" ).
                  append( " not associated with job " ).append( job.getID() );
            mLogger.log( message.toString(), LogManager.TRACE_MESSAGE_LEVEL );
        }
        else{
            gridResource.append( queue );
        }
*/
        return gridResource.toString();
    }

}
