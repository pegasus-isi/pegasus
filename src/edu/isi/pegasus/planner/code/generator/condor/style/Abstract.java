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

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleFactoryException;
import edu.isi.pegasus.planner.common.PegasusProperties;


import java.util.Iterator;
import java.util.Set;

import java.util.Map;

/**
 * An abstract implementation of the CondorStyle interface.
 * Implements the initialization method.
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
     * Handle to the Credential Handler Factory
     */
    protected CredentialHandlerFactory mCredentialFactory ;

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
     * @param credentialFactory   the credential handler factory
     *
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public void initialize( PegasusBag bag , CredentialHandlerFactory credentialFactory )throws CondorStyleException{

        mProps     = bag.getPegasusProperties();
        mSiteStore = bag.getHandleToSiteStore();
        mLogger    = bag.getLogger();
        mCredentialFactory = credentialFactory;
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

    /**
     * Empty implementation.
     * 
     * @param site  the site catalog entry object
     * 
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( SiteCatalogEntry site ) throws CondorStyleException{
        //do nothing
    }

    /**
     * Examines the credential requirements for a job and adds appropiate
     * transfer and environment directives for the credentials to be staged
     * and picked up by the job.
     * @param job
     */
    protected void applyCredentialsForRemoteExec(Job job) throws CondorStyleException {
        //sanity check
        if ( !job.requiresCredentials() ) {
            return;
        }
        
        applyCredentialsForJobSubmission( job );
        
        // jobs can have multiple credential requirements
        //and may need credentials associated with different sites PM-731
        for( Map.Entry<String,Set<CredentialHandler.TYPE>> entry : job.getCredentialTypes().entrySet()  ){
            String siteHandle = entry.getKey();
            for( CredentialHandler.TYPE credType: entry.getValue()){
                CredentialHandler handler = mCredentialFactory.loadInstance( credType );
            
                // if the credential is listed in the remote sites environment, don't do anything
                SiteCatalogEntry site = mSiteStore.lookup(job.getSiteHandle());
                if (site.getEnvironmentVariable(handler.getProfileKey()) != null &&
                        siteHandle.equals( job.getSiteHandle() )) {
                    //the user has the enviornment variable specified in the site
                    //catalog pointing to an existing credential on the remote 
                    //site and the job is going to run on the site for which we
                    //need the credential
                    continue;
                }
                
                //make sure we can have a path to credential
                String credentialPath = handler.getPath( siteHandle );
                if ( credentialPath == null ){
                    this.complainForCredential( job, handler.getProfileKey(), siteHandle );
                }
                
                switch(credType) {
                    case x509:
                        //check if x509userproxy not already set. can be set
                        //as part of credentials for job submission
                        if ( job.condorVariables.containsKey( Condor.X509USERPROXY_KEY) ){
                            //we can transfer the credential only via condor file io
                            //sanity check to to make sure not same as already set
                            String existing = (String)job.condorVariables.get( Condor.X509USERPROXY_KEY);
                            if( !existing.equals( credentialPath)){
                                job.condorVariables.addIPFileForTransfer( credentialPath );
                                job.envVariables.construct(handler.getEnvironmentVariable( siteHandle ), handler.getBaseName( siteHandle ) );
                            }
                        }
                        else{
                            //PM-1099 set the x509userproxy key directly
                            //we don's set the environment variable based on site name
                            //as for GRAM submissions, the proxy is renmaed by GRAM on the
                            //remote end tp the x509_user_proxy when placed in ~/.globus/job 
                            //directory. GRAM then sets X509_USER_PROXY env variable to reflect
                            //the path to the proxy.
                            job.condorVariables.construct( Condor.X509USERPROXY_KEY, credentialPath );
                        }
                        
                        break;
                        
                    case irods:
                    case s3:
                    case boto:
                    case googlep12:
                    case ssh:
                        // transfer using condor file transfer, and advertise in env
                        // but first make sure it is specified in our environment
                        /*String path = handler.getPath( siteHandle );
                        if ( path == null ){
                            this.complainForCredential( job, handler.getProfileKey(), siteHandle );
                        }*/
                        job.condorVariables.addIPFileForTransfer( credentialPath );
                        job.envVariables.construct(handler.getEnvironmentVariable( siteHandle ), handler.getBaseName( siteHandle ) );
                        break;

                    default:
                        throw new CondorStyleException("Job has been tagged with unknown credential type");

                }
            }//for each credential for each site
        }
        
    }

    
    /**
     * Examines the credential requirements for a job and adds appropiate
     * transfer and environment directives for the credentials to be picked
     * up for the local job
     * @param job
     */
    protected void applyCredentialsForLocalExec(Job job) throws CondorStyleException {
        //sanity check
        if ( !job.requiresCredentials() ) {
            return;
        }
       
        //associate any credentials if reqd for job submission.
        this.applyCredentialsForJobSubmission(job);
        
        // jobs can have multiple credential requirements
        //and may need credentials associated with different sites PM-731
        for( Map.Entry<String,Set<CredentialHandler.TYPE>> entry : job.getCredentialTypes().entrySet()  ){
            // jobs can have multiple credential requirements
            String siteHandle = entry.getKey();
            for( CredentialHandler.TYPE credType: entry.getValue()){
                CredentialHandler handler = mCredentialFactory.loadInstance( credType );
        
                switch(credType) {

                    case x509:
                    case irods:
                    case s3:
                    case boto:
                    case googlep12:
                    case ssh:
                        // for local exec, just set envionment variables to full path
                        String path = handler.getPath( siteHandle );
                        if ( path == null) {
                            this.complainForCredential( job, handler.getProfileKey(), siteHandle );
                        }
                        job.envVariables.construct(handler.getEnvironmentVariable( siteHandle ), path );
                        break;

                    default:
                        throw new CondorStyleException("Job has been tagged with unknown credential type");

                }
            }
            
        }
        
    }

    
    /**
     * Associates credentials required for job submission.
     * 
     * @param job
     * @throws CondorStyleException 
     */
    protected void applyCredentialsForJobSubmission(Job job) throws CondorStyleException {
        //handle credential for job submission if set
        if( job.getSubmissionCredential() == null ){
            return;
        }
        //set the proxy for job submission
        CredentialHandler.TYPE cred = job.getSubmissionCredential();
        CredentialHandler handler = mCredentialFactory.loadInstance( cred ); 
        String path = handler.getPath( job.getSiteHandle());
        if ( path == null) {
            this.complainForCredential( job, handler.getProfileKey(), job.getSiteHandle() );
        }
        switch( cred ) {
            case x509:
                job.condorVariables.construct( Condor.X509USERPROXY_KEY, path );
                break;
            default:
                //only job submission via x509 is explicitly supported
                throw new CondorStyleException( "Invalid credential type for job submission " + cred + " for job " + job.getName() );

        }
        return;
    }
    
    /**
     * Complain if a particular credential key is not found for a site 
     * 
     * @param job
     * @param key
     * @param site
     * @throws CondorStyleException 
     */
    protected void complainForCredential( Job job, String key , String site ) throws CondorStyleException{
        StringBuilder error = new StringBuilder();
        
        error.append( "Unable to find required credential for file transfers for job ").
              append( job.getName() ).append(  " . Please make sure that the key " ).append( key ).
              append(  " is set as a Pegasus profile in the site catalog for site ").append( site ).
              append( " or in your environment.");
        throw new CondorStyleException( error.toString() );
    }
    
    /**
     * Constructs an error message in case of style mismatch.
     *
     * @param job      the job object.
     * @param style    the name of the style.
     * @param universe the universe associated with the job.
     */
    protected String errorMessage( Job job, String style, String universe){
        StringBuilder sb = new StringBuilder();
        sb.append( "( " ).
             append( style ).append( "," ).
             append( universe ).append( "," ).
             append( job.getSiteHandle() ).
             append( ")" ).
             append( " mismatch for job " ).append( job.getName() );

         return sb.toString();
    }
}
