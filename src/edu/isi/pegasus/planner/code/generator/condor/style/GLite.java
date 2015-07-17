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

import edu.isi.pegasus.common.util.GliteEscape;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.classes.Job;
import static edu.isi.pegasus.planner.classes.Profile.ENV;

import edu.isi.pegasus.planner.namespace.Condor;

import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParser;
import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParserException;

import java.util.Iterator;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.generator.condor.CondorEnvironmentEscape;
import edu.isi.pegasus.planner.namespace.Pegasus;

/**
 * This implementation enables a job to be submitted via gLite to a
 * grid sites. This is the style applied when job has a pegasus profile style key
 * with value GLite associated with it.
 *
 *
 * <p>
 * This style should only be used when the condor on the submit host can directly
 * talk to scheduler running on the cluster. In Pegasus there should  be a separate
 * compute site that has this style associated with it. This style should not be
 * specified for the local site.
 *
 * As part of applying the style to the job, this style adds the following
 * classads expressions to the job description
 * <pre>
 *      batch_queue  - value picked up from globus profile queue or can be
 *                     set directly as a Condor profile named batch_queue
 *      +remote_cerequirements - See below
 * </pre>
 *
 * <p>
 * The remote CE requirements are constructed from the following profiles
 * associated with the job.The profiles for a job are derived from various
 * sources
 *  - user properties
 *  - transformation catalog
 *  - site catalog
 *  - DAX
 *
 * Note it is upto the user to specify these or a subset of them.
 *
 * The following globus profiles if associated with the job are picked up and
 * translated to +remote_cerequirements key in the job submit files.
 * <pre>
 *
 * hostcount    -> NODES
 * xcount       -> PROCS
 * maxwalltime  -> WALLTIME
 * totalmemory  -> TOTAL_MEMORY
 * maxmemory    -> PER_PROCESS_MEMORY
 * </pre>
 *
 *
 * The following condor profiles if associated with the job are picked up
 * <pre>
 * priority  -> PRIORITY
 * </pre>
 *
 * All the env profiles are translated to MYENV
 *
 * For e.g. the expression in the submit file may look as
 * <pre>
 * +remote_cerequirements = "PROCS==18 && NODES==1 && PRIORITY==10 && WALLTIME==3600
 *   && PASSENV==1 && JOBNAME==\"TEST JOB\" && MYENV ==\"MONTAGE_HOME=/usr/montage,JAVA_HOME=/usr\""
 * </pre>
 *
 * The pbs_local_attributes.sh file in share/pegasus/htcondor/glite picks up
 * these values and translated to appropriate PBS parameters
 *
 * <pre>
 * NODES                 -> nodes
 * PROCS                 -> ppn
 * WALLTIME              -> walltime
 * TOTAL_MEMORY          -> mem
 * PER_PROCESS_MEMORY    -> pmem
 * </pre>
 *
 *
 *
 * All the jobs that have this style applied dont have a remote directory
 * specified in the submit directory. They rely on kickstart to change to the
 * working directory when the job is launched on the remote node.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class GLite extends Abstract {
    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "GLite";


    /**
     * The Condor remote directory classad key to be used with Glite
     */
    public static final String CONDOR_REMOTE_DIRECTORY_KEY = "+remote_iwd";
    
    /**
     * The condor key to set the remote environment via BLAHP
     */
    public static final String CONDOR_REMOTE_ENVIRONMENT_KEY = "+remote_environment";
    
    /**
     * Handle to escaping class for environment variables
     */
    private CondorEnvironmentEscape mEnvEscape;
    
    private CondorG mCondorG;

    /**
     * The default Constructor.
     */
    public GLite() {
        super();
        mEnvEscape = new CondorEnvironmentEscape();
        mCondorG = new CondorG();
    }



    /**
     * Applies the gLite style to the job.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( Job job ) throws CondorStyleException {

        String workdir = job.getDirectory();

        /* universe is always set to grid*/
        job.condorVariables.construct( Condor.UNIVERSE_KEY,Condor.GRID_UNIVERSE );

        /* figure out the remote scheduler. should be specified with the job*/
        if( !job.condorVariables.containsKey( Condor.GRID_RESOURCE_KEY )  ){
            throw new CondorStyleException( missingKeyError( job, Condor.GRID_RESOURCE_KEY ) );
        }


        job.condorVariables.construct( GLite.CONDOR_REMOTE_DIRECTORY_KEY,
                                       workdir == null ? null : quote(workdir) );

        //also set it as an environment variable, since for MPI jobs
        //glite and BLAHP dont honor +remote_iwd and we cannot use kickstart
        //the only way to get it to work is for the wrapper around the mpi
        //executable to a cd to the directory pointed to by this variable.
        if( workdir != null ){
            job.envVariables.construct( "_PEGASUS_SCRATCH_DIR", workdir);
        }

        /* transfer_executable does not work with gLite
         * Explicitly set to false */
        //PM-950 looks like it works now. for pegasus lite modes we need
        //the planner to be able to set it to true
        //job.condorVariables.construct( Condor.TRANSFER_EXECUTABLE_KEY, "false" );

        /* retrieve some keys from globus rsl and convert to gLite format */
        if( job.globusRSL.containsKey( "queue" ) ){
            job.condorVariables.construct(  "batch_queue" , (String)job.globusRSL.get( "queue" ) );
        }

        /* convert some condor keys and globus keys to remote ce requirements
         +remote_cerequirements = blah */
        job.condorVariables.construct( "+remote_cerequirements", getCERequirementsForJob( job ) );
        
        /*
         PM-934 construct environment accordingly
        */
        job.condorVariables.construct( GLite.CONDOR_REMOTE_ENVIRONMENT_KEY, 
                                       mEnvEscape.escape( job.envVariables ) );
        job.envVariables.reset();

        /* do special handling for jobs scheduled to local site
         * as condor file transfer mechanism does not work
         * Special handling for the JPL cluster */
        if( job.getSiteHandle().equals( "local" ) && job instanceof TransferJob ){
                /* remove the change dir requirments for the
                 * third party transfer on local host */
                job.condorVariables.removeKey( GLite.CONDOR_REMOTE_DIRECTORY_KEY );
        }

        /* similar handling for registration jobs */
        if( job.getSiteHandle().equals( "local" ) && job.getJobType() == Job.REPLICA_REG_JOB ){
                /* remove the change dir requirments for the
                 * third party transfer on local host */
                job.condorVariables.removeKey( GLite.CONDOR_REMOTE_DIRECTORY_KEY );
        }

        if ( job.getSiteHandle().equals("local") ) {
            applyCredentialsForLocalExec(job);
        }
        else {
            applyCredentialsForRemoteExec(job);
        }
    }



    /**
     * Constructs the value for remote CE requirements expression for the job .
     *
     * For e.g. the expression in the submit file may look as
     * <pre>
     * +remote_cerequirements = "PROCS==18 && NODES==1 && PRIORITY==10 && WALLTIME==3600
     *   && PASSENV==1 && JOBNAME==\"TEST JOB\" && MYENV ==\"GAURANG=MEHTA,KARAN=VAHI\""
     *
     * </pre>
     *
     * The requirements are generated on the basis of certain profiles associated
     * with the jobs.
     * The following globus profiles if associated with the job are picked up
     * <pre>
     * hostcount    -> NODES
     * xcount       -> PROCS
     * maxwalltime  -> WALLTIME
     * totalmemory  -> TOTAL_MEMORY
     * maxmemory    -> PER_PROCESS_MEMORY
     * </pre>
     *
     * The following condor profiles if associated with the job are picked up
     * <pre>
     * priority  -> PRIORITY
     * </pre>
     *
     * All the env profiles are translated to MYENV
     *
     * @param job
     *
     * @return the value to the expression and it is condor quoted
     *
     * @throws CondorStyleException in case of condor quoting error
     */
    protected String getCERequirementsForJob( Job job ) throws CondorStyleException {
        StringBuffer value = new StringBuffer();

        /* append the job name */
        /* job name cannot have - or _ */
        String id = job.getID().replace( "-", "" );
        id = id.replace( "_", "" );
        //the jobname in case of pbs can only be 15 characters long
        id = ( id.length() > 15 )? id.substring( 0, 15 ) : id;

        //add the jobname so that it appears when we do qstat
        addSubExpression( value, "JOBNAME" , id   );
	    value.append( " && ");

        /* always have PASSENV to true */
        //value.append( " && ");
        addSubExpression( value, "PASSENV", 1 );

        /* specifically pass the queue in the requirement since
           some versions dont handle +remote_queue correctly */
        if( job.globusRSL.containsKey( "queue" ) ){
            value.append( " && ");
            addSubExpression( value, "QUEUE", (String)job.globusRSL.get( "queue" ) );
        }

        //PM-962 we update the globus RSL keys on basis
        //of Pegasus profile keys before doing any translation
        //hack.
        mCondorG.handleResourceRequirements(job);

        /* the globus key hostCount is NODES */
        if( job.globusRSL.containsKey( "hostcount" ) ){
            value.append( " && " );
            addSubExpression( value, "NODES" ,  (String)job.globusRSL.get( "hostcount" ) )  ;
        }

        /* the globus key xcount is PROCS */
        if( job.globusRSL.containsKey( "xcount" ) ){
            value.append( " && " );
            addSubExpression( value, "PROCS" ,  (String)job.globusRSL.get( "xcount" )  );
        }

        /* the globus key maxwalltime is WALLTIME */
        if( job.globusRSL.containsKey( "maxwalltime" ) ){
            value.append( " && " );
            addSubExpression( value,"WALLTIME" , pbsFormattedTimestamp(   (String)job.globusRSL.get( "maxwalltime" ) ) );
        }

        /* the globus key maxmemory is PER_PROCESS_MEMORY */
        if( job.globusRSL.containsKey( "maxmemory" ) ){
            value.append( " && " );
            addSubExpression( value, "PER_PROCESS_MEMORY" ,  (String)job.globusRSL.get( "maxmemory" )  );
        }

        /* the globus key totalmemory is TOTAL_MEMORY */
        if( job.globusRSL.containsKey( "totalmemory" ) ){
            value.append( " && " );
            addSubExpression( value, "TOTAL_MEMORY" ,  (String)job.globusRSL.get( "totalmemory" )  );
        }

        /* the condor key priority is PRIORITY */
        if( job.condorVariables.containsKey( "priority" ) ){
            value.append( " && " );
            addSubExpression( value, "PRIORITY" , Integer.parseInt( (String)job.condorVariables.get( "priority" ) ) );
        }
        
        /* the pegasus key glite.arguments is EXTRA_ARGUMENTS */
        if( job.vdsNS.containsKey( Pegasus.GLITE_ARGUMENTS_KEY ) ){
            value.append( " && " );
            addSubExpression( value, "EXTRA_ARGUMENTS" , (String)job.vdsNS.get( Pegasus.GLITE_ARGUMENTS_KEY   ) );
        }

        return value.toString();
    }


    /**
     * Adds a sub expression to a string buffer
     *
     * @param sb    the StringBuffer
     * @param key   the key
     * @param value the value
     */
    protected void addSubExpression( StringBuffer sb, String key, String value ) {
        //PM-802
        sb.append( key ).append( "==" )
           .append( "\"" )
           .append( value )
           .append( "\"" );
    }


    /**
     * Adds a sub expression to a string buffer
     *
     * @param sb    the StringBuffer
     * @param key   the key
     * @param value the value
     */
    protected void addSubExpression( StringBuffer sb, String key, Integer value ) {
        sb.append( key ).append( "==" ).append( value );
    }

     /**
     * Constructs an error message in case of style mismatch.
     *
     * @param job      the job object.
     * @param key      the missing key
     */
    protected String missingKeyError( Job job, String key ){
        StringBuffer sb = new StringBuffer();
        sb.append( "( " ).
             append( "Missing key " ).append( key ).
             append( " for job " ).append( job.getName() ).
             append( "with style " ).append( STYLE_NAME );

         return sb.toString();
    }

    /**
     * Condor Quotes a string
     *
     * @param string   the string to be quoted.
     *
     * @return quoted string.
     *
     * @throws CondorStyleException in case of condor quoting error
     */
    private String quote( String string ) throws CondorStyleException{
        String result;
        try{
            mLogger.log("Unquoted string is  " + string, LogManager.TRACE_MESSAGE_LEVEL);
            result = CondorQuoteParser.quote( string, true );
            mLogger.log("Quoted string is  " + result, LogManager.TRACE_MESSAGE_LEVEL );
        }
        catch (CondorQuoteParserException e) {
            throw new CondorStyleException("CondorQuoting Problem " +
                                       e.getMessage());
        }
        return result;

    }

    /**
     * Converts minutes into hh:dd:ss for PBS formatting purposes
     * 
     * @param minutes
     * 
     * @return 
     */
    public String pbsFormattedTimestamp(String minutes ) {
        int minutesValue = Integer.parseInt(minutes);
        
        if( minutesValue < 0 ){
            throw new IllegalArgumentException( "Invalid value for minutes provided for conversion " + minutes );
        }
        
        int hours = minutesValue/60;
        int mins   = minutesValue%60;
        
        StringBuffer result = new StringBuffer();
        if( hours < 10 ){
            result.append( "0" ).append( hours );
        }
        else{
            result.append( hours );
        }
        result.append(":");
        if( mins < 10 ){
            result.append( "0" ).append( mins );
        }
        else{
            result.append( mins );
        }
        result.append( ":00" );//we don't have second precision
        
        return result.toString();
        
    }

    public static void main(String[] args ){
        GLite gl = new GLite();
        
        System.out.println( "0 mins is " + gl.pbsFormattedTimestamp( "0") );
        System.out.println( "11 mins is " + gl.pbsFormattedTimestamp( "11") );
        System.out.println( "60 mins is " + gl.pbsFormattedTimestamp( "60") );
        System.out.println( "69 mins is " + gl.pbsFormattedTimestamp( "69") );
        System.out.println( "169 mins is " + gl.pbsFormattedTimestamp( "169") );
        System.out.println( "1690 mins is " + gl.pbsFormattedTimestamp( "1690") );
    }

}
