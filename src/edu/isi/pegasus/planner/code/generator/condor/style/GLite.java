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

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.SubInfo;

import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParser;
import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParserException;

import java.util.Iterator;
import org.griphyn.cPlanner.classes.TransferJob;

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
 *      +remote_queue  - value picked up from globus profile queue
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
 * The following globus profiles if associated with the job are picked up
 * <pre>
 * hostcount  -> PROCS
 * count      -> NODES
 * maxwalltime-> WALLTIME
 * </pre>
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
 *   && PASSENV==1 && JOBNAME==\"TEST JOB\" && MYENV ==\"GAURANG=MEHTA,KARAN=VAHI\""
 * </pre>
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
     * The default Constructor.
     */
    public GLite() {
        super();
    }

    

    /**
     * Applies the gLite style to the job.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( SubInfo job ) throws CondorStyleException {
        String execSiteWorkDir = mSiteStore.getWorkDirectory( job );
        String workdir = (String) job.globusRSL.removeKey( "directory" ); // returns old value
        workdir = (workdir == null) ? execSiteWorkDir : workdir;

        /* universe is always set to grid*/
        job.condorVariables.construct( Condor.UNIVERSE_KEY,Condor.GRID_UNIVERSE );
        
        /* figure out the remote scheduler. should be specified with the job*/
        if( !job.condorVariables.containsKey( Condor.GRID_RESOURCE_KEY )  ){
            throw new CondorStyleException( missingKeyError( job, Condor.GRID_RESOURCE_KEY ) );
        }
        
        /* remote_initialdir does not work with gLite
         * Rely on Gridstart to do the right thing */
        job.condorVariables.construct( "remote_initialdir", workdir );
        job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY, "true" );
        
        /* transfer_executable does not work with gLite
         * Explicitly set to false */
        job.condorVariables.construct( Condor.TRANSFER_EXECUTABLE_KEY, "false" );
        
        /* retrieve some keys from globus rsl and convert to gLite format */
        if( job.globusRSL.containsKey( "queue" ) ){
            job.condorVariables.construct(  "+remote_queue" , (String)job.globusRSL.get( "queue" ) );
        }
        
        /* convert some condor keys and globus keys to remote ce requirements 
         +remote_cerequirements = blah */
        job.condorVariables.construct( "+remote_cerequirements", getCERequirementsForJob( job ) );

        /* do special handling for jobs scheduled to local site
         * as condor file transfer mechanism does not work
         * Special handling for the JPL cluster */
        if( job.getSiteHandle().equals( "local" ) && job instanceof TransferJob ){
                /* remove the change dir requirments for the 
                 * third party transfer on local host */
                job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY, "false" );
                job.condorVariables.removeKey( "remote_initialdir" );
        }
        
        /* similar handling for registration jobs */
        if( job.getSiteHandle().equals( "local" ) && job.getJobType() == SubInfo.REPLICA_REG_JOB ){
                /* remove the change dir requirments for the 
                 * third party transfer on local host */
                job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY, "false" );
                job.condorVariables.removeKey( "remote_initialdir" );
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
     * hostcount  -> PROCS
     * count      -> NODES
     * maxwalltime-> WALLTIME
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
    protected String getCERequirementsForJob( SubInfo job ) throws CondorStyleException {
        StringBuffer value = new StringBuffer();

        //do quoting ourselves
        value.append( "\"" );

        /* append the job name */
        /* job name cannot have - or _ */
        String id = job.getID().replace( "-", "" );
        id = id.replace( "_", "" );
        //the jobname in case of pbs can only be 15 characters long
        id = ( id.length() > 15 )? id.substring( 0, 15 ) : id;

        /* Not adding JOBNAME the GAHP keeps on crashing if specified
         * on pollux. Karan Feb 18, 2010
        addSubExpression( value, "JOBNAME" , id   );
         */

        /* always have PASSENV to true */
        //value.append( " && ");
        addSubExpression( value, "PASSENV", 1 );

        /* specifically pass the queue in the requirement since
           some versions dont handle +remote_queue correctly */
        if( job.globusRSL.containsKey( "queue" ) ){
            value.append( " && ");
            addSubExpression( value, "QUEUE", (String)job.globusRSL.get( "queue" ) );
        }
        
        
        /* the globus key hostCount is PROCS */
        if( job.globusRSL.containsKey( "hostcount" ) ){
            value.append( " && " );
            addSubExpression( value, "PROCS" , Integer.parseInt( (String)job.globusRSL.get( "hostcount" ) ) )  ;
        }
        
        /* the globus key count is NODES */
        if( job.globusRSL.containsKey( "count" ) ){
            value.append( " && " );
            addSubExpression( value, "NODES" , Integer.parseInt( (String)job.globusRSL.get( "count" ) ) );
        }
        
        /* the globus key maxwalltime is WALLTIME */
        if( job.globusRSL.containsKey( "maxwalltime" ) ){
            value.append( " && " );
            addSubExpression( value,"WALLTIME" , Integer.parseInt( (String)job.globusRSL.get( "maxwalltime" ) ) );            
        }
        
        /* the condor key priority is PRIORITY */
        if( job.condorVariables.containsKey( "priority" ) ){
            value.append( " && " );
            addSubExpression( value, "PRIORITY" , Integer.parseInt( (String)job.condorVariables.get( "priority" ) ) );            
        }
        
        /* add the environment that is to be associated with the job */
        StringBuffer env = new StringBuffer();
        for( Iterator it = job.envVariables.getProfileKeyIterator(); it.hasNext(); ){
           String key = (String)it.next();
           env.append( key ).append( "=" ).append( job.envVariables.get(key) );
           if( it.hasNext() ){
               env.append( "," );
           }
        }
        if( env.length() > 0 ){
            value.append( " && " );
            addSubExpression( value, "MYENV" , env.toString() );
        }
        
        //No quoting to be applied
        // JIRA PM-109
        //return this.quote( value.toString() );


        //do quoting ourselves
        value.append( "\"" );

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
        sb.append( key ).append( "==" ).
           append( "\\" ).append( "\"" ).
           append( value ).
           append( "\\" ).append( "\"" );
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
    protected String missingKeyError( SubInfo job, String key ){
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
            mLogger.log("Unquoted Prejob is  " + string, LogManager.DEBUG_MESSAGE_LEVEL);
            result = CondorQuoteParser.quote( string, true );
            mLogger.log("Quoted Prejob is  " + result, LogManager.DEBUG_MESSAGE_LEVEL );
        }
        catch (CondorQuoteParserException e) {
            throw new CondorStyleException("CondorQuoting Problem " +
                                       e.getMessage());
        }
        return result;

    }


}
