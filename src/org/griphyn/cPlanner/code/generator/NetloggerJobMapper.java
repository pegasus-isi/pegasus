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

package org.griphyn.cPlanner.code.generator;

import edu.isi.pegasus.common.logging.LogFormatterFactory;
import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LoggingKeys;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.SubInfo;


import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

/**
 * This class can write out the job mappings that link jobs with jobs in the DAX
 * to a Writer stream in the netlogger format.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class NetloggerJobMapper{
    
    public static final String NETLOGGER_LOG_FORMATTER_IMPLEMENTOR = "Netlogger";
   
    /**
     * The handle to the netlogger log formatter.
     */
    private LogFormatter mLogFormatter;
            
    /**
     * The default constructor.
     */
    public NetloggerJobMapper(){
        mLogFormatter = LogFormatterFactory.loadInstance( NETLOGGER_LOG_FORMATTER_IMPLEMENTOR );
    }
    
    /**
     * Writes out the job mappings for a workflow.
     * 
     * @param writer  the writer stream to which to write out the mappings
     * @param dag     the dag for which to write out the mappings
     * 
     * @throws IOException
     */
    public void writeOutMappings( Writer writer , ADag dag ) throws IOException{
        
        for( Iterator<SubInfo> it = dag.jobIterator(); it.hasNext(); ){
            SubInfo job = it.next();
            mLogFormatter.addEvent( "pegasus.job", LoggingKeys.JOB_ID, job.getID() );
            writer.write( generateLogEvent( job, "" ) );
            writer.write( "\n" );
            mLogFormatter.popEvent();
            
            if ( job instanceof AggregatedJob ){
                //explicitly exclude cleanup jobs that are instance
                //of aggregated jobs. This is because while creating
                //the cleanup job we use the clone method. To be fixed.
                //Karan April 17 2009
                if( job.getJobType() == SubInfo.CLEANUP_JOB ){
                    continue;
                }
                
                AggregatedJob j = (AggregatedJob)job;
                for( Iterator<SubInfo> jit = j.constituentJobsIterator(); jit.hasNext(); ){
                    SubInfo cJob = jit.next();
                    
                    mLogFormatter.addEvent( "pegasus.job.map", LoggingKeys.JOB_ID, job.getID() );                            
                    writer.write( generateLogEvent( cJob,  "constituent." ) );
                    writer.write( "\n" );
                    mLogFormatter.popEvent();
                }
            }
        }
    }

    /**
     * Generates a log event message in the netlogger format for a job
     * 
     * @param job      the job
     * @param prefix   prefix if any to add to the keys
     * 
     * @return netlogger formatted message
     */
    private String generateLogEvent ( SubInfo job,  String prefix ) {
        String result = null;
        String taskID = (( job.getJobType() == SubInfo.COMPUTE_JOB || 
                              job.getJobType() == SubInfo.STAGED_COMPUTE_JOB ) &&
                                !(job instanceof AggregatedJob) )?
                        job.getLogicalID():
                        "";
        mLogFormatter.add( getKey( prefix, "task.id" ), taskID );
        mLogFormatter.add( getKey( prefix, "job.class" ), Integer.toString( job.getJobType() ) );
        mLogFormatter.add( getKey( prefix, "job.class.description" ), job.getJobTypeDescription() );
        mLogFormatter.add( getKey( prefix, "job.transformation" ), job.getCompleteTCName() );
        result = mLogFormatter.createLogMessage();
        return result;
    }
    
    /**
     * Adds a prefix to the key and returns it.
     * 
     * @param prefix  the prefix to be added
     * @param key     the key
     * 
     * @return  the key with prefix added.
     */
    private String getKey( String prefix, String key ){
        if( prefix == null || prefix.length() == 0 ){
            return key;
        }
        StringBuffer result = new StringBuffer();
        result.append( prefix ).append( key);
        return result.toString();
    }
}