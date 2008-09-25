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
package org.griphyn.logsummary;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.visualize.Callback;
import org.griphyn.cPlanner.visualize.JobMeasurements;
import org.griphyn.cPlanner.visualize.nodeusage.NumJobsMeasurement;
import org.griphyn.vdl.invocation.Machine;
import org.griphyn.vdl.invocation.StatInfo;
/**
 * Call back implementation for ShowLogSummary class. 
 *
 * @author Atul Kumar
 *
 * @version $Revision: 554 $
 */
public class JobstateCallback implements Callback{

    public static final LogManager mLogger =  LogManagerFactory.loadSingletonInstance();
        
    /**
     * The name of the log file.
     */
    public static final String JOBSTATE_LOG = "jobstate.log";
    
    /**
     * The name of the file which keeps count of the number of lines read
     * from the log file.
     */
    public static final String JOBSTATE_LINECOUNT = "jobstateLineCount.log";
    
    /**
     * The name of the brain dump file to get the version
     * information.
     */
    public static final String BRAINDUMP = "braindump.txt";
    
	/**
	 * Data structure to store meta data
	 */
	private JobStateMeasurement measure = new JobStateMeasurement();	
	
	public void cbInputFile(String filename, StatInfo info) {				
	}

	public void cbInvocationEnd() {				
	}

	public void cbInvocationStart(String job, String resource) {		
	}

	public void cbOutputFile(String filename, StatInfo info) {
	}
	public void cbStdERR(List jobs, String data) {		
		measure.getMetadata().put("stderr",data);
	}

	public void cbStdIN(List jobs, String data) {			
		measure.getMetadata().put("stdin",data);
	}

	public void cbStdOut(List jobs, String data) {	
		measure.getMetadata().put("stdout",data);
	}

	public void done() {				
	}

	public Object getConstructedObject() {		
		return measure;
	}

	public void initialize(String directory, boolean useStatInfo) {        
        File jobstate = new File( directory, this.JOBSTATE_LOG );
        File linecount = new File( directory, this.JOBSTATE_LINECOUNT );        
        
        //some sanity checks on file
        if ( jobstate.exists() ){
            if ( !jobstate.canRead() ){
                throw new RuntimeException( "The jobstate file does not exist " + jobstate );
            }
        }
        else{
            throw new RuntimeException( "Unable to read the jobstate file " + jobstate );
        }
        int count = 0;
        BufferedReader countreader = null;
        if ( linecount.exists() ){
        	try {
				countreader = new BufferedReader( new FileReader( linecount ) );
				String line = countreader.readLine();				
				if ( line != null) {
	            	count = Integer.parseInt(line);
	            }
			} catch (FileNotFoundException e) {				
				e.printStackTrace();				
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			finally{
	        	try {
	        		if(countreader != null) countreader.close();
				} catch (IOException e) {					
					e.printStackTrace();
				}
	        }
        }                
                                  
        BufferedReader reader = null;
        BufferedWriter writer = null;
        int lineno = 0;
        try{
            reader = new BufferedReader( new FileReader( jobstate ) );
            String line;                        
            while ( (line = reader.readLine()) != null) {
            	lineno++;
            	//if(lineno <= count) continue;
            	processLineJobState(line);
            }
            writer = new BufferedWriter( new FileWriter( linecount ) );
            writer.write(Integer.toString(lineno));           
        }
        catch( IOException ioe ){
            throw new RuntimeException( "While reading jobstate file " + jobstate, ioe );
        }
        finally{
        	try {
        		if(reader != null) reader.close();
        		if(writer != null)  writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        setPegasusVersionAndBuild(directory);
	}

	private void processLineJobState(String line) {
    	String[] tokens = line.split(" ");
        if(tokens[1].startsWith("INTERNAL")) return;
        if(tokens[2].startsWith("POST_SCRIPT_FAILURE") || tokens[2].startsWith("PRE_SCRIPT_FAILURE")){
        	measure.addFailedJob(tokens[1]);
        }		
	}

	private void setPegasusVersionAndBuild(String directory) {
		File braindump = new File( directory, this.BRAINDUMP );
		//some sanity checks on file
        if ( braindump.exists() ){
            if ( !braindump.canRead() ){
            	mLogger.log( "The braindump file does not exist " + braindump , LogManager.DEBUG_MESSAGE_LEVEL );
            }
        }
        else{
        	mLogger.log( "Unable to read the braindump file " + braindump , LogManager.DEBUG_MESSAGE_LEVEL );
        }
        BufferedReader braindumpreader = null;

        try{
        	braindumpreader = new BufferedReader( new FileReader( braindump ) );
            String line;                        
            while ( (line = braindumpreader.readLine()) != null) {
            	
            	String[] tokens = line.split(" ");            
                if(tokens[0].equals("pegasus_version")){                	
                	measure.getMetadata().put("pegasus_version", tokens[tokens.length - 1]);
                }
                else if(tokens[0].equals("pegasus_build")){
                	measure.getMetadata().put("pegasus_build", tokens[tokens.length - 1]);
                }
            }                      
        }
        catch( IOException ioe ){
        	mLogger.log( "While reading braindump file " + braindump + ioe, LogManager.DEBUG_MESSAGE_LEVEL );
        }
        finally{
        	try {
        		if(braindumpreader != null) braindumpreader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		
	}	
	
        /*
	public Map cbMetadata() {		
		return measure.getMetadata();		
	}*/
        /**
         * Callback for the metadata retrieved from the kickstart record.
         * 
         * @param metadata
         */
        public void cbMetadata( Map metadata ){
            measure.setMetadata(metadata);
            
        }
        
    /**
     * Callback to pass the machine information on which the job is executed.
     * 
     * @param machine
     */
    public void cbMachine(Machine machine) {
    }
}
