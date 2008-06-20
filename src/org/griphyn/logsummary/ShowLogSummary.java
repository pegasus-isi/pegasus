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

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.toolkit.Executable;
import org.griphyn.cPlanner.visualize.Callback;
import org.griphyn.cPlanner.visualize.KickstartParser;
import org.griphyn.common.util.FactoryException;
import org.griphyn.common.util.Separator;
import org.griphyn.common.util.Version;
import org.griphyn.vdl.toolkit.FriendlyNudge;
/**
 * This parses the jobstate.log records and generate the 
 * log for the failed jobs.
 *
 * @author Atul Kumar
 *
 * @version $Revision: 554 $
 */

/**
 * @author akumar
 *
 */
public class ShowLogSummary extends Executable{
	 /**
     * The input directory containing the kickstart records.
     */
    private String mInputDir;
    
    /**
     * The logging level to be used.
     */
    private int mLoggingLevel;
    /**
     * Default constructor.
     */
    public ShowLogSummary(){
        super();
        mLogMsg = new String();
        mVersion = Version.instance().toString();        
        mLoggingLevel = 0;        
    }

	/**
	 * The main method
	 * @param args is the command line arguments
	 */
	public static void main(String[] args){
		
		ShowLogSummary sum = new ShowLogSummary(); 
		int result = 0;
		double starttime = new Date().getTime();
		double execTime  = -1;

		try{
			sum.executeCommand( args );
		}
		catch ( FactoryException fe){
			sum.log( fe.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
			result = 2;
		}
		catch ( RuntimeException rte ) {
			//catch all runtime exceptions including our own that
			//are thrown that may have chained causes
			sum.log( convertException(rte),
					LogManager.FATAL_MESSAGE_LEVEL );
			result = 1;
		}
		catch ( Exception e ) {
			//unaccounted for exceptions
			sum.log(e.getMessage(),
					LogManager.FATAL_MESSAGE_LEVEL );
			e.printStackTrace();
			result = 3;
		} finally {
			double endtime = new Date().getTime();
			execTime = (endtime - starttime)/1000;
		}

		// warn about non zero exit code
		if ( result != 0 ) {
			sum.log("Non-zero exit-code " + result,
					LogManager.WARNING_MESSAGE_LEVEL );
		}
		else{
			//log the time taken to execute
			/*sum.log("Time taken to execute is " + execTime + " seconds",
					LogManager.INFO_MESSAGE_LEVEL);*/
		}

		System.exit(result);
	}
	/* (non-Javadoc)
	 * @see org.griphyn.cPlanner.toolkit.Executable#executeCommand(java.lang.String[])
	 */
	public void executeCommand(String[] args) {		
	    parseCommandLineArguments(args);

        //set logging level only if explicitly set by user
        if( mLoggingLevel > 0 ) { mLogger.setLevel( mLoggingLevel ); }


        //do sanity check on input directory
        if( mInputDir == null ){
            throw new RuntimeException(
                "You need to specify the directory containing kickstart records");

        }

		
		Callback c = new JobstateCallback();
		c.initialize(mInputDir, true);
		JobStateMeasurement jsmeasure = (JobStateMeasurement)c.getConstructedObject(); 		
		List failedJobs = jsmeasure.getFailedJobs();
		
		if((failedJobs == null) || (failedJobs.size() == 0)) {
			System.out.println("No Failed Jobs");
			return;
		}
		Iterator litr = failedJobs.iterator();
		File directory = new File( mInputDir );
		if ( directory.isDirectory() ){
			//see if it is readable
			if ( !directory.canRead() ){
				throw new RuntimeException( "Cannot read directory " + mInputDir);
			}
		}
		else{
			throw new RuntimeException( mInputDir + " is not a directory " );
		}

		KickstartParser su = new KickstartParser();
		
		su.setCallback(c);
		
		while(litr.hasNext()){
			String jobname = (String)litr.next();
			String[] files = directory.list( new JobStateParserFileFilter(jobname) );
			String filename = null;
			int k = -1;
			for(int i = 0; i < files.length; ++i){
				String[] temp = files[i].split("\\.");
				int t = Integer.parseInt(temp[temp.length - 1]);
				if(k < t){
					k = t;
					filename = files[i];
				}
			}
			String file = mInputDir + File.separator +  filename;
						
			try {
				log( "Parsing file " + file , LogManager.DEBUG_MESSAGE_LEVEL );
				su.parseKickstartFile(file);
			} 
			catch (IOException ioe) {
				log( "Unable to parse kickstart file " + file + convertException( ioe ),
						LogManager.DEBUG_MESSAGE_LEVEL);
			}
			catch( FriendlyNudge fn ){
				log( "Problem parsing file " + file + convertException( fn ),
						LogManager.WARNING_MESSAGE_LEVEL );
			}
			setPegasusJobClass(mInputDir, jobname, jsmeasure) ;
			printSummary(jobname, jsmeasure);						
		}	
	}
	
	/**
	 * prints the out put information 
	 * @param jobname name of the failed job 
	 * @param jsmeasure data structure to maintain the job information
	 */
	private void printSummary(String jobname, JobStateMeasurement jsmeasure) {
		
		System.out.println("************************BEGIN**********************");		
		System.out.println("Jobname                  = " + jobname);
		System.out.println("Status                   = FAILED");
		System.out.println("Exitcode                 = " + ((List)jsmeasure.getMetadata().get("exitcodes")).get(0));
		System.out.println("Transformation           = " + jsmeasure.getMetadata().get("transformation"));
		System.out.println("Job Class                = " + jsmeasure.getMetadata().get("pegasus_job_class"));
		System.out.println("Resource                 = " + jsmeasure.getMetadata().get("resource"));
		System.out.println("Host Name                = " + jsmeasure.getMetadata().get("hostname"));
		if(mLoggingLevel > 0){
		System.out.println("Host Address             = " + jsmeasure.getMetadata().get("hostaddr"));
		System.out.println("Remote Executable        = " + ((List)jsmeasure.getMetadata().get("executables")).get(0));
		System.out.println("Parameters               = " + ((List)jsmeasure.getMetadata().get("arguments")).get(0));
		}
		System.out.println("Start of job             = " + jsmeasure.getMetadata().get("start"));		
		System.out.println("Duration of job(seconds) = " + jsmeasure.getMetadata().get("duration"));
		if(mLoggingLevel > 0){
		System.out.println("User ID                  = " + jsmeasure.getMetadata().get("uid"));
		System.out.println("User Name                = " + jsmeasure.getMetadata().get("user"));
		System.out.println("Group ID                 = " + jsmeasure.getMetadata().get("gid"));
		System.out.println("Group Name               = " + jsmeasure.getMetadata().get("group"));				
		System.out.println("Pegasus Build            = " + jsmeasure.getMetadata().get("pegasus_build"));
		System.out.println("Pegasus Version          = " + jsmeasure.getMetadata().get("pegasus_version"));
		System.out.println("**********************STD ERR**********************");			
		System.out.println(jsmeasure.getMetadata().get("stderr"));
		System.out.println("***********************STD IN**********************");
		System.out.println(jsmeasure.getMetadata().get("stdin"));
		System.out.println("***********************STD OUT*********************");
		System.out.println(jsmeasure.getMetadata().get("stdout"));
		}
		System.out.println("************************END************************");
	}

	/**
	 * It parses the command line
	 * @param args command line arguments
	 */
	private void parseCommandLineArguments(String[] args) {
		LongOpt[] longOptions = generateValidOptions();

		Getopt g = new Getopt( "jobstate-summary", args,
				"i:hvV",
				longOptions, false);
		g.setOpterr(false);

		int option = 0;

		while( (option = g.getopt()) != -1){
			switch (option) {
			case 'i'://dir
				this.mInputDir =  g.getOptarg();
				break;

			case 'h'://help
				printLongVersion();
				System.exit( 0 );
				return;
	
			case 'v'://verbose
				mLoggingLevel++;
				break;

			case 'V'://version
				mLogger.log(getGVDSVersion(),LogManager.INFO_MESSAGE_LEVEL);
				System.exit(0);


			default: //same as help
				printShortVersion();
			throw new RuntimeException("Incorrect option or option usage " +
					(char)option);

			}
		}		
	}
	/* (non-Javadoc)
	 * @see org.griphyn.cPlanner.toolkit.Executable#generateValidOptions()
	 */
	public LongOpt[] generateValidOptions() {
		LongOpt[] longopts = new LongOpt[4];
		longopts[0]   = new LongOpt( "input", LongOpt.REQUIRED_ARGUMENT, null, 'i' );
		longopts[1]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
		longopts[2]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
		longopts[3]   = new LongOpt( "Version", LongOpt.NO_ARGUMENT, null, 'V' );
		return longopts;
	}
	/* (non-Javadoc)
	 * @see org.griphyn.cPlanner.toolkit.Executable#loadProperties()
	 */
	public void loadProperties() {
		
	}
	/* (non-Javadoc)
	 * @see org.griphyn.cPlanner.toolkit.Executable#printLongVersion()
	 */
	public void printLongVersion() {
	     String text =
	           "\n $Id: ShowLogSummary.java 554 2008-06-20 23:45:48Z akumar $ " +
	           "\n " + getGVDSVersion() +
	           "\n jobstate-summary - A debugging tool that show the information about failed jobs."  +
	           "\n Usage: plot_space_usage --i <input directory>  " +
	           "\n [--v(erbose)] [--V(ersion)] [--h(elp)] " +
	           "\n" +
	           "\n Mandatory Options " +
	           "\n --i                 the directory where the log files reside." +
	           "\n Other Options  " +
	           "\n -v |--verbose       increases the verbosity of messages about what is going on" +
	           "\n -V |--version       displays the version of the Pegasus Workflow Planner" +
	           "\n -h |--help          generates this help." +
	           "\n ";

	        System.out.println(text);
	}
	/* (non-Javadoc)
	 * @see org.griphyn.cPlanner.toolkit.Executable#printShortVersion()
	 */
	public void printShortVersion() {
		String text =
			"\n $Id: ShowLogSummary.java 554 2008-06-20 23:45:48Z akumar $ " +
			"\n " + getGVDSVersion() +
			"\n Usage : jobstate-summary -i <input directory>  " +			
			" [-v] [-V] [-h]";

		System.out.println(text);
	}
	/**
	 * This reads the .sub file to get the pegasus job class information
	 * @param directory directory where the .sub fiel is located
	 * @param jobname job name
	 * @param measure data structure to maintain the job information
	 */
	private void setPegasusJobClass(String directory, String jobname, JobStateMeasurement measure) {		
		File subfile = new File( directory, jobname + ".sub");
		//some sanity checks on file
		if ( subfile.exists() ){
			if ( !subfile.canRead() ){
				mLogger.log( "The .sub file does not exist " + subfile , LogManager.DEBUG_MESSAGE_LEVEL );
			}
		}
		else{
			mLogger.log( "Unable to read the .sub file " + subfile , LogManager.DEBUG_MESSAGE_LEVEL );
		}
		BufferedReader subfilereader = null;

		try{
			subfilereader = new BufferedReader( new FileReader( subfile ) );
			String line;                        
			while ( (line = subfilereader.readLine()) != null) {
				String[] tokens = line.split(" ");           
								
				if(tokens[0].equals("+pegasus_job_class")){                	
					measure.getMetadata().put("pegasus_job_class", tokens[tokens.length - 1]);
				}
			}                      
		}
		catch( IOException ioe ){
			mLogger.log( "While reading .sub file " + subfile + ioe, LogManager.DEBUG_MESSAGE_LEVEL );
		}
		finally{
			try {
				if(subfilereader != null) subfilereader.close();
			} catch (IOException e) {				
				e.printStackTrace();
			}
		}

	}	
}
