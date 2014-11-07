/**
 *  Copyright 2007-2014 University Of Southern California
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

package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.code.generator.Metrics;
import edu.isi.pegasus.planner.namespace.ENV;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A helper class, that mimics the functionality of pegasus-submit-dag, and 
 * generates a condor.sub for dagman using the condor_submit_dag -nosubmit option.
 * 
 *
 * @author Karan Vahi
 */
public class PegasusSubmitDAG {
    
    /**
     * The Bag of Pegasus initialization objects.
     */
    private PegasusBag mBag;
    
    private LogManager mLogger;
    
    public PegasusSubmitDAG(){
        
    }
    
    public void intialize( PegasusBag bag ){
        mBag = bag;
        mLogger = bag.getLogger();
    }
    
    /**
     * 
     * @param dag  the executable workflow.
     * @param dagFile
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag, File dagFile )throws CodeGeneratorException{
        Collection<File> result = new ArrayList();
        
        //find path to condor_submit_dag
        File condorSubmitDAG = FindExecutable.findExec( "condor_submit_dag" );
        if( condorSubmitDAG == null ){
            throw new CodeGeneratorException( "Unable to find path to condor_submit_dag" );
        }
        
        //construct arguments for condor_submit_dag
        String args = getCondorSubmitDagArgs(dag, dagFile );
       
        
        try{
            //set the callback and run the pegasus-run command
            Runtime r = Runtime.getRuntime();
            String invocation = condorSubmitDAG.getAbsolutePath() + " " + args;
            mLogger.log( "Executing  " + invocation,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            Process p = r.exec( invocation );

            //spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                new StreamGobbler( p.getInputStream(), new DefaultStreamGobblerCallback(
                                                                   LogManager.CONSOLE_MESSAGE_LEVEL ));
            StreamGobbler eps =
                new StreamGobbler( p.getErrorStream(), new PSDErrorStreamGobblerCallback(
                                                             LogManager.ERROR_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            //wait for the threads to finish off
            ips.join();
            eps.join();

            //get the status
            int status = p.waitFor();

            mLogger.log( "condor_submit_dag exited with status " + status,
                         LogManager.DEBUG_MESSAGE_LEVEL );

            if( status != 0 ){
                throw new CodeGeneratorException( "Command failed with non zero exit status " + invocation );
            }
        }
        catch(IOException ioe){
            mLogger.log("IOException while running condor_submit_dag ", ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new CodeGeneratorException( "IOException while running condor_submit_dag " , ioe );
        }
        catch( InterruptedException ie){
            //ignore
        }
        
        //we have the .condor.sub file now.
        File dagSubmitFile = new File( dagFile.getAbsolutePath() + ".condor.sub" );
        //sanity check to ensure no disconnect
        if( !dagSubmitFile.canRead() ){
            throw new CodeGeneratorException( "Unable to read the dagman condor submit file " + dagSubmitFile );
        }
        
        if( !modifyDAGManSubmitFileForMetrics( dagSubmitFile) ){
            mLogger.log( "DAGMan metrics reporting not enabled for dag " + dagFile, LogManager.DEBUG_MESSAGE_LEVEL );
        }
        
        
        return result;
    }

    /**
     * Modifies the dagman condor submit file for metrics reporting. 
     * 
     * @param file
     * @return true if file is modified, else false
     * 
     * @throws CodeGeneratorException 
     */
    protected boolean modifyDAGManSubmitFileForMetrics( File file ) throws CodeGeneratorException{
        //modify the environment string to add the environment for
        //enabling DAGMan metrics if so required.
        Metrics metricsReporter = new Metrics();
        metricsReporter.initialize(mBag);
        ENV env = metricsReporter.getDAGManMetricsEnv();
        if( env.isEmpty() ){
            return false;
        }
        else{
            //we read the DAGMan submit file in and grab the environment from it
            //and add the environment key to the second last line with the
            //Pegasus metrics environment variables added.
            try{
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                String dagmanEnvString = "";
                String line   = null;
                long previous = raf.getFilePointer();
                while( (line=raf.readLine()) != null ){
                    if( line.startsWith( "environment" ) ){
                        dagmanEnvString = line;
                    }
                    if( line.startsWith( "queue" ) ){
                        //backtrack to previous file position i.e just before queue
                        raf.seek(previous);
                        StringBuilder dagmanEnv = new StringBuilder( dagmanEnvString );
                        if( dagmanEnvString.isEmpty() ){
                            dagmanEnv.append( "environment=");
                        }
                        else{
                            dagmanEnv.append( ";" );
                        }
                        for( Iterator it = env.getProfileKeyIterator(); it.hasNext(); ){
                            String key = (String) it.next();
                            dagmanEnv.append( key ).append("=").append( env.get( key ) ).append( ";" );
                        }
                        mLogger.log( "Updated environment for dagman is " + dagmanEnv.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
                        raf.writeBytes( dagmanEnv.toString() );
                        raf.writeBytes( System.getProperty("line.separator", "\r\n") );
                        raf.writeBytes( "queue" );
                        break;
                    }
                    previous = raf.getFilePointer();
                }
                
                raf.close();
            }
            catch (IOException e) {
                 throw new CodeGeneratorException( "Error while reading dagman .condor.sub file " + file,
                                                  e );
            }
            
           
        }
        return true;
    }
    
    /**
     * Returns the arguments that need to be passed to condor_submit_dag to generate the
     * .condor.sub file corresponding to the dag file
     * 
     * @param dag       the dag
     * @param dagFile   the condor .dag file
     * @return  arguments
     * 
     * @throws CodeGeneratorException 
     */
    protected String getCondorSubmitDagArgs(ADag dag, File dagFile ) throws CodeGeneratorException{
        /*
        push( @arg, '-MaxPre', $maxpre ) if $maxpre > 0;
push( @arg, '-MaxPost', $maxpost ) if $maxpost > 0;
push( @arg, '-maxjobs', $maxjobs ) if $maxjobs > 0;
push( @arg, '-maxidle', $maxidle ) if $maxidle > 0;
push( @arg, '-notification', $notify );
push( @arg, '-verbose' ) if $verbose;
push( @arg, '-append', 'executable='.$dagman ) if $dagman;
push( @arg, '-append', '+pegasus_wf_uuid="'.$config{'wf_uuid'}.'"' );
push( @arg, '-append', '+pegasus_root_wf_uuid="'.$config{'root_wf_uuid'}.'"' );
push( @arg, '-append', '+pegasus_wf_name="'.$config{'pegasus_wf_name'}.'"' );
push( @arg, '-append', '+pegasus_wf_time="'.$config{timestamp}.'"' );
push( @arg, '-append', '+pegasus_version="'.$config{'planner_version'}.'"' );
push( @arg, '-append', '+pegasus_job_class=11' );
push( @arg, '-append', '+pegasus_cluster_size=1' );
push( @arg, '-append', '+pegasus_site="local"' );
push( @arg, '-append', '+pegasus_wf_xformation="pegasus::dagman"' );
        */
        StringBuilder args = new StringBuilder();
        
        //append the executable path to pegasus-dagman
        File pegasusDAGMan = FindExecutable.findExec( "pegasus-dagman" );
        if( pegasusDAGMan == null ){
            throw new CodeGeneratorException( "Unable to determine path to pegasus-dagman" );
        }
        
        args.append( "-append " ).append( "executable" ).
                 append( "=" ).append( pegasusDAGMan.getAbsolutePath() ).
                 append( " " );
        
        Map<String,Object> entries = new LinkedHashMap();
        //the root workflow and workflow uuid
        entries.put( ClassADSGenerator.WF_UUID_KEY,      dag.getWorkflowUUID()  );
        entries.put( ClassADSGenerator.ROOT_WF_UUID_KEY, dag.getRootWorkflowUUID() );
        entries.put( ClassADSGenerator.WF_NAME_AD_KEY,   dag.getFlowName() );
        //the workflow time
        if ( dag.getMTime() != null) {
            entries.put( ClassADSGenerator.WF_TIME_AD_KEY, dag.getFlowTimestamp());
        }
        entries.put( ClassADSGenerator.VERSION_AD_KEY,   dag.getReleaseVersion() );
        
        //update entries with some hardcode pegasus dagman specific ones
        entries.put( "pegasus_job_class", 11 );
        entries.put( "pegasus_cluster_size", 1 );
        entries.put( "pegasus_site", "local" );
        entries.put( "pegasus_wf_xformation", "pegasus::dagman" );
        
        //we do a -no_submit option
        args.append( "-no_submit " );
        //also for safety case -update_submit or -force?
        args.append( "-force " );
        
        //construct all the braindump entries as append options to dagman
        for( Map.Entry<String,Object> entry: entries.entrySet() ){
            String key = entry.getKey();
            args.append( "-append " ).append( "+" ).append( key );
            Object value = entry.getValue();
            if( value instanceof String ){
                args.append( "=\"" ).append( entry.getValue() ).append( "\"" );
            }
            else{
                args.append( "=" ).append( entry.getValue() );
            }
            args.append( " " );
            
        }
        
        //the last argument is the path to the .dag file
        args.append( dagFile.getAbsolutePath() );
        
        return args.toString();
    }

    private static class PSDErrorStreamGobblerCallback implements StreamGobblerCallback {

        public static final String IGNORE_LOG_LINE= "Renaming rescue DAGs newer than number 0";
        
        /**
          * 
          */
        private int mLevel;

        /**
         * The instance to the logger to log messages.
         */
        private LogManager mLogger;

        /**
         * The overloaded constructor.
         *
         * @param level   the level on which to log.
         */
        public PSDErrorStreamGobblerCallback(int level) {
            //should do a sanity check on the levels
            mLevel  = level;
            mLogger    = LogManagerFactory.loadSingletonInstance(  );
        }

        /**
         * Callback whenever a line is read from the stream by the StreamGobbler.
         * The line is logged to the level specified while initializing the
         * class.
         *
         * @param line   the line that is read.
         */
        public void work(String line) {
            if( !line.startsWith( IGNORE_LOG_LINE ) ){
                mLogger.log( line , mLevel);
            }
        }

    }
}
