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
package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.code.CodeGeneratorException;

import java.net.UnknownHostException;

import org.globus.gsi.GlobusCredentialException;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;

import org.globus.gsi.GlobusCredential;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedList;

/**
 * A Metrics file generator that generates a metrics file in the submit directory
 *
 * The following metrics are logged in the metrics file
 *
 * <pre>
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Metrics {


    /**
     * The suffix to use while constructing the name of the metrics file
     */
    public static final String METRICS_FILE_SUFFIX = ".metrics";

    /**
     * The bag of initialization objects.
     */
    protected PegasusBag mBag;


    /**
     * The directory where all the submit files are to be generated.
     */
    protected String mSubmitFileDir;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The object containing the command line options specified to the planner
     * at runtime.
     */
    protected PlannerOptions mPOptions;

    /**
     * The handle to the logging object.
     */
    protected LogManager mLogger;

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        mBag           = bag;
        mProps         = bag.getPegasusProperties();
        mPOptions      = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
        mLogger        = bag.getLogger();
    }

  

    
    /**
     * Generates the code for the executable workflow in terms of a braindump
     * file that contains workflow metadata useful for monitoring daemons etc.
     *
     * @param dag  the concrete workflow.
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode(ADag dag) throws CodeGeneratorException {
        try {

            Collection<File> result = new LinkedList();
            result.add( writeOutMetricsFile( dag) );
            return result;
        } catch (IOException ioe) {
            throw new CodeGeneratorException( "IOException while writing out the braindump file" ,
                                               ioe );
        }
    }
    
    
    /**
     * Method not implemented. Throws an exception.
     * 
     * @param dag  the workflow
     * @param job  the job for which the code is to be generated.
     * 
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    public void generateCode( ADag dag, Job job ) throws CodeGeneratorException {
        throw new CodeGeneratorException( "Metrics generator only generates code for the whole workflow" );
    }
    
    /**
     * Writes out the workflow metrics file in the submit directory
     *
     * @param dag  the final executable workflow
     *
     * @return the absolute path to the braindump file.txt written in the directory.
     *
     * @throws IOException in case of error while writing out file.
     */
    protected File writeOutMetricsFile( ADag dag ) throws IOException{
        
        
        
        //create a writer to the braindump.txt in the directory.
        File f = new File( mSubmitFileDir , Abstract.getDAGFilename( this.mPOptions,
                                                                     dag.dagInfo.nameOfADag,
                                                                     dag.dagInfo.index,
                                                                     Metrics.METRICS_FILE_SUFFIX ) );
        PrintWriter writer =
                  new PrintWriter(new BufferedWriter(new FileWriter(f)));
        
 
        writer.println( "{\n" );
        writer.println( dag.getWorkflowMetrics() );
        writer.write(  "}\n" );

        writer.close();
                
        return f;
    }
   
    /**
     * Returns the submit hostname
     * 
     * @return hostname
     * 
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    protected String getSubmitHostname( ) throws CodeGeneratorException{
        try {
            InetAddress localMachine = java.net.InetAddress.getLocalHost();
            return localMachine.getHostName();
        } catch ( UnknownHostException ex) {
            throw new CodeGeneratorException( "Unable to determine hostname", ex );
        }
    }
    
    /**
     * Returns the distinguished name from the proxy
     * 
     * 
     * @return the DN else null if proxy file not found.
     */
    protected String getGridDN( ){
        String dn = null;
        try {
            
            GlobusCredential credential = GlobusCredential.getDefaultCredential();
                    //new GlobusCredential(proxyFile);

            dn = credential.getIdentity();
        } catch (GlobusCredentialException ex) {
            mLogger.log( "Unable to determine GRID DN", ex, LogManager.DEBUG_MESSAGE_LEVEL );
        }
        return dn;
    }
    
    
}
