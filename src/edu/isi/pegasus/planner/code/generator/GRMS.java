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

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.ADag;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.planner.code.CodeGeneratorException;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.ENV;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * This generates the submit files in the xml format that can be used to submit
 * the workflow to a GRMS server.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class GRMS extends Abstract {

    /**
     * The "official" namespace URI of the GRMS workflow schema.
     */
    public static final String SCHEMA_NAMESPACE = "";

    /**
     * The "not-so-official" location URL of the GRMS workflow schema definition.
     */
    public static final String SCHEMA_LOCATION = "";

    /**
     * The workflow schema to which this writer conforms.
     */
    public static final String SCHEMA = "grms-workflow-schema_10.xsd";

    /**
     * The prefix that needs to be added to the stdout to make GRMS aware of
     * a kickstart output.
     */
    public static final String STDOUT_PREFIX = "kickstart__exitcode__of__";

    /**
     * The version to report.
     */
    public static final String SCHEMA_VERSION = "10";


    /**
     * The LogManager object which is used to log all the messages.
     */
    private LogManager mLogger;

    /**
     * The handle to the output file that is being written to.
     */
    private PrintWriter mWriteHandle;

    /**
     * The handle to the properties file.
     */
    private PegasusProperties mProps;

    /**
     * Handle to the pool provider.
     */
    //private PoolInfoProvider mPoolHandle;
    
    /**
     * Handle to the Site Store.
     */
    private SiteStore mSiteStore;

    /**
     * The default constructor.
     */
    public GRMS( ){
        super();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     *  @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        super.initialize( bag );
        mLogger = bag.getLogger();

        //get the handle to pool file
        mSiteStore = bag.getHandleToSiteStore();

    }

    /**
     * Generates the code for the concrete workflow in the GRMS input format.
     * The GRMS input format is xml based. One XML file is generated per
     * workflow.
     *
     * @param dag  the concrete workflow.
     *
     * @return handle to the GRMS output file.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        String opFileName = this.mSubmitFileDir + File.separator +
                            dag.dagInfo.nameOfADag + ".xml";

        initializeWriteHandle( opFileName );
        Collection result = new ArrayList( 1 );
        result.add( new File( opFileName ) );
        SubInfo job = null;


        writeString("\n<grmsjob " +
                    " xmlns:xsi = \"http://www.w3.org/2001/XMLSchema-instance\" " +
                    " xsi:noNamespaceSchemaLocation=\"" + SCHEMA + "\"" +
                    " appid=\"Pegasus\">");
        for (Iterator it = dag.vJobSubInfos.iterator(); it.hasNext(); ){
            job = (SubInfo)it.next();
            writeString( jobToXML( dag, job ) );
        }
        writeString("\n</grmsjob>");
        mWriteHandle.close();

        return result;
    }

    /**
     * Generates the code for a single job in the input format of the workflow
     * executor being used.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>SubInfo</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, SubInfo job ) throws CodeGeneratorException{
        throw new CodeGeneratorException(
            new UnsupportedOperationException(
                   "Method generateCode( ADag, SubInfo) not yet implemented."));
    }

    /**
     * It returns the corresponding xml description for a particular job.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job       object containing job info.
     *
     * @return the string containing the xml description.
     */
    protected String jobToXML( ADag dag, SubInfo job ){
        StringBuffer sb = new StringBuffer();
        boolean gridstart = true;

        sb.append("\n");
        sb.append("\t<task taskid = \"").append(job.jobName).append("\"").
           append(" persistent=\"").append("true").append("\">");

        //put the resource on which to run the job
        sb.append("\n\t\t<resource>");
        sb.append("\n\t\t\t<hostname>").append(job.globusScheduler).append("</hostname>");
        sb.append("\n\t\t</resource>");

        //at present only one process is launched by the jobmanager
        //while launching the job at GRMS site. No MPI
        if(gridstart){
            //launch the application at the GRMS end through gridstart.
            //i.e gridstart is the launching application
            sb.append(gridstart(job));
        }
        else{
            sb.append(executableToXML(job.executable,job.strargs,null,null,null));
        }

        //set the environment variables
        //hmm they have to appear in the executables tag
        //sb.append(envToXML(job.envVariables));

        //write in the relations
        sb.append(relationsToXML( dag, job ));

        sb.append("\n\t</task>");
        return sb.toString();
    }


    /**
     * This launches a particular job through gridstart, and accordingly
     * changes the stdio , stdin and stderr handling for the launched job.
     * The stdout and stderr of gridstart is propagated back to the submit host
     * to the directory where the output description was generated in tune
     * with how we do things with condor. Assumption is that either the submit
     * file dir is on shared file system, or on the file system accessible to the
     *  gridftp server specified on the local pool.
     *
     * @param job  the job description.
     *
     * @return the xml description containing the executable that is to be
     *         launched and it's arguments.
     */
    private String gridstart(SubInfo job){
//        SiteInfo site = mPoolHandle.getPoolEntry(job.executionPool, "vanilla");
//        SiteInfo submitSite = mPoolHandle.getPoolEntry("local","vanilla");
        
        SiteCatalogEntry site       = mSiteStore.lookup( job.getSiteHandle() );
        SiteCatalogEntry submitSite = mSiteStore.lookup( "local" );
        
        String gridStartPath = site.getKickstartPath();
        boolean isGlobusJob = true;

        //
        // with gridstart section
        //
        StringBuffer gridStartArgs = new StringBuffer();

        //the executable is gridstart, the application becomes its argument
        //writer.println("executable = " + gridStartPath);
        gridStartArgs.append("-n ");

        gridStartArgs.append(job.getCompleteTCName());
        gridStartArgs.append(' ');
        gridStartArgs.append("-N ");
        gridStartArgs.append(job.getCompleteDVName());
        gridStartArgs.append(' ');


        // HANDLING stdin for the moment

        if (job.stdIn.length() > 0) {

            //for using the transfer script the
            //input file is transferred from the
            //submit host by Condor to stdin.
            //We fool the kickstart to pick up
            //the input file from standard stdin
            //by giving the input file name as -
            if(job.logicalName.equals(
                edu.isi.pegasus.planner.transfer.implementation.Transfer.TRANSFORMATION_NAME)
                ||job.logicalName.equals(
                edu.isi.pegasus.planner.transfer.implementation.T2.TRANSFORMATION_NAME)
                ||job.logicalName.equals(edu.isi.pegasus.planner.cluster.aggregator.SeqExec.
                                         COLLAPSE_LOGICAL_NAME)
                ||job.logicalName.equals(edu.isi.pegasus.planner.cluster.aggregator.MPIExec.
                                         COLLAPSE_LOGICAL_NAME)){

                // handle stdin
                if (job.stdIn.length() > 0) {
                    // the output of gridstart is propagated back to the submit host
                    // to the submit file dir at the submit host
                    String stdIn = submitSite.getHeadNodeFS().getScratch().getLocalDirectory().selectFileServer().getURLPrefix() +
                        File.separatorChar + mSubmitFileDir +
                        File.separator + job.jobName + ".in";
                    job.stdIn = stdIn;

                    //writer.println("input = " + job.stdIn);
                    gridStartArgs.append("-i ").append("-").append(' ');
                }
                else{
                    //error in Pegasus Code
                    mLogger.log("Input file not generated for transfer job",
                                LogManager.ERROR_MESSAGE_LEVEL);
                }

            }
            else{
                // gridstart provides the app's *tracked* stdin
                gridStartArgs.append("-i ").append(job.stdIn).append(' ');
            }
        }


        // handle stdout
        if (job.stdOut.length() > 0) {
            // gridstart saves the app's *tracked* stdout
            gridStartArgs.append("-o ").append(job.stdOut).append(' ');
        }
        // the GRMS output variable and kickstart -o option
        // must not point to the same file for any local job.
        if (job.stdOut.equals(job.jobName + ".xml") && !isGlobusJob) {
            System.err.println("WARNING! Detected WAW conflict for stdout");
        }
        // the output of gridstart is propagated back to the submit host
        // to the submit file dir at the submit host
        String stdout = submitSite.getHeadNodeFS().getScratch().getLocalDirectory().selectFileServer().getURLPrefix() +
                        + File.separatorChar +
                        mSubmitFileDir +
                        File.separator + STDOUT_PREFIX + job.jobName + ".xml";
        job.stdOut = stdout;

        //handle the stderr
        if (job.stdErr.length() > 0) {
            // gridstart saves the app's *tracked* stderr
            gridStartArgs.append("-e ").append(job.stdErr).append(' ');
        }
        // the GRMS error variable and kickstart -e option
        // must not point to the same file for any local job.
        if (job.stdErr.equals(job.jobName + ".err") && !isGlobusJob) {
            System.err.println("WARNING! Detected WAW conflict for stderr");
        }

        // the error of gridstart is propagated back to the submit host
        // to the submit file dir at the submit host
        String stderr = submitSite.getHeadNodeFS().getScratch().getLocalDirectory().selectFileServer().getURLPrefix()
                        + File.separatorChar +     mSubmitFileDir +
                        File.separator +  job.jobName + ".err";
        job.stdErr = stderr;

        //GRMS invokes the job in it's own directory
        //make kickstart change to a directory that can be
        //tracked through the VDS by pass -w to kickstart
        gridStartArgs.append("-w ").append( mSiteStore.getWorkDirectory( job ) ).
            append(' ');


        gridStartArgs.append(job.executable).
            append(' ').append(job.strargs);

        //now the application to be launced is now kickstart instead of the
        //original application
        job.executable = gridStartPath;
        job.strargs    = gridStartArgs.toString();
        return executableToXML(job);

    }


    /**
     * This method returns the xml description for the executable that is to be
     * executed, that includes the arguments with which it is to be invoked,
     * the path to the executable and location of it's stdout , stdin and stderr.
     *
     * @param job  the GRMS job.
     *
     * @return String
     */
    protected String executableToXML(SubInfo job){
        StringBuffer sb = new StringBuffer();

        sb.append("\n\t\t<executable type=\"single\" count=\"1\">");
        sb.append("\n\t\t\t<execfile name=\"kickstart\" >").
           append("\n\t\t\t\t<url>").append("file:///").append(job.executable).append("</url>").
           append("\n\t\t\t</execfile>");

       //copy the arguments
       sb.append(argumentsToXML(job));

       if(job.stdIn != null && job.stdIn.length() > 0)
           sb.append(stdInToXML(job.stdIn));
       if(job.stdOut != null && job.stdOut.length() > 0)
           sb.append(stdOutToXML(job.stdOut));
       if(job.stdErr != null && job.stdErr.length() > 0)
           sb.append(stdErrToXML(job.stdErr));

       //set the environment variables
       sb.append(envToXML(job.envVariables));

       sb.append("\n\t\t</executable>");

       return sb.toString();
    }



    /**
     * This method returns the xml description for the executable that is to be
     * executed, that includes the arguments with which it is to be invoked,
     * the path to the executable and location of it's stdout , stdin and stderr.
     *
     * @param path   the path to the executable.
     * @param args   the arguments to the executable.
     * @param stdin  the url for the stdin of the job.
     * @param stdout the url for the stdin of the job.
     * @param stderr the url for the stdin of the job.
     *
     * @return String
     */
    protected String executableToXML(String path,String args,String stdin,
                                  String stdout, String stderr){
        StringBuffer sb = new StringBuffer();

        sb.append("\n\t\t<executable type=\"single\" count=\"1\">");
        sb.append("\n\t\t\t<file name=\"exec-file\" type=\"executable\">").
           append("\n\t\t\t\t<url>").append("file:///").append(path).append("</url>").
           append("\n\t\t\t</file>");

       //copy the arguments
       sb.append(argumentsToXML(null));

       if(stdin != null && stdin.length() > 0)
           sb.append(stdInToXML(stdin));
       if(stdout != null && stdout.length() > 0)
           sb.append(stdOutToXML(stdout));
       if(stderr != null && stderr.length() > 0)
           sb.append(stdErrToXML(stderr));

       sb.append("\n\t\t</executable>");

       return sb.toString();
    }


    /**
     * This method returns the xml description for specifying the stdout.
     *
     * @param url  the url to the stdout file.
     *
     * @return  the xml description.
     */
    protected String stdInToXML(String url){
        StringBuffer sb = new StringBuffer();
        sb.append("\n\t\t\t").append("<stdin>");
        sb.append("\n\t\t\t\t").append("<url>").append(url).append("</url>");
        sb.append("\n\t\t\t").append("</stdin>");
        return sb.toString();
    }


    /**
     * This method returns the xml description for specifying the stdout.
     *
     * @param url  the url to the stdout file.
     *
     * @return  the xml description.
     */
    protected String stdOutToXML(String url){
        StringBuffer sb = new StringBuffer();
        sb.append("\n\t\t\t").append("<stdout>");
        sb.append("\n\t\t\t\t").append("<url>").append(url).append("</url>");
        sb.append("\n\t\t\t").append("</stdout>");
        return sb.toString();
    }

    /**
     * This method returins the xml description for specifying the stdout.
     *
     * @param url  the url to the stdout file.
     *
     * @return  the xml description.
     */
    protected String stdErrToXML(String url){
        StringBuffer sb = new StringBuffer();
        sb.append("\n\t\t\t").append("<stderr>");
        sb.append("\n\t\t\t\t").append("<url>").append(url).append("</url>");
        sb.append("\n\t\t\t").append("</stderr>");
        return sb.toString();
    }


    /**
     * This method returns the xml description of the arguments that are passed
     * to the transformation that is being invoked.
     *
     * @param job the job description.
     *
     * @return  the xml description of the arguments.
     */
    protected String argumentsToXML(SubInfo job){
        StringBuffer sb = new StringBuffer();
        StringTokenizer st = new StringTokenizer(job.strargs);

        //typecast it to a GRMS job
        Iterator it;

        sb.append("\n\t\t\t<arguments>");
        //all the arguments in different tags
        while(st.hasMoreTokens()){
            sb.append("\n\t\t\t\t<value>").append(st.nextToken()).append("</value>");
        }
        //write in the input files
        // Not doing it as using expilicit transfers
        // instead of relying on GRMS to do it
        /*
        it = ((GRMSJob)job).iterator('i');
        while(it.hasNext()){
            NameValue nv = (NameValue)it.next();
            sb.append(urlToXML(nv.getKey(),nv.getValue(),'i'));
        }
        //write in the output files
        it = ((GRMSJob)job).iterator('o');
        while(it.hasNext()){
            NameValue nv = (NameValue)it.next();
            sb.append(urlToXML(nv.getKey(),nv.getValue(),'o'));
        }
        */
        sb.append("\n\t\t\t</arguments>");

        return sb.toString();
    }


    /**
     * This method returns the xml description of the url.
     *
     * @param lfn the logical name of the file associated with the url.
     * @param url the url
     * @param type i input url
     *             o output url
     *
     * @return String
     */
    protected String urlToXML(String lfn, String url,char type){
        StringBuffer sb = new StringBuffer();

        sb.append("\n\t\t\t").append("<file name = \"").append(lfn).
           append("\" type=\"");

       switch(type){
           case 'i':
               sb.append("in");
               break;
           case 'o':
               sb.append("out");
               break;
           default:
               return null;
       }

       sb.append("\">");
       sb.append("\n\t\t\t\t").append("<url>").append(url).append("</url>");
       sb.append("\n\t\t\t").append("</file>");

       return sb.toString();
    }

    /**
     * This method returns the xml description of the relations between the jobs.
     * It refers to the associated ADag object with this class to get hold of the
     * parents to the job.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job  the <code>SubInfo</code> object containing the job description.
     *
     * @return the xml element if there are any dependencies of the job
     *          else an empty string.
     */
    protected String relationsToXML( ADag dag, SubInfo job ){
        StringBuffer sb = new StringBuffer();
        Vector parents  = dag.getParents( job.getName() );
        if(parents.isEmpty())
            return sb.toString();

        sb.append("\n\t\t<workflow>");
        for( Iterator it = parents.iterator(); it.hasNext(); ){
            //all the parents should be finished before invokeing the
            //child.
            sb.append("\n\t\t\t<parent triggerState=\"FINISHED\">").
                append(it.next()).append("</parent>");
        }
        sb.append("\n\t\t</workflow>");
        return sb.toString();
    }

    /**
     * This method returns the xml description of the environment variables
     * associated with the job.
     *
     * @param env  the <code>EnvNS</code> object that contains the environment
     *             variables for the job.
     *
     * @return  the xml element if there are any environment variables
     *          else an empty string.
     */
    protected String envToXML(ENV env){
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;
        Set s = env.keySet();
        Iterator it = (s == null) ? null: s.iterator();
        if(it == null)
            return new String();

        st.append("\n\t\t\t<environment>");
        while(it.hasNext()){
            key = (String)it.next();
            value = (String)env.get(key);
            st.append("\n\t\t\t\t<variable name =\"").append(key).append("\">").
               append(value).append("</variable>");
        }
        st.append("\n\t\t\t</environment>");
        return st.toString();

    }

    /**
     * Returns the xml header for the output xml file.
     *
     * @return String
     */
    private String getXMLHeader(){
        String st = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " ;
        return st;

    }

    /**
     * It initializes the write handle to the output file.
     *
     * @param filename  the name of the file to which you want the write handle.
     */
    private void initializeWriteHandle(String filename){
        try {
            mWriteHandle = new PrintWriter(new FileWriter(filename));
            mLogger.log("Writing to file " + filename , LogManager.DEBUG_MESSAGE_LEVEL);
        }
        catch (Exception e) {
            mLogger.log("Error while initialising handle to file " + e.getMessage(),
                        LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }

        writeString(this.getXMLHeader());
    }

    /**
     * Writes a string to the associated write handle with the class
     *
     * @param st  the string to be written.
     */
    private void writeString(String st){
        //try{
            //write the xml header
            mWriteHandle.println(st);
        /*}
        catch(IOException ex){
            System.out.println("Error while writing to xml " + ex.getMessage());
        }*/
    }

}
