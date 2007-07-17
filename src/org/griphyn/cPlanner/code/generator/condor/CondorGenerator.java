/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.code.generator.condor;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.DagInfo;
import org.griphyn.cPlanner.classes.PCRelation;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.code.CodeGenerator;
import org.griphyn.cPlanner.code.CodeGeneratorException;
import org.griphyn.cPlanner.code.GridStart;
import org.griphyn.cPlanner.code.POSTScript;

import org.griphyn.cPlanner.code.gridstart.GridStartFactory;

import org.griphyn.cPlanner.code.generator.Abstract;
import org.griphyn.cPlanner.code.generator.CodeGeneratorFactory;

import org.griphyn.cPlanner.code.generator.condor.style.CondorStyleFactory;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.StreamGobbler;
import org.griphyn.cPlanner.common.StreamGobblerCallback;
import org.griphyn.cPlanner.common.DefaultStreamGobblerCallback;

import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.namespace.Dagman;
import org.griphyn.cPlanner.namespace.Globus;
import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.vdl.euryale.VTorInUseException;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.transformation.TCMode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.Writer;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * This class generates the condor submit files for the DAG which has to
 * be submitted to the Condor DagMan.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorGenerator extends Abstract {

    /**
     * The nice separator, define once, use often.
     */
    public  static final String mSeparator =
        "######################################################################";




    /**
     * The LogManager object which is used to log all the messages.
     */
    protected LogManager mLogger;

    /**
     * The variable containing the message to be logged.
     */
    private String mLogMsg;



    /**
     * Handle to the Transformation Catalog.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * Handle to the pool provider.
     */
    private PoolInfoProvider mPoolHandle;

    /**
     * Defines the read mode for transformation catalog and pool.config.
     * Whether we want to read all at once or as desired.
     *
     * @see org.griphyn.common.catalog.transformation.TCMode
     */
    private String mTCMode;

    /**
     * Specifies the implementing class for the pool interface. Contains
     * the name of the class that implements the pool interface the user has
     * asked at runtime.
     */
    protected String mPoolClass;

    /**
     * Maps pool handles to project names. This is needed for some remote
     * scheduling systems (not limited to LSF) for accounting purposes.
     */
    private java.util.Map mProjectMap;

    /**
     * Maps pool handles to queue names. This is needed for some remote
     * scheduling systems (not limited to LSF) for accounting purposes.
     */
    private java.util.Map mQueueMap;

    /**
     * Maps pool handles to walltimes for that pool.
     */
    private java.util.Map mWalltimeMap;

    /**
     * The file handle to the .dag file. A part of the dag file is printed
     * as we write the submit files, to insert the appropriate postscripts
     * for handling exit codes.
     */
    protected PrintWriter mDagWriter;


    /**
     * The name of the log file in the /tmp directory
     */
    protected String mTempLogFile;

    /**
     * A boolean indicating whether the files have been generated or not.
     */
    protected boolean mDone;

    /**
     * The workflow for which the code has to be generated.
     */
    protected ADag mConcreteWorkflow;

    /**
     * Handle to the Style factory, that is used for this workflow.
     */
    protected CondorStyleFactory mStyleFactory;

    /**
     * The handle to the GridStart Factory.
     */
    protected GridStartFactory mGridStartFactory;

    /**
     * A boolean indicating whether grid start has been initialized or not.
     */
    protected boolean mInitializeGridStart;


    /**
     * The default constructor.
     */
    public CondorGenerator(){
        super();
        mLogger = LogManager.getInstance();
        mInitializeGridStart = true;
        mStyleFactory     = new CondorStyleFactory();
        mGridStartFactory = new GridStartFactory();
    }

    /**
     * Initializes the Code Generator implementation. Initializes the various
     * writers.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param directory  the base directory where the generated code should reside.
     * @param options    the options passed to the planner at runtime.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusProperties properties,
                            String directory,
                            PlannerOptions options) throws CodeGeneratorException{

        super.initialize( properties, directory, options );

        //create the base directory recovery
        File wdir = new File(mSubmitFileDir);
        wdir.mkdirs();


        mTCHandle = TCMode.loadInstance();
        String poolmode = mProps.getPoolMode();
        mPoolClass = PoolMode.getImplementingClass(poolmode);
        mPoolHandle = PoolMode.loadPoolInstance(mPoolClass,mProps.getPoolFile(),
                                                PoolMode.SINGLETON_LOAD);
        mProjectMap  = constructMap(mProps.getRemoteSchedulerProjects());
        mQueueMap    = constructMap(mProps.getRemoteSchedulerQueues());
        mWalltimeMap = constructMap(mProps.getRemoteSchedulerMaxWallTimes());

        //instantiate and intialize the style factory
        mStyleFactory.initialize( properties, mPoolHandle );
    }



    /**
     * Generates the code for the concrete workflow in Condor DAGMAN and CondorG
     * input format.
     *
     * @param dag  the concrete workflow.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag ) throws CodeGeneratorException{
        DagInfo ndi        = dag.dagInfo;
        Vector vSubInfo    = dag.vJobSubInfos;

        if ( mInitializeGridStart ){
            mConcreteWorkflow = dag;
            mGridStartFactory.initialize( mProps, mSubmitFileDir, dag );
            mInitializeGridStart = false;
        }


        CodeGenerator storkGenerator = CodeGeneratorFactory.loadInstance(
                                  mProps, mPOptions, this.mSubmitFileDir, "Stork");

        String className   = this.getClass().getName();
        String dagFileName = getDAGFilename( dag, ".dag" );
        mDone = false;

        if (ndi.dagJobs.isEmpty()) {
            //call the callout before returns
            concreteDagEmpty( dagFileName, dag );
            return ;
        } else {
            //initialize the file handle to the dag
            //file and print it's header
            initializeDagFileWriter( dagFileName, ndi );
        }

        //Create a file in the /tmp for the log and symlink it to the submit directory.
        try{
           File f = File.createTempFile( dag.dagInfo.nameOfADag + "-" +
                                         dag.dagInfo.index,".log", null );
           mTempLogFile=f.getAbsolutePath();
        } catch (IOException ioe) {
            mLogger.log("Error while creating an empty log file in " +
                        "the local temp directory " + ioe.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
        }


        mLogger.log("Writing the Condor submit files ", LogManager.DEBUG_MESSAGE_LEVEL);
        for(Iterator it = vSubInfo.iterator();it.hasNext();){
            //get information about each job making the ADag
            SubInfo sinfo = (SubInfo) it.next();

            //write out the submit file for each job
            //in addition makes the corresponding
            //entries in the .dag file corresponding
            //to the job and it's postscript
            if ( sinfo.getSiteHandle().equals("stork") ) {
                //write the job information in the .dag file
                StringBuffer dagString = new StringBuffer();
                dagString.append("DaP ").append( sinfo.getName() ).append(" ");
                dagString.append( sinfo.getName() ).append(".sub");
                printDagString( dagString.toString() );
                storkGenerator.generateCode( dag, sinfo);
            }
            else {
                //write out a condor submit file
                generateCode( dag, sinfo );
            }

            mLogger.log("Written Submit file : " +
                        getFileBaseName(sinfo), LogManager.DEBUG_MESSAGE_LEVEL);
        }
        mLogger.logCompletion("Writing the Condor submit files",
                              LogManager.DEBUG_MESSAGE_LEVEL);

        //writing the tail of .dag file
        //that contains the relation pairs
        this.writeDagFileTail(ndi);
        mLogger.log("Written Dag File : " + dagFileName.toString(),
                    LogManager.DEBUG_MESSAGE_LEVEL);

        //symlink the log file to a file in the temp directory if possible
        this.generateLogFileSymlink( this.getLogFileName(),
                                     this.getLogFileSymlinkName( dag ) );


        //write out the DOT file
        mLogger.log( "Writing out the DOT file ", LogManager.DEBUG_MESSAGE_LEVEL );
        this.writeDOTFile( getDAGFilename( dag, ".dot"), dag );

        //we are done
        mDone = true;
    }

    /**
     * Generates the code (condor submit file) for a single job.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>SubInfo</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, SubInfo job ) throws CodeGeneratorException{
        String dagname  = dag.dagInfo.nameOfADag;
        String dagindex = dag.dagInfo.index;
        String dagcount = dag.dagInfo.count;
        String subfilename = this.getFileBaseName( job );
        String envStr = null;

        //initialize GridStart if required.
        if ( mInitializeGridStart ){
            mConcreteWorkflow = dag;
            mGridStartFactory.initialize( mProps, mSubmitFileDir, dag );
            mInitializeGridStart = false;
        }


        // intialize the print stream to the file
        PrintWriter writer = null;
        try{
            writer = getWriter(job);
        }catch(IOException ioe ){
            throw new CodeGeneratorException( "IOException while writing submit file for job " +
                                              job.getName(), ioe);
        }

        //handle the globus rsl parameters
        //for the job from various resources
        handleGlobusRSLForJob( job );

        writer.println(this.mSeparator);
        writer.println("# PEGASUS GENERATED SUBMIT FILE");
        writer.println("# DAG : " + dagname + ", Index = " + dagindex +
                       ", Count = " + dagcount);
        writer.println("# SUBMIT FILE NAME : " + subfilename);
        writer.println(this.mSeparator);

        //figure out the style to apply for a job
        applyStyle( job, writer );

        // handling of  log file is now done through condor profile
        //bwSubmit.println("log = " + dagname + "_" + dagindex + ".log");

        // handle environment settings
        handleEnvVarForJob( job );
        envStr = job.envVariables.toString();
        if (envStr != null) {
            writer.print( job.envVariables );
        }

        // handle Condor variables
        handleCondorVarForJob( job );
        writer.print( job.condorVariables );

        //write the classad's that have the information regarding
        //which VDS super node is a node part of, in addition to the
        //release version of Chimera/Pegasus, the jobClass and the
        //workflow id
        ClassADSGenerator.generate( writer, dag, job );

        // DONE
        writer.println("queue");
        writer.println(this.mSeparator);
        writer.println("# END OF SUBMIT FILE");
        writer.println(this.mSeparator);

        // close the print stream to the file (flush)
        writer.close();

    }


    /**
     * Starts monitoring of the workflow by invoking a workflow monitor daemon
     * tailstatd. The tailstatd is picked up from the default path of
     * $PEGASUS_HOME/bin/tailstatd.
     *
     * @return boolean indicating whether could successfully start the monitor
     *         daemon or not.
     *
     * @throws VTorInUseException in case the method is called before the
     *         submit files have been generated.
     */
    public boolean startMonitoring() throws VTorInUseException{
        //sanity check whether files are generated or not
        if(!mDone || mConcreteWorkflow == null ){
            throw new VTorInUseException(
                "Cannot launch tailstatd before submit Files have not been generated");
        }

        //tailstatd requires the braindump file first
        String bdump;
        try{
            bdump = writeOutBraindump(new File(mSubmitFileDir),
                                           mConcreteWorkflow,
                                           mPOptions.getDAX(),
                                           this.getDAGFilename( mConcreteWorkflow, ".dag" ));

            mLogger.log("Written out braindump to " + bdump, LogManager.DEBUG_MESSAGE_LEVEL);
            return true;
        }
        catch(IOException ioe){
            //log the message and return
            mLogger.log("Unable to write out the braindump file for tailstatd",
                        ioe, LogManager.ERROR_MESSAGE_LEVEL );
            return false;
        }

	//No longer launching tailstatd directly for the time being
        //Karan May 21, 2007
	/*
        //construct the default path to the tailstatd
        char sep = File.separatorChar;
        StringBuffer tsd = new StringBuffer();
        tsd.append(mProps.getPegasusHome())
           .append(sep).append("bin")
           .append(sep).append("tailstatd");

       boolean result = false;
       try{
           //set the callback and run the tailstatd command
           Runtime r = Runtime.getRuntime();

           //append the arguments to the constructed tsd
           tsd.append(" ").append(mSubmitFileDir).append(sep).
               append(this.getDAGMANOutFilename(mConcreteWorkflow));

           mLogger.log("Executing tailstatd " + tsd.toString(),
                       LogManager.DEBUG_MESSAGE_LEVEL);
           Process p = r.exec( tsd.toString() );

           //spawn off the gobblers with the already initialized default callback
           StreamGobbler ips =
               new StreamGobbler(p.getInputStream(), new DefaultStreamGobblerCallback(
                                                             LogManager.DEBUG_MESSAGE_LEVEL));
           StreamGobbler eps =
               new StreamGobbler(p.getErrorStream(), new DefaultStreamGobblerCallback(
                                                             LogManager.DEBUG_MESSAGE_LEVEL));

           ips.start();
           eps.start();

           //wait for the threads to finish off
           ips.join();
           eps.join();

           //get the status
           int status = p.waitFor();

           mLogger.log("Command " + tsd + " exited with status " + status,
                       LogManager.DEBUG_MESSAGE_LEVEL);

           result = (status == 0) ?true : false;
       }
       catch(IOException ioe){
           mLogger.log("IOException while running tailstatd ", ioe,
                       LogManager.ERROR_MESSAGE_LEVEL);
       }
       catch( InterruptedException ie){
           //ignore
       }
       return result;
	*/

    }

    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset( )throws CodeGeneratorException{
        super.reset();
        mDone = false;
        mInitializeGridStart = true;

    }


    /**
     * Constructs a map with the numbers/values to be passed in the RSL handle
     * for certain pools. The user ends up specifying these through the
     * properties file. The value of the property is of the form
     * poolname1=value,poolname2=value....
     *
     * @param propValue the value of the property got from the properties file.
     *
     * @return Map
     */
    private Map constructMap(String propValue) {
        Map map = new java.util.TreeMap();

        if (propValue != null) {
            StringTokenizer st = new StringTokenizer(propValue, ",");
            while (st.hasMoreTokens()) {
                String raw = st.nextToken();
                int pos = raw.indexOf('=');
                if (pos > 0) {
                    map.put(raw.substring(0, pos).trim(),
                            raw.substring(pos + 1).trim());
                }
            }
        }

        return map;
    }

    /**
     * Initializes the file handler to the dag file and writes the header to it.
     *
     * @param filename     basename of dag file to be written.
     * @param dinfo        object containing daginfo of type DagInfo .
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void initializeDagFileWriter(String filename, DagInfo dinfo)
                                                       throws CodeGeneratorException{
        // initialize file handler

        filename = mSubmitFileDir + "/" + filename;
        File dag = new File(filename);


        try {

            //initialize the print stream to the file
            mDagWriter = new PrintWriter(new BufferedWriter(new
                FileWriter(dag)));

            printDagString(this.mSeparator);
            printDagString("# PEGASUS GENERATED SUBMIT FILE");
            printDagString("# DAG " + dinfo.nameOfADag);
            printDagString("# Index = " + dinfo.index + ", Count = " +
                           dinfo.count);
            printDagString(this.mSeparator);
        } catch (Exception e) {
            throw new CodeGeneratorException( "While writing to DAG FILE " + filename,
                                              e);
        }

    }

    /**
     * Writes out the DOT file in the submit directory.
     *
     * @param filename  basename of dot file to be written .
     * @param dag       the <code>ADag</code> object.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void writeDOTFile( String filename, ADag dag )
                                                       throws CodeGeneratorException{
        // initialize file handler

        filename = mSubmitFileDir + File.separator + filename;


        try {
            Writer stream = new PrintWriter( new BufferedWriter ( new FileWriter( filename ) ) );
            dag.toDOT( stream, null );
            stream.close();

        } catch (Exception e) {
            throw new CodeGeneratorException( "While writing to DOT FILE " + filename,
                                              e);
        }

    }



    /**
     * It writes the relations making up the  DAG in the dag file and and closes
     * the file handle to it.
     *
     * @param dinfo   object containing daginfo of type DagInfo.
     *
     * @throws CodeGeneratorException
     */
    protected void writeDagFileTail(DagInfo dinfo) throws CodeGeneratorException{
        try {

            // read the contents from the Daginfo object and
            //print out the parent child relations.

            for (Enumeration dagrelationsenum = dinfo.relations.elements();
                 dagrelationsenum.hasMoreElements(); ) {
                PCRelation pcrl = (PCRelation) dagrelationsenum.nextElement();
                printDagString("PARENT " + pcrl.parent + " CHILD " + pcrl.child);
            }

            printDagString(this.mSeparator);
            printDagString("# End of DAG");
            printDagString(this.mSeparator);

            // close the print stream to the file
            mDagWriter.close();

        } catch (Exception e) {
            throw new CodeGeneratorException( "Error Writing to Dag file " + e.getMessage(),
                                              e );
        }

    }

    /**
     * Writes a string to the dag file. When calling this function the
     * file handle to file is already initialized.
     *
     * @param  str   The String to be printed to the dag file.
     *
     * @throws CodeGeneratorException
     */
    protected void printDagString(String str) throws CodeGeneratorException{
        try {
            mDagWriter.println(str);
        } catch (Exception e) {
            throw new CodeGeneratorException( "Writing to Dag file " + e.getMessage(),
                                              e );
        }

    }


    /**
     * The name of the condor log file that is used for the dag that is being
     * written into the condor formal. This name is used as the default logfile
     * for all the jobs unless it is over ridden in the condor namespace. It is
     * the full path to the log file stored in the local temporary directory.
     *
     * @return  the name of the log file.
     */
    protected String getLogFileName(){
       return this.mTempLogFile;
    }

    /**
     * Returns the path to the symlink that is created to the original log file
     * in the temporary directory.
     *
     * @param dag  the concrete workflow.
     *
     * @return the path to symlink.
     */
    protected String getLogFileSymlinkName( ADag dag ){
        StringBuffer sb = new StringBuffer();
        sb.append(this.mSubmitFileDir)
           .append(File.separator);

       String bprefix = mPOptions.getBasenamePrefix();
       if( bprefix != null){
           //the prefix is not null using it
           sb.append(bprefix);
       }
       else{
           //generate the prefix from the name of the dag
           sb.append(dag.dagInfo.nameOfADag).append("-").
               append(dag.dagInfo.index);
       }
       //append the suffix
       sb.append(".log");
       return sb.toString();
    }


    /**
     * Writes out the braindump.txt file for a partition in the partition submit
     * directory. The braindump.txt file is used for passing to the tailstatd
     * daemon that monitors the state of execution of the workflow.
     *
     * @param directory  the directory in which the braindump file needs to
     *                   be written to.
     * @param workflow   the concerte workflow.
     * @param dax        the corresponding DAX file containing the abstract workflow.
     * @param dagFile    the basename of the .dag file that is written out while
     *                   generating output.
     *
     * @return the absolute path to the braindump file.txt written in the directory.
     *
     * @throws IOException in case of error while writing out file.
     */
    protected String writeOutBraindump( File directory,
                                        ADag workflow,
                                        String dax,
                                        String dagFile)
                                        throws IOException{


        //create a writer to the braindump.txt in the directory.
        File f = new File( directory , "braindump.txt");
        PrintWriter writer =
                  new PrintWriter(new BufferedWriter(new FileWriter(f)));

        //get absolute directory just once
        String absPath = directory.getAbsolutePath();
        char sep = File.separatorChar;

        //assemble all the contents in a buffer before writing out
        StringBuffer contents = new StringBuffer();
        contents.append( "dax " ).append(dax).append("\n").
                 append( "dag " ).append(dagFile).append("\n").
                 append( "basedir ").append( mPOptions.getBaseSubmitDirectory() ).append("\n").
                 append( "run ").append(absPath).append("\n").
                 append( "jsd ").append(absPath).append(sep).append("jobstate.log").append("\n").
                 append( "rundir ").append(directory.getName()).append("\n").
                 append( "pegasushome ").append(mProps.getPegasusHome()).append("\n").
                 append( "vogroup ").append( mPOptions.getVOGroup()).append("\n").//for time being
                 append( "label " + workflow.getLabel()).append("\n").
                 append( "planner " ).append(mProps.getPegasusHome()).append(sep).
                                    append("bin").append(sep).append("pegasus-plan").
                 append( "\n" );

        writer.write( contents.toString());

        //write out the classads that are required to be
        //inserted in the dagman condor submit file
        ClassADSGenerator.generateBraindumpEntries( writer, workflow );

        writer.close();

        return f.getAbsolutePath();
    }


    /**
     * This method generates a symlink to the actual log file written in the
     * local temp directory. The symlink is placed in the dag directory.
     *
     * @param logFile the full path to the log file.
     * @param symlink the full path to the symlink.
     *
     * @return boolean indicating if creation of symlink was successful or not
     */
    protected boolean generateLogFileSymlink(String logFile, String symlink) {
        try{
            Runtime rt = Runtime.getRuntime();
            String command = "ln -s " +logFile + " " + symlink;
            mLogger.log("Creating symlink to the log file in the local temp directory\n"
                        + command ,LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = rt.exec(command,null);

            // set up to read subprogram output
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            // set up to read subprogram error
            InputStream er = p.getErrorStream();
            InputStreamReader err = new InputStreamReader(er);
            BufferedReader ebr = new BufferedReader(err);

            // read output from subprogram
            // and display it

            String s,se=null;
            while ( ((s = br.readLine()) != null) || ((se = ebr.readLine()) != null ) ) {
               if(s!=null){
                   mLogger.log(s,LogManager.DEBUG_MESSAGE_LEVEL);
               }
               else {
                   mLogger.log(se,LogManager.ERROR_MESSAGE_LEVEL );
               }
            }

            br.close();
            return true;
        }
        catch(Exception ex){
            mLogger.log("Unable to create symlink to the log file" , ex,
                        LogManager.ERROR_MESSAGE_LEVEL);
            return false;
       }

    }


    /**
     * Returns the name of the file on the basis of the metadata associated
     * with the DAG.
     * In case of Condor dagman, it is the name of the .dag file that is
     * written out. The basename of the .dag file is dependant on whether the
     * basename prefix has been specified at runtime or not by command line
     * options.
     *
     * @param dag    the dag for which the .dag file has to be created.
     * @param suffix the suffix to be applied at the end.
     *
     * @return the name of the dagfile.
     */
    protected String getDAGFilename( ADag dag, String suffix ){
        //constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();
        if( bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append(dag.dagInfo.nameOfADag).append("-").
                append(dag.dagInfo.index);
        }
        //append the suffix
        sb.append( suffix );



        return sb.toString();

    }

    /**
     * Returns the basename of the file, that contains the output of the
     * dagman while running the dag generated for the workflow.
     * The basename of the .out file is dependant on whether the
     * basename prefix has been specified at runtime or not by command line
     * options.
     *
     * @param dag  the DAG containing the concrete workflow
     *
     * @return the name of the dagfile.
     */
    protected String getDAGMANOutFilename( ADag dag ){
        //constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();
        if( bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append(dag.dagInfo.nameOfADag).append("-").
                append(dag.dagInfo.index);
        }
        //append the suffix
        sb.append(".dag.dagman.out");

        return sb.toString();

    }



    /**
     * A callout method that dictates what needs to be done in case the concrete
     * plan that is generated is empty.
     * It just logs a message saying the plan is empty.
     *
     * @param filename  Filename of the dag to be written of type String.
     * @param dag       the concrete dag that is empty.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void concreteDagEmpty(String filename, ADag dag)
                                                 throws CodeGeneratorException{
        StringBuffer sb = new StringBuffer();
        sb.append( "The concrete plan generated contains no nodes. ").
           append( "\nIt seems that the output files are already at the output pool" );

        mLogger.log( sb.toString(), LogManager.INFO_MESSAGE_LEVEL );

   }



    /**
     * It updates/adds the condor variables that are  got through the Dax with
     * the values specified in the properties file, pool config file or adds some
     * variables internally. In case of clashes of Condor variables from
     * various sources the following order is followed,property file, pool config
     * file and then dax.
     *
     * @param sinfo  The SubInfo object containing the information about the job.
     *
     *
     * @throws CodeGeneratorException
     */
    protected void handleCondorVarForJob(SubInfo sinfo) throws CodeGeneratorException{
        Condor cvar = sinfo.condorVariables;

        String key = null;
        String value = null;

        //put in the classad expression for the values
        //construct the periodic_release and periodic_remove
        //values only if their final computed values are > 0
        value = (String)cvar.removeKey("periodic_release");
        if(value != null && Integer.parseInt(value) > 0){
            value = "(NumSystemHolds <= " + value + ")";
            cvar.construct("periodic_release", value);
        }
        value = (String)cvar.removeKey("periodic_remove");
        if(value != null && Integer.parseInt(value) > 0){
            value = "(NumSystemHolds > " + value + ")";
            cvar.construct("periodic_remove", value);
        }


        // have to change this later maybe
        key = "notification";
        value = (String) cvar.removeKey(key);
        if (value == null) {
            cvar.construct(key, "NEVER");
        } else {
            cvar.construct(key, value);

            //check if transfer_executable was set to
            //true by the user at runtime
        }
        key = "transfer_executable";
        if (cvar.containsKey(key)) {
            //we do not put in the default value
        } else {
            // we assume pre-staged executables through the GVDS
            cvar.construct(key, "false");

        }

        key = "copy_to_spool";
        if (cvar.containsKey(key)) {
            //we do not put in the default value
        } else
            // no sense copying files to spool for globus jobs
            // and is mandatory for the archstart stuff to work
            // for local jobs
            cvar.construct(key, "false");

        //construct the log file for the submit job
        key = "log";
        if(!cvar.containsKey(key)){
            //we put in the default value
            //cvar.construct("log",dagname + "_" + dagindex + ".log");
            cvar.construct("log",this.getLogFileName());
        }

        //also add the information as for the submit event trigger
        //for mei retry mechanism
        cvar.construct("submit_event_user_notes","pool:" + sinfo.executionPool);


        //on the basis of the
        //transfer_executable key do some magic
        handleTransferOfExecutable(sinfo);

        //correctly quote the arguments according to
        //Condor Quoting Rules.
        String args = (String) sinfo.condorVariables.get("arguments");
        if( mProps.useCondorQuotingForArguments() && args != null){
            try {
                mLogger.log("Unquoted arguments are " + args,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                //insert a comment for the old args
                sinfo.condorVariables.construct("#arguments",args);
                args = CondorQuoteParser.quote(args, true);
                sinfo.condorVariables.construct("arguments", args);
                mLogger.log("Quoted arguments are " + args,
                            LogManager.DEBUG_MESSAGE_LEVEL);
            }
            catch (CondorQuoteParserException e) {
                throw new RuntimeException("CondorQuoting Problem " +
                                           e.getMessage());
            }
        }


        return;

    }


    /**
     * It changes the paths to the executable depending on whether we want to
     * transfer the executable or not. If the transfer_executable is set to true,
     * then the executable needs to be shipped from the submit host meaning the
     * local pool. This function changes the path of the executable to the one on
     * the local pool, so that it can be shipped.
     *
     * @param sinfo the <code>SubInfo</code> containing the job description.
     *
     * @throws CodeGeneratorException
     */
    protected void handleTransferOfExecutable(SubInfo sinfo) throws CodeGeneratorException{
        Condor cvar = sinfo.condorVariables;

        if (!cvar.getBooleanValue("transfer_executable")) {
            //the executable paths are correct and
            //point to the executable on the remote pool
            return;
        }

        SiteInfo site = mPoolHandle.getPoolEntry(sinfo.executionPool, Condor.VANILLA_UNIVERSE);
        String gridStartPath = site.getKickstartPath();

        if (gridStartPath == null) {
            //not using grid start
            //we need to stage in the executable from
            //the local pool. Not yet implemented
            mLogger.log("At present only the transfer of gridstart is supported",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return;
        } else {
            site = mPoolHandle.getPoolEntry("local", Condor.VANILLA_UNIVERSE);
            gridStartPath = site.getKickstartPath();
            if (gridStartPath == null) {
                mLogger.log(
                    "Gridstart needs to be shipped from the submit host to pool" +
                    sinfo.executionPool + ".\nNo entry for it in pool local",
                    LogManager.ERROR_MESSAGE_LEVEL);
                throw new CodeGeneratorException( "GridStart needs to be shipped from submit host to site " +
                                                  sinfo.getSiteHandle() );

            } else {
                //the jobs path to executable is updated
                //by the path on the submit host
                cvar.construct("executable", gridStartPath);

                //the arguments to gridstart need to be
                //appended with the true remote directory
                String args = (String) cvar.removeKey("arguments");
                args = " -w " +
                    mPoolHandle.getExecPoolWorkDir(sinfo) +
                    " " + args;
                cvar.construct("arguments", args);

                //we have to remove the remote_initial dir for it.
                //as this is required for the LCG sites
                //Actually this should be done thru a LCG flag
                cvar.removeKey("remote_initialdir");

            }

        }
    }

    /**
     * Applies a submit file style to the job, according to the fact whether
     * the job has to be submitted directly to condor or to a remote jobmanager
     * via CondorG and GRAM.
     * If no style is associated with the job, then for the job running on
     * local site, condor style is applied. For a job running on non local sites,
     * globus style is applied if none is associated with the job.
     *
     * @param job  the job on which the style needs to be applied.
     * @param writer the PrintWriter stream to the submit file for the job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void applyStyle( SubInfo job, PrintWriter writer )
                                                  throws CodeGeneratorException{

        //load  the appropriate style for the job
        CondorStyle cs = mStyleFactory.loadInstance( job );
        String style   = (String)job.vdsNS.get( VDS.STYLE_KEY );

        boolean isGlobus =  style.equals( VDS.GLOBUS_STYLE ) ? true : false;

        //apply the appropriate style on the job.
        cs.apply( job );

        //handle GLOBUS RSL if required, and stdio appropriately
        String rslString = job.globusRSL.toString();
        rslString += gridstart( writer, job, isGlobus );
        if( isGlobus ){
            //only for CondorG style does RSL make sense
            //instead of writing directly
            //incorporate as condor profile
            job.condorVariables.construct( "globusrsl", rslString );

        }
    }


    /**
     * It updates/adds the environment variables that are got through the Dax with
     * the values specified in the properties file, pool config file or adds some
     * variables internally. In case of clashes of environment variables from
     * various sources the following order is followed,property file,
     * transformation catalog, pool config file and then dax.
     * At present values are not picked from the properties file.
     *
     * @param sinfo  The SubInfo object containing the information about the job.
     */
    protected void handleEnvVarForJob(SubInfo sinfo) {

    }

    /**
     * It updates/adds the the Globus RSL parameters got through the dax that are
     * in SubInfo object. In addition inserts the additional rsl attributes
     * that can be specified in the properties file or the pool config files in
     * the profiles tags. In case of clashes of RSL attributes from various
     * sources the following order is followed,property file, pool config file
     * and then dax.
     *
     * @param sinfo  The SubInfo object containing the information about the job.
     */
    protected void handleGlobusRSLForJob(SubInfo sinfo) {
        Globus rsl = sinfo.globusRSL;

        String key = null;
        String value = null;

        //Getting all the rsl parameters specified
        //in dax
        /*
                 if (sinfo.globusRSL != null) {
            rsl.putAll(sinfo.globusRSL);

            // 19-05 jsv: Need to change to {remote_}initialdir commands
            // allow TR to spec its own directory
                 }
         */

        //check if any projects name need to be
        //added
        value = (String) mProjectMap.get(sinfo.executionPool);
        if (value != null) {
            rsl.construct("project", value);

        }

        //check if the walltimes for the pool
        //has been specified
        value = (String) mWalltimeMap.get(sinfo.executionPool);
        if (value != null) {
            rsl.construct("maxwalltime", value);

        }

        //check if any queue needs to be specified
        //for the execution pool in the RSL string.
        value = (String) mQueueMap.get(sinfo.executionPool);
        if (value != null) {
            rsl.construct("queue", value);
        }

        // check job type, unless already specified
        // Note, we may need to adjust this again later
        if (!rsl.containsKey("jobtype")) {
            rsl.construct("jobtype", "single");
        }
        //sanitize jobtype on basis of jobmanager
        //Karan Sept 12,2005
        //This is to overcome specifically Duncan's problem
        //while running condor universe standard jobs.
        //For that the jobtype=condor needs to be set for the compute
        //job. This is set in the site catalog, but ends up
        //breaking transfer jobs that are run on jobmanager-fork
        String jmURL = sinfo.globusScheduler;
        if(jmURL != null && jmURL.endsWith("fork")){
            rsl.construct("jobtype","single");
        }


    }






    /**
     * This function creates the stdio handling with and without gridstart.
     * Please note that gridstart will become the default by end 2003, and
     * no gridstart support will be phased out.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param job is the job information structure.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return A possibly empty string which contains things that
     *         need to be added to the "globusrsl" clause. The return
     *         value is only of interest for isGlobusJob==true calls.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    private String gridstart(PrintWriter writer,
                             SubInfo job,
                             boolean isGlobusJob) throws CodeGeneratorException {
        //To get the gridstart/kickstart path on the remote
        //pool, querying with entry for vanilla universe.
        //In the new format the gridstart is associated with the
        //pool not pool, condor universe
        SiteInfo site = mPoolHandle.getPoolEntry(job.executionPool,
                                                 Condor.VANILLA_UNIVERSE);
        String gridStartPath = site.getKickstartPath();

        StringBuffer rslString = new StringBuffer();
        String jobName = job.jobName;
        String script = null;
        job.dagmanVariables.checkKeyInNS(Dagman.JOB_KEY,
                                         getFileBaseName(job));

        //remove the prescript arguments key
        //should be already be set to the prescript key
//        //NO NEED TO REMOVE AS WE ARE HANDLING CORRECTLY IN DAGMAN NAMESPACE
//        //NOW. THERE THE ARGUMENTS AND KEY ARE COMBINED. Karan May 11,2006
//        //job.dagmanVariables.removeKey(Dagman.PRE_SCRIPT_ARGUMENTS_KEY);

//        script = (String)job.dagmanVariables.removeKey(Dagman.PRE_SCRIPT_KEY);
//        if(script != null){
//            //put in the new key with the prescript
//            job.dagmanVariables.checkKeyInNS(PRE_SCRIPT_KEY,script);
//        }


        if (isGlobusJob) {
            //check if we want to stream
            //the output and error or stage
            //it in.
            if (!mProps.streamCondorError()) {
                //we want it to be staged
                writer.println("stream_error  = false");
            }
            if (!mProps.streamCondorOutput()) {
                //we want it to be staged
                writer.println("stream_output = false");
            }
        }

        GridStart gridStart = mGridStartFactory.loadGridStart( job, gridStartPath );

        //enable the job
        if( !gridStart.enable( job,isGlobusJob ) ){
            String msg = "Job " +  jobName + " cannot be enabled by " +
                         gridStart.shortDescribe() + " to run at " +
                         job.getSiteHandle();
            mLogger.log( msg, LogManager.FATAL_MESSAGE_LEVEL );
            throw new CodeGeneratorException( msg );
        }


        //apply the appropriate POSTScript
        POSTScript ps       = mGridStartFactory.loadPOSTScript( job, gridStart );
        boolean constructed = ps.construct( job, Dagman.POST_SCRIPT_KEY );

        //write out all the dagman profile variables associated
        //with the job to the .dag file.
        printDagString(job.dagmanVariables.toString(jobName));

        return rslString.toString();
    }



}
