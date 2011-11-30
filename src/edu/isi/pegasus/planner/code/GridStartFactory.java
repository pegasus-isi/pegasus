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

package edu.isi.pegasus.planner.code;

import edu.isi.pegasus.planner.code.gridstart.*;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.POSTScript;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.AggregatedJob;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.namespace.Dagman;

import edu.isi.pegasus.common.util.DynamicLoader;

import java.util.Map;
import java.util.HashMap;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * An abstract factory class to load the appropriate type of GridStart
 * implementations, and their corresponding POSTScript classes.
 * This factory class is different from other factories, in the sense that it
 * must be instantiated first and intialized first before calling out to any
 * of the Factory methods.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class GridStartFactory {

    /**
     * The package name where the implementations of this interface reside
     * by default.
     */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.code.gridstart";


    /**
     * The corresponding short names for the implementations.
     */
    public static String[] GRIDSTART_SHORT_NAMES = {
                                           "kickstart",
                                           "none",
                                           "seqexec"
                                          };

    /**
     * The index in the constant arrays for NoGridStart.
     */
    public static final int KICKSTART_INDEX = 0;

    /**
     * The index in the constant arrays for NoGridStart.
     */
    public static final int NO_GRIDSTART_INDEX = 1;

    /**
     * The index in constant arrays for SeqExec
     */
    public static final int SEQEXEC_INDEX = 2;

    /**
     * The postscript mode in which post scripts are added only for essential
     * jobs.
     */
    public static final String ESSENTIAL_POST_SCRIPT_SCOPE = "essential";

    /**
     * The postscript mode in which post scripts are added only for all
     * jobs.
     */
    public static final String ALL_POST_SCRIPT_SCOPE = "all";


    /**
     * The known gridstart implementations.
     */
    public static String[] GRIDSTART_IMPLEMENTING_CLASSES = {
                                                     "Kickstart",
                                                     "NoGridStart"
                                                    };

    //

    /**
     * A table that associates POSTScript implementing classes with their
     * SHORT_NAMES.
     */
    private static Map POSTSCRIPT_IMPLEMENTING_CLASS_TABLE;

    /**
     * Initializes the <code>POSTScript</code> implementation table, associating
     * short names for the POSTScript with the name of the classes itself.
     */
    static{
        POSTSCRIPT_IMPLEMENTING_CLASS_TABLE = new HashMap( 8 );
        //not really the best way. should have avoided creating objects
        //but then too many constants everywhere.
        associate( new ExitPOST() );
        associate( new ExitCode() );
        associate( new UserPOSTScript() );
        associate( new NoPOSTScript() );
        associate( new NetloggerPostScript() );
        associate( new PegasusExitCode() );
    }


    /**
     * Associates a shortname with the classname.
     *
     * @param ps  the <code>POSTScript</code> implementation.
     */
    private static void associate( POSTScript ps ){
        POSTSCRIPT_IMPLEMENTING_CLASS_TABLE.put( ps.shortDescribe(),
                                                 ps.getClass().getName() );
    }


    /**
     * Associates a shortname with the classname.
     *
     * @param shortName  the shortName for the POSTScript implementation
     * @param className  the fully qualified className of the implementing class.
     */
    private static void associate( String shortName, String className ){
        POSTSCRIPT_IMPLEMENTING_CLASS_TABLE.put( shortName, className );
    }

    /**
     * Returns the name of the implementing POSTSCript class.
     *
     * @param shortName  the shortName for the POSTScript implementation
     *
     * @return the className  the fully qualified className of the implementing class,
     *         else null.
     */
    private static String implementingPOSTScriptClass( String shortName ){
        Object obj =  POSTSCRIPT_IMPLEMENTING_CLASS_TABLE.get( shortName );
        return ( obj == null ) ? null : ( String )obj;
    }

    /**
     * The postscript mode. Whether to add postscripts for the jobs or not.
     * At present just two modes supported
     *         all   add postscripts for jobs where kickstart is present.
     *         none  do not add postscripts to anyjob
     */
//    private String mPostScriptScope;



    /**
     * A table that maps short names of <code>POSTScript</code> implementations
     * with the implementations themselves.
     */
    private  Map mPOSTScriptImplementationTable;


    /**
     * A table that maps short names of <code>GridStart</code> implementations
     * with the implementations themselves.
     */
    private  Map mGridStartImplementationTable ;

    /**
     * The bag of objects used for initialization.
     */
    private PegasusBag mBag;

    /**
     * The properties object holding all the properties.
     */
    private PegasusProperties mProps;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    private String mSubmitDir;

    /**
     * The workflow object.
     */
    private ADag mDAG;

    /**
     * A boolean indicating that the factory has been initialized.
     */
    private boolean mInitialized;

    /**
     * The default constructor.
     */
    public GridStartFactory() {
        mGridStartImplementationTable = new HashMap( 3 );
        mPOSTScriptImplementationTable     = new HashMap( 3 );
        mInitialized = false;
    }


    /**
     * Initializes the factory with known GridStart implementations.
     *
     * @param bag   the bag of objects that is used for initialization.
     * @param dag   the concrete dag so far.
     */
    public void initialize( PegasusBag bag, ADag dag ){
        mBag       = bag;
        mProps     = bag.getPegasusProperties();
        mSubmitDir = bag.getPlannerOptions().getSubmitDirectory() ;
        mDAG       = dag;
//        mPostScriptScope = mProps.getPOSTScriptScope();

        //load all the known implementations and initialize them
        for( int i = 0; i < GRIDSTART_IMPLEMENTING_CLASSES.length; i++){
            //load via reflection just once
            registerGridStart( GRIDSTART_SHORT_NAMES[i],
                               this.loadGridStart( bag, dag,
                                                    GRIDSTART_IMPLEMENTING_CLASSES[i] )
                             );
        }

        mInitialized = true;
    }





    /**
     * Loads the appropriate gridstart implementation for a job on the basis of
     * the value of the GRIDSTART_KEY in the Pegasus namepsace. If no value is
     * specified then the value in the properties file is picked up.
     *
     * @param job           the job for which we want the gridstart handle.
     * @param gridStartPath the path to the gridstart from the site catalog.
     *
     * @return a handle to appropriate GridStart implementation.
     *
     * @see org.griphyn.cPlanner.namespace.Pegasus#GRIDSTART_KEY
     * @see org.griphyn.cPlanner.common.PegasusProperties#getGridStart()
     *
     * @throws GridStartFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public GridStart loadGridStart( Job job, String gridStartPath )
                                                   throws GridStartFactoryException {

        //sanity checks first
        if( !mInitialized ){
            throw new GridStartFactoryException(
                "GridStartFactory needs to be initialized first before using" );
        }
        GridStart gs = null;
        if ( /*gridStartPath == null ||*/ job.isMPIJob()){
            //return NoGridStart implementation
            gs = (GridStart)this.gridStart( GRIDSTART_SHORT_NAMES[ NO_GRIDSTART_INDEX ] );
        }
        else{
            //determine the short name of GridStart implementation
            //on the basis of any profile associated or from the properties file
            String shortName = ( job.vdsNS.containsKey( Pegasus.GRIDSTART_KEY) ) ?
                                //pick the one associated in profiles
                                ( String ) job.vdsNS.get( Pegasus.GRIDSTART_KEY ):
                                //pick the one in the properties file
                                mProps.getGridStart();

            //try loading on the basis of short name from the cache
            Object obj = this.gridStart( shortName );

            if( obj == null ){
                //load via reflection and register in the cache
                obj = this.loadGridStart( mBag, mDAG, shortName );
                this.registerGridStart( shortName, (GridStart)obj );
            }
            gs = (GridStart) obj;
        }
        return gs;
     }


     /**
     * Loads the appropriate POST Script implementation for a job on the basis of
     * the value of the Pegasus profile GRIDSTART_KEY, and the DAGMan profile
     * POST_SCRIPT_KEY in the Pegasus namepsace. If no value is
     * specified then the value in the properties file is picked up.
     *
     * @param job       the job for which we want the gridstart handle.
     * @param gridStart the <code>GridStart</code> for which we want to load
     *                  the POSTSCRIPT implementation.
     *
     * @return a handle to appropriate POSTScript implementation.
     *
     * @see org.griphyn.cPlanner.namespace.Pegasus#GRIDSTART_KEY
     * @see org.griphyn.cPlanner.namespace.Dagman#POST_SCRIPT_KEY
     * @see org.griphyn.cPlanner.common.PegasusProperties#getGridStart()
     *
     * @throws GridStartFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public POSTScript loadPOSTScript( Job job, GridStart gridStart )
                                                   throws GridStartFactoryException {

        //sanity checks first
        if( !mInitialized ){
            throw new GridStartFactoryException(
                "GridStartFactory needs to be initialized first before using" );
        }

        if ( gridStart == null ){
            throw new GridStartFactoryException(
                "POSTScript can only be instantiated if supplied a GridStart implementation" );
        }

        //figure out the postscript type. the scope takes precedence
        String postScriptType;
        String postScriptScope = (String) job.dagmanVariables.get( Dagman.POST_SCRIPT_SCOPE_KEY );
        postScriptScope = ( postScriptScope == null )? 
                          GridStartFactory.ALL_POST_SCRIPT_SCOPE:
                          postScriptScope;
        
        if ( postScriptScope.equals( GridStartFactory.ALL_POST_SCRIPT_SCOPE ) ||
            ( postScriptScope.equals( GridStartFactory.ESSENTIAL_POST_SCRIPT_SCOPE ) &&
             job.getJobType() != Job.REPLICA_REG_JOB)
            ) {
                //we need to apply some postscript
                //let us figure out the type of postscript to instantiate
                Object profileValue = job.dagmanVariables.get( Dagman.POST_SCRIPT_KEY );
                postScriptType = ( profileValue == null )?
                                //get the default associated with gridstart
                                gridStart.defaultPOSTScript():
                                //use the one specified in profiles/properties
                                ( String ) profileValue;

        }
        else{
            //mode is none , make sure to remove post key and the arguments
            postScriptType = NoPOSTScript.SHORT_NAME;
        }



        //try loading on the basis of postscript type from the cache
        Object obj = this.postScript( postScriptType );

        POSTScript ps = null;
        if( obj == null ){
            //determine the className for postScriptType
            String className = GridStartFactory.implementingPOSTScriptClass( postScriptType );

            if( className == null ){
                //so this is a user specified postscript
                className = GridStartFactory.implementingPOSTScriptClass( UserPOSTScript.SHORT_NAME );
            }


            //load via reflection and register in the cache
            obj = this.loadPOSTScript( mProps,
                                       mSubmitDir,
                                       //mProps.getPOSTScriptPath( postScriptType ),
                                       job.dagmanVariables.getPOSTScriptPath( postScriptType ),
                                       className );
            this.registerPOSTScript( postScriptType, (POSTScript)obj );
        }
        ps = ( POSTScript ) obj;

        return ps;
     }


    /**
     * Loads the implementing class corresponding to the class. If the package
     * name is not specified with the class, then class is assumed to be
     * in the DEFAULT_PACKAGE. The properties object passed should not be null.
     *
     * @param bag        the bag of initialization objects
     * @param dag        the concrete dag so far.
     * @param className  the name of the class that implements the mode. It is the
     *                   name of the class, not the complete name with package. That
     *                   is added by itself.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws GridStartFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    private GridStart loadGridStart( PegasusBag bag,
                                     ADag dag,
                                     String className )
                                   throws GridStartFactoryException {


        //prepend the package name
        className = (className.indexOf('.') == -1)?
                     //pick up from the default package
                     DEFAULT_PACKAGE_NAME + "." + className:
                     //load directly
                     className;

        //try loading the class dynamically
        GridStart gs = null;
        try{
            DynamicLoader dl = new DynamicLoader( className);
            gs = (GridStart) dl.instantiate( new Object[0] );
            gs.initialize( bag, dag);
        }
        catch (Exception e) {
            throw new GridStartFactoryException("Instantiating GridStart ",
                                                className,
                                                e);
        }

        return gs;
    }

    /**
     * Loads the implementing class corresponding to the class. If the package
     * name is not specified with the class, then class is assumed to be
     * in the DEFAULT_PACKAGE. The properties object passed should not be null.
     *
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param path       the path to the postscript on the submit host.
     * @param className  the name of the class that implements the mode. It is the
     *                   name of the class, not the complete name with package. That
     *                   is added by itself.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws GridStartFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    private POSTScript loadPOSTScript( PegasusProperties properties,
                                       String submitDir,
                                       String path,
                                       String className )
                                   throws GridStartFactoryException {


        //prepend the package name
        className = (className.indexOf('.') == -1)?
                     //pick up from the default package
                     DEFAULT_PACKAGE_NAME + "." + className:
                     //load directly
                     className;

        //try loading the class dynamically
        POSTScript ps = null;
        try{
            DynamicLoader dl = new DynamicLoader( className);
            ps = ( POSTScript ) dl.instantiate( new Object[0] );
            ps.initialize( properties, path, submitDir );
        }
        catch (Exception e) {
            throw new GridStartFactoryException("Instantiating GridStart ",
                                                className,
                                                e);
        }

        return ps;
    }



    /**
     * Returns the cached implementation of <code>POSTScript</code>
     * from the implementing class table.
     *
     * @param type       the short name for a <code>POSTScript</code> implementation
     *
     * @return implementation  the object class implementing that style, else null
     */
    private POSTScript postScript( String type ){
        Object obj = mPOSTScriptImplementationTable.get( type.toLowerCase() );
        return ( obj == null ) ? null : (POSTScript)obj ;
    }


    /**
     * Inserts an entry into the implementing class table. The name is
     * converted to lower case before being stored.
     *
     * @param name       the short name for a <code>POSTScript</code> implementation
     * @param implementation  the object of the class implementing that style.
     */
    private void registerPOSTScript( String name, POSTScript implementation){
        mPOSTScriptImplementationTable.put( name.toLowerCase(), implementation );
    }


    /**
     * Returns the cached implementation of GridStart from the implementing
     * class table.
     *
     * @param name       the short name for a GridStart implementation
     *
     * @return implementation  the object of the class implementing that style, else null
     */
    private GridStart gridStart( String name ){
        Object obj = mGridStartImplementationTable.get( name.toLowerCase() );
        return ( obj == null ) ? null : (GridStart)obj ;
    }


    /**
     * Inserts an entry into the implementing class table. The name is
     * converted to lower case before being stored.
     *
     * @param name       the short name for a GridStart implementation
     * @param implementation  the object of the class implementing that style.
     */
    private void registerGridStart( String name, GridStart implementation){
        mGridStartImplementationTable.put( name.toLowerCase(), implementation );
    }

}
