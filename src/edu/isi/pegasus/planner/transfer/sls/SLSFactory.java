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

package edu.isi.pegasus.planner.transfer.sls;


import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.HashMap;



/**
 * A factory class to load the appropriate type of SLS Implementation to do
 * the Second Level Staging.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SLSFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "edu.isi.pegasus.planner.transfer.sls";

    /**
     * The name of the class implementing the condor code generator.
     */
    public static final String DEFAULT_SLS_IMPL_CLASS  =  "Transfer";

    /**
     * Name of class implementing condorio
     */
    public static final String CONDORIO_SLS_IMPL_CLASS = "Condor";

    /**
     * The known SLS implementations.
     */
    public static String[] SLS_IMPLEMENTING_CLASSES = {
                                                     "Transfer",
                                                     "Condor"
                                                    };

    /**
     * The handle to the logging manager.
     */
    protected LogManager mLogger;
    
    private final HashMap mSLSImplementationTable;
    private boolean mInitialized;
    private PegasusBag mBag;

    /**
     * The default constructor.
     */
    public SLSFactory() {
        mSLSImplementationTable = new HashMap( 3 );
        mInitialized = false;
    }


    /**
     * Initializes the factory with known GridStart implementations.
     *
     * @param bag   the bag of objects that is used for initialization.
     */
    public void initialize( PegasusBag bag ){
        mBag       = bag;
        mLogger    = bag.getLogger();

        //load all the known implementations and initialize them
        for( int i = 0; i < SLS_IMPLEMENTING_CLASSES.length; i++){
            //load via reflection just once
            registerSLS( SLS_IMPLEMENTING_CLASSES[i],
                         this.loadInstance( bag,SLS_IMPLEMENTING_CLASSES[i] ));
        }

        mInitialized = true;
    }

    
    /**
     * This method loads the appropriate implementing code generator as specified
     * by the user at runtime. If the megadag mode is specified in the options,
     * then that is used to load the implementing class, overriding the submit
     * mode specified in the properties file.
     *
     *
     * @param bag   the bag of initialization objects.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception SLSFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     *
     * @throws SLSFactoryException
     */
    public SLS loadInstance( Job job )  throws SLSFactoryException{

        //sanity checks first
        if( !mInitialized ){
            throw new SLSFactoryException(
                "SLSFactory needs to be initialized first before using" );
        }

        //determine the short name of SLS implementation
        String shortName = this.getSLSShortName(job);
        mLogger.log(job.getName() + " uses SLS " + shortName, LogManager.DEBUG_MESSAGE_LEVEL);

        //try loading on the basis of short name from the cache
        Object obj = this.mSLSImplementationTable.get( shortName );
        if( obj == null ){
            //load via reflection and register in the cache
            obj = this.loadInstance( mBag,  shortName );
            this.registerSLS( shortName, (SLS)obj );
        }
       
        return (SLS)obj;

    }

    /**
     * Inserts an entry into the implementing class table. The name is
     * converted to lower case before being stored.
     *
     * @param name       the short name for a GridStart implementation
     * @param implementation  the object of the class implementing that style.
     */
    private void registerSLS( String name, SLS implementation){
        mSLSImplementationTable.put( name.toLowerCase(), implementation );
    }
    
    /**
     * This method loads the appropriate code generator as specified by the
     * user at runtime.
     *
     *
     * @param bag   the bag of initialization objects.
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @see #DEFAULT_PACKAGE_NAME
     *
     * @throws SLSFactoryException
     */
    private SLS loadInstance( PegasusBag bag, String className)
        throws SLSFactoryException{


        PegasusProperties properties = bag.getPegasusProperties();
        PlannerOptions options       = bag.getPlannerOptions();


        //sanity check
        if (properties == null) {
            throw new SLSFactoryException( "Invalid properties passed" );
        }
        if (className == null) {
            throw new SLSFactoryException( "Invalid className specified" );
        }

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        SLS sls = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            sls = ( SLS ) dl.instantiate( new Object[0] );
            //initialize the loaded code generator
            sls.initialize( bag );
        }
        catch ( Exception e ) {
            throw new SLSFactoryException( "Instantiating SLS Implementor ", className, e );
        }

        return sls;
    }

    /**
     * Returns the short name for the job that needs to be loaded.
     * 
     * @param job
     * 
     * @return 
     */
    private String getSLSShortName(Job job) {
       String conf = job.getDataConfiguration();
            
       return ( conf != null && (conf.equalsIgnoreCase( PegasusConfiguration.CONDOR_CONFIGURATION_VALUE) ))?
               CONDORIO_SLS_IMPL_CLASS:
               DEFAULT_SLS_IMPL_CLASS;
           
    }

}
