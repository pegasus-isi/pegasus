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

package edu.isi.pegasus.planner.code.generator.condor;


import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;

import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.common.PegasusProperties;


import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.common.util.DynamicLoader;

import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import java.io.IOException;

/**
 * A factory class to load the appropriate type of Condor Style impelementations.
 * This factory class is different from other factories, in the sense that it
 * must be instantiated first and intialized first before calling out to any
 * of the Factory methods.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorStyleFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "edu.isi.pegasus.planner.code.generator.condor.style";
    //

    /**
     * The name of the class implementing the Condor Style.
     */
    private static final String CONDOR_STYLE_IMPLEMENTING_CLASS = "Condor";

    /**
     * The name of the class implementing the Condor GlideIN Style.
     */
    private static final String GLIDEIN_STYLE_IMPLEMENTING_CLASS = "CondorGlideIN";

    /**
     * The name of the class implementing the Condor GlideinWMS Style.
     */
    private static final String GLIDEINWMS_STYLE_IMPLEMENTING_CLASS = "CondorGlideinWMS";
    
    /**
     * The name of the class implementing the CondorG Style.
     */
    private static final String GLOBUS_STYLE_IMPLEMENTING_CLASS = "CondorG";
    
    /**
     * The name of the class implementing the CondorC Style.
     */
    private static final String CONDORC_STYLE_IMPLEMENTING_CLASS = "CondorC";

    /**
     * The name of the class implementing the CondorG Style.
     */
    private static final String GLITE_STYLE_IMPLEMENTING_CLASS = "GLite";

    /**
     * Returns a table that maps, the Pegasus style keys to the names of implementing
     * classes.
     *
     * @return a Map indexed by Pegasus styles, and values as names of implementing
     *         classes.
     */
    private static Map implementingClassNameTable(){
        if( mImplementingClassNameTable == null ){
            mImplementingClassNameTable = new HashMap(3);
            mImplementingClassNameTable.put( Pegasus.CONDOR_STYLE, CONDOR_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( Pegasus.GLIDEIN_STYLE, GLIDEIN_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( Pegasus.GLIDEINWMS_STYLE, GLIDEINWMS_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( Pegasus.GLOBUS_STYLE, GLOBUS_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( Pegasus.GLITE_STYLE, GLITE_STYLE_IMPLEMENTING_CLASS);
            mImplementingClassNameTable.put( Pegasus.CONDORC_STYLE, CONDORC_STYLE_IMPLEMENTING_CLASS );
        }
        return mImplementingClassNameTable;
    }
    
    /**
     * A table that maps, Pegasus style keys to the names of the corresponding classes
     * implementing the CondorStyle interface.
     */
    private static Map mImplementingClassNameTable;


    /**
     * A table that maps, Pegasus style keys to appropriate classes implementing the
     * CondorStyle interface
     */
    private  Map mImplementingClassTable ;

    /**
     * A boolean indicating that the factory has been initialized.
     */
    private boolean mInitialized;

    /**
     * Handler to the Credential Handler factory.
     */
    private CredentialHandlerFactory mCredentialFactory;

    /**
     * The default constructor.
     */
    public CondorStyleFactory(){
        mImplementingClassTable = new HashMap(3);
        mInitialized = false;
    }

    /**
     * Initializes the Factory. Loads all the implementations just once.
     *
     * @param bag  the bag of initialization objects
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public void initialize( PegasusBag bag  ) throws CondorStyleFactoryException{


        //load and intialize the CredentialHandler Factory
        mCredentialFactory = new CredentialHandlerFactory();
        mCredentialFactory.initialize( bag );
        
        //load all the implementations that correspond to the Pegasus style keys
        for( Iterator it = this.implementingClassNameTable().entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry) it.next();
            String style    = (String)entry.getKey();
            String className= (String)entry.getValue();

            //load via reflection. not required in this case though
            put( style, this.loadInstance( bag, className ));
        }

        //we have successfully loaded all implementations
        mInitialized = true;
    }


    /**
     * This method loads the appropriate implementing CondorStyle as specified
     * by the user at runtime. The CondorStyle is initialized and returned.
     *
     * @param job         the job for which the corresponding style is required.
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     */
    public CondorStyle loadInstance( Job job )
                                            throws CondorStyleFactoryException{

        //sanity checks first
        if( !mInitialized ){
            throw new CondorStyleFactoryException(
                "CondorStyleFactory needs to be initialized first before using" );
        }
        String defaultStyle = job.getSiteHandle().equalsIgnoreCase( "local" )?
                              //jobs scheduled on local site have
                              //default style as condor
                              Pegasus.CONDOR_STYLE:
                              Pegasus.GLOBUS_STYLE;

        String style = job.vdsNS.containsKey( Pegasus.STYLE_KEY )?
                       (String)job.vdsNS.get( Pegasus.STYLE_KEY ):
                       defaultStyle;

        //need to check if the style isvalid or not
        //missing for now.

        //update the job with style determined
        job.vdsNS.construct( Pegasus.STYLE_KEY, style );

        //now just load from the implementing classes
        Object cs = this.get( style );
        if ( cs == null ) {
            throw new CondorStyleFactoryException( "Unsupported style " + style);
        }

        return (CondorStyle)cs;
    }

    /**
     * This method loads the appropriate Condor Style using reflection.
     *
     *
     * @param bag  the bag of initialization objects
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @throws CondorStyleFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    private  CondorStyle loadInstance( PegasusBag bag,
                                       String className )
                                              throws CondorStyleFactoryException{

        //sanity check
        PegasusProperties properties = bag.getPegasusProperties();
        if (properties == null) {
            throw new RuntimeException( "Invalid properties passed" );
        }
        if (className == null) {
            throw new RuntimeException( "Invalid className specified" );
        }

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        CondorStyle cs = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            cs = (CondorStyle) dl.instantiate( new Object[0] );
            //initialize the loaded condor style
            cs.initialize( bag, mCredentialFactory );
        }
        catch (Exception e) {
            throw new CondorStyleFactoryException( "Instantiating Condor Style ",
                                                   className,
                                                   e);
        }

        return cs;
    }

    /**
     * Returns the implementation from the implementing class table.
     *
     * @param style           the Pegasus style
     *
     * @return implementation  the class implementing that style, else null
     */
    private Object get( String style ){
        return mImplementingClassTable.get( style);
    }


    /**
     * Inserts an entry into the implementing class table.
     *
     * @param style           the Pegasus style
     * @param implementation  the class implementing that style.
     */
    private void put( String style, Object implementation){
        mImplementingClassTable.put( style, implementation );
    }


    

}
