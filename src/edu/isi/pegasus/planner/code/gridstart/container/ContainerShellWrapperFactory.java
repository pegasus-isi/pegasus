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

package edu.isi.pegasus.planner.code.gridstart.container;


import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.Job;
import java.util.HashMap;
import java.util.Map;




/**
 * A factory class to load the appropriate type of Container Shell Wrapper.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class ContainerShellWrapperFactory {

    /**
     * The default package where the all the implementing classes are supposed to
     * reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                          "edu.isi.pegasus.planner.code.gridstart.container.impl";

    /**
     * The name of the class implementing the condor code generator.
     */
    public static final String DOCKER_SHELL_WRAPPER_CLASS =
                    "Docker";

    /**
     * The name of the class implementing the Stampede Event Generator
     */
    public static final String NO_SHELL_WRAPPER_CLASS =
                    "None";
    
    /**
     * The corresponding short names for the implementations.
     */
    public static String[] CONTAINER_SHORT_NAMES = {
                                           "docker",
                                           "none"
                                          };
    /**
     * The known container implementations.
     */
    public static String[] CONTAINER_IMPLEMENTING_CLASSES = {
                                                     "Docker",
                                                     "None"
                                                    };
    /**
     * A table that maps short names of <code>ContainerShellWrapper</code> implementations
     * with the implementations themselves.
     */
    private  Map<String, ContainerShellWrapper> mContainerWrapperImplementationTable ;

    /**
     * The bag of objects used for initialization.
     */
    private PegasusBag mBag;
    /**
     * A boolean indicating that the factory has been initialized.
     */
    private boolean mInitialized;
    
    /**
     * 
     */
    public ContainerShellWrapperFactory(){
        mContainerWrapperImplementationTable = new HashMap( 3 );
        mInitialized = false;
    }
    
    /**
     * Initializes the factory with known GridStart implementations.
     *
     * @param bag   the bag of objects that is used for initialization.
     */
    public void initialize( PegasusBag bag ){
        mBag       = bag;
       
        //load all the known implementations and initialize them
        for( int i = 0; i < CONTAINER_IMPLEMENTING_CLASSES.length; i++){
            //load via reflection just once
            registerContainerShellWrapper( CONTAINER_SHORT_NAMES[i],
                               this.loadInstance( bag, CONTAINER_IMPLEMENTING_CLASSES[i] )
                             );
        }

        mInitialized = true;
    }

    
    /**
     * This method loads the appropriate implementing Container Shell Wrapper
     * as specified by the user at runtime. 
     *
     *
     * @param job  the job for which wrapper is requried.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception ContainerShellWrapperFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public  ContainerShellWrapper loadInstance( Job job )
        throws ContainerShellWrapperFactoryException{

        Container c = job.getContainer();
        String shortName = null;
        if( c == null ){
            shortName = ContainerShellWrapperFactory.NO_SHELL_WRAPPER_CLASS;
        }
        else{
            Container.TYPE type = c.getType();
            if( c.getType().equals( Container.TYPE.docker) ){
                shortName = ContainerShellWrapperFactory.DOCKER_SHELL_WRAPPER_CLASS;
            }
            else{
                throw new ContainerShellWrapperFactoryException( "Unsupported Container Shell Wrapper of type" + type );
            }
        }
        
        return loadInstance( mBag, shortName );

    }

    /**
     * This method loads the appropriate code generator as specified by the
     * user at runtime.
     *
     *
     * @param className  the name of the implementing class.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception ContainerShellWrapperFactoryException that nests any error that
     *            might occur during the instantiation of the implementation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    private static ContainerShellWrapper loadInstance( PegasusBag bag, String className )
        throws ContainerShellWrapperFactoryException{

        //prepend the package name if classname is actually just a basename
        className = (className.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + className :
            //load directly
            className;

        //try loading the class dynamically
        ContainerShellWrapper containerShellWrapper = null;
        try {
            DynamicLoader dl = new DynamicLoader( className );
            containerShellWrapper = (ContainerShellWrapper) dl.instantiate( new Object[0] );
            //initialize the loaded code generator
            containerShellWrapper.initialize( bag );
        }
        catch (Exception e) {
            throw new ContainerShellWrapperFactoryException(
                "Instantiating Container Shell Wrapper ",
                className, e);
        }

        return containerShellWrapper;
    }

    /**
     * Inserts an entry into the implementing class table. The name is
     * converted to lower case before being stored.
     *
     * @param name       the short name for a GridStart implementation
     * @param implementation  the object of the class implementing that style.
     */
    private void registerContainerShellWrapper( String name, ContainerShellWrapper implementation){
        mContainerWrapperImplementationTable.put( name.toLowerCase(), implementation );
    }
    
    
    
}
