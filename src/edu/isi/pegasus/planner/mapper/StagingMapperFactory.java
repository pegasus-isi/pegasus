/*
 *  Copyright 2007-2016 University Of Southern California
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
package edu.isi.pegasus.planner.mapper;


import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import java.util.Properties;


/**
 * A factory class to load the appropriate type of Directory StagingMapper
 specified by the user at runtime in properties. 
 * 
 * @author Karan Vahi
 * @version $Revision$
 */

public class StagingMapperFactory {

    /**
     * The default package where the all the implementing classes reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
                                        "edu.isi.pegasus.planner.mapper.staging";

    /**
     * The name of the class that corresponds to the Hashed Staging Mapper
     */
    public static final String HASHED_STAGING_MAPPER = "Hashed";
    
    /**
     * he name of the class that corresponds to the Flat Staging Mapper
     */
    public static final String FLAT_STAGING_MAPPER = "Flat";



    /**
     * Loads the implementing class corresponding to the mode specified by the user
     * at runtime in the properties file. A default replica selector is loaded
     * if property is not specified in the properties.
     *
     * @param bag    the bag of objects that is required.
     *
     * @return the instance of the class implementing this interface.
     * @throws StagingMapperFactoryException that chains any error that
            might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     * @see #HASHED_STAGING_MAPPER
     */
    public static StagingMapper loadInstance( PegasusBag bag )
                                         throws StagingMapperFactoryException {

        PegasusProperties properties = ( PegasusProperties )bag.get( PegasusBag.PEGASUS_PROPERTIES );
        String className = null;
        StagingMapper creator;

        //sanity check
        try{
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            
            //figure out the default  mapper
            //we use Hashed as default only if pegasus.data.configuration is nonsharedfs
            String value = properties.getProperty( PegasusConfiguration.PEGASUS_CONFIGURATION_PROPERTY_KEY);
            String dfault = FLAT_STAGING_MAPPER;
            if( value != null && value.equalsIgnoreCase( PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE ) ){
                dfault = HASHED_STAGING_MAPPER;
            }
            
            //figure out the implementing class
            //that needs to be instantiated.
            className = properties.getProperty( StagingMapper.PROPERTY_PREFIX );
            className = ( className == null || className.trim().length() < 2) ?
                          dfault :
                          className;

            //prepend the package name if required
            className = (className.indexOf('.') == -1)?
                         //pick up from the default package
                         DEFAULT_PACKAGE_NAME + "." + className:
                         //load directly
                         className;

            Properties mapperProps = properties.matchingSubset( StagingMapper.PROPERTY_PREFIX, false );

            //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            creator = ( StagingMapper ) dl.instantiate( new Object[ 0 ] );
            creator.initialize( bag , mapperProps );
        }
        catch(Exception e){
            //chain the exception caught into the appropriate Factory Exception
            throw new StagingMapperFactoryException( "Instantiating Staging Mapper ",
                                                     className, e );
        }

        return creator;
    }

}
