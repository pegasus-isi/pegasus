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


package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.logging.LogManager;
import java.util.Iterator;
import java.util.Properties;

/**
 * A utility class that returns JAVA Properties that need to be set based on
 * a configuration value
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusConfiguration {
    
    /**
     * The property key for pegasus configuration.
     */
    public static final String PEGASUS_CONFIGURATION_PROPERTY_KEY = "pegasus.configuration";

    /**
     * The value for the S3 configuration.
     */
    public static final String S3_CONFIGURATION_VALUE = "S3";
    
    
    /**
     * The value for the condor configuration.
     */
    public static final String CONDOR_CONFIGURATION_VALUE = "Condor";
    
    /**
     * The logger to use.
     */
    private LogManager mLogger;
    
    /**
     * Overloaded Constructor
     * 
     * @param logger   the logger to use.
     */
    public PegasusConfiguration( LogManager logger ){
        mLogger = logger;
    }
    
    /**
     * Loads configuration specific properties into PegasusProperties
     * 
     * @param properties   the Pegasus Properties.
     */
    public void loadConfigurationProperties( PegasusProperties properties ){
        String configuration  = properties.getProperty( PEGASUS_CONFIGURATION_PROPERTY_KEY ) ;
        
        Properties props = this.getConfigurationProperties(configuration);
        for( Iterator it = props.keySet().iterator(); it.hasNext(); ){
            String key = (String) it.next();
            String value = props.getProperty( key );
            this.checkAndSetProperty( properties, key, value );
        }
        
    }
    
    /**
     * Returns Properties corresponding to a particular configuration.
     * 
     * @param configuration  the configuration value.
     * 
     * @return Properties
     */
    public Properties getConfigurationProperties( String configuration ){
        Properties p = new Properties( );
        
        if( configuration.equalsIgnoreCase( S3_CONFIGURATION_VALUE ) ){
            p.setProperty( "pegasus.execute.*.filesystem.local", "true" );
            p.setProperty( "pegasus.dir.create.impl", "S3" );
            p.setProperty( "pegasus.file.cleanup.impl", "S3" );
            p.setProperty( "pegasus.transfer.*.impl", "S3" );
            p.setProperty( "pegasus.transfer.stage.sls.file", "false" );
            p.setProperty( "pegasus.gridstart", "SeqExec" );
        }
        else if ( configuration.equalsIgnoreCase( CONDOR_CONFIGURATION_VALUE )  ){
            p.setProperty( "pegasus.transfer.refiner", "Condor" );
            p.setProperty( "pegasus.transfer.sls.*.impl", "Condor" );
            p.setProperty( "pegasus.selector.replica", "Local" );
            p.setProperty( "pegasus.execute.*.filesystem.local", "true" );

        }
        
        return p;
    }
    
    /**
     * Checks for a property, if it does not exist then sets the property to 
     * the value passed
     * 
     * @param key   the property key
     * @param value the value to set to 
     */
    protected void checkAndSetProperty( PegasusProperties properties, String key, String value ) {
        String propValue = properties.getProperty( key );
        if( propValue == null ){
            //set the value
            properties.setProperty( key, value );
        }
        else{
            //log a warning 
            StringBuffer sb = new StringBuffer();
            sb.append( "Property Key " ).append( key ).append( " already set to " ).
               append( value ).append( ". Will not be set to - ").append( value );
            mLogger.log( sb.toString(), LogManager.WARNING_MESSAGE_LEVEL );
        }
    }

}
