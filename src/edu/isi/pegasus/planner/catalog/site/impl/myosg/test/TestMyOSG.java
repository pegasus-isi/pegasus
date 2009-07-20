/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package edu.isi.pegasus.planner.catalog.site.impl.myosg.test;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;

import edu.isi.pegasus.planner.catalog.site.impl.MYOSG;

import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class TestMyOSG {

    private static final String TMP_FILE_NAME  ="MYOSG_SC.xml";
    private static final String DATE_FORMAT  ="MM/dd/yyyy";
        
        
    /**
     * @param args
     */
    public static void main(String[] args) {
            
        LogManager logger = LogManagerFactory.loadSingletonInstance( PegasusProperties.nonSingletonInstance() );
        logger.logEventStart( "event.pegasus.test.myosg", "planner.version", "test" );
            
        MYOSG myOSG = new MYOSG();
        Properties properties = new Properties();
                
        /* dont delete the tmp file created to store the xml
         * contents of the MyOSG website */
        properties.setProperty( "myosg.keep.tmp.file", "true" );
                
        myOSG.connect(properties);
		
        List <String> sitesList = new ArrayList();
        sitesList.add("*");
        myOSG.load(sitesList);
        System.out.println(myOSG.list().size() +"  " + myOSG.list());
        
        logger.logEventCompletion();
    }
	
	
}
