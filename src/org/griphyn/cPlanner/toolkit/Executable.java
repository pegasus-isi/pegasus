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

package org.griphyn.cPlanner.toolkit;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.util.FactoryException;

import org.griphyn.common.util.Version;
import org.griphyn.common.util.VDSProperties;

import gnu.getopt.LongOpt;

/**
 * The interface which defines all the methods , any executable should implement.
 *
 * @author GAURANG MEHTA
 * @author KARAN VAHI
 * @version $Revision$
 *
 */
public abstract class Executable {

    /**
     * The LogManager object which is used to log all the messages.
     *
     * @see org.griphyn.cPlanner.common.LogManager
     */
    protected LogManager mLogger ;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * It stores the verison of the Griphyn Virtual Data System software.
     */
    protected String mVersion;

    /**
     * The error message to be logged.
     */
    protected String mLogMsg;

    /**
     * The constructor which ends up initialising the PegasusProperties object.
     */
    public Executable() {
        mProps = PegasusProperties.getInstance();
        mVersion = Version.instance().toString();
        //setup logging before doing anything with properties
        setupLogging();
        mLogMsg = new String();
        loadProperties();
    }

    /**
     * Returns an error message that chains all the lower order error messages
     * that might have been thrown.
     *
     * @param e  the Exception for which the error message has to be composed.
     * @return  the error message.
     */
    public static String convertException( Exception e ){
        StringBuffer message = new StringBuffer();
        int i = 0;
        //append all the causes
        for(Throwable cause = e; cause != null ; cause  = cause.getCause()){
            if( cause instanceof FactoryException ){
                //do the specialized convert for Factory Exceptions
                message.append(((FactoryException)cause).convertException(i));
                break;
            }
            message.append("\n [").append( Integer.toString(++i)).append("] ").
                    append(cause.getClass().getName()).append(": ").
                    append(cause.getMessage());

            //append just one elment of stack trace for each exception
            message.append( " at " ).append( cause.getStackTrace()[0] );
        }
        return message.toString();
    }

    /**
     * Sets up the logging options for this class. Looking at the properties
     * file, sets up the appropriate writers for output and stderr.
     */
    protected void setupLogging(){
        //setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance( mProps );
        mLogger.logEventStart( "pegasus", "planner", mVersion );

        //get the logging value set in properties
        //cannot ask for PegasusProperties, as deprecation warnings could be
        //logged. So get it directly from VDS Properties. Karan May 11, 2006.
        String value = VDSProperties.noHassleInstance().getProperty("pegasus.log.*");

        //use defaults if nothing is set.
        if( value == null){
            mLogger.log("Logging to default streams",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            return;
        }
        else{
            //log both output and error messages to value specified
            mLogger.setWriters(value);
        }


    }



    /**
     * Loads all the properties that would be needed by the Toolkit classes.
     */
    public abstract void loadProperties();

    /**
     * This method is used to print the long version of the command.
     */
    public abstract void printLongVersion();

    /**
     * This is used to print the short version of the command.
     */
    public abstract void printShortVersion();

    /**
     * This function is passed command line arguments. In this function you
     * generate the valid options and parse the options specified at run time.
     */
    //public abstract void executeCommand(String[] args);

    /**
     * Generates an array of valid <code>LongOpt</code> objects which contain
     * all the valid options to the Executable.
     */
    public abstract LongOpt[] generateValidOptions();

    /**
     * Returns the version of the Griphyn Virtual Data System.
     */
    public String getGVDSVersion() {
        StringBuffer sb = new StringBuffer();
        sb.append( "Pegasus Release Version " ).append(mVersion);
        return sb.toString();
    }

    /**
     * Logs messages to the singleton logger.
     *
     * @param msg is the message itself.
     * @param level is the level to generate the log message for.
     */
    public void log( String msg, int level ){
        mLogger.log( msg, level );
    }



    /**
     * Get the value of the environment variable.
     *
     * @param envVariable   the environment variable whose value you want.
     *
     * @return  String corresponding to the value of the environment
     *          variable if it is set.
     *          null if the environment variable is not set
     */
    public String getEnvValue(String envVariable) {
        String value = null;
        value = System.getProperty(envVariable);
        return value;
    }
}
