/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.transformation.client;

/**
 * This is a helper class which all TC client components (like tcAdd, tcDelete and tcQuery) must
 * extend.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.common.util.ProfileParser;
import edu.isi.pegasus.common.util.ProfileParserException;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

public class Client {
    protected int trigger = 0;

    protected String lfn = null;

    protected String pfn = null;

    protected String profile = null;

    protected String type = null;

    protected String resource = null;

    protected String systemstring = null;

    protected String namespace = null;

    protected String name = null;

    protected String version = null;

    protected List profiles = null;

    protected SysInfo system = null;

    protected String file = null;

    protected LogManager mLogger = null;

    protected TransformationCatalog tc = null;

    protected boolean isxml = false;

    protected boolean isoldformat = false;

    public Client() {}

    /**
     * Takes the arguments from the TCClient and stores it for acess to the other TC Client modules.
     *
     * @param argsmap Map
     */
    public void fillArgs(Map argsmap) {
        lfn = (String) argsmap.get("lfn");
        pfn = (String) argsmap.get("pfn");
        resource = (String) argsmap.get("resource");
        type = (String) argsmap.get("type");
        profile = (String) argsmap.get("profile");
        systemstring = (String) argsmap.get("system");
        trigger = ((Integer) argsmap.get("trigger")).intValue();
        file = (String) argsmap.get("file");
        isxml = ((Boolean) argsmap.get("isxml")).booleanValue();
        isoldformat = ((Boolean) argsmap.get("isoldformat")).booleanValue();
        if (lfn != null) {
            String[] logicalname = Separator.split(lfn);
            namespace = logicalname[0];
            name = logicalname[1];
            version = logicalname[2];
        }
        if (profile != null) {
            try {
                profiles = ProfileParser.parse(profile);
            } catch (ProfileParserException ppe) {
                mLogger.log(
                        "Parsing profiles "
                                + ppe.getMessage()
                                + " at position "
                                + ppe.getPosition(),
                        ppe,
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        if (systemstring != null) {
            system = new SysInfo(systemstring);
        }
    }

    /**
     * Returns an error message that chains all the lower order error messages that might have been
     * thrown.
     *
     * @param e the Exception for which the error message has to be composed.
     * @param logLevel the user specified level for the logger
     * @return the error message.
     */
    public static String convertException(Exception e, int logLevel) {
        StringBuffer message = new StringBuffer();
        int i = 0;

        // check if we want to throw the whole stack trace
        if (logLevel >= LogManager.INFO_MESSAGE_LEVEL) {
            // we want the stack trace to a String Writer.
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            return sw.toString();
        }

        // append all the causes
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof FactoryException) {
                // do the specialized convert for Factory Exceptions
                message.append(((FactoryException) cause).convertException(i));
                break;
            }
            message.append("\n [")
                    .append(Integer.toString(++i))
                    .append("] ")
                    .append(cause.getClass().getName())
                    .append(": ")
                    .append(cause.getMessage());

            // append just one elment of stack trace for each exception
            message.append(" at ").append(cause.getStackTrace()[0]);
        }
        return message.toString();
    }
}
