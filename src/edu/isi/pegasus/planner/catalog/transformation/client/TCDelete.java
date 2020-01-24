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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import java.util.Map;

/**
 * This is a TCClient class which handles the Delete Operations.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
public class TCDelete extends Client {

    public TCDelete(TransformationCatalog tc, LogManager mLogger, Map argsmap) {
        this.fillArgs(argsmap);
        this.tc = tc;
        this.mLogger = mLogger;
    }

    public void doDelete() {
        // SWitch for what triggers are defined.
        int status = -1;
        try {
            switch (trigger) {
                case 2: // delete TC by logical name
                    if (name == null) {
                        mLogger.log(
                                "You need to provide the logical name by which you want to delete",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC by logical name "
                                        + lfn
                                        + " on resource "
                                        + ((resource == null) ? "ALL" : resource)
                                        + " and type "
                                        + ((type == null) ? "ALL" : type),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status =
                                tc.removeByLFN(
                                        namespace,
                                        name,
                                        version,
                                        resource,
                                        ((type == null) ? null : TCType.valueOf(type)));
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC entries by logical name "
                                            + lfn
                                            + " on resource "
                                            + ((resource == null) ? "ALL" : resource)
                                            + " and type "
                                            + ((type == null) ? null : type),
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC entries by logical name "
                                            + lfn
                                            + " on resource "
                                            + ((resource == null) ? "ALL" : resource)
                                            + " and type "
                                            + ((type == null) ? null : type),
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to detele TC by logical name "
                                            + lfn
                                            + " on resource "
                                            + ((resource == null) ? null : resource)
                                            + " and type "
                                            + ((type == null) ? "ALL" : type),
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }
                    break;

                case 4: // delete TC by physical name
                    if (pfn == null || name == null) {
                        mLogger.log(
                                "You need to provide the pfn and logical "
                                        + "name by which you want to delete",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC by physicalname "
                                        + pfn
                                        + " and logical name "
                                        + lfn
                                        + " on resource "
                                        + ((resource == null) ? "ALL" : resource)
                                        + " and type "
                                        + ((type == null) ? "ALL" : type),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status =
                                tc.removeByPFN(
                                        pfn,
                                        namespace,
                                        name,
                                        version,
                                        resource,
                                        ((type == null) ? null : TCType.valueOf(type)));
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC entries by physicalname "
                                            + pfn
                                            + " and logical name "
                                            + lfn
                                            + " on resource "
                                            + ((resource == null) ? "ALL" : resource)
                                            + " and type "
                                            + ((type == null) ? "ALL" : type),
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC entries by physicalname "
                                            + pfn
                                            + " and logical name "
                                            + lfn
                                            + " on resource "
                                            + ((resource == null) ? "ALL" : resource)
                                            + " and type "
                                            + ((type == null) ? "ALL" : type),
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to delete TC by physicalname "
                                            + pfn
                                            + " and logical name "
                                            + lfn
                                            + " on resource "
                                            + ((resource == null) ? "ALL" : resource)
                                            + " and type "
                                            + ((type == null) ? "ALL" : type),
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }
                    break;
                case 8: // delete TC by resource
                    if (resource == null) {
                        mLogger.log(
                                "You need to provide the resourceid by which you want to delete",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC by resourceid " + resource,
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status = tc.removeBySiteID(resource);
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC entries by resourceid " + resource,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC entries by resourceid " + resource,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to delete TC by resourceid",
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }
                    break;
                case 18: // delete TC lfnprofiles
                    if (name == null) {
                        mLogger.log(
                                "You need to provide the logical transformation by "
                                        + "which you want to delete the profiles",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC LFN profiles for LFN " + lfn,
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status = tc.deleteLFNProfiles(namespace, name, version, profiles);
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC LFN profile entries for LFN " + lfn,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC LFN profile entries for LFN " + lfn,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to delete the TC LFN profiles",
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }
                    break;
                case 20: // delete TC pfnprofiles
                    if (pfn == null || resource == null || type == null) {
                        mLogger.log(
                                " You need to provide the physical transformation, "
                                        + " resource and type by which you want to delete the profiles",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC PFN profiles for PFN "
                                        + pfn
                                        + " type "
                                        + type
                                        + " resource "
                                        + resource,
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status =
                                tc.deletePFNProfiles(pfn, TCType.valueOf(type), resource, profiles);
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC PFN profile entries for PFN "
                                            + pfn
                                            + " type "
                                            + type
                                            + " resource "
                                            + resource,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC PFN profile entries for PFN "
                                            + pfn
                                            + " type "
                                            + type
                                            + " resource "
                                            + resource,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to delete the TC PFN profiles",
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }
                    break;
                case 32: // delete by TC type
                    if (type == null) {
                        mLogger.log(
                                "You need to provide the transformation type by "
                                        + "which you want to delete the TC.",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC by Type "
                                        + type
                                        + "and resource "
                                        + ((resource == null) ? "ALL" : resource),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status = tc.removeByType(TCType.valueOf(type), resource);
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC entries for Type "
                                            + type
                                            + " resource "
                                            + ((resource == null) ? "ALL" : resource),
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC entries for Type "
                                            + type
                                            + " resource "
                                            + ((resource == null) ? "ALL" : resource),
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to delete the TC by type",
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }
                    break;

                case 64: // delete the  TC by sysinfo.
                    if (system == null) {
                        mLogger.log(
                                "You need to provide the transformation sysinfo "
                                        + "by which you want to delete the TC.",
                                LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                                "See pegasus-tc-client --help or man pegasus-tc-client for more details ",
                                LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    } else {
                        mLogger.log(
                                "Trying to delete the TC by VDSSysInfo " + systemstring,
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        status = tc.removeBySysInfo(system);
                        if (status == 0) {
                            mLogger.log(
                                    "No match found for TC entries for VDSSysInfo " + systemstring,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else if (status >= 1) {
                            mLogger.log(
                                    "Deleted the TC entries for VDSSysInfo " + systemstring,
                                    LogManager.CONSOLE_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                    "Unable to delete the TC by VDSSysInfo",
                                    LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit(1);
                        }
                    }

                    break;
                case 127: // delete entire TC. whoopa.
                    mLogger.log("Trying to delete the entire TC ", LogManager.DEBUG_MESSAGE_LEVEL);
                    if (tc.clear() >= 0) {
                        mLogger.log(
                                "Deleted the entire tc succesfully",
                                LogManager.CONSOLE_MESSAGE_LEVEL);
                    } else {
                        mLogger.log(
                                "Error while deleting entire TC", LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit(1);
                    }
                    break;

                default:
                    mLogger.log(
                            "Wrong trigger invoked in TC Delete. Try pegasus-tc-client --help for a detailed help."
                                    + trigger,
                            LogManager.FATAL_MESSAGE_LEVEL);
                    System.exit(1);
            }
        } catch (Exception e) {
            mLogger.log("Unable to do delete operation", LogManager.FATAL_MESSAGE_LEVEL);
            mLogger.log(convertException(e, mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
    }
}
