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
 * This is a TCClient class which handles the Query Operations.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.ProfileParser;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TCQuery extends Client {

    private static final int TABSPACE = 4;

    private static final String XML_NAMESPACE = "http://pegasus.isi.edu/schema";
    private static final String XML_VERSION = "2.0";

    public TCQuery(TransformationCatalog tc, LogManager mLogger, Map argsmap) {
        this.fillArgs(argsmap);
        this.tc = tc;
        this.mLogger = mLogger;
    }

    public void doQuery() {

        // SWitch for what triggers are defined.
        switch (trigger) {
            case 1: // query and return entire tc
                if (!isxml) {
                    getTC();
                } else {
                    getTCXML();
                }
                break;
            case 2: // query lfns
                getLfn(resource, type);
                break;
            case 4: // query for PFN
                getPfn(namespace, name, version, resource, type);
                break;
            case 8: // query for Resource
                getResource(type);
                break;
            case 18: // query for LFN profiles
                getLfnProfile(namespace, name, version);
                break;
            case 20: // query for PFN profiles
                getPfnProfile(pfn, resource, type);
                break;
            default:
                mLogger.log(
                        "Wrong trigger invoked in TC Query. Try pegasus-tc-client --help for a detailed help.",
                        LogManager.FATAL_MESSAGE_LEVEL);
                System.exit(1);
        }
    }

    /**
     * Get logical transformations on a given resource and/or of a particular type.
     *
     * @param resource The resource on which the transformations exist
     * @param type the type of the transformation.
     */
    private void getLfn(String resource, String type) {
        List l = null;
        TCType t = (type == null) ? null : TCType.valueOf(type);
        try {
            mLogger.log(
                    "Querying the TC for logical names on resource " + resource + " and type " + t,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            l = tc.getTCLogicalNames(resource, t);
        } catch (Exception e) {
            mLogger.log("Unable to query for logicalnames", e, LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
        if (l != null) {
            // int[] count = ( int[] ) l.get( l.size() - 1 );
            int[] count = {0, 0};
            // l.remove( l.size() - 1 );
            for (Iterator itr = l.iterator(); itr.hasNext(); ) {
                String[] s = (String[]) itr.next();
                columnLength(s, count);
            }
            System.out.println(
                    "#RESID"
                            + getSpace(count[0], "#RESID".length())
                            + "  LTX"
                            + getSpace(count[1], "  LTX".length())
                            + "TYPE");
            System.out.println("");

            for (Iterator i = l.iterator(); i.hasNext(); ) {
                String[] s = (String[]) i.next();
                System.out.println(
                        " "
                                + s[0]
                                + getSpace(count[0], s[0].length())
                                + s[1]
                                + getSpace(count[1], s[1].length())
                                + s[2]);
            }
        } else {
            mLogger.log("No Logical Transformations found.", LogManager.CONSOLE_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    /**
     * Get physical transformation for a particular logical transformation and/or on a resource
     * and/or of a particular type
     *
     * @param namespace String Namespace for the transformation.
     * @param name String Logical name for the transformation.
     * @param version String Version for the transformation.
     * @param resource String The resource for the transformation
     * @param type String The type of the transformation.
     */
    private void getPfn(
            String namespace, String name, String version, String resource, String type) {
        if (name != null) {
            List<TransformationCatalogEntry> l = null;
            TCType t = (type == null) ? null : TCType.valueOf(type);
            try {
                mLogger.log(
                        "Querying the TC for physical names for lfn "
                                + lfn
                                + " resource "
                                + resource
                                + " type "
                                + type,
                        LogManager.DEBUG_MESSAGE_LEVEL);
                l = tc.lookupNoProfiles(namespace, name, version, resource, t);
            } catch (Exception e) {
                mLogger.log(
                        "Unable to query for physical names", e, LogManager.FATAL_MESSAGE_LEVEL);
                System.exit(1);
            }
            if (l != null && !l.isEmpty()) {
                if (isoldformat) {
                    int count[] = {0, 0, 0};
                    for (TransformationCatalogEntry entry : l) {
                        String[] s = {
                            entry.getResourceId(),
                            entry.getPhysicalTransformation(),
                            entry.getType().toString(),
                            entry.getSysInfo().toString()
                        };
                        columnLength(s, count);
                    }

                    System.out.println(
                            "#RESID"
                                    + getSpace(count[0], "#RESID".length())
                                    + "  LTX"
                                    + getSpace(lfn.length(), "  LTX".length())
                                    + "  PFN"
                                    + getSpace(count[1], "PFN".length())
                                    + " TYPE"
                                    + getSpace(count[2], "TYPE".length())
                                    + " SYSINFO");
                    System.out.println("");
                    for (TransformationCatalogEntry entry : l) {
                        String[] s = {
                            entry.getResourceId(),
                            entry.getPhysicalTransformation(),
                            entry.getType().toString(),
                            entry.getSysInfo().toString()
                        };
                        System.out.println(
                                s[0]
                                        + getSpace(count[0], s[0].length())
                                        + lfn
                                        + getSpace(lfn.length(), lfn.length())
                                        + s[1]
                                        + getSpace(count[1], s[1].length())
                                        + s[2]
                                        + getSpace(count[2], s[2].length())
                                        + s[3]);
                    }
                } else {
                    TransformationStore tcStore = new TransformationStore();
                    for (TransformationCatalogEntry entry : l) {
                        tcStore.addEntry(entry);
                    }
                    if (isxml) {
                        TCFormatUtility.printXMLFormat(tcStore);
                    } else {
                        String textFormat = TCFormatUtility.toTextFormat(tcStore);
                        System.out.println(textFormat);
                    }
                }
            } else {
                mLogger.log("No Physical Transformations found.", LogManager.CONSOLE_MESSAGE_LEVEL);
                System.exit(1);
            }
        } else {
            mLogger.log("Provide an lfn to list the pfns", LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    /**
     * Get the LFn profiles associated with a logical transformation
     *
     * @param namespace String
     * @param name String
     * @param version String
     */
    private void getLfnProfile(String namespace, String name, String version) {
        if (name != null) {
            List l = null;
            try {
                mLogger.log(
                        "Querying the TC for Profiles for lfn " + lfn,
                        LogManager.DEBUG_MESSAGE_LEVEL);
                l = tc.lookupLFNProfiles(namespace, name, version);
            } catch (Exception e) {
                mLogger.log("Unable to query the lfn profiles", e, LogManager.FATAL_MESSAGE_LEVEL);
                System.exit(1);
            }
            if (l != null) {
                System.out.println("LFN Profiles :");
                for (Iterator i = l.iterator(); i.hasNext(); ) {
                    System.out.println("  " + (Profile) i.next());
                }
            } else {
                mLogger.log("No LFN Profiles found.", LogManager.CONSOLE_MESSAGE_LEVEL);
                System.exit(1);
            }
        } else {
            mLogger.log("Provide an lfn to list the lfn profiles", LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    /**
     * Get the profiles associated with a physical transformation.
     *
     * @param pfn String
     * @param resource String
     * @param type String
     */
    private void getPfnProfile(String pfn, String resource, String type) {
        if (pfn != null && resource != null && type != null) {
            List l = null;
            try {
                mLogger.log(
                        "Query the TC for profiles with pfn="
                                + pfn
                                + " type="
                                + type
                                + " resource="
                                + resource,
                        LogManager.DEBUG_MESSAGE_LEVEL);
                l = tc.lookupPFNProfiles(pfn, resource, TCType.valueOf(type));
            } catch (Exception e) {

                mLogger.log("Unable to query the pfn profiles", e, LogManager.FATAL_MESSAGE_LEVEL);
                System.exit(1);
            }
            if (l != null) {
                System.out.println("PFN Profiles :");
                for (Iterator i = l.iterator(); i.hasNext(); ) {
                    System.out.println("  " + (Profile) i.next());
                }
            } else {
                mLogger.log("No PFN Profiles found.", LogManager.FATAL_MESSAGE_LEVEL);
                System.exit(1);
            }
        } else {
            mLogger.log(
                    "Please provide an pfn, resource and type to list the pfn profiles",
                    LogManager.CONSOLE_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    /**
     * Get and print the Resource entries for a given logical transformation and transformation type
     *
     * @param type the type of the transformation
     * @throws Exception Throws all kinds of exception
     */
    private void getResource(String type) {
        List l = null;
        TCType t = (type == null) ? null : TCType.valueOf(type);
        try {
            l = tc.lookupSites(namespace, name, version, t);
        } catch (Exception e) {
            mLogger.log("Unable to query TC for resources", e, LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
        if (l != null) {
            System.out.println("Resources :");
            for (Iterator i = l.iterator(); i.hasNext(); ) {
                System.out.println("  " + (String) i.next());
            }
        } else {
            mLogger.log("No resources found.", LogManager.CONSOLE_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    /** Print all the contents of the TC in pretty print */
    private void getTC() {
        try {
            List<TransformationCatalogEntry> l = tc.getContents();
            if (l != null && !l.isEmpty()) {
                if (isoldformat) {
                    // this means entries are there.
                    // get the pretty print column size information.
                    int[] count = {0, 0, 0, 0, 0};
                    for (Iterator i = l.iterator(); i.hasNext(); ) {
                        TransformationCatalogEntry tcentry = (TransformationCatalogEntry) i.next();
                        String[] s = {
                            tcentry.getResourceId(),
                            tcentry.getLogicalTransformation(),
                            tcentry.getPhysicalTransformation(),
                            tcentry.getType().toString(),
                            tcentry.getSysInfo().toString(),
                            ((tcentry.getProfiles() != null)
                                    ? ProfileParser.combine(tcentry.getProfiles())
                                    : "NULL")
                        };
                        columnLength(s, count);
                    }
                    System.out.println(
                            "#RESID"
                                    + getSpace(count[0], "#RESID".length())
                                    + "  LTX"
                                    + getSpace(count[1], "  LTX".length())
                                    + "  PFN"
                                    + getSpace(count[2], "  PFN".length())
                                    + " TYPE"
                                    + getSpace(count[3], "TYPE".length())
                                    + " SYSINFO"
                                    + getSpace(count[4], "SYSINFO".length())
                                    + "  PROFILES");
                    System.out.println("");
                    // start printing the results.
                    for (Iterator i = l.iterator(); i.hasNext(); ) {
                        TransformationCatalogEntry tcentry = (TransformationCatalogEntry) i.next();
                        StringBuffer sb = new StringBuffer();
                        sb.append(tcentry.getResourceId());
                        sb.append(getSpace(count[0], tcentry.getResourceId().length()));
                        sb.append(tcentry.getLogicalTransformation());
                        sb.append(getSpace(count[1], tcentry.getLogicalTransformation().length()));
                        sb.append(tcentry.getPhysicalTransformation());
                        sb.append(getSpace(count[2], tcentry.getPhysicalTransformation().length()));
                        sb.append(tcentry.getType());
                        sb.append(getSpace(count[3], tcentry.getType().toString().length()));
                        sb.append(tcentry.getSysInfo());
                        sb.append(getSpace(count[4], tcentry.getSysInfo().toString().length()));
                        if (tcentry.getProfiles() != null) {
                            sb.append(ProfileParser.combine(tcentry.getProfiles()));
                        } else {
                            sb.append("NULL");
                        }
                        System.out.println(sb);
                    }
                } else {
                    TransformationStore tcStore = new TransformationStore();
                    for (TransformationCatalogEntry entry : l) {
                        tcStore.addEntry(entry);
                    }
                    String textFormat = TCFormatUtility.toTextFormat(tcStore);
                    System.out.println(textFormat);
                }
            } else {
                mLogger.log("No Entries found in the TC.", LogManager.CONSOLE_MESSAGE_LEVEL);
                System.exit(1);
            }
        } catch (Exception e) {
            mLogger.log("Unable to query entire TC", LogManager.FATAL_MESSAGE_LEVEL);
            mLogger.log(convertException(e, mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    private void getTCXML() {
        try {
            List<TransformationCatalogEntry> l = tc.getContents();
            if (l != null && !l.isEmpty()) {
                TransformationStore tcStore = new TransformationStore();
                for (TransformationCatalogEntry entry : l) {
                    tcStore.addEntry(entry);
                }
                TCFormatUtility.printXMLFormat(tcStore);
            }
        } catch (Exception e) {
            mLogger.log("Unable to query entire TC", LogManager.FATAL_MESSAGE_LEVEL);
            mLogger.log(convertException(e, mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
    }

    /**
     * Gets the required space for pretty printing.
     *
     * @param maxlength int
     * @param currlength int
     * @return String
     */
    private static String getSpace(int maxlength, int currlength) {

        int length = maxlength + TABSPACE - currlength;
        StringBuffer sb = new StringBuffer(length);
        for (int i = 0; i < length; i++) {
            sb.append(" ");
        }
        return sb.toString();
    }

    /**
     * Computes the maximum column lenght for pretty printing.
     *
     * @param s String[]
     * @param length int[]
     */
    private static void columnLength(String[] s, int[] length) {
        for (int i = 0; i < length.length; i++) {
            if (s[i].length() > length[i]) {
                length[i] = s[i].length();
            }
        }
    }

    /**
     * The comparator that is used to group the RLSAttributeObject objects by the value in the key
     * field. This comparator should only be used for grouping purposes not in Sets or Maps etc.
     */
    private class LFNComparator implements Comparator {

        /**
         * Compares this object with the specified object for order. Returns a negative integer,
         * zero, or a positive integer if the first argument is less than, equal to, or greater than
         * the specified object. The TransformationCatalogEntry object are compared by their lfn
         * field.
         *
         * @param o1 is the first object to be compared.
         * @param o2 is the second object to be compared.
         * @return a negative number, zero, or a positive number, if the object compared against is
         *     less than, equals or greater than this object.
         * @exception ClassCastException if the specified object's type prevents it from being
         *     compared to this Object.
         */
        public int compare(Object o1, Object o2) {
            if (o1 instanceof TransformationCatalogEntry
                    && o2 instanceof TransformationCatalogEntry) {
                return ((TransformationCatalogEntry) o1)
                        .getLogicalTransformation()
                        .compareTo(((TransformationCatalogEntry) o2).getLogicalTransformation());
            } else {
                throw new ClassCastException("object is not TranformationCatalogEntry");
            }
        }
    }
}
