package edu.isi.pegasus.planner.catalog.transformation.client;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.XMLWriter;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.dax.Executable;
import edu.isi.pegasus.planner.dax.Executable.ARCH;
import edu.isi.pegasus.planner.dax.Executable.OS;
import edu.isi.pegasus.planner.dax.PFN;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * This is a utility class for converting transformation catalog into different formats.
 *
 * @author prasanth@isi.edu
 * @version $Revision $
 */
public class TCFormatUtility {

    /**
     * Converts the transformations into multi line text format
     *
     * @param mTCStore the transformation store
     * @return the text format
     */
    public static String toTextFormat(TransformationStore mTCStore) {
        String newline = System.getProperty("line.separator", "\r\n");
        String indent = "";
        StringBuffer buf = new StringBuffer();
        String newIndent = indent + "\t";

        // write header
        buf.append(
                "# multiple line text-based transformation catalog: "
                        + Currently.iso8601(false, true, true, new Date()));
        buf.append(newline);

        // write out data
        // traverse through all the logical transformations in the
        // catalog
        for (Iterator i = mTCStore.getTransformations(null, null).iterator(); i.hasNext(); ) {
            // transformation is the complete name comprised of namespace,name,version
            String transformation = (String) i.next();

            buf.append(indent);
            buf.append("tr ");
            buf.append(transformation);
            buf.append(" {");
            buf.append(newline);

            // get all the entries for that transformations on all sites
            for (TransformationCatalogEntry entry :
                    mTCStore.getEntries(transformation, (String) null)) {
                // write out all the entries for the transformation
                buf.append(toText(entry, newline, newIndent));
            }

            buf.append(indent);
            buf.append("}");
            buf.append(newline);
            buf.append(newline);
        }

        return buf.toString();
    }

    /**
     * Converts the transformation catalog entry object to the multi line textual representation.
     * e.g.
     *
     * <p>site wind { profile env "me" "with" profile condor "more" "test" pfn "/path/to/keg" arch
     * "x86" os "linux" osrelease "fc" osversion "4" type "STAGEABLE" }
     *
     * @param entry the transformation catalog entry
     * @param newline the newline characters
     * @param indent the indentation to use
     * @return the textual description
     */
    private static String toText(TransformationCatalogEntry entry, String newline, String indent) {
        StringBuffer sb = new StringBuffer();
        indent = (indent != null && indent.length() > 0) ? indent : "";
        String newIndent = indent + "\t";

        sb.append(indent);
        sb.append("site").append(" ").append(entry.getResourceId()).append(" {").append(newline);

        // list out all the profiles
        List<Profile> profiles = entry.getProfiles();
        if (profiles != null) {
            for (Profile p : profiles) {
                sb.append(newIndent)
                        .append("profile")
                        .append(" ")
                        .append(p.getProfileNamespace())
                        .append(" ")
                        .append(quote(p.getProfileKey()))
                        .append(" ")
                        .append(quote(p.getProfileValue()))
                        .append(" ")
                        .append(newline);
            }
        }

        // write out the pfn
        addLineToText(sb, newIndent, newline, "pfn", entry.getPhysicalTransformation());

        // write out sysinfo
        SysInfo s = entry.getSysInfo();
        SysInfo.Architecture arch = s.getArchitecture();
        if (arch != null) {
            addLineToText(sb, newIndent, newline, "arch", arch.toString());
        }
        SysInfo.OS os = s.getOS();
        if (os != null) {
            addLineToText(sb, newIndent, newline, "os", os.toString());
        }
        String osrelease = s.getOSRelease();
        if (osrelease != null && osrelease.length() > 0) {
            addLineToText(sb, newIndent, newline, "osrelease", osrelease);
        }
        String osversion = s.getOSVersion();
        if (osversion != null && osversion.length() > 0) {
            addLineToText(sb, newIndent, newline, "osversion", osversion);
        }
        String glibc = s.getGlibc();
        if (glibc != null && glibc.length() > 0) {
            addLineToText(sb, newIndent, newline, "glibc", glibc);
        }

        // write out the type
        addLineToText(sb, newIndent, newline, "type", entry.getType().toString());

        sb.append(indent).append("}").append(newline);

        return sb.toString();
    }

    /**
     * Convenience method to add a line to the internal textual representation.
     *
     * @param sb the StringBuffer to which contents are to be added.
     * @param newIndent the indentation
     * @paran newline the newline character
     * @param key the key
     * @param value the value
     */
    private static void addLineToText(
            StringBuffer sb, String newIndent, String newline, String key, String value) {

        sb.append(newIndent).append(key).append(" ").append(quote(value)).append(newline);
    }

    /**
     * Quotes a String.
     *
     * @param str the String to be quoted.
     * @return quoted version
     */
    private static String quote(String str) {
        // maybe should use the escape class also?
        StringBuffer sb = new StringBuffer();
        sb.append("\"").append(str).append("\"");
        return sb.toString();
    }

    /**
     * Prints the transformations in XML format
     *
     * @param tStore the transformation store
     */
    // Note : xml format ignores logical profiles associated with a transformation.
    public static void printXMLFormat(TransformationStore tStore) {
        BufferedWriter pw = new BufferedWriter(new OutputStreamWriter(System.out));
        XMLWriter writer = new XMLWriter(pw);
        for (TransformationCatalogEntry entry : tStore.getEntries(null, (TCType) null)) {
            Executable exec =
                    new Executable(
                            entry.getLogicalNamespace(),
                            entry.getLogicalName(),
                            entry.getLogicalVersion());
            exec.setArchitecture(ARCH.valueOf(entry.getSysInfo().getArchitecture().toString()));
            exec.setOS(OS.valueOf(entry.getSysInfo().getOS().toString()));
            exec.setOSVersion(entry.getSysInfo().getOSVersion());
            exec.setGlibc(entry.getSysInfo().getGlibc());
            if (entry.getType().equals(TCType.INSTALLED)) {
                exec.setInstalled(true);
            } else {
                exec.setInstalled(false);
            }
            PFN pfn = new PFN(entry.getPhysicalTransformation(), entry.getResourceId());
            if (entry.getProfiles() != null) {
                for (Profile profile : ((List<Profile>) entry.getProfiles())) {
                    pfn.addProfile(
                            profile.getProfileNamespace(),
                            profile.getProfileKey(),
                            profile.getProfileValue());
                }
            }

            exec.addPhysicalFile(pfn);
            exec.toXML(writer);
        }
        writer.close();
        return;
    }
}
