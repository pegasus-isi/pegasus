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
package org.griphyn.vdl.dax;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import org.griphyn.vdl.classes.LFN;

/**
 * This class defines the DAX job in an abstract manner.
 *
 * @author Karan Vahi
 */
public class DAXJob extends Job implements Cloneable {

    /** The DAX LFN. */
    private Filename mDAXLFN;

    /** The DAX PFN. They are separate till the Filename object encompasses a PFN object. */
    private String mPFN;

    /** The site with which PFN is associated. */
    private String mSite;

    /**
     * Overloaded constructor.
     *
     * @param id the id to be assigned to a job.
     * @param daxLFN the lfn to be assigned to the DAX
     */
    public DAXJob(String id, String daxLFN) {
        this.setID(id);
        this.mDAXLFN = new Filename(daxLFN, LFN.INPUT);
        this.mDAXLFN.setRegister(false);
        this.mDAXLFN.setTransfer(LFN.XFER_MANDATORY);
    }

    /**
     * Sets the PFN for the DAX.
     *
     * @param pfn
     * @param site
     */
    public void setDAXPFN(String pfn, String site) {
        this.mPFN = pfn;
        this.mSite = site;
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":dax" : "dax";
        String tag2 =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":argument"
                        : "argument";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);

        writeAttribute(stream, " file=\"", this.mDAXLFN.getFilename());

        writeAttribute(stream, " id=\"", this.getID());

        // misc. attributes like the search tree depth
        if (this.getLevel() != -1)
            writeAttribute(stream, " level=\"", Integer.toString(this.getLevel()));

        // open tag: finish opening tag
        stream.write('>');
        if (indent != null) stream.write(newline);

        // concat all command line fragments into one big string.
        String newindent = indent == null ? null : indent + "  ";
        if (this.getArgumentCount() > 0) {
            if (newindent != null) stream.write(newindent);

            stream.write('<');
            stream.write(tag2);
            stream.write('>');

            for (Iterator i = this.getArgumentList().iterator(); i.hasNext(); ) {
                // casting will print a mixed content string or Filename element
                ((Leaf) i.next()).shortXML(stream, "", namespace, 0x00);
            }
            stream.write("</");
            stream.write(tag2);
            stream.write('>');
            if (indent != null) stream.write(newline);
        }

        // profiles to be dumped next
        for (Iterator i = this.getProfileList().iterator(); i.hasNext(); ) {
            ((Profile) i.next()).toXML(stream, newindent, namespace);
        }

        // finally any bound stdio descriptor
        // FIXME: really need to dump a Filename element!
        /* not populating stdin/stdout/stderr/other filenames for now */
        /*
        if ( this.getStdin() != null )
            stream.write( formatFilename( "stdin", newindent, namespace,
        this.getStdin(), false) );
        if ( this.m_stdout != null )
            stream.write( formatFilename( "stdout", newindent, namespace,
        this.m_stdout, false) );
        if ( this.m_stderr != null )
            stream.write( formatFilename( "stderr", newindent, namespace,
        this.m_stderr, false) );

        // VDL referenced Filenames to be dumped next
        for ( Iterator i=this.m_usesList.iterator(); i.hasNext(); ) {
            stream.write( formatFilename( "uses", newindent, namespace,
        (Filename) i.next(), true) );
        }
        */

        // write out the DAX file contents
        stream.write(this.formatFilename(newindent, namespace, mDAXLFN, mPFN, mSite));

        // finish job
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }

    /**
     * Helper: Formats the uses tags with the nested PFN tag.
     *
     * @param indent is the indentation of the element, may be null.
     * @param namespace is an optional namespace to use in the tag.
     * @param f is an instance of a <code>Filename</code> object.
     * @param pfn the pfn for the dax file
     * @param site the site attribute for the PFN
     * @return the XML-formatted attributes without the element tags.
     */
    private String formatFilename(
            String indent, String namespace, Filename f, String pfn, String site) {

        String newline = System.getProperty("line.separator", "\r\n");
        String tag = "uses";
        StringBuffer result = new StringBuffer(128);
        if (namespace != null && namespace.length() > 0) tag = namespace + ":" + tag;

        if (indent != null && indent.length() > 0) result.append(indent);

        result.append('<').append(tag);
        result.append(" file=\"").append(quote(f.getFilename(), true)).append('"');
        result.append(" link=\"").append(LFN.toString(f.getLink())).append('"');

        result.append(" register=\"").append(Boolean.toString(f.getRegister())).append('"');
        result.append(" transfer=\"").append(LFN.transferString(f.getTransfer())).append('"');
        result.append(" type=\"").append(LFN.typeString(f.getType())).append('"');

        if (f.getOptional()) result.append(" optional=\"").append(f.getOptional()).append("\"");

        if (f.getTemporary() != null)
            result.append(" temporaryHint=\"").append(quote(f.getTemporary(), true)).append('"');

        // add a nested pfn if required.
        if (pfn != null) {
            result.append(">").append(newline);
            String newindent = indent == null ? null : indent + "  ";
            result.append(newindent);

            result.append("<pfn url=\"")
                    .append(this.mPFN)
                    .append("\"")
                    .append(" site=\"")
                    .append(this.mSite)
                    .append("\"")
                    .append("/>");

            result.append(newline);
            if (indent != null && indent.length() > 0) result.append(indent);

            result.append("</").append(tag).append(">");

        } else { // add newline and done
            result.append("/>");
        }

        if (indent != null) result.append(newline);

        return result.toString();
    }

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance, semi-deep copy
     */
    public Object clone() {
        DAXJob result = (DAXJob) super.clone();
        result.setDAXPFN(this.mPFN, this.mSite);
        result.mDAXLFN = (Filename) this.mDAXLFN.clone();

        return result;
    }
}
