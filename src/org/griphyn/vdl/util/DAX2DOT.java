/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.vdl.util;

import edu.isi.pegasus.common.util.Separator;
import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.*;

/**
 * Convert a dag structure into GraphViz dot format.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class DAX2DOT {
    /** Separator for strings. */
    public static final String SEPARATOR = "/";

    /** Linefeed element for labels. */
    public static final String LINEFEED = "\\n";

    /** height in inches? */
    private double m_height;

    /** width in inches? */
    private double m_width;

    /** predicate to show the derivation (DV) name. */
    private boolean m_showDV;

    /** predicate to show the transformation (TR) name. */
    private boolean m_showTR;

    /** Maintains namespace to color mappings. */
    private Map m_color;

    /** Maintains the color cycle. */
    private int m_index;

    /** Map of default colors to cycle through for coloration of job nodes by TR namespace. */
    private static final String c_color[] = {"#FFAAFF", "#FFFFAA", "#FFAAAA"};

    /** Constructor */
    public DAX2DOT() {
        m_height = 10;
        m_width = 8;
        m_showDV = false;
        m_showTR = true;

        m_color = new TreeMap();
        m_index = 0;
    }

    /**
     * Convenience constructor sets the size of the graph.
     *
     * @param h is the height in inches
     * @param w is the width in inches
     */
    public DAX2DOT(double h, double w) {
        m_height = h;
        m_width = w;
        m_showDV = false;
        m_showTR = true;

        m_color = new TreeMap();
        m_index = 0;
    }

    /**
     * Sets the size of the graph.
     *
     * @param h is the height in inches
     * @param w is the width in inches
     * @see #getHeight()
     * @see #getWidth()
     */
    public void setSize(double h, double w) {
        m_height = h;
        m_width = w;
    }

    /**
     * Determines the height of the graph.
     *
     * @return height in inches
     * @see #setSize( double, double )
     * @see #getWidth()
     */
    public double getHeight() {
        return m_height;
    }

    /**
     * Determines the width of the graph.
     *
     * @return width in inches
     * @see #setSize( double, double )
     * @see #getHeight()
     */
    public double getWidth() {
        return m_width;
    }

    /**
     * Determines, if DV identifiers are show.
     *
     * @return true, if the DV identifier is shown
     * @see #setShowDV( boolean )
     */
    public boolean getShowDV() {
        return m_showDV;
    }

    /**
     * Sets the showing of derivation names.
     *
     * @param showDV is true to show derivation identifiers.
     * @see #getShowDV()
     */
    public void setShowDV(boolean showDV) {
        m_showDV = showDV;
    }

    /**
     * Determines, if TR identifiers are show.
     *
     * @return true, if the TR identifier is shown
     * @see #setShowTR( boolean )
     */
    public boolean getShowTR() {
        return m_showTR;
    }

    /**
     * Sets the showing of derivation names.
     *
     * @param showTR is true to show derivation identifiers.
     * @see #getShowTR()
     */
    public void setShowTR(boolean showTR) {
        m_showTR = showTR;
    }

    /**
     * Generates GraphViz .dot format from the specified ADAG
     *
     * @param adag is the ADAG instance
     * @return a string representing .dot format
     */
    public String toDOT(ADAG adag) throws IOException {
        // do not show files in the graph by default
        StringWriter sw = new StringWriter();
        toDOT(adag, sw, false);
        return sw.toString();
    }

    /**
     * Generates GraphViz .dot format from the specified ADAG
     *
     * @param adag is the ADAG instance
     * @param showFiles if set to true, then display files in the graph
     * @return a string representing .dot format
     * @see #toDOT( ADAG, Writer, boolean )
     * @see #toDOT( ADAG, Writer, boolean, String, String )
     */
    public String toDOT(ADAG adag, boolean showFiles) throws IOException {
        StringWriter sw = new StringWriter();
        toDOT(adag, sw, showFiles);
        return sw.toString();
    }

    /**
     * Generates GraphViz .dot format from the specified ADAG
     *
     * @param adag is the ADAG instance
     * @param writer is the target to output the dot specification
     * @param showFiles if set to true, then display files in the graph
     * @see #toDOT( ADAG, Writer, boolean, String, String )
     */
    public void toDOT(ADAG adag, Writer writer, boolean showFiles) throws IOException {
        toDOT(adag, writer, showFiles, null, null);
    }

    /**
     * Prepares and prints the job node of the graph. The job's unique identifier assigned in the
     * DAX is taken as the job's identifier, but the TR, ID, and DV are used as a label.
     *
     * @param w is the open file writer to print to
     * @param j is a Job element.
     * @param url is the job URL, which may be <code>null</code>.
     * @return the identifier for the job to connect the graph.
     */
    private String showJob(Writer w, Job j, String url) throws IOException {
        StringBuffer label = new StringBuffer(48);
        String id = j.getID();
        String tr = Separator.combine(j.getNamespace(), j.getName(), j.getVersion());

        label.append(id);
        if (m_showTR && tr != null) label.append(LINEFEED).append("TR ").append(tr);
        if (m_showDV) {
            String dv = Separator.combine(j.getDVNamespace(), j.getDVName(), j.getDVVersion());
            if (dv != null) label.append(LINEFEED).append("DV ").append(dv);
        }

        //
        // Doug's wish: color by namespace
        //
        String color = null;
        String ns = j.getNamespace(); // may be null!
        if (ns != null) {
            if (m_color.containsKey(ns)) {
                // existing namespace, recycle color
                color = (String) m_color.get(ns);
            } else {
                // insert new color for new namespace
                color = c_color[m_index];
                m_index = (m_index + 1) % c_color.length;
                m_color.put(ns, color);
            }
        }

        // write output for job node
        w.write("   \"");
        w.write(id);
        w.write("\" [label=\"");
        w.write(label.toString());
        if (url != null) {
            w.write("\" URL=\"");
            w.write(url);
            w.write(tr);
        }
        if (color != null) {
            w.write("\" color=\"");
            w.write(color);
        }
        w.write("\"]\n");
        return id;
    }

    /**
     * Prepares and prints the file node of the graph. The file's LFN will be its unique identifier,
     * and its label.
     *
     * @param w is the open file writer to print to
     * @param f is a Filename element.
     * @param url is the file URL, which may be <code>null</code>.
     * @return the identifier for the file to connect the graph.
     */
    private String showFile(Writer w, Filename f, String url) throws IOException {
        String lfn = f.getFilename();

        // write output for filename node
        w.write("   \"");
        w.write(lfn);
        w.write("\" [color=\"#88");
        w.write(((f.getLink() & LFN.INPUT) > 0 ? "FF" : "AA"));
        w.write(((f.getLink() & LFN.OUTPUT) > 0 ? "FF" : "AA"));

        if (url != null) {
            w.write("\" URL=\"");
            w.write(url);
            w.write(lfn);
        }
        w.write("\"]\n");
        return lfn;
    }

    /**
     * Generates GraphViz .dot format from the specified ADAG, also generates the client side HTML
     * map for nodes.
     *
     * @param adag is the ADAG instance
     * @param writer is the target to output the dot specification
     * @param showFiles if set to true, then display files in the graph
     * @param jobURL is the base URL for jobs
     * @param fileURL is the base URL for files
     */
    public void toDOT(ADAG adag, Writer writer, boolean showFiles, String jobURL, String fileURL)
            throws IOException {
        this.m_index = 0;

        writer.write("digraph DAG {\n");
        writer.write("   size=\"" + m_width + "," + m_height + "\"\n");
        writer.write("   ratio = fill\n");

        if (showFiles) {
            writer.write("   node[shape=parallelogram]\n");
            for (Iterator i = adag.iterateFilename(); i.hasNext(); ) {
                Filename fn = (Filename) i.next();
                String lfn = showFile(writer, fn, fileURL);
            }

            writer.write("   node [shape=ellipse, color=orange, style=filled]\n");
            for (Iterator i = adag.iterateJob(); i.hasNext(); ) {
                Job job = (Job) i.next();
                String jid = showJob(writer, job, jobURL);

                for (Iterator j = job.iterateUses(); j.hasNext(); ) {
                    Filename fn = (Filename) j.next();
                    String lfn = fn.getFilename();

                    // this covers in, out, and io (two arrows)
                    if ((fn.getLink() & LFN.INPUT) > 0)
                        writer.write("   \"" + lfn + "\" -> \"" + jid + "\"\n");
                    if ((fn.getLink() & LFN.OUTPUT) > 0)
                        writer.write("   \"" + jid + "\" -> \"" + lfn + "\"\n");
                }
            }
        } else {
            writer.write("   node [shape=ellipse, color=orange, style=filled]\n");
            for (Iterator i = adag.iterateJob(); i.hasNext(); ) {
                Job job = (Job) i.next();
                String jid = showJob(writer, job, jobURL);
            }

            for (Iterator c = adag.iterateChild(); c.hasNext(); ) {
                Child chld = (Child) c.next();
                String ch = chld.getChild();
                Job cjob = adag.getJob(ch);
                String cid = cjob.getID();

                for (Iterator p = chld.iterateParent(); p.hasNext(); ) {
                    String pr = (String) p.next();
                    Job pjob = adag.getJob(pr);
                    String pid = pjob.getID();
                    writer.write("   \"" + pid + "\" -> \"" + cid + "\"\n");
                }
            }
        }

        writer.write("}\n");
        writer.flush();
    }

    /** Simple test */
    public static void main(String[] args) throws IOException {
        ADAG adag = new ADAG();
        Job A = new Job("ns1", "trA", null, "ID000001");
        Job B = new Job("ns2", "trB", null, "ID000002");
        Job C = new Job("ns3", "trC", null, "ID000003");
        Job D = new Job(null, "trD", null, "ID000004");
        A.setDV("ns2", "dvA", null);
        B.setDV("ns2", "dvB", null);
        C.setDV("ns3", "dvC", null);
        D.setDV("ns3", "dvD", null);

        A.addUses(new Filename("f.1", LFN.INPUT));
        adag.addFilename("f.1", true, "true", false, LFN.XFER_MANDATORY);
        A.addUses(new Filename("f.2", LFN.OUTPUT));
        adag.addFilename("f.2", false, "true", false, LFN.XFER_MANDATORY);

        B.addUses(new Filename("f.2", LFN.INPUT));
        adag.addFilename("f.2", true, "true", false, LFN.XFER_MANDATORY);
        B.addUses(new Filename("f.3", LFN.OUTPUT));
        adag.addFilename("f.3", false, "true", false, LFN.XFER_MANDATORY);

        C.addUses(new Filename("f.2", LFN.INPUT));
        adag.addFilename("f.2", true, "true", false, LFN.XFER_MANDATORY);
        C.addUses(new Filename("f.4", LFN.OUTPUT));
        adag.addFilename("f.4", false, "true", false, LFN.XFER_MANDATORY);

        D.addUses(new Filename("f.3", LFN.INPUT));
        adag.addFilename("f.3", true, "true", false, LFN.XFER_MANDATORY);
        D.addUses(new Filename("f.4", LFN.INPUT));
        adag.addFilename("f.4", true, "true", false, LFN.XFER_MANDATORY);
        D.addUses(new Filename("f.5", LFN.OUTPUT));
        adag.addFilename("f.5", false, "true", false, LFN.XFER_MANDATORY);

        adag.addJob(A);
        adag.addJob(B);
        adag.addJob(C);
        adag.addJob(D);
        adag.addChild("ID000003", "ID000001");
        adag.addChild("ID000003", "ID000002");
        adag.addChild("ID000004", "ID000003");
        DAX2DOT d2d = new DAX2DOT(5, 5);
        d2d.setShowDV(true);
        String dot = d2d.toDOT(adag, true);
        System.out.println(dot);
    }
}
