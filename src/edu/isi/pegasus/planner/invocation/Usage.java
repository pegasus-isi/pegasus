/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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
package edu.isi.pegasus.planner.invocation;

import java.io.IOException;
import java.io.Writer;
import java.text.*;

/**
 * This class is contains some excerpt from the getrusage call. Due to Linux not populating a lot of
 * records, the amount of information is restricted. Adjustments to LP64 architecture may be
 * necessary.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Usage extends Invocation // implements Cloneable
{
    /** user time - time spent in user mode, seconds with fraction. */
    private double m_utime;

    /** system time - time spent in system mode, seconds with fraction. */
    private double m_stime;

    /** minor page faults - sometimes also recovered pages. */
    private int m_minflt;

    /** major page faults - incurred subsystem IO, sometimes includes swap. */
    private int m_majflt;

    /** number of swap operations - unused in Linux unless kernel patched. */
    private int m_nswap;

    /** number of signals sent to process. */
    private int m_nsignals;

    /** voluntary context switches. */
    private int m_nvcsw;

    /** involuntary conext switches. */
    private int m_nivcsw;

    /** maximum resident set size. */
    private int m_maxrss;

    /** integral shared memory size. */
    private int m_ixrss;

    /** integral unshared data size, or private integral resident set size. */
    private int m_idrss;

    /** integral stoack size. */
    private int m_isrss;

    /** block input operations. */
    private int m_inblock;

    /** block output operations. */
    private int m_outblock;

    /** messages sent. */
    private int m_msgsnd;

    /** messages received. */
    private int m_msgrcv;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Usage() {
        m_utime = m_stime = 0.0;
        m_minflt = m_majflt = m_nswap = m_nsignals = m_nvcsw = m_nivcsw = 0;
        m_maxrss = m_ixrss = m_idrss = m_isrss = 0;
        m_inblock = m_outblock = m_msgsnd = m_msgrcv = 0;
    }

    /**
     * Full c'tor: All values are provided.
     *
     * @param utime is the time spent in user mode
     * @param stime is the time spent in system mode
     * @param minflt are minor page faults and page reclaims
     * @param majflt are major page faults and s.t. swaps
     * @param nswap are the number of swap operations
     * @param nsignals are the number of signals sent
     * @param nvcsw are voluntary context switches
     * @param nivcsw are involuntary context switches
     * @param maxrss is the maximum resident set size
     * @param ixrss is the integral shared memory size
     * @param idrss is the integral unshared data size
     * @param isrss is the integral unshared stack size
     * @param inblock are block input operations
     * @param outblock are block output operations
     * @param msgsnd are messages sent
     * @param msgrcv are messages received
     */
    public Usage(
            double utime,
            double stime,
            int minflt,
            int majflt,
            int nswap,
            int nsignals,
            int nvcsw,
            int nivcsw,
            int maxrss,
            int ixrss,
            int idrss,
            int isrss,
            int inblock,
            int outblock,
            int msgsnd,
            int msgrcv) {
        m_utime = utime;
        m_stime = stime;
        m_minflt = minflt;
        m_majflt = majflt;
        m_nswap = nswap;
        m_nsignals = nsignals;
        m_nvcsw = nvcsw;
        m_nivcsw = nivcsw;

        m_maxrss = maxrss;
        m_ixrss = ixrss;
        m_idrss = idrss;
        m_isrss = isrss;
        m_inblock = inblock;
        m_outblock = outblock;
        m_msgsnd = msgsnd;
        m_msgrcv = msgrcv;
    }

    /**
     * Accessor: Obtains the user time from the object.
     *
     * @return the time spent in user mode.
     * @see #setUserTime(double)
     */
    public double getUserTime() {
        return this.m_utime;
    }

    /**
     * Accessor: Obtains the system time from the object.
     *
     * @return the time spent in system mode.
     * @see #setSystemTime(double)
     */
    public double getSystemTime() {
        return this.m_stime;
    }

    /**
     * Accessor: Obtains the minfor page faults.
     *
     * @return the number of page reclaims.
     * @see #setMinorFaults(int)
     */
    public int getMinorFaults() {
        return this.m_minflt;
    }

    /**
     * Accessor: Obtains the major page faults.
     *
     * @return the number of major page faults.
     * @see #setMajorFaults(int)
     */
    public int getMajorFaults() {
        return this.m_majflt;
    }

    /**
     * Accessor: Obtains number of swap operations.
     *
     * @return the number of swaps.
     * @see #setSwaps(int)
     */
    public int getSwaps() {
        return this.m_nswap;
    }

    /**
     * Accessor: Obtains the system signals sent.
     *
     * @return the number of signals sent to the process.
     * @see #setSignals(int)
     */
    public int getSignals() {
        return this.m_nsignals;
    }

    /**
     * Accessor: Obtains the voluntary context switches.
     *
     * @return the number of voluntary context switches.
     * @see #setVoluntarySwitches(int)
     */
    public int getVoluntarySwitches() {
        return this.m_nvcsw;
    }

    /**
     * Accessor: Obtains the involuntary context switches.
     *
     * @return the number of involuntary context switches.
     * @see #setInvoluntarySwitches(int)
     */
    public int getInvoluntarySwitches() {
        return this.m_nivcsw;
    }

    /**
     * Accessor: Sets the user time.
     *
     * @param utime is the new user time in seconds with fraction.
     * @see #getUserTime()
     */
    public void setUserTime(double utime) {
        this.m_utime = utime;
    }

    /**
     * Accessor: Sets the system time.
     *
     * @param stime is the new user time in seconds with fraction.
     * @see #getSystemTime()
     */
    public void setSystemTime(double stime) {
        this.m_stime = stime;
    }

    /**
     * Accessor: Sets the number of minor faults.
     *
     * @param minflt is the new number of minor faults.
     * @see #getMinorFaults()
     */
    public void setMinorFaults(int minflt) {
        this.m_minflt = minflt;
    }

    /**
     * Accessor: Sets the number of major page faults.
     *
     * @param majflt is the new number of major page faults.
     * @see #getMajorFaults()
     */
    public void setMajorFaults(int majflt) {
        this.m_majflt = majflt;
    }

    /**
     * Accessor: Sets the number of swap ops.
     *
     * @param nswap is the new number of swap operations.
     * @see #getSwaps()
     */
    public void setSwaps(int nswap) {
        this.m_nswap = nswap;
    }

    /**
     * Accessor: Sets the number of signalss sent.
     *
     * @param nsignals is the new number of signals.
     * @see #getSignals()
     */
    public void setSignals(int nsignals) {
        this.m_nsignals = nsignals;
    }

    /**
     * Accessor: Sets the number of voluntary context switches.
     *
     * @param nvcsw is the new number voluntary context switches.
     * @see #getVoluntarySwitches()
     */
    public void setVoluntarySwitches(int nvcsw) {
        this.m_nvcsw = nvcsw;
    }

    /**
     * Accessor: Sets the number of involuntary context switches.
     *
     * @param nivcsw is the new number involuntary context switches.
     * @see #getInvoluntarySwitches()
     */
    public void setInvoluntarySwitches(int nivcsw) {
        this.m_nivcsw = nivcsw;
    }

    /**
     * Accessor.
     *
     * @see #setMaximumRSS(int)
     */
    public int getMaximumRSS() {
        return this.m_maxrss;
    }

    /**
     * Accessor.
     *
     * @param maxrss
     * @see #getMaximumRSS()
     */
    public void setMaximumRSS(int maxrss) {
        this.m_maxrss = maxrss;
    }

    /**
     * Accessor.
     *
     * @see #setSharedRSS(int)
     */
    public int getSharedRSS() {
        return this.m_ixrss;
    }

    /**
     * Accessor.
     *
     * @param ixrss
     * @see #getSharedRSS()
     */
    public void setSharedRSS(int ixrss) {
        this.m_ixrss = ixrss;
    }

    /**
     * Accessor.
     *
     * @see #setUnsharedRSS(int)
     */
    public int getUnsharedRSS() {
        return this.m_idrss;
    }

    /**
     * Accessor.
     *
     * @param idrss
     * @see #getUnsharedRSS()
     */
    public void setUnsharedRSS(int idrss) {
        this.m_idrss = idrss;
    }

    /**
     * Accessor.
     *
     * @see #setStackRSS(int)
     */
    public int getStackRSS() {
        return this.m_isrss;
    }

    /**
     * Accessor.
     *
     * @param isrss
     * @see #getStackRSS()
     */
    public void setStackRSS(int isrss) {
        this.m_isrss = isrss;
    }

    /**
     * Accessor.
     *
     * @see #setInputBlocks(int)
     */
    public int getInputBlocks() {
        return this.m_inblock;
    }

    /**
     * Accessor.
     *
     * @param inblock
     * @see #getInputBlocks()
     */
    public void setInputBlocks(int inblock) {
        this.m_inblock = inblock;
    }

    /**
     * Accessor.
     *
     * @see #setOutputBlocks(int)
     */
    public int getOutputBlocks() {
        return this.m_outblock;
    }

    /**
     * Accessor.
     *
     * @param outblock
     * @see #getOutputBlocks()
     */
    public void setOutputBlocks(int outblock) {
        this.m_outblock = outblock;
    }

    /**
     * Accessor.
     *
     * @see #setSent(int)
     */
    public int getSent() {
        return this.m_msgsnd;
    }

    /**
     * Accessor.
     *
     * @param msgsnd
     * @see #getSent()
     */
    public void setSent(int msgsnd) {
        this.m_msgsnd = msgsnd;
    }

    /**
     * Accessor.
     *
     * @see #setReceived(int)
     */
    public int getReceived() {
        return this.m_msgrcv;
    }

    /**
     * Accessor.
     *
     * @param msgrcv
     * @see #getReceived()
     */
    public void setReceived(int msgrcv) {
        this.m_msgrcv = msgrcv;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact vdl-support@griphyn.org");
    }

    /**
     * Dump the state of the current element as XML output.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        DecimalFormat d = new DecimalFormat("0.000");

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("usage");
        writeAttribute(stream, " utime=\"", d.format(m_utime));
        writeAttribute(stream, " stime=\"", d.format(m_stime));
        writeAttribute(stream, " minflt=\"", Integer.toString(m_minflt));
        writeAttribute(stream, " majflt=\"", Integer.toString(m_majflt));
        writeAttribute(stream, " nswap=\"", Integer.toString(m_nswap));
        writeAttribute(stream, " nsignals=\"", Integer.toString(m_nsignals));
        writeAttribute(stream, " nvcsw=\"", Integer.toString(m_nvcsw));
        writeAttribute(stream, " nivcsw=\"", Integer.toString(m_nivcsw));

        writeAttribute(stream, " maxrss=\"", Integer.toString(m_maxrss));
        writeAttribute(stream, " ixrss=\"", Integer.toString(m_ixrss));
        writeAttribute(stream, " idrss=\"", Integer.toString(m_idrss));
        writeAttribute(stream, " isrss=\"", Integer.toString(m_isrss));
        writeAttribute(stream, " inblock=\"", Integer.toString(m_inblock));
        writeAttribute(stream, " outblock=\"", Integer.toString(m_outblock));
        writeAttribute(stream, " msgsnd=\"", Integer.toString(m_msgsnd));
        writeAttribute(stream, " msgrcv=\"", Integer.toString(m_msgrcv));

        // done
        stream.write("/>");
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
