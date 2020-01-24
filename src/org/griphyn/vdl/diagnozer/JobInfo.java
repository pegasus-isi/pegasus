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
package org.griphyn.vdl.diagnozer;

import java.io.*;
import java.util.*;

/** Description of a job. */
class JobInfo {
    /** ID of the job. */
    private String m_id;

    /** Job has finished (successfully?) */
    private boolean m_done;

    /** Messages from error files */
    private ArrayList m_errorFileMessage;

    /** lists of prescript exit code, with retries */
    private String m_prescriptExitCode;

    /** lists of postscript exit code, with retries */
    private String m_postscriptExitCode;

    /** the pools they ran. */
    private String m_pool;

    /** Fatal error messages from the remote host */
    private ArrayList m_fatalErrorMessages;

    /** Error messages found in the JobID.out file */
    private ArrayList m_outFileMessages;

    /** The last stage it was performing before failur */
    private String m_lastStage;

    /** The Outfile Exitcodes */
    private List m_outExitCodes;

    private String m_retry;

    /** c'tor */
    public JobInfo() {
        m_id = null;
        m_done = false;
        m_outFileMessages = new ArrayList();
        m_errorFileMessage = new ArrayList();
        m_prescriptExitCode = null;
        m_postscriptExitCode = null;
        m_pool = null;
        m_fatalErrorMessages = new ArrayList();
        m_lastStage = null;
        m_outExitCodes = new ArrayList();
        m_retry = null;
    }

    public void setRetry(String retry) {
        m_retry = retry;
    }

    public void addFatalErrorMessage(String message) {
        m_fatalErrorMessages.add(message);
    }

    public void setId(String id) {
        m_id = id;
    }

    public void setDone(boolean done) {
        m_done = done;
    }

    public void addErroFileMessage(String message) {
        m_errorFileMessage.add(message);
    }

    public void setOutfileExit(List exitcodes) {
        m_outExitCodes = exitcodes;
    }

    public void setPrescriptErrorCode(String exit) {
        m_prescriptExitCode = exit;
    }

    public void setPostcriptErrorCode(String exit) {
        m_postscriptExitCode = exit;
    }

    public void setPool(String pool) {
        m_pool = pool;
    }

    public void updateLastStage(String stage) {

        m_lastStage = null;
        m_lastStage = stage;
    }

    public String getLastStage() {
        return m_lastStage;
    }

    /**
     * Dumps the contents of this record into the given writer.
     *
     * @param pw is a writable print writer.
     */
    public void dump(PrintWriter pw) throws IOException {

        pw.println("*retry: " + m_retry);
        if (this.m_lastStage == null) {
            pw.println("The " + this.m_id + " wasn't submitted");
        } else {
            pw.println(this.m_id + " Last Know Stage " + this.m_lastStage.toUpperCase());
        }
        if (m_prescriptExitCode != null) {
            pw.println("Prescript Exit Codes: " + m_prescriptExitCode);
        } else {
            pw.println("Prescript Exit Codes: N/A ");
        }
        if (m_postscriptExitCode != null) {
            pw.println("Postscript Exit Codes: " + m_postscriptExitCode);
        } else {
            pw.println("Postscript Exit Codes: N/A ");
        }

        if (this.m_outExitCodes.size() != 0) {

            pw.println("kickstart Exit Codes ");
            for (Iterator i = this.m_outExitCodes.iterator(); i.hasNext(); ) {
                int exitcode = ((Integer) i.next()).intValue();
                switch (exitcode) {
                    case 0:
                        pw.println("\t0   regular exit with exit code 0");
                        break;
                    case 1:
                        pw.println("\t1   regular exit with exit code > 0");
                        break;
                    case 2:
                        pw.println("\t2   failure to run program from kickstart");
                        break;
                    case 3:
                        pw.println("\t3   application had died on signal");
                        break;
                    case 4:
                        pw.println("\t4   application was suspended (should not happen)");
                        break;
                    case 5:
                        pw.println("\t5   failure in exit code parsing");
                        break;
                    case 6:
                        pw.println("\t6   impossible case");
                }
            }
            pw.println("");
        }

        if (this.m_pool != null) {
            pw.println("Ran At " + m_pool);
        } else {
            pw.println("No available pool");
        }

        if (this.m_fatalErrorMessages.size() > 0) {
            pw.println("Fatal Error Message: ");
            for (Iterator i = this.m_fatalErrorMessages.iterator(); i.hasNext(); ) {
                pw.println((String) i.next());
            }
            pw.println();
        }

        if (this.m_errorFileMessage.size() > 0) {
            pw.println("Errors File: ");
            for (Iterator i = this.m_errorFileMessage.iterator(); i.hasNext(); ) {
                pw.println((String) i.next());
            }
        }
        pw.println();

        if (this.m_outFileMessages.size() > 0) {
            pw.println("Job ID :" + this.m_id + " out File: ");
            for (Iterator i = this.m_outFileMessages.iterator(); i.hasNext(); ) {
                pw.println((String) i.next());
            }
            pw.println();
        }
        pw.println("---------------------------------------------------");
        pw.flush();
    }
}
