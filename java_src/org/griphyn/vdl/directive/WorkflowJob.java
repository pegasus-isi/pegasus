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

package org.griphyn.vdl.directive;

import java.io.*;
import org.griphyn.vdl.workflow.*;

/**
 * A <code>WorkflowJob</code> object defines the state of a job within a Workflow
 *
 * @author Mike Wilde
 * @author Eric Gilbert
 * @version $Revision$
 * @see org.griphyn.vdl.toolkit.VDLc
 */
public class WorkflowJob {

    /* Class Variables */

    public static final int MAXJOBS = 100000;
    public static WorkflowJob[] jobs = new WorkflowJob[MAXJOBS];
    public static int njobs;

    public static String wfjobsCmd = "/home/dscheftn/vds/bin/vds-WorkflowJob-script-wfjobs";

    /* Instance Variables */

    /* db fields

    select * from wf_jobstate;

     wfid |  jobid   |        state        |         mtime          |    site
    ------+----------+---------------------+------------------------+-------------
        2 | ID000001 | POST_SCRIPT_SUCCESS | 2005-08-21 15:55:10-05 | terminable
        4 | ID000001 | PRE_SCRIPT_FAILURE  | 2005-08-24 15:51:11-05 |
        5 | ID000001 | POST_SCRIPT_SUCCESS | 2005-08-24 16:23:43-05 | terminable
        6 | ID000001 | JOB_RELEASED        | 2005-08-24 17:26:59-05 |
    */

    public String wfid;
    public String jobid;
    public String state;
    public String mtime;
    public String site;

    /* Class Methods */

    public static boolean refresh() {
        Process p;
        int rc;
        Reader is;
        StringBuffer sb = new StringBuffer();
        char[] b = new char[100000];
        int n;

        /* Run status command to get job states */

        try {
            p = Runtime.getRuntime().exec(wfjobsCmd);

            InputStream out = p.getInputStream();
            InputStreamReader r = new InputStreamReader(out);
            BufferedReader in = new BufferedReader(r);

            String line;
            njobs = 0;

            while ((line = in.readLine()) != null) {
                WorkflowJob j = new WorkflowJob();
                String[] t = line.split("\\|");
                int nt = t.length;
                if (nt > 1) j.wfid = t[1];
                if (nt > 2) j.jobid = t[2];
                if (nt > 3) j.state = t[3];
                if (nt > 4) j.mtime = t[4];
                if (nt > 5) j.site = t[5];
                if (njobs < MAXJOBS) {
                    jobs[njobs++] = j;
                } else {
                    return false;
                }
            }

            rc = p.waitFor();

            return true;

        } catch (Exception e) {
            System.out.println("WorkflowJob.refresh: Exception: " + e.toString());
            return false;
        }
    }

    /* Instance Methods */

    public String asStatusString() {
        return "jobid=" + jobid + " wfid=" + wfid + " state=" + state + " mtime=" + mtime + " site="
                + site;
    }
}
