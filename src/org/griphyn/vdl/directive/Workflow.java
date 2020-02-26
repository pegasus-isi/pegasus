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
import org.griphyn.vdl.util.*;

/**
 * A <code>Workflow</code> object defines a context for running a derivation graph on the Grid as a
 * DAG, and managing its execution. It serves as a front-end to an associated shell script (by
 * default located in $PEGASUS_HOME/bin/vds-Workflow-script-runwf)
 *
 * <p>The workflow to be executed is designated by its terminal derivation (DV).
 *
 * <p>The Workflow instance is returned by the class method Workflow.run().
 *
 * <p>The Workflow class variables contain status cached from period queries of the workflow
 * database.
 *
 * @author Douglas Scheftner
 * @author Mike Wilde
 * @author Eric Gilbert
 * @version $Revision$
 */
public class Workflow {

    /* Class Variables */

    public static String runwfCmd = "/home/dscheftn/vds/bin/vds-Workflow-script-runwf";
    public static String wfstatCmd = "/home/dscheftn/vds/bin/vds-Workflow-script-wfstat";

    public static String defaultVOGroup = "quarknet";

    public static String logicalFileNameBase = "/export/d1/dscheftn/quarknet_testing/runs";

    /** parent directory for run-dir tree ala vds-plan/vds-run. */
    public static String defaultBaseDir = "/no/base/dir";

    public static String rlsURL = "rls://terminable.uchicago.edu";

    public static final int MAXWF = 100000;
    public static Workflow[] workflows = new Workflow[MAXWF];
    public static int nworkflows;

    public static long millisecsToRefreshStatus = 30000; // 30 secs between auto-refresh of status
    public static long timeOfLastRefresh = 0; // really want this publically read-only

    //    private static String voGroup;

    /* Instance Variables */

    /* sample data from database:

     id |     basedir     | vogroup |  workflow  |   run   | creator |
    ----+-----------------+---------+------------+---------+---------+
      1 | /home/wilde/run | ivdgl1  | test       | run0001 | wilde   |

             ctime          | state |         mtime
    ------------------------+-------+------------------------
     2005-08-20 13:25:27-05 |    -2 | 2005-08-20 13:28:09-05

    */

    /* Instance variables that mirror the database fields */

    public String wfid;
    public String basedir;
    public String vogroup;
    public String wfname;
    public String run;
    public String creator;
    public String ctime;
    public String state;
    public String exitstatus;
    public String mtime;

    /* Instance variables to track workflow state */

    public static final int WFMAXJOBS = 20000; /* FIX: can we avoid this hard limit? */
    public WorkflowJob[] jobs;
    public int njobs;
    public String tmpdir;
    public String errorMessage;

    /* Class Methods */

    public static Workflow run(String namespace, String dvName) {
        Process p;
        int rc;
        Reader is;
        StringBuffer sb = new StringBuffer();
        char[] b = new char[100000];
        int n;

        Workflow wf = new Workflow();

        wf.basedir = defaultBaseDir;
        wf.vogroup = defaultVOGroup;
        wf.wfname = dvName;

        try {
            System.out.println("Running Process " + namespace + " " + dvName);

            String[] cmd = {
                runwfCmd,
                defaultVOGroup,
                logicalFileNameBase,
                defaultBaseDir,
                rlsURL,
                namespace,
                dvName
            };

            p = Runtime.getRuntime().exec(cmd);

            InputStream out = p.getInputStream();
            InputStreamReader r = new InputStreamReader(out);
            BufferedReader in = new BufferedReader(r);

            wf.tmpdir = in.readLine();
            System.out.println("output from runwf: tmpdir=" + wf.tmpdir);
            wf.run = in.readLine();
            System.out.println("output from runwf: run=" + wf.run);
            wf.errorMessage = in.readLine();
            System.out.println("output from runwf: errorMessage=" + wf.errorMessage);

            rc = p.waitFor();
            System.out.println("Process returned rc=" + rc);

            return (wf);
        } catch (Exception e) {
            System.out.println("Prepare: Exception: " + e.toString());
            return wf;
        }
    }

    public static boolean refresh() {
        Process p;
        int rc;
        Reader is;
        StringBuffer sb = new StringBuffer();
        char[] b = new char[100000];
        int n;

        /* Run status command to get workflow states */

        try {

            p = Runtime.getRuntime().exec(wfstatCmd);

            InputStream out = p.getInputStream();
            InputStreamReader r = new InputStreamReader(out);
            BufferedReader in = new BufferedReader(r);

            String line;
            nworkflows = 0;
            while ((line = in.readLine()) != null) {

                Workflow w = new Workflow();
                String[] t = line.split("\\|");
                int nt = t.length;
                if (nt > 1) w.wfid = t[1];
                if (nt > 2) w.basedir = t[2];
                if (nt > 3) w.vogroup = t[3];
                if (nt > 4) w.wfname = t[4];
                if (nt > 5) w.run = t[5];
                if (nt > 6) w.creator = t[6];
                if (nt > 7) w.ctime = t[7];
                if (nt > 8) {
                    switch (Integer.parseInt(t[8], 10)) {
                        case -2:
                            w.state = "WFSTATE_PLANNED";
                            w.exitstatus = "";
                            break;
                        case -1:
                            w.state = "WFSTATE_RUNNING";
                            w.exitstatus = "";
                            break;
                        default:
                            w.state = "WFSTATE_FINISHED";
                            w.exitstatus = t[8];
                            break;
                    }
                }
                if (nt > 9) w.mtime = t[9];

                if (nworkflows < (MAXWF)) {
                    workflows[nworkflows++] = w;
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

    /**
     * Sets the status fields in a Workflow instance.
     *
     * @return true if status was successfully obtained, false if not.
     */
    public boolean updateStatus() {
        boolean rc;

        long now = System.currentTimeMillis();
        if (now > (timeOfLastRefresh + millisecsToRefreshStatus)) {
            rc = Workflow.refresh();
            rc = WorkflowJob.refresh();
            timeOfLastRefresh = now;
        }

        for (int i = 0; i < nworkflows; i++) {
            if (workflows[i].basedir.equals(basedir)
                    && workflows[i].vogroup.equals(vogroup)
                    && workflows[i].wfname.equals(wfname)
                    && workflows[i].run.equals(run)) {
                wfid = workflows[i].wfid;
                creator = workflows[i].creator;
                ctime = workflows[i].ctime;
                state = workflows[i].state;
                exitstatus = workflows[i].exitstatus;
                mtime = workflows[i].mtime;
                break;
            }
        }

        if (wfid == null) return false;

        //	System.out.println ("basedir = " + basedir + " vogroup = " + vogroup + " wfname = " +
        // wfname + " run = " + run + " wfid = " + wfid + " creator = " + creator + " ctime = " +
        // ctime + " state = " + state + " exitstatus = " + exitstatus + " mtime = " + mtime);

        jobs = new WorkflowJob[WFMAXJOBS];
        njobs = 0;
        for (int i = 0; i < WorkflowJob.njobs; i++) {
            if (WorkflowJob.jobs[i].wfid.equals(wfid))
                if (njobs < WFMAXJOBS) jobs[njobs++] = WorkflowJob.jobs[i];
        }
        return true;
    }

    public String toWFStatusString() {
        updateStatus();
        return "wfid="
                + wfid
                + " run="
                + run
                + " ctime="
                + ctime
                + " state="
                + state
                + " exitstatus="
                + exitstatus
                + " mtime="
                + mtime
                + " refreshed="
                + timeOfLastRefresh;
    }

    public String toJobStatusString() {
        updateStatus();
        if (njobs == 0) return "";
        String s = jobs[0].asStatusString();
        for (int i = 1; i < njobs; i++) {
            s += "\n" + jobs[i].asStatusString();
        }
        return (s);
    }

    public String toDetailStatusString() {
        return toWFStatusString() + "\n" + toJobStatusString();
    }

    public int stop() {
        return 0;
    }

    public int cleanup() {
        return 0;
    }
}
