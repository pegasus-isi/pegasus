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
class JobInfos {
    private Map m_jobs;
    String m_id;

    public JobInfos(String id) {
        m_jobs = new HashMap();
        m_id = id;
    }

    public void addJobInfo(JobInfo job, String key) {
        m_jobs.put(key, job);
    }

    public JobInfo getJobInfo(String key) {
        return (JobInfo) m_jobs.get(key);
    }

    public void dump(PrintWriter pw) {
        try {
            System.out.println("****************************************************");
            System.out.println("*             " + m_id + "'s record                    *");
            System.out.println("****************************************************");
            pw.flush();
            for (Iterator i = m_jobs.keySet().iterator(); i.hasNext(); ) {
                String jid = (String) i.next();
                ((JobInfo) m_jobs.get(jid)).dump(pw);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
