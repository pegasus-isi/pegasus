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
import java.util.regex.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.toolkit.FriendlyNudge;

/**
 * Inspects various files in a run directory to help diagnoze an error.
 *
 * @author Jin Soon Chang
 * @version $Revision$
 */
public class Diagnozer {
    /** Contains the base directory in and underneath which all files reside. */
    private File m_basedir;

    /** Names the .dag file */
    private File m_dagfile;

    /** Maps a job ID to its submit file. */
    private Map m_job;

    /** Remembers job IDs from jobs that were done (successfully). */
    private Set m_done;

    /** Maps a job ID to a JobInfo record. */
    private Map m_jobRecord;

    /** Maps a parent job ID to all dependent children. */
    private Map m_parents;

    /** Maps a child job ID to all dependent parents. */
    private Map m_children;

    /** work directory */
    String m_workdir;

    /** any signal from user */
    String m_signal;

    public void parseDAG(File dag) throws IOException {
        String line, parent, child;
        StringTokenizer st = null;
        LineNumberReader lnr = new LineNumberReader(new FileReader(dag));
        while ((line = lnr.readLine()) != null) {
            String lower = line.toLowerCase().trim();
            if (lower.startsWith("job ")) {
                // JOB ID000001 00/00/ID000001.sub [ DONE ]
                st = new StringTokenizer(line.trim());
                st.nextToken(); // JOB
                String jobid = st.nextToken(); // jobid
                String subfn = st.nextToken(); // submit file
                m_job.put(jobid, subfn);
                /*JobInfo job=new JobInfo();
                job.m_id=jobid;
                m_jobRecord.put(jobid,job);*/

                if (m_done != null
                        && st.hasMoreTokens()
                        && st.nextToken().toLowerCase().equals("done")) m_done.add(jobid);
                /*
                else{
                    JobInfos jobs=new JobInfos (jobid);
                    JobInfo job=new JobInfo();
                    job.setId(jobid);
                    job.setRetry("0");
                    jobs.addJobInfo(job,"0");
                    m_jobRecord.put(jobid,jobs);
                }
                */
            } else if (lower.startsWith("parent ")) {
                // PARENT ID000093 CHILD ID000094

                TreeSet parents = new TreeSet();
                TreeSet children = new TreeSet();

                st = new StringTokenizer(line.trim());
                st.nextToken(); // PARENT
                do {
                    parent = st.nextToken();
                    if (parent.toLowerCase().equals("child")) break;
                    parents.add(parent);
                } while (st.hasMoreTokens());
                while (st.hasMoreTokens()) {
                    children.add(st.nextToken());
                }

                for (Iterator i = parents.iterator(); i.hasNext(); ) {
                    parent = (String) i.next();
                    if (!m_parents.containsKey(parent)) m_parents.put(parent, new TreeSet());
                    ((Set) m_parents.get(parent)).addAll(children);
                }
                for (Iterator i = children.iterator(); i.hasNext(); ) {
                    child = (String) i.next();
                    if (!m_children.containsKey(child)) m_children.put(child, new TreeSet());
                    ((Set) m_children.get(child)).addAll(parents);
                }
            }
        }
        lnr.close();
    }

    /** c'tor. */
    public Diagnozer(String basedir) throws IOException {
        m_basedir = new File(basedir);
        if (!m_basedir.isDirectory()) throw new IOException(basedir + " is not a directory");
        // post-condition: itsa dir

        File[] dagfiles = m_basedir.listFiles(new FindTheFile(".dag"));
        // File[] dagfiles = m_basedir.listFiles(new FindTheRegex("\\.dag$"));
        if (dagfiles.length != 1) throw new RuntimeException("too many dag files in " + basedir);
        m_dagfile = dagfiles[0];
        m_workdir = basedir;

        m_job = new TreeMap();
        m_parents = new TreeMap();
        m_children = new TreeMap();
        m_jobRecord = new HashMap();
        m_signal = null;
        // find the rescue dag
        File rescuedag = new File(m_dagfile.getPath() + ".rescue");

        if (rescuedag.exists()) {
            // parse rescue dag instead
            m_done = new TreeSet();
            parseDAG(rescuedag);
        } else {
            // parse regular dag file
            m_done = null;
            parseDAG(m_dagfile);
        }

        for (Iterator i = m_job.keySet().iterator(); i.hasNext(); ) {
            String jobid = (String) i.next();
            String subfn = (String) m_job.get(jobid);
            if (allParentsDone(jobid) && !m_done.contains(jobid)) {
                JobInfos jobs = new JobInfos(jobid);
                JobInfo job = new JobInfo();
                job.setId(jobid);
                job.setRetry("0");
                jobs.addJobInfo(job, "0");
                m_jobRecord.put(jobid, jobs);
            }
        }
    }

    /** Dumps knowledge about the DAG for debugging purposes. */
    public void dump() {
        for (Iterator i = m_job.keySet().iterator(); i.hasNext(); ) {
            String jobid = (String) i.next();
            String subfn = (String) m_job.get(jobid);
            String done = m_done == null ? "" : (m_done.contains(jobid) ? "is done" : "NOT done");
            System.out.println(jobid + " -> " + subfn + ": " + done);
        }

        for (Iterator i = m_parents.keySet().iterator(); i.hasNext(); ) {
            String parent = (String) i.next();
            System.out.println("PARENT " + parent + " CHILD " + m_parents.get(parent).toString());
        }
    }

    public void parseDebug(String dbgfile) {
        String line;
        File dbg = new File(dbgfile);
        LineNumberReader lnr = null;

        try {
            lnr = new LineNumberReader(new FileReader(dbg));
            while ((line = lnr.readLine()) != null) {}
            lnr.close();
        } catch (IOException ioe) {
            System.err.println("Warning: Unable to read " + dbgfile);
        }
    }

    public void getDebugInfo() {
        for (Iterator i = m_job.keySet().iterator(); i.hasNext(); ) {
            String jobid = (String) i.next();
            String subfn = (String) m_job.get(jobid);

            if (!m_done.contains(jobid) && allParentsDone(jobid)) {
                String dbgfile = subfn.replaceAll(".sub", "");
                ParseDbg(m_dagfile.getParent() + "/" + dbgfile.trim() + ".dbg", jobid);
            }
        }
    }

    public void ParseDbg(String dbgfile, String jobid) {
        try {
            File dbg = null;
            LineNumberReader lnr = null;

            try {
                dbg = new File(dbgfile);
                lnr = new LineNumberReader(new FileReader(dbg));
            } catch (FileNotFoundException fne) {
                System.err.println(dbgfile + " doesn't exists");
                return;
            }

            int retries = -1;

            String line;
            // 20041010T140006.218 [16939] PRE: chose site "term"
            Pattern site = Pattern.compile(".*chose\\ssite\\s(.*)");

            // 20040901T184209.513  [8299] PRE: starting
            // /home/changjs/vds/contrib/Euryale/prescript.pl
            Pattern retry = Pattern.compile(".*pre:\\sstarting\\s.*prescript.*");

            // 20041028T143022.783  [4579] PRE: server gsiftp://gainly.uchicago.edu problem:
            // connect: Connection refused
            Pattern badsite = Pattern.compile(".*pre:\\s(.*)problem:\\s(.*)");
            // Unable to stage-in "fmri.1129-5_anonymized.img": no replicas found at
            // /home/changjs/vds-1.3.2/contrib/Euryale/prescript.pl line 338.
            Pattern fe = Pattern.compile("\\s*([a-zA-Z].*)");

            // 20041029T014559.358  [8844] PRE: [transfer|T2] # [0x00004002] 1 1/0 2.477s
            // "fmri.3472-5_anonymized.warp" error: the server sent an error response: 530 530 No
            // local mapping for Globus ID
            // 20040901T173703.111 [30512] PRE: stage-in exit code 42, trying to replan

            Pattern tr =
                    Pattern.compile(
                            ".*\\[transfer\\].*\\s(.*)\\serror:\\s(.*)|.*\\[t2\\].*\\s(.*)\\serror:\\s(.*)");

            // 20041104T100447.069  [4466] PRE: [T2] error: globus_ftp_client: the server responded
            // with an error
            Pattern tr2 =
                    Pattern.compile(".*\\[t2\\]\\serror:\\s(.*)|.*\\[transfer\\]\\serror:\\s(.*)");

            String currentSite = null;

            while ((line = lnr.readLine()) != null) {
                String lower = line.toLowerCase().trim();
                Matcher siteM = site.matcher(lower);
                Matcher badsiteM = badsite.matcher(lower);
                Matcher feM = fe.matcher(lower);
                Matcher trM = tr.matcher(lower);
                Matcher tr2M = tr2.matcher(lower);
                Matcher retryM = retry.matcher(lower);

                if (retryM.matches()) {
                    retries = retries + 1;
                }

                if (trM.matches()) {
                    String filename = trM.group(1);
                    String error = trM.group(2);
                    String currentRetry = Integer.toString(retries);
                    String errorS = "Transfer Error: " + filename + " " + error + " " + currentSite;
                    // ((JobInfo) m_jobRecord.get(jobid)).m_fatalErrorMessages.add(errorS);
                    ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                            .addFatalErrorMessage(errorS);
                }

                if (tr2M.matches()) {
                    String error = tr2M.group(1);
                    String currentRetry = Integer.toString(retries);
                    String errorS = "Transfer Error: " + error + " " + currentSite;
                    ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                            .addFatalErrorMessage(errorS);
                }

                if (badsiteM.matches()) {
                    String server = badsiteM.group(1);
                    String error = badsiteM.group(2);
                    String currentRetry = Integer.toString(retries);
                    ;
                    error = server + " " + error;
                    // System.out.println(error);

                    ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                            .addFatalErrorMessage(error);

                } else if (feM.matches()) {
                    String currentRetry = Integer.toString(retries);
                    String feMes = feM.group(1);
                    if (m_jobRecord.get(jobid) == null) {
                        System.out.println("dsadsad");
                    }
                    if ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry)
                            == null) {
                        System.out.println("dsadsa" + currentRetry);
                    }
                    ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                            .addFatalErrorMessage(feMes);
                } else if (siteM.matches()) {
                    String currentRetry = Integer.toString(retries);
                    ;
                    currentSite = siteM.group(1);
                    ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                            .setPool(currentSite);
                } else if (lower.matches(".*out\\sof\\ssite\\scandidates.*")) {
                    String currentRetry = Integer.toString(retries);
                    // 20041028T143028.143  [4605] PRE: out of site candidates, giving up!
                    ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                            .setPool(currentSite);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void parseDagmanOut() {
        try {
            String line;
            File dagmanout = new File(m_dagfile + ".dagman.out");
            // 10/10 13:47:43 PRE Script of Job ID000003 failed with status 1

            Map retries = new HashMap();
            for (Iterator i = m_jobRecord.keySet().iterator(); i.hasNext(); ) {
                retries.put(((String) i.next()), "0");
            }

            Pattern exit =
                    Pattern.compile(
                            ".+\\s(.*)\\sscript\\sof\\sjob\\s(.*)failed\\swith\\sstatus\\s(.*)");

            // PRE Script of Job ID000001 completed successfully.
            Pattern success =
                    Pattern.compile(
                            ".+\\s(.*)\\sscript\\sof\\sjob\\s(.*)\\scompleted\\ssuccessfully.*");
            LineNumberReader lnr = new LineNumberReader(new FileReader(dagmanout));

            // 10/29 01:48:02 Retrying node ID000001 (retry #1 of 5)...
            Pattern retry =
                    Pattern.compile(".+\\sretrying\\snode\\s(.*)\\s\\(retry\\s#(.*)\\sof.*");
            // compile(".+\\sretrying.*");
            // 11/4 10:33:42 Received SIGUSR1
            Pattern sig = Pattern.compile(".*\\sreceived\\s(.*).*");

            // 10/29 01:46:17 Event: ULOG_GLOBUS_SUBMIT for Condor Job ID000001 (50119.0.0)
            Pattern stage = Pattern.compile(".*event:\\s(.*)\\sfor\\scondor\\sjob\\s(.*)\\s.*");

            while ((line = lnr.readLine()) != null) {
                String lower = line.toLowerCase().trim();
                Matcher preExitM = exit.matcher(lower);
                Matcher successM = success.matcher(lower);
                Matcher stageM = stage.matcher(lower);
                Matcher sigM = sig.matcher(lower);
                Matcher retryM = retry.matcher(lower);

                if (retryM.matches()) {
                    // System.out.println(lower);
                    String id = retryM.group(1);
                    String retryN = retryM.group(2);
                    if (m_jobRecord.containsKey(id.toUpperCase().trim())) {
                        retries.put(id.trim().toUpperCase(), retryN.trim());
                        JobInfo j = new JobInfo();
                        j.setId(id.toUpperCase().trim());
                        j.setRetry(retryN);

                        ((JobInfos) m_jobRecord.get(id.toUpperCase().trim())).addJobInfo(j, retryN);
                    }
                }

                if (sigM.matches()) {
                    String signal = sigM.group(1);
                    m_signal = signal.toUpperCase().trim();
                }

                if (preExitM.matches()) {
                    String prepost = preExitM.group(1);
                    String ID = preExitM.group(2);
                    String exitCode = preExitM.group(3);
                    String currentRetry = (String) retries.get(ID.toUpperCase().trim());
                    if (m_jobRecord.containsKey(ID.toUpperCase().trim())) {
                        if (prepost.equals("pre")) {
                            ((JobInfo)
                                            ((JobInfos) m_jobRecord.get(ID.toUpperCase().trim()))
                                                    .getJobInfo(currentRetry))
                                    .setPrescriptErrorCode(exitCode);
                            ((JobInfo)
                                            ((JobInfos) m_jobRecord.get(ID.toUpperCase().trim()))
                                                    .getJobInfo(currentRetry))
                                    .setPostcriptErrorCode("N/A");
                        }
                        if (prepost.equals("post")) {
                            ((JobInfo)
                                            ((JobInfos) m_jobRecord.get(ID.toUpperCase().trim()))
                                                    .getJobInfo(currentRetry))
                                    .setPostcriptErrorCode(exitCode);
                            String subfn = (String) m_job.get(ID.toUpperCase().trim());
                            File subDir =
                                    (new File(m_dagfile.getParent() + "/" + subfn)).getParentFile();
                            File outFile =
                                    new File(
                                            subDir.getPath()
                                                    + "/"
                                                    + ID.toUpperCase().trim()
                                                    + ".out."
                                                    + currentRetry);
                            File errFile =
                                    new File(
                                            subDir.getPath()
                                                    + "/"
                                                    + ID.toUpperCase().trim()
                                                    + ".err."
                                                    + currentRetry);
                            if (outFile == null) {
                                System.out.println(
                                        subDir.getPath()
                                                + ".out."
                                                + currentRetry
                                                + " doesn't exits");
                            }
                            if (errFile == null) {
                                System.out.println(
                                        subDir.getPath()
                                                + ".err."
                                                + currentRetry
                                                + " doestn't exites");
                            }
                            ParseOut(outFile, ID.toUpperCase().trim(), currentRetry);
                            ParseError(errFile, ID.toUpperCase().trim(), currentRetry);
                        }
                    }
                }
                if (successM.matches()) {
                    String prepost = successM.group(1);
                    String jid = successM.group(2);
                    String currentRetry = (String) retries.get(jid.trim().toUpperCase());
                    prepost.trim();
                    if (m_jobRecord.containsKey(jid.toUpperCase().trim())) {
                        if (prepost.equals("pre")) {
                            ((JobInfo)
                                            ((JobInfos) m_jobRecord.get(jid.toUpperCase().trim()))
                                                    .getJobInfo(currentRetry))
                                    .setPrescriptErrorCode("0");
                        }
                        if (prepost.equals("post")) {
                            ((JobInfo)
                                            ((JobInfos) m_jobRecord.get(jid.toUpperCase().trim()))
                                                    .getJobInfo(currentRetry))
                                    .setPostcriptErrorCode("0");
                        }
                    }
                }

                if (stageM.matches()) {
                    String jobID = stageM.group(2);
                    String stageS = stageM.group(1);
                    String currentRetry = (String) retries.get(jobID.trim().toUpperCase());
                    if (m_jobRecord.containsKey(jobID.toUpperCase().trim())) {
                        ((JobInfo)
                                        ((JobInfos) m_jobRecord.get(jobID.toUpperCase().trim()))
                                                .getJobInfo(currentRetry))
                                .updateLastStage(stageS);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void dumpJobRecords(PrintWriter pw) throws IOException {
        if (m_signal != null) pw.println("THIS JOB WAS TERMINATED BY SIGNAL " + m_signal);
        pw.flush();
        for (Iterator i = m_jobRecord.keySet().iterator(); i.hasNext(); ) {
            String ID = (String) i.next();
            ((JobInfos) m_jobRecord.get(ID)).dump(pw);
        }
        pw.flush();
    }

    public void ParseError(File errFile, String jobid, String currentRetry) {
        try {
            LineNumberReader lnr = null;
            String line;
            try {
                lnr = new LineNumberReader(new FileReader(errFile));
            } catch (FileNotFoundException fne) {
                // System.err.println(errFile.getName()+" doesn't exists");
                return;
            }

            while ((line = lnr.readLine()) != null) {
                JobInfo job = (JobInfo) m_jobRecord.get(jobid);
                ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                        .addFatalErrorMessage(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ParseOut(File outFile, String jobid, String currentRetry) {
        try {
            LineNumberReader lnr = null;
            String line;
            try {
                lnr = new LineNumberReader(new FileReader(outFile));
            } catch (FileNotFoundException fne) {
                System.err.println(outFile.getName() + "is missing");
                return;
            }

            // <data>/home/changjs/vdldemo/bin/align_warp dsadsa dsadsa dasdsa  -m 12 -q
            // do_align_warp.c: 157: problem with file dsadsa
            // The specified file does not exist. (AIR_NO_FILE_READ_ERROR)
            // </data>

            ParseKickstart pks = new ParseKickstart();

            String filename = outFile.getPath();
            try {
                ((JobInfo) ((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
                        .setOutfileExit(pks.parseFile(filename));
            } catch (FriendlyNudge fn) {
                fn.toString();
            } catch (ClassCastException cce) {
                System.out.println("class cast exception");
                cce.printStackTrace();
            }

            /*
               Pattern data = Pattern.
            compile("<data>(.*)");
               //<data>/home/changjs/vdldemo/bin/align_warp dsadsa dsadsa dasdsa  -m 12 -q
               //do_align_warp.c: 157: problem with file dsadsa
               //The specified file does not exist. (AIR_NO_FILE_READ_ERROR)
               //</data>

               Pattern endData=Pattern.compile(".*</data>.*");

                   while ( (line = lnr.readLine()) != null ) {
            //String lower = line.toLowerCase().trim();
            Matcher dataM = data.matcher(line.trim());
            Matcher endM=endData.matcher(line.trim());
            if ( dataM.matches() ) {
                String dataS=dataM.group(1);
                //System.out.println(dataS);
                do{
            	//System.out.println(line);
            	((JobInfo)((JobInfos) m_jobRecord.get(jobid)).getJobInfo(currentRetry))
            	    .setOutfileExit(errorS);

            	line = lnr.readLine();
            	line=line.trim();
                }while ( !endM.matches()) ;
            }
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
        public void getOutFileInfo()
        {
    	for ( Iterator i=m_job.keySet().iterator(); i.hasNext(); ) {
    	    String jobid = (String) i.next();
    	    String subfn = (String) m_job.get(jobid);
    	    if ( ! m_done.contains(jobid)&&
    		 ((JobInfo)m_jobRecord.get(jobid)).getLastStage()!=null) {

    		File subDir=(new File(m_dagfile.getParent()+"/"+subfn)).getParentFile();
    		File[] outFiles=subDir.listFiles(new FindTheRegex(jobid+"\\.out.*"));
    		//System.out.println("the sub "+ subDir.getPath());
    		if(outFiles==null)
    		    System.out.println("dsadsa");
    		System.out.println("out files:"+outFiles.length);
    		for(int j=0;j<outFiles.length;++j){
    		    System.out.println( outFiles[j].getName());
    		    ParseOut(outFiles[j],jobid);
    		}
    	    }
    	}
        }
    */
    private boolean allParentsDone(String cid) {
        TreeSet parents = new TreeSet();
        parents = (TreeSet) m_children.get(cid);
        if (parents == null) return true;
        for (Iterator j = parents.iterator(); j.hasNext(); ) {
            if (!m_done.contains((String) j.next())) {
                return false;
            }
        }

        return true;
    }

    /*
       public void getErrorFileInfo()
       {
    for ( Iterator i=m_job.keySet().iterator(); i.hasNext(); ) {
        String jobid = (String) i.next();
        String subfn = (String) m_job.get(jobid);
        if ( !m_done.contains(jobid) &&
    	 ((JobInfo)m_jobRecord.get(jobid)).getLastStage()!=null) {
    	//String dbgfile=subfn.replaceAll(".sub","");
    	File subDir=(new File(m_dagfile.getParent()+"/"+subfn)).getParentFile();
    	File[] errFiles=subDir.listFiles(new FindTheRegex(jobid+"\\.err.*"));
    	System.out.println("the sub "+ subDir.getPath());
    	if(errFiles==null)
    	    System.out.println("dsadsa");
    	System.out.println("err files:"+errFiles.length);
    	for(int j=0;j<errFiles.length;++j){
    	    System.out.println( errFiles[j].getName());
    	    ParseError(errFiles[j],jobid);
    	}

        }
    }
       }

       private String getSignal(){
    return m_signal;
       }
     */
    public static void main(String args[]) {
        Diagnozer me = null;
        int result = 0;

        if (args.length != 1) {
            System.err.println("Need the base directory");
            System.exit(1);
        }

        try {
            me = new Diagnozer(args[0]);
            me.parseDagmanOut();
            me.getDebugInfo();
            //    me.getErrorFileInfo();
            //     me.getOutFileInfo();
            me.dumpJobRecords(new PrintWriter(System.out));
            // me.dump();
        } catch (IOException ioe) {
            System.err.println("ERROR: " + ioe.getMessage());
            result = 1;
        } catch (RuntimeException rte) {
            System.err.println("RTE: " + rte.getMessage());
            rte.printStackTrace(System.err);
            result = 1;
        } catch (Exception e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace(System.err);
            result = 2;
        }

        if (result != 0) System.exit(result);
    }
}
