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
import java.util.*;
import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.parser.DAXParser;
import org.griphyn.vdl.planner.*;

/**
 * This class makes concrete plans for a DAX, when planning using the shell planner.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.planner.Scriptor
 */
public class Derive extends Directive {
    public Derive() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Generates shell scripts for the workflow described by the dax. For each derivation, there is
     * a shell script generated, and there is a control script to control the execution sequence of
     * these shell scripts according to their dependencies.
     *
     * @param dax is the InputStream for the dax representation
     * @param dir is the directory name in which to generate these scripts
     * @param build specifies whether to force build mode
     * @param register specifies whether to register output files
     * @return true if successful, false otherwise
     */
    public boolean genShellScripts(InputStream dax, String dir, boolean build, boolean register)
            throws java.sql.SQLException, IOException, InterruptedException {
        return genShellScripts(dax, dir, build, register, null);
    }

    /**
     * Generates shell scripts for the workflow described by the dax. For each derivation, there is
     * a shell script generated, and there is a control script to control the execution sequence of
     * these shell scripts according to their dependencies.
     *
     * @param dax is the InputStream for the dax representation
     * @param dir is the directory name in which to generate these scripts
     * @param build specifies whether to force build mode
     * @param register specifies whether to register output files
     * @param kickstart_path specifies the location of kickstart. If null, kickstart will not be
     *     used.
     * @return true if successful, false otherwise
     */
    public boolean genShellScripts(
            InputStream dax, String dir, boolean build, boolean register, String kickstart_path)
            throws java.sql.SQLException, IOException, InterruptedException {
        // sanity check -- is there a destination directory
        if (dir == null || dir.equals("")) {
            m_logger.log("planner", 0, "Output directory not specified, using default: test");
            dir = "test";
        } else {
            m_logger.log("planner", 0, "Using output directory " + dir);
        }

        // parse the dax file
        m_logger.log("planner", 1, "Initializing dax parser");
        DAXParser daxparser = new DAXParser(m_props.getDAXSchemaLocation());
        m_logger.log("planner", 1, "parsing the dax...");
        ADAG adag = daxparser.parse(dax);

        // sanity check -- do we have a DAX
        if (adag == null) {
            m_logger.log("planner", 0, "failed parsing the dax.");
            return false;
        }

        // check output directory -- does it exist?
        File f = new File(dir);
        if (f.exists()) {
            if (!f.isDirectory()) {
                m_logger.log("planner", 0, "ERROR: '" + dir + "' is not a directory!");
                throw new IOException(dir + " is not a directory!");
            }
        } else {
            m_logger.log("planner", 0, "directory '" + dir + "' does not exist. Creating.");
            f.mkdirs();
        }

        // connect to replica catalog
        RCWrapper rc = null;
        try {
            rc = new RCWrapper();
        } catch (Exception e) {
            throw new Error(e.getMessage());
        }
        m_logger.log("planner", 2, "Using RC " + rc.getName());

        // connect to transformation catalog
        TCWrapper tc = new TCWrapper();
        m_logger.log("planner", 2, "Using TC " + tc.getName());

        // connect to site catalog, optional
        SCWrapper sc = new SCWrapper();
        m_logger.log("planner", 2, "Using SC " + sc.getName());

        // lookup all filenames in replica catalog, and populate the
        // filename map that is passed around.
        m_logger.log("planner", 1, "processing logical filenames");
        HashMap filenameMap = new HashMap();
        for (Iterator i = adag.iterateFilename(); i.hasNext(); ) {
            Filename fn = (Filename) i.next();
            String lfn = fn.getFilename();
            String pfn = rc.lookup("local", lfn);
            if (pfn == null) {
                // can't find the lfn->pfn mapping in rc
                m_logger.log(
                        "planner",
                        1,
                        "Info: Failed to find LFN " + lfn + " in RC, assuming PFN==LFN");
                pfn = lfn;
            }
            filenameMap.put(lfn, pfn);
        }

        // convert adag to graph
        Graph graph = DAX2Graph.DAG2Graph(adag);

        // to build or to make?
        if (build) {
            // build mode
            m_logger.log("planner", 0, "Running in build mode, DAG pruning skipped");
        } else {
            // make mode
            m_logger.log("planner", 0, "Checking nodes whose outputs already exist");
            // check output file existence, if all output files exist, then
            // cut this node
            boolean cut;

            // make reverse topological sort to the graph, i.e. find last
            // finished jobs first.
            Topology rtp = new Topology(graph.reverseGraph());

            // Hash to keep all existing files
            HashMap existMap = new HashMap();

            // Hash to keep files to add to exist list for this stage
            HashMap addMap = new HashMap();

            // Hash to keep files to remove from exist list for this stage
            HashMap removeMap = new HashMap();

            String jobs[];

            // whether we are dealing with last finished jobs
            boolean last = true;

            while ((jobs = rtp.stageSort()) != null) {
                int number = jobs.length;
                int count = 0;

                for (int i = 0; i < number; i++) {
                    String jobID = jobs[i];
                    cut = true;
                    Job job = adag.getJob(jobID);

                    // Hash to keep input files of this job
                    HashMap inputMap = new HashMap();

                    for (Iterator e = job.iterateUses(); e.hasNext(); ) {
                        Filename fn = (Filename) e.next();
                        String lfn = fn.getFilename();

                        // check exist file hash first
                        if (!existMap.containsKey(lfn)) {
                            // look up lfn in filename hash
                            String pfn = (String) filenameMap.get(lfn);
                            if (pfn == null) {
                                // lfn is not in the filename list
                                m_logger.log(
                                        "planner",
                                        0,
                                        "ERROR: File '"
                                                + lfn
                                                + "' is not in the <filename> list, "
                                                + "please check the DAX!");
                                return false;
                            }

                            // check if output file exists
                            if (fn.getLink() == LFN.OUTPUT) {
                                File fp = new File(pfn);
                                if (!fp.exists()) {
                                    // some output file does not exist.
                                    cut = false;
                                }
                            }
                            if (fn.getLink() == LFN.INPUT) {
                                inputMap.put(lfn, pfn);
                            }
                        }
                    }

                    if (cut) {
                        // cut node
                        m_logger.log("planner", 1, "Removed job " + jobID + " from DAG");
                        graph.removeVertex(jobID);

                        // assume all input files (outputs from upper stages exist)
                        addMap.putAll(inputMap);
                        count++;
                    } else {
                        // assume all input files not exist.
                        removeMap.putAll(inputMap);
                    }
                } // for enum

                if (count == number) {
                    // output files for all the jobs in this stage exist
                    if (last) {
                        // this is the last stage, no need to run the dag
                        m_logger.log(
                                "planner",
                                0,
                                "All output files already exist, " + "no computation is needed!");
                        return true;
                    }

                    // cut all the upper stage jobs
                    while ((jobs = rtp.stageSort()) != null) {
                        for (int i = 0; i < jobs.length; i++) {
                            m_logger.log("planner", 1, "Removed job " + jobs[i] + " from DAG");
                            graph.removeVertex(jobs[i]);
                        }
                    }
                } else {
                    if (count == 0) {
                        // none gets cut in this stage
                        last = false;
                        continue;
                    }

                    // put assumed existing files into exist map
                    existMap.putAll(addMap);
                    TreeSet temp = new TreeSet(removeMap.keySet());
                    for (Iterator it = temp.iterator(); it.hasNext(); ) {
                        String lfn = (String) it.next();
                        existMap.remove(lfn);
                    }
                }
                // now the last stage has been processed
                last = false;
            } // end while
        } // end else

        // make topological sort to the graph
        Topology tp = new Topology(graph);

        // get the topmost jobs
        String[] jobs = tp.stageSort();

        // dax maybe invalid (empty or has cycle in it)
        if (jobs == null) {
            m_logger.log(
                    "planner",
                    0,
                    "ERROR: No starting job(s) found, " + "please check the DAX file!");
            return false;
        }

        // create a Scriptor instance
        Scriptor spt = new Scriptor(dir, adag, rc, sc, tc, filenameMap, m_props.getDataDir());
        spt.setRegister(register);

        // Only set kickstart path if CLI argument was specified.
        // However, permit "-k ''" to remote kickstart invocations
        if (kickstart_path != null) {
            int x = kickstart_path.trim().length();
            spt.setKickstart(x == 0 ? null : kickstart_path);
        }

        String ctrlScript = spt.initializeControlScript();

        // check involved LFN's for these jobs
        for (int i = 0; i < jobs.length; i++) {
            // process each job and check input file existence
            String scriptFile = spt.processJob(jobs[i], true);
            if (scriptFile == null) {
                m_logger.log("planner", 0, "ERROR: failed processing job " + jobs[i]);
                return false;
            }
        }

        spt.intermediateControlScript();

        // process jobs in the following stages
        while ((jobs = tp.stageSort()) != null) {
            for (int i = 0; i < jobs.length; i++) {
                // not the first stage, no need to check input file existence
                String scriptFile = spt.processJob(jobs[i], false);
                if (scriptFile == null) {
                    m_logger.log("planner", 0, "ERROR: failed processing job " + jobs[i]);
                    return false;
                }
            }

            spt.intermediateControlScript();
        }

        spt.finalizeControlScript();
        m_logger.log("planner", 0, "DAG processed successfully");
        m_logger.log("planner", 1, "changing file permission");
        changePermission(dir);

        spt = null;
        if (rc != null) rc.close();
        if (sc != null) sc.close();
        if (tc != null) tc.close();
        m_logger.log(
                "planner",
                0,
                "To run the DAG, execute '" + ctrlScript + "'" + " in directory " + dir);
        return true;
    }

    /** Helper method to change the permissions of shell scripts to be executable. */
    protected static int changePermission(String dir) throws IOException, InterruptedException {
        if (System.getProperty("line.separator").equals("\n")
                && System.getProperty("file.separator").equals("/")
                && System.getProperty("path.separator").equals(":")) {
            String[] me = new String[3];
            me[0] = "/bin/sh";
            me[1] = "-c";
            me[2] = "chmod 0755 *.sh";

            Process p = Runtime.getRuntime().exec(me, null, new File(dir));
            return p.waitFor();
        } else {
            return -1;
        }
    }
}
