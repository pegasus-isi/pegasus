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
package edu.isi.pegasus.planner.invocation;

import edu.isi.pegasus.planner.parser.InvocationParser;
import java.io.*;
import java.net.*;
import java.util.Iterator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.toolkit.*;
import org.griphyn.vdl.util.ChimeraProperties;

public class SimpleServer extends Toolkit {
    private static final int port = 65533;

    public static boolean c_terminate = false;
    public static Logger c_logger = null;

    private boolean m_emptyFail = true;
    private boolean m_noDBase;

    DatabaseSchema m_dbschema;
    InvocationParser m_parser;
    ServerSocket m_server;

    public static void setTerminate(boolean b) {
        c_terminate = b;
    }

    public static boolean getTerminate() {
        return c_terminate;
    }

    public void showUsage() {
        // empty for now
    }

    public SimpleServer(int port) throws Exception {
        super("SimpleServer");

        // stand up the connection to the PTC
        this.m_noDBase = false;
        ChimeraProperties props = ChimeraProperties.instance();
        String ptcSchemaName = props.getPTCSchemaName();
        if (ptcSchemaName == null) m_noDBase = true;
        if (!m_noDBase) {
            // ignore -d option for now - grumbl, why?!
            Connect connect = new Connect();
            this.m_dbschema = connect.connectDatabase(ptcSchemaName);

            // check for invocation record support
            if (!(m_dbschema instanceof PTC)) {
                c_logger.warn(
                        "Your database cannot store invocation records"
                                + ", assuming no-database-mode");
                m_noDBase = true;
            }
        }

        // create one XML parser -- once
        m_parser = new InvocationParser(props.getPTCSchemaLocation());

        // setup socket
        this.m_server = null;
        try {
            byte[] loopback = {127, 0, 0, 1};
            this.m_server = new ServerSocket(port, 5, InetAddress.getByAddress(loopback));
            //	new ServerSocket( port, 5, InetAddress..getLocalHost() );
            //	new ServerSocket( port, 5 );
        } catch (UnknownHostException e) {
            c_logger.fatal("Unable to determine own hostname: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            c_logger.fatal("Could not listen on port " + port + ": " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Copy the content of the file into memory. The copy operation also weeds out anything that may
     * have been added by the remote batch scheduler. For instance, PBS is prone to add headers and
     * footers.
     *
     * @param input is the file instance from which to read contents.
     * @return the result code from reading the file
     */
    private String extractToMemory(java.io.File input) throws FriendlyNudge {
        StringWriter out = null;

        // open the files
        int p1, p2, state = 0;
        try {
            BufferedReader in = new BufferedReader(new FileReader(input));
            out = new StringWriter(4096);

            String line = null;
            while ((line = in.readLine()) != null) {
                if ((state & 1) == 0) {
                    // try to copy the XML line in any case
                    if ((p1 = line.indexOf("<?xml")) > -1)
                        if ((p2 = line.indexOf("?>", p1)) > -1) out.write(line, p1, p2 + 2);
                    // start state with the correct root element
                    if ((p1 = line.indexOf("<invocation")) > -1) {
                        if (p1 > 0) line = line.substring(p1);
                        ++state;
                    }
                }
                if ((state & 1) == 1) {
                    out.write(line);
                    if ((p1 = line.indexOf("</invocation>")) > -1) ++state;
                }
            }

            in.close();
            out.flush();
            out.close();
        } catch (IOException ioe) {
            throw new FriendlyNudge(
                    "While copying " + input.getPath() + " into temp. file: " + ioe.getMessage(),
                    5);
        }

        // some sanity checks
        if (state == 0)
            throw new FriendlyNudge(
                    "File "
                            + input.getPath()
                            + " does not contain invocation records, assuming failure",
                    5);
        if ((state & 1) == 1)
            throw new FriendlyNudge(
                    "File "
                            + input.getPath()
                            + " contains an incomplete invocation record, assuming failure",
                    5);

        // done
        return out.toString();
    }

    /**
     * Determines the exit code of an invocation record. Currently, we will determine the exit code
     * from the main job only.
     *
     * @param ivr is the invocation record to put into the database
     * @return the status code as exit code to signal failure etc.
     *     <pre>
     *   0   regular exit with exit code 0
     *   1   regular exit with exit code > 0
     *   2   failure to run program from kickstart
     *   3   application had died on signal
     *   4   application was suspended (should not happen)
     *   5   failure in exit code parsing
     *   6   impossible case
     * </pre>
     */
    private int determineExitStatus(InvocationRecord ivr) {
        boolean seen = false;
        for (Iterator i = ivr.iterateJob(); i.hasNext(); ) {
            Job job = (Job) i.next();

            // clean-up jobs don't count in failure modes
            if (job.getTag().equals("cleanup")) continue;

            // obtains status from job
            Status status = job.getStatus();
            if (status == null) return 6;

            JobStatus js = status.getJobStatus();
            if (js == null) {
                // should not happen
                return 6;
            } else if (js instanceof JobStatusRegular) {
                // regular exit code - success or failure?
                int exitcode = ((JobStatusRegular) js).getExitCode();
                if (exitcode != 0) return 1;
                else seen = true;
                // continue, if exitcode of 0 to implement chaining !!!!
            } else if (js instanceof JobStatusFailure) {
                // kickstart failure
                return 2;
            } else if (js instanceof JobStatusSignal) {
                // died on signal
                return 3;
            } else if (js instanceof JobStatusSuspend) {
                // suspended???
                return 4;
            } else {
                // impossible/unknown case
                return 6;
            }
        }

        // success, or no [matching] jobs
        return seen ? 0 : 5;
    }

    /**
     * Reads the contents of the specified file, and returns with the remote exit code contained in
     * the job chain.
     *
     * @param filename is the name of the file with the kickstart record.
     * @return the exit code derived from the remote exit code.
     */
    public int checkFile(String filename) {
        int result = 0;

        try {
            // check input file
            java.io.File check = new java.io.File(filename);

            // test 1: file exists
            if (!check.exists())
                throw new FriendlyNudge(
                        "file does not exist " + filename + ", assuming failure", 5);

            // test 2: file is readable
            if (!check.canRead())
                throw new FriendlyNudge(
                        "unable to read file " + filename + ", assuming failure", 5);

            // test 3: file has nonzero size
            if (check.length() == 0) {
                if (m_emptyFail) {
                    throw new FriendlyNudge(
                            "file " + filename + " has zero length" + ", assuming failure", 5);
                } else {
                    throw new FriendlyNudge(
                            "file " + filename + " has zero length" + ", assuming success", 0);
                }
            }

            // test 4: extract XML into tmp file
            String temp = extractToMemory(check);

            // test 5: try to parse XML -- but there is only one parser
            InvocationRecord invocation = null;
            synchronized (m_parser) {
                c_logger.info("starting to parse invocation");
                invocation = m_parser.parse(new StringReader(temp));
            }
            ;

            if (invocation == null)
                throw new FriendlyNudge(
                        "invalid XML invocation record in " + filename + ", assuming failure", 5);
            else c_logger.info("invocation was parsed successfully");

            // insert into database. This trickery works, because we already
            // checked previously that the dbschema does support invocations.
            // However, there is only one database connection at a time.
            if (!m_noDBase) {
                PTC ptc = (PTC) m_dbschema;

                synchronized (ptc) {
                    // FIXME: (start,host,pid) may not be a sufficient secondary key
                    if (ptc.getInvocationID(
                                    invocation.getStart(),
                                    invocation.getHostAddress(),
                                    invocation.getPID())
                            == -1) {
                        c_logger.info("adding invocation to database");
                        // may throw SQLException
                        ptc.saveInvocation(invocation);
                    } else {
                        c_logger.info("invocation already exists, skipping!");
                    }
                }
            }

            // determine result code, just look at the main job for now
            c_logger.info("determining exit status of main job");
            result = determineExitStatus(invocation);
            c_logger.info("exit status = " + result);

        } catch (FriendlyNudge fn) {
            c_logger.warn(fn.getMessage());
            result = fn.getResult();

        } catch (Exception e) {
            c_logger.warn(e.getMessage());
            result = 5;
        }

        // done
        return result;
    }

    public static void main(String args[]) throws IOException {
        // setup logging
        System.setProperty("log4j.defaultInitOverride", "true");
        Logger root = Logger.getRootLogger();
        root.addAppender(
                new ConsoleAppender(
                        new PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n")));
        root.setLevel(Level.INFO);
        c_logger = Logger.getLogger(SimpleServer.class);
        c_logger.info("starting");

        SimpleServer me = null;
        try {
            me = new SimpleServer(port);
        } catch (Exception e) {
            c_logger.fatal("Unable to instantiate a server: " + e.getMessage());
            System.exit(1);
        }

        // run forever
        try {
            while (!c_terminate) {
                new SimpleServerThread(me, me.m_server.accept()).start();
            }
        } catch (SocketException se) {
            // ignore -- closing the server socket in a thread during shutdown
            // will have accept fail with a socket exception in main()
        }

        // done
        c_logger.info("received shutdown");

        // count your threads, and the last one locks the
        // door and switches off the light... Grrr.
        synchronized (me) {
            while (SimpleServerThread.c_count > SimpleServerThread.c_cdone) {
                try {
                    me.wait(5000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        try {
            me.m_dbschema.close();
        } catch (Exception e) {
            c_logger.warn("During database disconnect: " + e.getMessage());
        }
        c_logger.warn("finished shutdown");
    }
}
