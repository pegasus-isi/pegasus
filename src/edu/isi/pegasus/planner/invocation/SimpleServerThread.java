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

import java.io.*;
import java.net.*;
import org.apache.log4j.*;

public class SimpleServerThread extends Thread {
    private String m_remote = null;
    private Socket m_socket = null;
    private SimpleServer m_server = null;
    private static Logger c_logger = null;

    static int c_count = 0;
    static int c_cdone = 0;

    public void log(Level l, String msg) {
        c_logger.log(l, m_remote + ": " + msg);
    }

    public SimpleServerThread(SimpleServer me, Socket socket) {
        super("SimpleServerThread#" + ++c_count);
        this.m_server = me;
        this.m_socket = socket;
        if (c_logger == null) {
            // Singleton-like init
            c_logger = Logger.getLogger(SimpleServerThread.class);
            c_logger.setLevel(Level.DEBUG);
            c_logger.addAppender(
                    new ConsoleAppender(
                            new PatternLayout("%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%t] %m%n")));
            c_logger.setAdditivity(false);
        }

        InetSocketAddress remote = (InetSocketAddress) m_socket.getRemoteSocketAddress();
        this.m_remote = remote.getAddress().getHostAddress() + ":" + remote.getPort();
    }

    public void run() {
        String line = null;
        log(Level.INFO, "starting");

        try {
            PrintWriter out = new PrintWriter(m_socket.getOutputStream(), true);
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(m_socket.getInputStream()));

            while ((line = in.readLine()) != null) {
                if (c_logger.isDebugEnabled()) log(Level.DEBUG, "received >>" + line + "<<");
                if (line.startsWith("PARSE")) {
                    // request to parse a given file

                    String[] request = line.split("[ \t]", 3);
                    if (request.length != 3) {
                        out.println("400 Illegal request format");
                        continue;
                    }

                    if (!request[2].equals("ECP/1.0")) {
                        out.println("501 Unrecognized version");
                        continue;
                    }

                    int result = m_server.checkFile(request[1]);
                    out.println("300 Result code " + result);

                } else if (line.equals("QUIT")) {
                    // done
                    out.println("200 Good-bye");
                    break;
                } else if (line.equals("SHUTDOWN")) {
                    out.println("200 Shutting down server, good-bye");
                    SimpleServer.setTerminate(true);
                    m_server.m_server.close(); // close server socket
                    break;
                } else {
                    // illegal request
                    out.println("500 Illegal request");
                    break;
                }
            }

            out.close();
            in.close();

            m_socket.close();
            synchronized (m_server) {
                ++c_cdone;
                m_server.notifyAll();
            }
            log(Level.INFO, "finished [" + c_count + ":" + c_cdone + "]");
        } catch (IOException e) {
            log(Level.WARN, "I/O error: " + e.getMessage());
        }
    }
}
