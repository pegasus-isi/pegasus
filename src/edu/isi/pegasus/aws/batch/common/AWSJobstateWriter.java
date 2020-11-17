/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.common;

import edu.isi.pegasus.aws.batch.classes.AWSJob;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import org.apache.log4j.Logger;

/** @author Karan Vahi */
public class AWSJobstateWriter {

    public static final String JOBSTATE_LOG_FILENAME = ".jobstate.log";

    private Logger mLogger;

    private PrintWriter mJobstateWriter;

    /** Default constructor. */
    public AWSJobstateWriter() {}

    public void initialze(File directory, String prefix, Logger logger) {
        // "405596411149";
        mLogger = Logger.getLogger(AWSJobstateWriter.class.getName());
        try {
            mJobstateWriter =
                    new PrintWriter(
                            new BufferedWriter(
                                    new FileWriter(
                                            new File(directory, prefix + JOBSTATE_LOG_FILENAME))));
        } catch (IOException ex) {
            mLogger.error("Unable to initalized jobstate writer to directory " + directory, ex);
            throw new RuntimeException(
                    "Unable to initalized jobstate writer to directory " + directory, ex);
        }
    }

    /**
     * @param name name of the job
     * @param awsID aws id
     * @param state the state
     */
    public synchronized void log(String name, String awsID, AWSJob.JOBSTATE state) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(new Date().getTime())
                    .append(" ")
                    .append(name)
                    .append(" ")
                    .append(state.toString().toUpperCase())
                    .append(" ")
                    .append(awsID);

            mLogger.debug("Writing to jobstate.log - " + sb.toString());
            mJobstateWriter.println(sb.toString());
            mJobstateWriter.flush();
        } catch (Exception ex) {
            mLogger.error("Unable to write to jobstate.log ", ex);
            throw new RuntimeException("Unable to write to jobstate.log ", ex);
        }
    }
}
