/**
 * Copyright 2007-2015 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

/**
 * Helper class to call out to pegasus-worker-create to create a pegasus worker package on the
 * submit host
 *
 * @author Karan Vahi
 */
public class CreateWorkerPackage {
    private LogManager mLogger;

    private PegasusBag mBag;

    public CreateWorkerPackage(PegasusBag bag) {
        mBag = bag;
        mLogger = bag.getLogger();
    }

    /**
     * Copies the worker package from the pegasus installation directory to the submit directory.
     *
     * @return file object to copied worker package
     * @throws RuntimeException in case of errors
     */
    public File copy() {
        PlannerOptions options = mBag.getPlannerOptions();
        if (options == null) {
            throw new RuntimeException("No planner options specified " + options);
        }
        return this.copy(new File(options.getSubmitDirectory()));
    }

    /**
     * Copies the worker package from the pegasus installation directory to the directory passed
     *
     * @param directory
     * @return file object to copied worker package
     * @throws RuntimeException in case of errors
     */
    public File copy(File directory) {
        // the source directory is in share/pegasus/worker-packages/
        File shareDir = mBag.getPegasusProperties().getSharedDir();
        File sourceDir = new File(shareDir, "worker-packages");
        if (!sourceDir.exists()) {
            throw new RuntimeException(
                    "Source directory for worker package does not exist " + sourceDir);
        }

        // construct the basename for the worker package on submit host
        Version v = new Version();
        StringBuffer basename = new StringBuffer();
        basename.append("pegasus-worker-")
                .append(v.getVersion())
                .append("-")
                .append(v.getPlatform())
                .append(".tar.gz");

        File workerPackage = new File(sourceDir, basename.toString());
        if (!workerPackage.exists() || !workerPackage.canRead()) {
            throw new RuntimeException("Unable to find worker package at " + workerPackage);
        }

        // copy the worker package to directory
        File destFile = new File(directory, basename.toString());
        try {

            if (!directory.exists()) directory.createNewFile();

            FileChannel fcSrc = null;
            FileChannel fcDst = null;
            try {
                fcSrc = new FileInputStream(workerPackage).getChannel();
                fcDst = new FileOutputStream(destFile).getChannel();
                fcDst.transferFrom(fcSrc, 0, fcSrc.size());
            } finally {
                if (fcSrc != null) fcSrc.close();
                if (fcDst != null) fcDst.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to copy worker package "
                            + workerPackage
                            + " to directory "
                            + directory);
        }
        return destFile;
    }
}

/**
 * An inner class, that implements the StreamGobblerCallback to go through the output of
 * pegasus-worker-create
 */
class WorkerPackageCallback implements StreamGobblerCallback {

    /** The version detected. */
    private String mWorkerPackage;

    /** echo "# PEGASUS_WORKER_PACKAGE=${WORKER_PACKAGE_NAME}" */
    public static final String VARIABLE_NAME = "PEGASUS_WORKER_PACKAGE";

    private final LogManager mLogger;

    /** The Default Constructor */
    public WorkerPackageCallback(LogManager logger) {
        mLogger = logger;
        mWorkerPackage = null;
    }

    /**
     * Callback whenever a line is read from the stream by the StreamGobbler. Counts the occurences
     * of the word that are in the line, and increments to the global counter.
     *
     * @param line the line that is read.
     */
    public void work(String line) {

        if (line == null) {
            return;
        }

        mLogger.log(line, LogManager.DEBUG_MESSAGE_LEVEL);
        if (line.startsWith("# ")) {
            line = line.substring(2);
            String[] arr = line.split("=", 2);
            String key = arr[0];
            if (key == null) {
                return;
            }

            if (key.equals(VARIABLE_NAME)) {
                if (arr.length != 2) {
                    throw new RuntimeException("Output of pegasus-woker-create malformed " + line);
                }
                mWorkerPackage = arr[1];
            }
        }
    }

    /**
     * Returns the worker package that was created
     *
     * @return the created worker package
     */
    public String getWorkerPackage() {
        return mWorkerPackage;
    }

    public static void main(String[] args) {
        LogManager logger = LogManagerFactory.loadSingletonInstance();
        logger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        logger.logEventStart("Main function", "test", "l", LogManager.DEBUG_MESSAGE_LEVEL);

        CreateWorkerPackage cw = new CreateWorkerPackage(bag);

        // File wp = cw.create( new File( "/tmp/") );
        // System.out.println( "Created worker package " + wp );
    }
}
