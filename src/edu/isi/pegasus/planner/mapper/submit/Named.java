/**
 * Copyright 2007-2016 University Of Southern California
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
package edu.isi.pegasus.planner.mapper.submit;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;
import java.util.Properties;

/**
 * A Named implementation for the Submit Directory Mapper that allows users to specify the relative
 * directory into which the submit files for the compute jobs are written out to by the Planner
 * during Code Generation. The relative directory is specified by associated a Pegasus profile key
 * named {@link Pegasus#RELATIVE_SUBMIT_DIR_KEY}
 *
 * <p>For jobs that don't have the profile key associated, the mapper assigns the relative directory
 * to be the logical transformation name for the job. All auxiliary jobs that are added by the
 * planner go to the base submit directory of the workflow.
 *
 * @see Pegasus#RELATIVE_SUBMIT_DIR_KEY
 * @author Karan Vahi
 */
public class Named implements SubmitMapper {

    /** Short description. */
    private static final String DESCRIPTION = "Relative Submit Directory Mapper";

    /** The root of the directory tree under which other directories are created */
    private File mBaseDir;

    /** Handle to the logger */
    private LogManager mLogger;

    private File mBaseSubmitDirectory;

    /** Default constructor. */
    public Named() {}

    /**
     * Initializes the submit mapper
     *
     * @param bag the bag of Pegasus objects
     * @param properties properties that can be used to control the behavior of the mapper
     * @param base the base directory relative to which all job directories are created
     */
    public void initialize(PegasusBag bag, Properties properties, File base) {
        mBaseDir = base;
        mLogger = bag.getLogger();
        PlannerOptions options = bag.getPlannerOptions();
        mBaseSubmitDirectory = new File(options.getSubmitDirectory());
    }

    /**
     * Returns the relative submit directory for the job. The directory is also created if need be.
     *
     * @param job
     * @return the relative submit directory for the job
     */
    public File getRelativeDir(Job job) {
        String relative = this.determineRelativeDirectory(job);

        // create the relative dir on the submit host if not created already
        File fullDir = new File(mBaseSubmitDirectory, relative);
        if (!fullDir.exists()) {
            if (!fullDir.mkdirs()) {
                throw new MapperException("Unable to create directory " + fullDir);
            }
        }

        return new File(relative);
    }

    /**
     * Returns the full path to the submit directory to be used for the job
     *
     * @param job
     * @return
     */
    public File getDir(Job job) {
        return new File(this.mBaseSubmitDirectory, this.determineRelativeDirectory(job));
    }

    /**
     * Returns a short description of the mapper.
     *
     * @return String
     */
    public String description() {
        return Named.DESCRIPTION;
    }

    /**
     * Returns the relative directory to use for the job.
     *
     * @param job the job
     * @return
     */
    protected String determineRelativeDirectory(Job job) throws RuntimeException {
        String relative = null;

        switch (job.getJobType()) {
                // for compute jobs look for the profile
            case Job.COMPUTE_JOB:
                relative = job.vdsNS.getStringValue(Pegasus.RELATIVE_SUBMIT_DIR_KEY);
                if (relative == null) {
                    // fall back to the transformation name
                    relative = job.getTXName();
                    if (relative != null && relative.length() == 0) {
                        // empty transformation we should throw an error
                        relative = null;
                    }
                }
                break;

                // all other jobs use . to indicate we generate in the base
                // submit directory of the workflow
            default:
                relative = ".";
                break;
        }

        if (relative == null) {
            throw new MapperException(
                    "Pegasus Profile Key "
                            + Pegasus.RELATIVE_SUBMIT_DIR_KEY
                            + " not specified. Unable to determine relative directory for job "
                            + job.getName());
        }
        return relative;
    }
}
