/**
 * Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.partitioner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This callback writes out a <code>DAX</code> file for each of the partitions, and also writes out
 * a <code>PDAX</code> file that captures the relations between the partitions.
 *
 * @author not attributable
 * @version $Revision$
 */
public class WriterCallback implements Callback {

    /** The handle to the partition graph writer. */
    protected PDAXWriter mPDAXWriter;

    /**
     * The handle to the dax writer that writes out the dax corresponding to the partition
     * identified. The base name of the partition is gotten from it.
     */
    protected DAXWriter mDAXWriter;

    /** The path to the PDAX file written out. */
    protected String mPDAX;

    /** Handle to the properties available. */
    protected PegasusProperties mProps;

    /** The handle to the logger object. */
    protected LogManager mLogger;

    /**
     * A boolean indicating that the partitioning has started. This is set, by the first call to the
     * cbPartition( Partition ) callback.
     */
    protected boolean mPartitioningStarted;

    /** The default constructor. */
    public WriterCallback() {
        // mLogger = LogManager.getInstance();
    }

    /**
     * Initializes the Writer Callback.
     *
     * @param properties the properties passed to the planner.
     * @param daxFile the path to the DAX file that is being partitioned.
     * @param daxName the namelabel of the DAX as set in the root element of the DAX.
     * @param directory the directory where the partitioned daxes have to reside.
     */
    public void initialize(
            PegasusProperties properties, String daxFile, String daxName, String directory) {

        mProps = properties;
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
        // load the writer for the partitioned daxes
        mDAXWriter = DAXWriter.loadInstance(properties, daxFile, directory);
        mDAXWriter.setPartitionName(daxName);

        // name of pdax file is same as the dax file
        // meaning the name attribute in root element are same.
        mPDAXWriter = getHandletoPDAXWriter(daxFile, daxName, directory);

        // write out the XML header for the PDAX file
        mPDAXWriter.writeHeader();
    }

    /**
     * Callback for when a partitioner determines that partition has been constructed. A DAX file is
     * written out for the partition.
     *
     * @param p the constructed partition.
     * @throws RuntimeException in case of any error while writing out the DAX or the PDAX files.
     */
    public void cbPartition(Partition p) {
        mPartitioningStarted = true;

        // not sure if we still need it
        p.setName(mDAXWriter.getPartitionName());

        // for time being do a localize catch
        // till i change the interface
        try {
            // write out the partition information to the PDAX file
            mLogger.log(
                    "Writing to the pdax file for partition " + p.getID(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            mPDAXWriter.write(p);
            mLogger.log(
                    "Writing to the pdax file for partition -DONE" + p.getID(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            // write out the DAX file
            mDAXWriter.writePartitionDax(p);

        } catch (IOException ioe) {
            // wrap and throw in Runtime Exception
            throw new RuntimeException("Writer Callback for partition " + p.getID(), ioe);
        }
    }

    /**
     * Callback for when a partitioner determines the relations between partitions that it has
     * previously constructed.
     *
     * @param child the id of a partition.
     * @param parents the list of <code>String</code> objects that contain the id's of the parents
     *     of the partition.
     * @throws RuntimeException in case of any error while writing out the DAX or the PDAX files.
     */
    public void cbParents(String child, List parents) {
        mPDAXWriter.write(partitionRelation2XML(child, parents));
    }

    /**
     * Callback for the partitioner to signal that it is done with the processing. This internally
     * closes all the handles to the DAX and PDAX writers.
     */
    public void cbDone() {
        // change internal state to signal
        // that we are done with partitioning.
        mPartitioningStarted = false;

        mPDAXWriter.close();
        mDAXWriter.close();
    }

    /**
     * Returns the name of the pdax file written out. Will be null if the partitioning has not
     * completed.
     *
     * @return path to the pdax file.
     */
    public String getPDAX() {
        return this.mPDAX;
    }

    /**
     * Returns the name of the partition, that needs to be set while creating the Partition object
     * corresponding to each partition.
     *
     * @return the name of the partition.
     */
    protected String getPartitionName() {
        return mDAXWriter.getPartitionName();
    }

    /**
     * It returns the handle to the writer for writing out the pdax file that contains the relations
     * amongst the partitions and the jobs making up the partitions.
     *
     * @param daxFile the path to the DAX file that is being partitioned.
     * @param name the name/label that is to be assigned to the pdax file.
     * @param directory the directory where the partitioned daxes have to reside.
     * @return handle to the writer of pdax file.
     */
    protected PDAXWriter getHandletoPDAXWriter(String daxFile, String name, String directory) {
        String pdaxPath;
        // get the name of dax file sans the path
        String daxName = new java.io.File(daxFile).getName();
        // construct the basename of the pdax file
        pdaxPath =
                (daxName == null)
                        ? "partition"
                        : ((daxName.indexOf('.') > 0)
                                ? daxName.substring(0, daxName.indexOf('.'))
                                : daxName);
        // now the complete path
        pdaxPath = directory + File.separator + pdaxPath + ".pdax";
        // System.out.println("Name is " + nameOfPDAX);
        mPDAX = pdaxPath;

        return new PDAXWriter(name, pdaxPath);
    }

    /**
     * Returns the xml description of a relation between 2 partitions.
     *
     * @param childID the ID of the child.
     * @param parentID the ID of the parent.
     * @return the XML description of child parent relation.
     */
    protected String partitionRelation2XML(String childID, String parentID) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n\t<child ref=\"").append(childID).append("\">");
        sb.append("\n\t\t<parent ref=\"").append(parentID).append("\"/>");
        sb.append("\n\t</child>");
        return sb.toString();
    }

    /**
     * Returns the xml description of a relation between 2 partitions.
     *
     * @param childID the ID of the child
     * @param parentIDs <code>List</code> of parent IDs.
     * @return the XML description of child parent relations.
     */
    protected String partitionRelation2XML(String childID, List parentIDs) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n\t<child ref=\"").append(childID).append("\">");
        for (Iterator it = parentIDs.iterator(); it.hasNext(); ) {
            sb.append("\n\t\t<parent ref=\"").append(it.next()).append("\"/>");
        }
        sb.append("\n\t</child>");
        return sb.toString();
    }

    /**
     * Returns the xml description of a relation between 2 partitions.
     *
     * @param childID the ID of the child
     * @param parentIDs <code>Set</code> of parent IDs.
     * @return the XML description of child parent relations.
     */
    protected String partitionRelation2XML(String childID, Set parentIDs) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n\t<child ref=\"").append(childID).append("\">");
        for (Iterator it = parentIDs.iterator(); it.hasNext(); ) {
            sb.append("\n\t\t<parent ref=\"").append(it.next()).append("\"/>");
        }
        sb.append("\n\t</child>");
        return sb.toString();
    }
}
