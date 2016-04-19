/**
 *  Copyright 2007-2015 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.refiner.cleanup;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.refiner.cleanup.constraint.Choice;
import edu.isi.pegasus.planner.refiner.cleanup.constraint.FloatingFile;
import edu.isi.pegasus.planner.refiner.cleanup.constraint.OutOfSpaceError;
import edu.isi.pegasus.planner.refiner.cleanup.constraint.Utilities;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * @author Sudarshan Srinivasan
 * @author Rafael Ferreira da Silva
 */
public class Constraint extends AbstractCleanupStrategy {

    /**
     * The property prefix for all properties used by this cleanup algorithm.
     */
    private static final String PROPERTY_PREFIX = "pegasus.file.cleanup.constraint";

    /**
     * The property suffix for determining the max space available for a site x.
     */
    private static final String PROPERTY_MAXSPACE_SUFFIX = "maxspace";

    /**
     * Default maximum space per site.
     */
    private static final String DEFAULT_MAX_SPACE = "10737418240";

    /**
     * Maximum available space per site.
     */
    private static long maxSpacePerSite;

    /**
     * Map of max available space per site.
     */
    private static Map<String, Long> maxAvailableSpacePerSite;

    /**
     * Maps of how much space is still available per site.
     */
    private static Map<String, Long> availableSpacePerSite;

    /**
     *
     */
    private static boolean deferStageins;

    /**
     * Dependency list. Maps from a node to the set of nodes dependent on it.
     */
    private static Map<GraphNode, Set<GraphNode>> dependencies;

    /**
     * Set of current heads (jobs that can be run immediately.
     */
    private static Set<GraphNode> heads;

    /**
     * Set of jobs that have finished execution.
     */
    private static Set<GraphNode> executed;

    /**
     * List of files that are pending cleanup.
     */
    private static NavigableMap<Long, List<FloatingFile>> floatingFiles;

    /**
     * Set of external stage-ins for which space is reserved in advance.
     */
    private static HashSet<Job> reservations;

    /**
     * Adds cleanup jobs to the workflow.
     *
     * @param workflow the workflow to add cleanup jobs to.
     *
     * @return the workflow with cleanup jobs added to it.
     */
    @Override
    public Graph addCleanupJobs(Graph workflow) {
        // invoke addCleanupJobs from super class.
        workflow = super.addCleanupJobs(workflow);

        // Initialize constrainer
        long constrainerStartTime = System.currentTimeMillis();
        String maxSpace = mProps.getCleanupConstraintMaxSpace();
        if (maxSpace == null) {
            maxSpace = DEFAULT_MAX_SPACE;
            mLogger.log("Could not determine the storage constraint. The "
                    + "constraint will be set to " + DEFAULT_MAX_SPACE
                    + " bytes", LogManager.WARNING_MESSAGE_LEVEL);
        }
        maxSpacePerSite = Long.parseLong(maxSpace);
        availableSpacePerSite = new HashMap<String, Long>();
        maxAvailableSpacePerSite = new HashMap<String, Long>();
        deferStageins = (mProps.getProperty("pegasus.file.cleanup.constraint.deferstageins") != null);
        
        // read file sizes from a CSV file
        String CSVName = System.getProperty("pegasus.file.cleanup.constraint.csv");
        if (CSVName != null) {
            try {
                Utilities.loadHashMap(CSVName);
            } catch (IOException e) {
                mLogger.log("Falling back to the old mechanism due to IOException while reading CSV: " + CSVName, LogManager.WARNING_MESSAGE_LEVEL);
            }
        }
        dependencies = Utilities.calculateDependencies(workflow, mLogger);

        //for each site do the process of adding cleanup jobs
        for (Iterator it = mResMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            addCleanUpJobs((String) entry.getKey(), (Set) entry.getValue(), workflow);
        }

        mLogger.log("Constrainer completed in " + (System.currentTimeMillis()
                - constrainerStartTime) + " ms", LogManager.DEBUG_MESSAGE_LEVEL);

        return workflow;
    }

    /**
     * Adds cleanup jobs for the workflow scheduled to a particular site a
     * breadth first search strategy is implemented based on the depth of the
     * job in the workflow
     *
     * @param site the site ID
     * @param leaves the leaf jobs that are scheduled to site
     * @param workflow the Graph into which new cleanup jobs can be added
     */
    private void addCleanUpJobs(String site, Set<GraphNode> leaves, Graph workflow) {

        mLogger.log(site + " " + leaves.size(), LogManager.DEBUG_MESSAGE_LEVEL);

        for (GraphNode currentNode : leaves) {
            mLogger.log("Found node " + currentNode.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
        }

        simulatedExecute(workflow, site, leaves);
    }

    /**
     * Function to estimate size of files. Stub that uses Pegasus reported file
     * sizes for now.
     *
     * @param workflow the workflow to add cleanup jobs to
     * @param site
     * @param currentSiteJobs
     */
    private void simulatedExecute(Graph workflow, String site, Set<GraphNode> currentSiteJobs) {
        heads = new HashSet<GraphNode>();
        executed = new HashSet<GraphNode>();
        floatingFiles = new TreeMap<Long, List<FloatingFile>>();
        reservations = new HashSet<Job>();

        // Set available space from the property
        String maxSiteSpace = mProps.getProperty(getPropertyName(site, PROPERTY_MAXSPACE_SUFFIX));
        if (maxSiteSpace == null) {
            availableSpacePerSite.put(site, maxSpacePerSite);
            maxAvailableSpacePerSite.put(site, maxSpacePerSite);
        } else {
            availableSpacePerSite.put(site, Long.parseLong(maxSiteSpace));
            maxAvailableSpacePerSite.put(site, Long.parseLong(maxSiteSpace));
        }
        mLogger.log("Performing constraint optimization on site " + site
                + " with maximum storage limit " + maxAvailableSpacePerSite.get(site), LogManager.INFO_MESSAGE_LEVEL);

        //if stage in jobs should not be deferred,
        //locate all stage in jobs whose output
        //site is the current site and mark them as
        //executing on this site
        if (!deferStageins) {
            currentSiteJobs = new HashSet<GraphNode>(currentSiteJobs);
            markStageIns(workflow, site, currentSiteJobs);
        }

        //locate initial set of heads
        locateInitialHeads(site, currentSiteJobs);
        mLogger.log(site + ": All jobs processed, " + availableSpacePerSite.get(site) + "/"
                + maxAvailableSpacePerSite.get(site) + " space left", LogManager.DEBUG_MESSAGE_LEVEL);

        //we should have a list of heads for this site by this point
        for (GraphNode currentNode : heads) {
            mLogger.log("Found head " + currentNode.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
        }

        while (true) {
            List<Choice> choices = getCurrentChoices(site);
            Choice selected = choose(choices);
            if (selected == null) {
                if (!floatingFiles.isEmpty()) {
                    //we have to remove the last few floating files
                    Choice dummy = new Choice(0, 0, new ArrayList<GraphNode>(floatingFiles.firstEntry().getValue().iterator().next().dependencies), null);
                    freeSpace(workflow, site, dummy, 0);
                }
                break;
            }
            execute(workflow, site, selected, currentSiteJobs);
        }
    }

    /**
     *
     * @param currentSiteJobs
     */
    private void markStageIns(Graph workflow, String site, Set<GraphNode> currentSiteJobs) {
        for (GraphNode node : workflow.getRoots()) {
            //we only deal with create dir jobs or stage in jobs
            Job j = (Job) node.getContent();
            switch (j.getJobType()) {
                case Job.CREATE_DIR_JOB:
                    //search for child jobs that are stage in
                    //jobs that also execute here
                    for (GraphNode child : node.getChildren()) {
                        Job childJob = (Job) child.getContent();
                        if (childJob.getJobType() == Job.STAGE_IN_JOB) {
                            TransferJob transferJob = (TransferJob) childJob;
                            if (transferJob.getNonThirdPartySite().equals(site)) {
                                currentSiteJobs.add(child);
                            }
                        }
                    }
                    break;
                case Job.STAGE_IN_JOB:
                    //if this job's non third party site is the current
                    //site, then mark it as executing here
                    TransferJob transferJob = (TransferJob) j;
                    if (transferJob.getNonThirdPartySite() != null && transferJob.getNonThirdPartySite().equals(site)) {
                        currentSiteJobs.add(node);
                    }
                    break;
                default:
                //other job types are ignored
            }
        }
    }

    /**
     * Locate the head nodes for the current site, simultaneously reserving
     * space for stage-ins from other sites.
     * <p/>
     * The algorithm used is based on the logic that if all dependencies are
     * elsewhere too the node is a head. Space is reserved for stage ins from
     * other sites to this site
     *
     * @param site Site name
     * @param currentSiteJobs The set of jobs at the current site
     */
    private void locateInitialHeads(String site, Set<GraphNode> currentSiteJobs) {
        for (GraphNode currentNode : currentSiteJobs) {
            //assume all nodes are head nodes
            boolean currentNodeIsHead = true;
            //iterate over all dependencies
            Set<GraphNode> dependenciesForNode = dependencies.get(currentNode);
            for (GraphNode dependency : dependenciesForNode) {
                //if we find a dependency thats running here this is not a head job
                if (currentSiteJobs.contains(dependency)) {
                    //the dependency is scheduled to run here
                    currentNodeIsHead = false;
                } else {
                    //this dependency node is running elsewhere
                    //we must check if it is an inter-site stage-in and reserve space if so
                    Job j = (Job) dependency.getContent();
                    int type = j.getJobType();
                    if ((type == Job.STAGE_IN_JOB || type == Job.INTER_POOL_JOB) && !reservations.contains(j)) {
                        reservations.add(j);
                        mLogger.log("Input stage in job " + j.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                        //figure out sizes and reserve that much space
                        Set<PegasusFile> outputs = j.getOutputFiles();
                        for (PegasusFile currentOutput : outputs) {
                            long currentOutputFileSize = Utilities.getFileSize(currentOutput);
                            mLogger.log("Found stage in of file " + currentOutput.getLFN() + " of size "
                                    + currentOutputFileSize, LogManager.DEBUG_MESSAGE_LEVEL);
                            availableSpacePerSite.put(site, availableSpacePerSite.get(site) - currentOutputFileSize);
                        }
                    }
                }
            }
            if (currentNodeIsHead) {
                if (((Job) currentNode.getContent()).getJobType() == Job.CREATE_DIR_JOB) {
                    mLogger.log("Job " + currentNode.getID() + " is a create dir.", LogManager.DEBUG_MESSAGE_LEVEL);
                    //when create dir, add immediate children if they are scheduled to run here
                    executed.add(currentNode);
                    for (GraphNode child : currentNode.getChildren()) {
                        if (currentSiteJobs.contains(child)) {
                            heads.add(child);
                        }
                    }
                } else {
                    //It may still not be a head if its indirect dependencies run on this node
                    for (GraphNode dependency : dependencies.get(currentNode)) {
                        if (currentSiteJobs.contains(dependency)) {
                            currentNodeIsHead = false;
                            break;
                        }
                    }
                    if (currentNodeIsHead) {
                        heads.add(currentNode);
                    }
                }
            }
        }
    }

    /**
     *
     * @return
     */
    private List<Choice> getCurrentChoices(String site) {
        List<Choice> choices = new LinkedList<Choice>();
        //Look at the execution of each head to decide how to proceed
        for (GraphNode currentHead : heads) {
            Choice c = calcSpaceFreedBy(site, currentHead);
            choices.add(c);
        }
        return choices;
    }

    /**
     *
     * @param site
     * @param toExecute
     * @return
     */
    private Choice calcSpaceFreedBy(String site, GraphNode toExecute) {
        //to calculate which files can be removed on running this head
        Job currentJob = (Job) toExecute.getContent();
        Map<Long, List<FloatingFile>> floatsForChoice = new HashMap<Long, List<FloatingFile>>();
        long intermediateRequirement;

        //Special case for stage out jobs:
        //No intermediate space, space freed is equal to sum of outputs
        if (currentJob.getJobType() == Job.STAGE_OUT_JOB && noChildrenRunHere(site, toExecute)) {
            intermediateRequirement = 0;
            for (PegasusFile outputFile : (Set<PegasusFile>) currentJob.getOutputFiles()) {
                if (mDoNotClean.contains(outputFile)) {
                    mLogger.log("Cannot clean file " + outputFile.getLFN() + "!", LogManager.WARNING_MESSAGE_LEVEL);
                } else {
                    mLogger.log("We can free file '" + outputFile.getLFN()
                            + "' on executing '" + currentJob.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                    final long fileSize = Utilities.getFileSize(outputFile);
                    if (!floatsForChoice.containsKey(fileSize)) {
                        List<FloatingFile> floats = new ArrayList<FloatingFile>(1);
                        floats.add(new FloatingFile(new HashSet<GraphNode>(toExecute.getParents()), outputFile));
                        floatsForChoice.put(fileSize, floats);
                    } else {
                        //floatsForChoice contains a key, append to it
                        floatsForChoice.get(fileSize).add(new FloatingFile(new HashSet<GraphNode>(toExecute.getParents()), outputFile));
                    }
                }
            }
        } else if (currentJob.getJobType() == Job.STAGE_IN_JOB || currentJob.getJobType() == Job.INTER_POOL_JOB) {
            if (reservations.contains(currentJob)) {
                //space has been reserved for the stage in already
                //however we can't free any files after running this
                intermediateRequirement = 0;
            } else {
                //we must check whether the target site of the stage in is this site or not
                TransferJob transferJob = (TransferJob) currentJob;
                if (transferJob.getNonThirdPartySite() != null && !transferJob.getNonThirdPartySite().equals(site)) {
                    intermediateRequirement = 0;
                } else {
                    intermediateRequirement = Utilities.getIntermediateRequirement(currentJob);
                }
            }
        } else {
            //Not a stage out job
            //Iterate one by one over all the parents
            for (GraphNode currentParent : toExecute.getParents()) {
                //Get the Job object corresponding to the parent
                Job currentParentJob = (Job) currentParent.getContent();

                mLogger.log("Analysing parent " + currentParentJob.getID(), LogManager.DEBUG_MESSAGE_LEVEL);

                //Iterate over each output file of this parent
                for (PegasusFile candidateFile : (Set<PegasusFile>) currentParentJob.getOutputFiles()) {

                    //If this  output is used only by this job it can be removed
                    if (currentJob.getInputFiles().contains(candidateFile)) {
                        //current job takes this file as input
                        //Populate the floaters table
                        Set<GraphNode> dependenciesForFile = new HashSet<GraphNode>();

                        //ensure that any other job that takes this file as input has already been executed
                        //have we seen another yet-to-execute job that needs this file?
                        boolean candidateFileUsed = false;

                        //Iterate over all jobs that could potentially use this file
                        for (GraphNode currentPeer : currentParent.getChildren()) {

                            //Get the Job object corresponding to this file
                            Job currentPeerJob = (Job) currentPeer.getContent();

                            if (currentPeerJob.getInputFiles().contains(candidateFile)) {
                                //current peer job has to be run to delete the candidate file
                                dependenciesForFile.add(currentPeer);

                                //We know that the current job uses this file and has not yet run
                                //So we're only interested in other jobs that use this file
                                //(and that too only if they are yet to run)
                                if ((!executed.contains(currentPeer)) //job yet to run
                                        && currentPeerJob != currentJob) {
                                    candidateFileUsed = true;
                                }
                            }
                        }

                        //If no one else uses the current file, we can free it
                        if (!candidateFileUsed) {
                            if (mDoNotClean.contains(candidateFile)) {
                                mLogger.log("Cannot clean file " + candidateFile.getLFN(), LogManager.DEBUG_MESSAGE_LEVEL);
                            } else {
                                mLogger.log("We can free file '" + candidateFile.getLFN()
                                        + "' on executing '" + currentJob.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                                final long fileSize = Utilities.getFileSize(candidateFile);
                                if (!floatsForChoice.containsKey(fileSize)) {
                                    List<FloatingFile> files = new ArrayList<FloatingFile>(1);
                                    files.add(new FloatingFile(dependenciesForFile, candidateFile));
                                    floatsForChoice.put(fileSize, files);
                                } else {
                                    floatsForChoice.get(fileSize).add(new FloatingFile(dependenciesForFile, candidateFile));
                                }
                            }
                        }
                    }
                }
            }
            //There may also be output files created by this job that are not used by any of its children
            if (currentJob.getJobType() != Job.STAGE_OUT_JOB) {
                for (PegasusFile outputFile : (Set<PegasusFile>) currentJob.getOutputFiles()) {
                    //check if any children use this file
                    boolean outputFileUsed = false;
                    for (GraphNode child : toExecute.getChildren()) {
                        //check if this child uses this file
                        Job childJob = (Job) child.getContent();
                        if (childJob.getInputFiles().contains(outputFile)) {
                            outputFileUsed = true;
                        }
                    }
                    if (!outputFileUsed) {
                        mLogger.log("We can free file '" + outputFile.getLFN()
                                + "' on executing '" + toExecute.getID()
                                + "' since no children need", LogManager.DEBUG_MESSAGE_LEVEL);
                        Set<GraphNode> dependenciesForFile = new HashSet<GraphNode>(1);
                        dependenciesForFile.add(toExecute);
                        Long fileSize = Utilities.getFileSize(outputFile);
                        if (!floatsForChoice.containsKey(fileSize)) {
                            List<FloatingFile> files = new ArrayList<FloatingFile>(1);
                            files.add(new FloatingFile(dependenciesForFile, outputFile));
                            floatsForChoice.put(fileSize, files);
                        } else {
                            floatsForChoice.get(fileSize).add(new FloatingFile(dependenciesForFile, outputFile));
                        }
                    }
                }
            }
            intermediateRequirement = Utilities.getIntermediateRequirement(currentJob);
        }
        //The list of jobs run on executing this file is a singleton containing only this job
        LinkedList<GraphNode> list = new LinkedList<GraphNode>();
        list.add(toExecute);

        long spaceFreedByRunningCurrentHead = 0;
        for (Long entry : floatsForChoice.keySet()) {
            spaceFreedByRunningCurrentHead += entry;
        }

        //Note: we are interested in the 'balance' of a job i.e. the overall effect of running the job
        //Balance = size(outputs) - size(inputs)
        //We look for jobs with negative balance
        //Therefore, the 'balance' is calculated as:
        //		intermediateRequirement - spaceFreedByRunningCurrentHead
        //intermediateRequirement represents the output size
        //spaceFreedByRunningCurrentHead represents the deletable inputs
        return new Choice(intermediateRequirement, intermediateRequirement - spaceFreedByRunningCurrentHead, list, floatsForChoice);
    }

    /**
     *
     * @param parent
     * @return
     */
    private boolean noChildrenRunHere(String site, GraphNode parent) {
        for (GraphNode child : parent.getChildren()) {
            Job j = (Job) child.getContent();
            //If we find even one child who runs here return false
            if (!j.getSiteHandle().equals(site)) {
                return false;
            }
        }
        return true;
    }

    /**
     *
     * @param choices
     * @return
     */
    private Choice choose(Iterable<Choice> choices) {
        final Choice placeholder = new Choice(Long.MAX_VALUE, Long.MAX_VALUE, null, null);
        Choice candidateChoice = placeholder;
        mLogger.log("Current choices are:", LogManager.DEBUG_MESSAGE_LEVEL);

        for (Choice currentChoice : choices) {
            mLogger.log(currentChoice.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
            //We're interested in the choice (amongst the choices that free) space with the least intermediate requirement
            if (currentChoice.balance <= 0) {

                //We're interested in the job with the least intermediate space requirement
                if (currentChoice.intermediateSpaceRequirement < candidateChoice.intermediateSpaceRequirement) {
                    candidateChoice = currentChoice;
                }
            }
        }
        if (candidateChoice == placeholder) {
            //There was no choice that released space. So, select the choice with least balance from all the choices
            for (Choice currentChoice : choices) {
                if (currentChoice.balance < candidateChoice.balance) {
                    candidateChoice = currentChoice;
                }
            }
        }
        Choice selectedChoice = (candidateChoice == placeholder ? null : candidateChoice);
        return selectedChoice;
    }

    /**
     *
     * @param workflow
     * @param site
     * @param selected
     * @param requiredSpace
     */
    private void freeSpace(Graph workflow, String site, Choice selected, long requiredSpace) {
        List<PegasusFile> listOfFiles = new ArrayList<PegasusFile>();
        //temporarily use the checkpoint method
        Set<GraphNode> parents = new HashSet<GraphNode>();
        Iterator<Map.Entry<Long, List<FloatingFile>>> i = floatingFiles.entrySet().iterator();
        if (i.hasNext()) {
            for (Map.Entry<Long, List<FloatingFile>> entry; i.hasNext();) {
                entry = i.next();
                for (FloatingFile f : entry.getValue()) {
                    parents.addAll(f.dependencies);
                    availableSpacePerSite.put(site, availableSpacePerSite.get(site) + entry.getKey());
                    requiredSpace -= entry.getKey();
                    listOfFiles.add(f.file);
                }
                i.remove();
            }
        }
        if (requiredSpace > 0) {
            throw new OutOfSpaceError("The storage provided is insufficient ("
                    + maxAvailableSpacePerSite.get(site) + "), need "
                    + requiredSpace + " more space on site '" + site + "'.");
        }
        // For 1 cleanup job
        String id = CLEANUP_JOB_PREFIX + new Random().nextInt(Integer.MAX_VALUE);
        if (!parents.isEmpty()) {
            GraphNode node = new GraphNode(id, mImpl.createCleanupJob(id, listOfFiles, (Job) parents.iterator().next().getContent()));
            node.setParents(new ArrayList<GraphNode>(parents));
            for (GraphNode parent : parents) {
                boolean hasStageOut = false;
                for (GraphNode child : parent.getChildren()) {
                    Job currentJob = (Job) child.getContent();
                    if (currentJob.getJobType() == Job.STAGE_OUT_JOB) {
                        child.addChild(node);
                        hasStageOut = true;
                    }
                }
                if (!hasStageOut) {
                    parent.addChild(node);
                }
            }
            node.setChildren(new ArrayList<GraphNode>(heads));
            for (GraphNode child : heads) {
                child.addParent(node);
            }
            mLogger.log(Utilities.cleanUpJobToString(parents, heads, listOfFiles), LogManager.DEBUG_MESSAGE_LEVEL);
            workflow.addNode(node);
        }

        mLogger.log(site + ": Space available is now " + availableSpacePerSite.get(site), LogManager.DEBUG_MESSAGE_LEVEL);
    }

    /**
     * Add all jobs in selected list of jobs to the executed set and update the
     * list of heads.
     *
     * @param workflow
     * @param site
     * @param selected
     * @param currentSiteJobs
     */
    private void execute(Graph workflow, String site, Choice selected, Set<GraphNode> currentSiteJobs) {
        //We must add all the jobs in selected's list of jobs to the executed set
        //and also update the list of heads
        Set<GraphNode> candidateHeads = new HashSet<GraphNode>();

        if (availableSpacePerSite.get(site) < selected.intermediateSpaceRequirement) {
            final long requiredSpace = selected.intermediateSpaceRequirement - availableSpacePerSite.get(site);
            mLogger.log("Require " + requiredSpace + " more space, creating cleanup job", LogManager.DEBUG_MESSAGE_LEVEL);
            freeSpace(workflow, site, selected, requiredSpace);
        }
        availableSpacePerSite.put(site, availableSpacePerSite.get(site) - selected.intermediateSpaceRequirement);

        mLogger.log(site + ": Selected choice (" + availableSpacePerSite.get(site) + "/" + maxAvailableSpacePerSite.get(site)
                + " free after exec): " + selected, LogManager.DEBUG_MESSAGE_LEVEL);

        //Phase I: Mark nodes as executed and remove them from head
        for (GraphNode node : selected.listOfJobs) {
            executed.add(node);
            heads.remove(node);
            candidateHeads.addAll(node.getChildren());
        }

        //Phase II:Examine candidate heads and add if necessary
        for (GraphNode candidateHead : candidateHeads) {
            boolean unsatisfiedDependency = false;
            for (GraphNode dependency : dependencies.get(candidateHead)) {
                if (!executed.contains(dependency) && currentSiteJobs.contains(dependency)) {
                    unsatisfiedDependency = true;
                    break;
                }
            }
            if (!unsatisfiedDependency && currentSiteJobs.contains(candidateHead)) {
                mLogger.log("Can now execute " + candidateHead.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                heads.add(candidateHead);
            }
        }

        //finally update the floating file list
        updateFloats(selected.floatingFiles);
    }

    /**
     * Safely merge a new floating file list into the internal floating files
     * list.
     *
     * @param floatingFiles the list to be merged into the internal floating
     * files list
     */
    private void updateFloats(Map<Long, List<FloatingFile>> floatingFilesList) {
        for (Map.Entry<Long, List<FloatingFile>> entry : floatingFilesList.entrySet()) {
            //for each key, iterate over values
            for (FloatingFile f : entry.getValue()) {
                if (floatingFiles.containsKey(entry.getKey())) {
                    //insert 'f' into the corresponding list
                    floatingFiles.get(entry.getKey()).add(f);
                } else {
                    //insert list directly
                    floatingFiles.put(entry.getKey(), entry.getValue());
                    break;
                }
            }
        }
    }

    /**
     * Returns the name of the property, for a particular site X.
     *
     * @param site the site X.
     * @param suffix the property suffix to be applied.
     *
     * @return the name of the property.
     */
    private String getPropertyName(String site, String suffix) {
        StringBuilder sb = new StringBuilder();
        sb.append(PROPERTY_PREFIX).append('.')
                .append(site).append('.').append(suffix);
        return sb.toString();
    }
}
