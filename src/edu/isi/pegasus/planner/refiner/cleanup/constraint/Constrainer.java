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
package edu.isi.pegasus.planner.refiner.cleanup.constraint;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.refiner.cleanup.CleanupImplementation;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

/**
 *
 * @author Sudarshan Srinivasan
 * @author Rafael Ferreira da Silva
 */
public class Constrainer {

    private static boolean printChoices;
    private static boolean verbose;
    private static boolean silent;
    private static boolean deferStageins;
    private static long maxSpacePerSite;

    private static boolean turnedOff = false;
    private static PrintWriter out;
    //Dependency list. Maps from a node to the set of nodes dependent on it
    private static Map<GraphNode, Set<GraphNode>> dependencies;
    //The entire workflow
    private static Graph workflow;
    //Set of current heads (jobs that can be run immediately
    private static Set<GraphNode> heads;
    //Set of jobs that have finished execution
    private static Set<GraphNode> executed;
    //How much space we have available right now
    private static long availableSpace;
    //List of files that are pending cleanup
    private static NavigableMap<Long, List<FloatingFile>> floatingFiles;
    //The cleanup implementation we're supposed to use
    private static CleanupImplementation cleanUpImplementation;
    //The files we're not supposed to clean
    private static Set<PegasusFile> filesNotToClean;
    //The name of the current size
    private static String site;
    //Set of external stage-ins for which space is reserved in advance
    private static HashSet<Job> reservations;
    //The time when the constrainer started constraining
    private static long constrainerStartTime;

    /**
     *
     * @param workflow
     * @param cleanupImplementation
     * @param filesNotToClean
     * @param mProps
     * @throws FileNotFoundException
     */
    public static void initialize(Graph workflow, CleanupImplementation cleanupImplementation,
            Set<PegasusFile> filesNotToClean, PegasusProperties mProps) throws FileNotFoundException {

        constrainerStartTime = System.currentTimeMillis();
        String logFileName = mProps.getProperty("pegasus.file.cleanup.constraint.log");
        if (logFileName == null) {
            System.err.println("Could not get location for constrainer log. Turning off constrainer");
            turnedOff = true;
        } else {
            out = new PrintWriter(new FileOutputStream(logFileName));
        }
        
        maxSpacePerSite = Long.parseLong(mProps.getProperty("pegasus.file.cleanup.constraint.maxspace"));

        //turn off printing if not needed
        boolean shouldWePrintChoices, shouldWeBeVerbose;
        shouldWePrintChoices = (mProps.getProperty("pegasus.file.cleanup.constraint.printChoices") != null);
        deferStageins = (mProps.getProperty("pegasus.file.cleanup.constraint.deferStageIns") != null);

        if (mProps.getProperty("pegasus.file.cleanup.constraint.verbose") == null) {
            shouldWeBeVerbose = false;
        } else {
            shouldWeBeVerbose = true;
            shouldWePrintChoices = true;
        }

        if (mProps.getProperty("pegasus.file.cleanup.constraint.silent") == null) {
            silent = false;
        } else {
            silent = true;
            shouldWeBeVerbose = false;
            shouldWePrintChoices = false;
        }

        verbose = shouldWeBeVerbose;
        printChoices = shouldWePrintChoices;

        dependencies = Utilities.calculateDependencies(workflow, verbose, out);
        Constrainer.workflow = workflow;
        Constrainer.cleanUpImplementation = cleanupImplementation;
        Constrainer.filesNotToClean = filesNotToClean;
    }

    /**
     *
     * @param site
     * @param nodes
     */
    public static void constrainify(String site, Set<GraphNode> nodes) {
        if (turnedOff) {
            return;
        }

        if (out == null) {
            System.out.println("OUT IS NULL");
            System.exit(1);
        }
        out.println(new StringBuilder("Performing constraint optimization on site ").append(site).append(" with maximum storage limit ").append(maxSpacePerSite));
        if (verbose) {
            for (GraphNode currentNode : nodes) {
                out.println("Found node " + currentNode.getID());
            }
        }

        out.println();
        simulatedExecute(site, nodes);
        out.println();
    }

    /**
     * Function to estimate size of files. Stub that uses Pegasus reported file
     * sizes for now.
     *
     * @param site
     * @param currentSiteJobs
     */
    private static void simulatedExecute(String site, Set<GraphNode> currentSiteJobs) {
        heads = new HashSet<GraphNode>();
        executed = new HashSet<GraphNode>();
        floatingFiles = new TreeMap<Long, List<FloatingFile>>();
        availableSpace = maxSpacePerSite;
        Constrainer.site = site;
        reservations = new HashSet<Job>();

        //if stage in jobs should not be deferred,
        //locate all stage in jobs whose output
        //site is the current site and mark them as
        //executing on this site
        if (!deferStageins) {
            currentSiteJobs = new HashSet<GraphNode>(currentSiteJobs);
            markStageIns(currentSiteJobs);
        }

        //locate initial set of heads
        locateInitialHeads(currentSiteJobs);

        out.printf("\n%s:All jobs processed, %d/%d space left\n", site, availableSpace, maxSpacePerSite);

        //we should have a list of heads for this site by this point
        if (verbose) {
            out.println();
            for (GraphNode currentNode : heads) {
                out.println("Found head " + currentNode.getID());
            }
        }

        while (true) {
            List<Choice> choices = getCurrentChoices();
            Choice selected = choose(choices);
            if (selected == null) {
                if (!floatingFiles.isEmpty()) {
                    //we have to remove the last few floating files
                    Choice dummy = new Choice(0, 0, new ArrayList<GraphNode>(floatingFiles.firstEntry().getValue().iterator().next().dependencies), null);
                    freeSpace(dummy, 0);
                }
                break;
            }
            execute(selected, currentSiteJobs);
        }
    }

    /**
     *
     * @param currentSiteJobs
     */
    private static void markStageIns(Set<GraphNode> currentSiteJobs) {
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
                    if (transferJob.getNonThirdPartySite().equals(site)) {
                        currentSiteJobs.add(node);
                    }
                    break;
                default:
                    //other job types are ignored
                    break;
            }
        }
    }

    /**
     *
     * @param selected
     * @param currentSiteJobs
     */
    private static void execute(Choice selected, Set<GraphNode> currentSiteJobs) {
        //We must add all the jobs in selected's list of jobs to the executed set
        //and also update the list of heads
        Set<GraphNode> candidateHeads = new HashSet<GraphNode>();

        if (availableSpace < selected.intermediateSpaceRequirement) {
            final long requiredSpace = selected.intermediateSpaceRequirement - availableSpace;
            out.printf("Require %d more space, creating cleanup job\n", requiredSpace);

            freeSpace(selected, requiredSpace);
        }
        availableSpace -= selected.intermediateSpaceRequirement;

        if (!silent) {
            out.printf("Selected choice (%d/%d free after exec): %s\n", availableSpace, maxSpacePerSite, selected);
        }

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
                if (verbose) {
                    out.println("Can now execute " + candidateHead.getID());
                }
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
    private static void updateFloats(Map<Long, List<FloatingFile>> floatingFiles) {
        for (Map.Entry<Long, List<FloatingFile>> entry : floatingFiles.entrySet()) {
            //for each key, iterate over values
            for (FloatingFile f : entry.getValue()) {
                if (Constrainer.floatingFiles.containsKey(entry.getKey())) {
                    //insert 'f' into the corresponding list
                    Constrainer.floatingFiles.get(entry.getKey()).add(f);
                } else {
                    //insert list directly
                    Constrainer.floatingFiles.put(entry.getKey(), entry.getValue());
                    break;
                }
            }
        }
    }

    /**
     *
     * @param selected
     * @param requiredSpace
     */
    private static void freeSpace(Choice selected, long requiredSpace) {
        StringBuilder builder = new StringBuilder("cleanup_");
        Random r = new Random();
        builder.append(r.nextLong());
        List<PegasusFile> listOfFiles = new ArrayList<PegasusFile>();
        //temporarily use the checkpoint method
        Set<GraphNode> parents = new HashSet<GraphNode>();
        Iterator<Map.Entry<Long, List<FloatingFile>>> i = floatingFiles.entrySet().iterator();
        if (i.hasNext()) {
            for (Map.Entry<Long, List<FloatingFile>> entry; i.hasNext();) {
                entry = i.next();
                for (FloatingFile f : entry.getValue()) {
                    parents.addAll(f.dependencies);
                    availableSpace += entry.getKey();
                    requiredSpace -= entry.getKey();
                    listOfFiles.add(f.file);
                }
                i.remove();
            }
        }
        if (requiredSpace > 0) {
            out.println("I'm DEAD out of space! I give up!");
            throw new OutOfSpaceError("The storage you have provided is insufficient, need " + requiredSpace + " more space");
        } else {
//            // For the number of queued jobs
//            if (!heads.isEmpty()) {
//                Collections.sort(listOfFiles, new Comparator<PegasusFile>() {
//
//                    @Override
//                    public int compare(PegasusFile o1, PegasusFile o2) {
//                        return o1.getSize() < o2.getSize() ? -1 : o1.getSize() == o2.getSize() ? 0 : 1;
//                    }
//                });
////                System.out.println("-- Heads: " + heads.size());
////                int limit = (new Random()).nextInt(heads.size());
//                int limit = 256;
////                int limit = heads.size();
//                if (limit == 0) limit = 1;
////                System.out.println("-- Limit: " + limit);
//                List<PegasusFile>[] lists = new List[limit];
//                for (int j = 0; j < limit; j++) {
//                    lists[j] = new ArrayList<PegasusFile>();
//                }
//                int index = 0;
//                for (PegasusFile file : listOfFiles) {
//                    lists[index].add(file);
//                    if (index == limit - 1) {
//                        index = 0;
//                    } else {
//                        index++;
//                    }
//                }
//
//                index = 0;
////                for (GraphNode hNode : heads) {
//                for (int j = 0; j < limit; j++) {
//                    String id = "cleanup_" + Math.abs(new Random().nextLong());
//                    GraphNode node = new GraphNode(id, cleanUpImplementation.createCleanupJob(id, lists[index], (Job) parents.iterator().next().getContent()));
//
//                    node.setParents(new ArrayList<GraphNode>(parents));
//                    for (GraphNode parent : parents) {
//                        parent.addChild(node);
//                    }
//                    node.setChildren(new ArrayList<GraphNode>(heads));
//                    for (GraphNode child : heads) {
//                        child.addParent(node);
//                    }
//
//                    workflow.addNode(node);
//                    if (index == limit - 1) {
//                        index = 0;
//                    } else {
//                        index++;
//                    }
//                }
//            } else {
//                String id = builder.toString();
//                GraphNode node = new GraphNode(id, cleanUpImplementation.createCleanupJob(id, listOfFiles, (Job) parents.iterator().next().getContent()));
//                node.setParents(new ArrayList<GraphNode>(parents));
//                for (GraphNode parent : parents) {
//                    parent.addChild(node);
//                }
//                node.setChildren(new ArrayList<GraphNode>(heads));
//                for (GraphNode child : heads) {
//                    child.addParent(node);
//                }
//                out.println(Utilities.cleanUpJobToString(parents, heads, listOfFiles));
//                workflow.addNode(node);
//            }

            // For 1 cleanup job
            String id = builder.toString();
            GraphNode node = new GraphNode(id, cleanUpImplementation.createCleanupJob(id, listOfFiles, (Job) parents.iterator().next().getContent()));
            node.setParents(new ArrayList<GraphNode>(parents));
            for (GraphNode parent : parents) {
                parent.addChild(node);
            }
            node.setChildren(new ArrayList<GraphNode>(heads));
            for (GraphNode child : heads) {
                child.addParent(node);
            }
            out.println(Utilities.cleanUpJobToString(parents, heads, listOfFiles));
            workflow.addNode(node);
        }
        out.println("Space available is now " + availableSpace);
    }

    /**
     *
     * @param choices
     * @return
     */
    private static Choice choose(Iterable<Choice> choices) {
        final Choice placeholder = new Choice(Long.MAX_VALUE, Long.MAX_VALUE, null, null);
        Choice candidateChoice = placeholder;
        if (printChoices) {
            out.println();
            out.println("Current choices are:");
        }

//        if (choices.iterator().hasNext()) {
//            candidateChoice = choices.iterator().next();
//        }
//        List<Choice> list = new ArrayList<Choice>();
//        for (Choice currentChoice : choices) {
//            list.add(currentChoice);
//        }
//        if (!list.isEmpty()) {
//            candidateChoice = list.get((new Random()).nextInt(list.size()));
//        }
        for (Choice currentChoice : choices) {
            if (printChoices) {
                out.println(currentChoice);
            }
//            if (currentChoice.balance < candidateChoice.intermediateSpaceRequirement) {
//                candidateChoice = currentChoice;
//            }
//            if (currentChoice.balance > candidateChoice.balance) {
//                candidateChoice = currentChoice;
//            }
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
     * Locate the head nodes for the current site, simultaneously reserving
     * space for stage-ins from other sites
     * <p/>
     * The algorithm used is based on the logic that if all dependencies are
     * elsewhere too the node is a head. Space is reserved for stage ins from
     * other sites to this site
     *
     * @param currentSiteJobs The set of jobs at the current site
     */
    private static void locateInitialHeads(Set<GraphNode> currentSiteJobs) {
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
                        out.println();
                        out.println("Input stage in job " + j.getID());
                        //figure out sizes and reserve that much space
                        Set<PegasusFile> outputs = j.getOutputFiles();
                        for (PegasusFile currentOutput : outputs) {
                            long currentOutputFileSize = Utilities.getFileSize(currentOutput);
                            if (verbose) {
                                out.println("Found stage in of file " + currentOutput.getLFN() + " of size "
                                        + currentOutputFileSize);
                            }
                            availableSpace -= currentOutputFileSize;
                        }
                    }
                }
            }
            if (currentNodeIsHead) {
                if (((Job) currentNode.getContent()).getJobType() == Job.CREATE_DIR_JOB) {
                    out.println("Job " + currentNode.getID() + " is a create dir.");
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
    private static List<Choice> getCurrentChoices() {
        List<Choice> choices = new LinkedList<Choice>();
        //Look at the execution of each head to decide how to proceed
        for (GraphNode currentHead : heads) {
            Choice c = calcSpaceFreedBy(currentHead);
            choices.add(c);
        }
        return choices;
    }

    /**
     *
     * @param toExecute
     * @return
     */
    private static Choice calcSpaceFreedBy(GraphNode toExecute) {
        //to calculate which files can be removed on running this head
        Job currentJob = (Job) toExecute.getContent();
        Map<Long, List<FloatingFile>> floatsForChoice = new HashMap<Long, List<FloatingFile>>();
        long intermediateRequirement;

        //Special case for stage out jobs:
        //No intermediate space, space freed is equal to sum of outputs
        if (currentJob.getJobType() == Job.STAGE_OUT_JOB && noChildrenRunHere(toExecute)) {
            intermediateRequirement = 0;
            for (PegasusFile outputFile : (Set<PegasusFile>) currentJob.getOutputFiles()) {
                if (filesNotToClean.contains(outputFile)) {
                    out.printf("WARNING: Can't clean file %s!\n", outputFile.getLFN());
                } else {
                    if (verbose) {
                        out.printf("We can free file %s on executing %s!\n", outputFile.getLFN(), currentJob.getID());
                    }
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
                if (!transferJob.getNonThirdPartySite().equals(site)) {
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

                if (verbose) {
                    out.printf("Analysing parent %s\n", currentParentJob.getID());
                }

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
                            if (filesNotToClean.contains(candidateFile)) {
                                out.printf("WARNING: Can't clean file %s!\n", candidateFile.getLFN());
                            } else {
                                if (verbose) {
                                    out.printf("We can free file %s on executing %s!\n", candidateFile.getLFN(), currentJob.getID());
                                }
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
                        if (verbose) {
                            out.printf("We can free file %s on executing %s since no children need it\n", outputFile.getLFN(), toExecute.getID());
                        }
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
//                return new Choice(intermediateRequirement, intermediateRequirement, list, floatsForChoice);
//                return new Choice(intermediateRequirement, spaceFreedByRunningCurrentHead, list, floatsForChoice);
    }

    /**
     *
     * @param parent
     * @return
     */
    private static boolean noChildrenRunHere(GraphNode parent) {
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
     */
    public static void terminate() {
        if (!turnedOff) {
            out.println("Constrainer completed in " + (System.currentTimeMillis() - constrainerStartTime) + " ms");
            out.flush();
            out.close();
        }
    }
}
