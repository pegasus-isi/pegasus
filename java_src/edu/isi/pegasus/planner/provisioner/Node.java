/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.provisioner;

import java.util.*;

/**
 * An instance of this class represents an independent task of a workflow. A task has PET(predicted
 * execution time), list of input data sources(parent tasks), and list of output data(child tasks).
 * This class also used for BTS algorithm such as EST, LFT, schdeuled start/finish time and core
 * methods for BTS algorithm used by OccupationDiagram class.
 *
 * @author Eunkyu Byun
 */
public class Node {
    public static final long DEFAULT_WEIGHT = 1;
    private LinkedList inEdges;
    private LinkedList outEdges;
    private Edge critEdge;

    private String id;
    private String taskName;
    private long weight;

    private long upLen;
    private long downLen;

    private HashSet ancestors;
    private HashSet descendants;

    public void init() {
        this.st = 0;
        this.ancET = -1;
        this.desET = -1;
    }

    /**
     * constructor
     *
     * @param id the unique ID of this task
     * @param name task name(executable name)
     * @param w predicted execution time
     */
    public Node(String id, String name, long w) {
        this(id, name, -1, w);
    }

    public Node(String id, String name, int n, long w) {
        this.upLen = -1;
        this.downLen = -1;
        this.id = id;
        this.taskName = name;
        this.weight = w;
        inEdges = new LinkedList();
        outEdges = new LinkedList();

        ancestors = new HashSet();
        descendants = new HashSet();
        init();
    }

    public Node(String id) {
        this(id, "NullTask", DEFAULT_WEIGHT);
    }

    /**
     * set execution time of this task
     *
     * @param w excution time
     */
    public void setWeight(long w) {
        this.weight = w;
    }

    /**
     * return execution time of this task
     *
     * @return excution time
     */
    public long evalWeight() {
        return weight;
    }

    /**
     * add a data dependency edge to this task
     *
     * @param e the edge to be added
     */
    public void addIn(Edge e) {
        if (!inEdges.contains(e)) {
            inEdges.add(e);
        }
    }

    /**
     * remove a specified data dependency edge from this task
     *
     * @param e the edge to be removed
     */
    public void removeIn(Edge e) {
        //	System.out.println("in "+id+" edge "+e.getID() + " is removedIn");
        inEdges.remove(e);
    }

    /**
     * add a data dependency edge from this task
     *
     * @param e the edge to be added
     */
    public void addOut(Edge e) {
        if (!outEdges.contains(e)) {
            outEdges.add(e);
        }
    }

    /**
     * remove a specified data dependency edge from this task
     *
     * @param e the edge to be removed
     */
    public void removeOut(Edge e) {
        //	System.out.println("in "+id+" edge "+e.getID() + " is removedOut");
        outEdges.remove(e);
    }

    /**
     * check whether all parent tasks are finished.
     *
     * @return true if all parent tasks are finished.
     */
    public boolean checkIn() {
        boolean ret = true;
        Iterator iter = inEdges.iterator();
        while (iter.hasNext()) {
            Edge next = (Edge) iter.next();
            ret &= next.complete;
            st = Math.max(st, next.compTime + next.getCost());
        }
        return ret;
    }

    /**
     * return the list of incoming edges.
     *
     * @return LinkedList of edges.
     */
    public LinkedList getIn() {
        return inEdges;
    }

    /**
     * return the list of outgoing edges.
     *
     * @return LinkedList of edges.
     */
    public LinkedList getOut() {
        return outEdges;
    }

    /**
     * notify child nodes that this task will finish at time et
     *
     * @param et finish time
     */
    public void initOut(boolean s, long et) {
        Iterator iter = outEdges.iterator();
        while (iter.hasNext()) {
            Edge next = (Edge) iter.next();
            next.complete = s;
            if (s) {
                next.compTime = Math.max(next.compTime, et);
            } else {
                next.compTime = et;
            }
        }
    }

    public long st = 0; // scheduled time, used for HEFT algorithm

    // Calculate UpLength
    private void updateUpLen() {
        if (isTop()) {
            this.upLen = 0;
            return;
        }
        long maxUpLen = -1;
        Iterator parents = inEdges.iterator();
        while (parents.hasNext()) {
            Edge next = (Edge) parents.next();
            Node cur = next.getFrom();
            long curUpLen = cur.getUpLen() + cur.evalWeight() + next.getCost();
            if (maxUpLen < curUpLen) {
                maxUpLen = curUpLen;
                critEdge = next;
            }
        }
        this.upLen = maxUpLen;
    }

    /**
     * return the UpLength of this task. UpLength is the longest path from the entry task
     *
     * @return UpLength
     */
    public long getUpLen() {
        if (this.upLen < 0) {
            updateUpLen();
        }
        return this.upLen;
    }

    private void updateDownLen() {
        if (isBottom()) {
            downLen = 0;
            return;
        }

        Iterator iter = outEdges.iterator();
        while (iter.hasNext()) {
            Edge ce = (Edge) iter.next();
            Node cn = ce.getTo();
            downLen = Math.max(downLen, cn.getDownLen() + cn.evalWeight() + ce.getCost());
        }
    }

    /**
     * return the DnLength of this task. DnLength is the longest path to the exit task
     *
     * @return DnLength
     */
    public long getDownLen() {
        if (downLen == -1) updateDownLen();
        return this.downLen;
    }

    /**
     * Check whether this task is the entry task or not.
     *
     * @return true or false
     */
    public boolean isTop() {
        return (inEdges.size() == 0);
    }

    /**
     * Check whether this task is the exit task or not.
     *
     * @return true or false
     */
    public boolean isBottom() {
        return (outEdges.size() == 0);
    }

    public String getID() {
        return id;
    }

    /**
     * update EST(earliest start time) of this task.
     *
     * @param nlb new EST
     * @param set set of ancestor tasks
     */
    public void updateLeftBound(long nlb, /*LinkedList*/ TreeSet set) {
        //	System.out.print("updateLeftBound in "+id+" from "+lb);
        if (nlb > lb) {
            //		System.out.print(" to "+nlb+ "\r\n");
            boolean contain = false;
            if (set != null) contain = set.remove(this);
            this.lb = nlb;
            if (contain) {
                //			System.out.println(id+ " is removed in updateLeftBound");
                boolean test = set.add(this);
                //			System.out.println(test);
            }

            for (int i = 0; i < outEdges.size(); i++) {
                Edge ce = (Edge) outEdges.get(i);
                Node n = ce.getTo();
                n.updateLeftBound(nlb + weight + ce.getCost(), set);
            }
        } // else System.out.print("\r\n");
    }

    /**
     * update LFT(latest finish time) of this task.
     *
     * @param nrb new LFT
     * @param set set of descendant tasks
     */
    public void updateRightBound(long nrb, /*LinkedList*/ TreeSet set) {
        //	System.out.print("updateRightBound in "+id+" from "+rb);
        if (nrb < rb) {
            //		System.out.print(" to "+nrb+"\r\n");
            boolean contain = false;
            if (set != null) contain = set.remove(this);
            this.rb = nrb;
            if (contain) {
                //			System.out.println(id+ " is removed in updateRightBound");
                boolean test = set.add(this);
                //			System.out.println(test);
            }

            for (int i = 0; i < inEdges.size(); i++) {
                Edge ce = (Edge) inEdges.get(i);
                Node n = ce.getFrom();
                n.updateRightBound(nrb - weight - ce.getCost(), set);
            }
        } // else System.out.print("\r\n");
    }

    public boolean stacked = false; // bit for stacked in Occupation diagram
    public long lb; // left bound (EST)
    public long rb; // right bound (LFT)
    public long olb; // original left bound
    public long orb; // original right bound

    public long tempST; // scheduled start time
    public long tempFT; // scheduled finish time

    /**
     * Non-propagate redistribution of this task
     *
     * @param timeMap List of scheduled tasks at each time slot. i.e., Occupation diagram itself
     * @param limit maximum height of Occupation diagram. The result of redistribution should cause
     *     taller height of this limit.
     * @param force Not used.
     * @return true if this task is redistributed.
     */
    public boolean NPbalance(LinkedList[] timeMap, int limit, boolean force) {
        if (weight == 0) return false;
        if (!force && (tempST - lb) < weight && (rb - tempFT) < weight) return false;

        int min = limit;
        int cursor = (int) lb;
        boolean rightBias = (getDesSize() < getAncSize());

        for (int i = (int) lb; i < rb - weight; i++) {
            int localMax = 0;
            for (int j = i; j < (i + weight); j++) {
                int overlap = (j >= tempST && j < tempFT) ? 1 : 0;
                localMax = Math.max(localMax, timeMap[j].size() - overlap);
            }
            if (min > localMax) {
                min = localMax;
                cursor = i;
            }
            if (min == localMax && rightBias) {
                cursor = i;
            }
        }

        if ((min + 1) >= limit && !force) return false;

        //	System.out.println("NPbalance ID:"+id+" limit:"+limit+" from: "+ tempST+" to: " +
        // cursor);

        for (int i = (int) tempST; i < tempFT; i++) timeMap[i].remove(this);
        tempST = cursor;
        tempFT = cursor + weight;
        for (int i = (int) tempST; i < tempFT; i++) timeMap[i].add(this);

        for (int i = 0; i < outEdges.size(); i++) {
            Edge ce = (Edge) outEdges.get(i);
            Node n = ce.getTo();
            n.updateLeftBound(tempFT + ce.getCost(), null);
        }
        for (int i = 0; i < inEdges.size(); i++) {
            Edge ce = (Edge) inEdges.get(i);
            Node n = ce.getFrom();
            n.updateRightBound(tempST - ce.getCost(), null);
        }

        return (min < limit);
    }

    /**
     * Redistribute this task to the earlier time. This causes ancestor tasks to be redistributed
     * too.
     *
     * @param timeMap List of scheduled tasks at each time slot. i.e., Occupation diagram itself
     * @param limit maximum height of Occupation diagram. The result of redistribution should cause
     *     taller height of this limit.
     * @param bound Earliest time this tasks can be scheduled.
     * @return true if this task is redistributed.
     */
    public boolean moveLeft(LinkedList[] timeMap, int limit, long bound) {
        boolean found = false;
        int cursor = (int) (bound - weight);
        for (int i = (int) (bound - weight); i >= olb; i--) {
            int localMax = 0;
            for (int j = i; j < (i + weight); j++) {
                int overlap = (j >= tempST && j < tempFT) ? 1 : 0;
                localMax = Math.max(localMax, timeMap[j].size() - overlap);
            }
            if (limit > (localMax + 1 - pushedParents(i))) {
                cursor = i;
                found = true;
                break;
            }
        }

        //	System.out.println("moveLeft ID:"+id+" limit:"+limit + " bound:"+ bound +" found:" +
        // found+ " " + cursor);

        if (!found) return false; // not possible to move

        // push parent nodes
        for (int i = 0; i < inEdges.size() && found; i++) {
            Edge ce = (Edge) inEdges.get(i);
            Node n = ce.getFrom();
            if (cursor >= lb) {
                n.updateRightBound(cursor - ce.getCost(), null);
            } else {
                found &= n.moveLeft(timeMap, limit, cursor - ce.getCost());
            }
        }
        if (!found) return false; // child block!!

        // re-schedule
        lb = Math.min(lb, cursor);
        for (int i = (int) tempST; i < tempFT; i++) timeMap[i].remove(this);
        tempST = cursor;
        tempFT = cursor + weight;
        for (int i = (int) tempST; i < tempFT; i++) timeMap[i].add(this);

        // release leftbound of child nodes
        for (int i = 0; i < outEdges.size(); i++) {
            Edge ce = (Edge) outEdges.get(i);
            Node n = ce.getTo();
            n.updateLeftBound(tempFT + ce.getCost(), null);
        }
        return true;
    }

    /**
     * Redistribute this task to the later time. This causes descensant tasks to be redistributed
     * too.
     *
     * @param timeMap List of scheduled tasks at each time slot. i.e., Occupation diagram itself
     * @param limit maximum height of Occupation diagram. The result of redistribution should cause
     *     taller height of this limit.
     * @param bound Lastest time this tasks can be scheduled.
     * @return true if this task is redistributed.
     */
    public boolean moveRight(LinkedList[] timeMap, int limit, long bound) {
        boolean found = false;
        int cursor = (int) bound;
        for (int i = (int) bound; i <= (orb - weight); i++) {
            int localMax = 0;
            for (int j = i; j < (i + weight); j++) {
                int overlap = (j >= tempST && j < tempFT) ? 1 : 0;
                localMax = Math.max(localMax, timeMap[j].size() - overlap);
            }
            if (limit > (localMax + 1 - pushedChildren(i + weight))) {
                cursor = i;
                found = true;
                break;
            }
        }

        //	System.out.println("moveRight ID:"+id+" limit:"+limit + " bound:"+ bound +" found:" +
        // found+ " " + cursor);
        if (!found) return false; // not possible to move

        // push child nodes
        for (int i = 0; i < outEdges.size() && found; i++) {
            Edge ce = (Edge) outEdges.get(i);
            Node n = ce.getTo();
            if (cursor + weight <= rb) {
                n.updateLeftBound(cursor + weight + ce.getCost(), null);
            } else {
                found &= n.moveRight(timeMap, limit, cursor + weight + ce.getCost());
            }
        }
        if (!found) return false; // child block!!

        // re-schedule
        rb = Math.max(rb, cursor);
        for (int i = (int) tempST; i < tempFT; i++) timeMap[i].remove(this);
        tempST = cursor;
        tempFT = cursor + weight;
        for (int i = (int) tempST; i < tempFT; i++) timeMap[i].add(this);

        // release leftbound of child nodes
        for (int i = 0; i < inEdges.size(); i++) {
            Edge ce = (Edge) inEdges.get(i);
            Node n = ce.getFrom();
            n.updateRightBound(tempST - ce.getCost(), null);
        }
        return true;
    }

    private int pushedParents(long timeLimit) {
        int result = 0;
        for (int i = 0; i < inEdges.size(); i++) {
            Edge ce = (Edge) inEdges.get(i);
            Node cn = ce.getFrom();
            if (timeLimit < cn.tempFT) {
                result++;
            }
        }
        return result;
    }

    private int pushedChildren(long timeLimit) {
        int result = 0;
        for (int i = 0; i < outEdges.size(); i++) {
            Edge ce = (Edge) outEdges.get(i);
            Node cn = ce.getTo();
            if (timeLimit > cn.tempST) {
                result++;
            }
        }
        return result;
    }

    /** check all child task's start time is scheduled after the finishtime of this task */
    public void checkIntegrity() {
        for (int i = 0; i < outEdges.size(); i++) {
            Edge ce = (Edge) outEdges.get(i);
            Node cn = ce.getTo();
            if ((tempFT + ce.getCost()) > cn.tempST) {
                System.out.println(
                        "Violation! from:"
                                + id
                                + "("
                                + tempFT
                                + ") to "
                                + cn.getID()
                                + "("
                                + cn.tempST
                                + ")");
            }
            cn.checkIntegrity();
        }
    }

    /**
     * build the list of descendant tasks
     *
     * @return the list of descensant tasks
     */
    public Set buildDescendants() {
        if (!isBottom() && descendants.size() == 0) {
            Iterator iter = outEdges.iterator();
            while (iter.hasNext()) {
                Edge ce = (Edge) iter.next();
                Node cn = ce.getTo();
                if (!cn.isBottom()) {
                    descendants.add(cn);
                    descendants.addAll(cn.buildDescendants());
                }
            }
        }
        return descendants;
    }

    /**
     * build the list of ancestor tasks
     *
     * @return the list of ancestor tasks
     */
    public Set buildAncestors() {
        if (!isTop() && ancestors.size() == 0) {
            Iterator iter = inEdges.iterator();
            while (iter.hasNext()) {
                Edge ce = (Edge) iter.next();
                Node cn = ce.getFrom();
                if (!cn.isTop()) {
                    ancestors.add(cn);
                    ancestors.addAll(cn.buildAncestors());
                }
            }
        }
        return ancestors;
    }

    public int getDepSize() {
        return descendants.size() + ancestors.size();
    }

    public int getAncSize() {
        return ancestors.size();
    }

    public int getDesSize() {
        return descendants.size();
    }

    private long ancET;
    private long desET;

    /** @return the sum of execution time of all ancestor tasks */
    public long getAncET() {
        if (ancET < 0) {
            ancET = 0;
            Iterator iter = ancestors.iterator();
            while (iter.hasNext()) {
                Node cn = (Node) iter.next();
                ancET += cn.evalWeight();
            }
        }
        return ancET;
    }

    /** @return the sum of execution time of all descendant tasks */
    public long getDesET() {
        if (desET < 0) {
            desET = 0;
            Iterator iter = descendants.iterator();
            while (iter.hasNext()) {
                Node cn = (Node) iter.next();
                desET += cn.evalWeight();
            }
        }
        return desET;
    }

    /** @return the sum of execution time of all dependent tasks */
    public long getDepET() {
        return getAncET() + getDesET();
    }

    public boolean isAnc(Node n) {
        return ancestors.contains(n);
    }

    public boolean isDes(Node n) {
        return descendants.contains(n);
    }

    public void print() {
        String inList = "";
        Iterator iter = inEdges.iterator();
        while (iter.hasNext()) {
            inList += ((Edge) iter.next()).getID() + ",";
        }

        String outList = "";
        iter = outEdges.iterator();
        while (iter.hasNext()) {
            outList += ((Edge) iter.next()).getID() + ",";
        }

        //	System.out.println("Node_"+id+"("+ taskName +","+ weight +"sec) IN:"+inList+",
        // OUT:"+outList);
        System.out.println(
                "Node_"
                        + id
                        + "("
                        + taskName
                        + ","
                        + weight
                        + ") upLen:"
                        + upLen
                        + ", downLen:"
                        + downLen);
    }

    public int cluster = -1;
    public long tlevel = 0;
    public boolean examined = false;

    /** used for DSC algorithm */
    public boolean isFree() {
        if (examined) return false;
        boolean result = true;
        for (int i = 0; i < inEdges.size(); i++) {
            Edge ce = (Edge) inEdges.get(i);
            Node cn = ce.getFrom();
            if (!cn.examined) {
                result = false;
                break;
            }
        }
        return result;
    }

    public boolean equals(Object e) {
        //	System.out.println("comparison is called this");
        if (e instanceof Node) {
            Node cn = (Node) e;
            boolean same = cn.getID().equals(id);
            //	System.out.println(id+ " " + cn.getID() + " " + same);
            return same;
        } else return false;
    }
}
