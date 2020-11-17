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
 * This class keeps structure of an Occupation Diagram and conduct BTS algorithm
 *
 * @author Eunkyu Byun
 */
public class OccupationDiagram {
    TreeSet nodes;
    long RFT;
    LinkedList[] timeMap;
    int max;
    int maxIndex;

    /**
     * Constructor
     *
     * @param rft Requested finish time(deadline) of algorithm.
     */
    public OccupationDiagram(long rft) {
        nodes = new TreeSet(new NodeComp());
        this.RFT = rft;
        max = 0;
        maxIndex = 0;
        timeMap = new LinkedList[(int) rft];
        for (int i = 0; i < rft; i++) {
            timeMap[i] = new LinkedList();
        }
    }
    /** Comparator which compare the schedulable duration of two nodes */
    private class NodeComp implements Comparator<Node> {
        // compare schedulable duration of two nodes
        public int compare(Node a, Node b) {
            if ((a.rb - a.lb) > (b.rb - b.lb)) return 1;
            if ((a.rb - a.lb) < (b.rb - b.lb)) return -1;

            // tie is broken by the number of dependent nodes
            if (a.getDepET() > b.getDepET()) return 1;
            if (a.getID() == b.getID()) return 0;
            return -1;
        }
    }

    /**
     * Add tasks into the occupation diagram. But task placement is not conducted yet
     *
     * @param node Node to be added
     */
    public void add(Node node) {
        // node with narrower has priority
        if (node.evalWeight() > 0) {
            nodes.add(node);
        }
    }

    private void printNodes() {
        System.out.println("******************");
        Iterator iter = nodes.iterator();
        while (iter.hasNext()) {
            Node n = (Node) iter.next();
            System.out.println(
                    n.getID()
                            + " lb:"
                            + n.lb
                            + " lb:"
                            + n.rb
                            + " diff:"
                            + (n.rb - n.lb)
                            + " dep:"
                            + n.getDepET());
        }
    }

    /**
     * Run Task placement phase of BTS algorithm
     *
     * @param print if true print debug message to stdout
     * @return maxinum height of Occupation diagram when Task placement is done
     */
    public int stack(boolean print) {
        while (nodes.size() > 0) {
            double t00 = Calendar.getInstance().getTimeInMillis();
            //	printNodes();
            Node cn = (Node) nodes.first();
            nodes.remove(cn);
            cn.stacked = true;

            int leftB = (int) cn.lb;
            int rightB = (int) cn.rb;
            int et = (int) cn.evalWeight();

            if (et <= 0) continue;

            boolean rightBias = (cn.getDesSize() < cn.getAncSize());

            int min = 100000;
            int cursor = leftB;
            for (int i = leftB; i <= (rightB - et); i++) {
                int localMax = 0;
                for (int j = i; j < (i + et); j++) {
                    localMax = Math.max(localMax, timeMap[j].size());
                }
                if (min > localMax) {
                    min = localMax;
                    cursor = i;
                }
                if (min == localMax && rightBias) {
                    cursor = i;
                }
            }

            for (int i = cursor; i < (cursor + et); i++) {
                timeMap[i].add(cn);
                max = Math.max(max, timeMap[i].size());
            }
            cn.tempST = (long) cursor;
            cn.tempFT = (long) (cursor + et);
            //	System.out.println(cn.getID()+"(lb:"+leftB+",rb:"+rightB+",et:"+et+") ->
            // <"+cn.tempST+","+cn.tempFT+"> height:"+(min+1));

            LinkedList parents = cn.getIn();
            for (int i = 0; i < parents.size(); i++) {
                Edge ce = (Edge) parents.get(i);
                Node n = ce.getFrom();
                n.updateRightBound(cn.tempST - ce.getCost(), nodes);
            }
            LinkedList childs = cn.getOut();
            for (int i = 0; i < childs.size(); i++) {
                Edge ce = (Edge) childs.get(i);
                Node n = ce.getTo();
                n.updateLeftBound(cn.tempFT + ce.getCost(), nodes);
            }
            double t01 = Calendar.getInstance().getTimeInMillis();
            //	System.out.println("stack :"+cn.getID()+" takes " + (t01-t00) );
        }

        return max;
    }

    /**
     * Run Task redistribution phase of BTS algorithm
     *
     * @param goal Redistribution loop is stop if maximum height of OD reach this goal value.
     * @param print if true print debug message to stdout
     * @return maxinum height of Occupation diagram when Task redistribution is done
     */
    public int balance(int goal, boolean print) {
        int max = 0;
        int maxIndexL = 0;
        int maxIndexR = 0;

        int cntWhile = 0;
        int cntNPt = 0;
        int cntNP = 0;
        int cntPt = 0;
        int cntP = 0;

        LinkedList sortTemp = null;

        while (true) {
            cntWhile++;
            max = 0;
            // find maximum time slot
            for (int i = 0; i < RFT; i++) {
                //	System.out.println("ts: "+i+ " h: "+ timeMap[i].size() );
                if (timeMap[i].size() == max) maxIndexR = i;
                if (timeMap[i].size() > max) {
                    max = timeMap[i].size();
                    maxIndexL = i;
                    maxIndexR = i;
                }
            }
            if (max <= goal) break;
            //	System.out.println("RFT: "+ RFT+" max: "+max+" maxIndexL: "+maxIndexL+" maxIndexR:
            // "+maxIndexR);

            boolean reduced = false;

            // non propagated balancing
            for (int k = 0; k < max; k++) {
                cntNPt++;
                Node cn = (Node) timeMap[maxIndexL].get(k);
                if (cn.NPbalance(timeMap, max, false)) {
                    reduced = true;
                    cntNP++;
                    break;
                }
                cn = (Node) timeMap[maxIndexR].get(k);
                if (cn.NPbalance(timeMap, max, false)) {
                    reduced = true;
                    cntNP++;
                    break;
                }
            }
            if (reduced) continue;

            // propagated balancing

            // redistribute to left direction
            sortTemp = new LinkedList();
            for (int k = 0; k < max; k++) { // among tasks in the highest time slot
                Node cn = (Node) timeMap[maxIndexL].get(k);
                if ((cn.tempST - cn.olb)
                        >= cn.evalWeight()) { // check whether the tasks can be moved or not
                    boolean mid = false;
                    for (int ii = 0;
                            ii < sortTemp.size();
                            ii++) { // sorting movable tasks with the number ancestor tasks and ET
                        Node tn = (Node) sortTemp.get(ii);
                        if (tn.getAncET() > cn.getAncET()) {
                            sortTemp.add(ii, cn);
                            mid = true;
                            break;
                        } else if (tn.getAncET() < cn.getAncET()) {
                            continue;
                        } else if (tn.evalWeight() > cn.evalWeight()) { // tie break
                            sortTemp.add(ii, cn);
                            mid = true;
                        }
                    }
                    if (!mid) sortTemp.add(cn);
                }
            }

            boolean succ = false;
            for (int k = 0; k < sortTemp.size(); k++) {
                Node cn = (Node) sortTemp.get(k);
                succ = cn.moveLeft(timeMap, max, maxIndexL);
                if (succ) break;
            }
            if (succ) continue;

            // redistribute to right direction
            sortTemp = new LinkedList();
            for (int k = 0; k < max; k++) { // among tasks in the highest time slot
                Node cn = (Node) timeMap[maxIndexR].get(k);
                if ((cn.orb - cn.tempFT)
                        >= cn.evalWeight()) { // check whether the tasks can be moved or not
                    boolean mid = false;
                    for (int ii = 0;
                            ii < sortTemp.size();
                            ii++) { // sorting movable tasks with the number descendant tasks and ET
                        Node tn = (Node) sortTemp.get(ii);
                        if (tn.getDesET() > cn.getDesET()) {
                            sortTemp.add(ii, cn);
                            mid = true;
                            break;
                        } else if (tn.getDesET() < cn.getDesET()) {
                            continue;
                        } else if (tn.evalWeight() > cn.evalWeight()) { // tie break
                            sortTemp.add(ii, cn);
                            mid = true;
                        }
                    }
                    if (!mid) sortTemp.add(cn);
                }
            }

            for (int k = 0; k < sortTemp.size(); k++) {
                Node cn = (Node) sortTemp.get(k);
                succ = cn.moveRight(timeMap, max, maxIndexR + 1);
                if (succ) break;
            }

            if (!succ) break;
        }

        //	System.out.println("cntWhile: "+cntWhile+" cntNP: "+cntNP+" cntNPt:"+cntNPt+" cntP:
        // "+cntP+" cntPt:"+cntPt+" max:"+max);
        return max;
    }
}
