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

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

/**
 * This Estimator is used to find the near-optimal number of processors required to complete
 * workflow within a given RFT(requested finish time). This estimator read workflow from a DAX file
 * and user can select one estimation method among BTS, DSC, and IterHEFT. User also need to provide
 * RFT and the precision of the predictied execution time.
 *
 * @author Eunkyu Byun
 */
public class Estimator {
    private String fileName;
    private String method;
    private long RFT;
    private int prec;

    private Node topNode;
    private Node bottomNode;
    private HashSet edges;
    private HashMap nodes;

    private long totalET = 0;

    /**
     * Constructor
     *
     * @param fileName DAX file describing the workflow
     * @param methodID One of those; BTS, DSC, IterHEFT
     * @param RFT requested finish time. i.e., deadline
     * @param prec The precision of the predicted execution time
     */
    public Estimator(String fileName, String methodID, long RFT, int prec) {
        this.fileName = fileName;
        this.prec = prec;
        this.method = methodID;

        edges = new HashSet();
        nodes = new HashMap();

        topNode = new Node("TOP", "NullTask", 0);
        bottomNode = new Node("BOTTOM", "NullTask", 0);

        nodes.put("TOP", topNode);
        nodes.put("BOTTOM", bottomNode);
    }

    private void readDAX() throws Exception {
        // for dummy input
        DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Element dax = db.parse(new File(fileName)).getDocumentElement();

        if (!dax.getTagName().equals("adag")) {
            throw new Exception("This input is not a adag file");
        }

        NodeList nodelist = dax.getChildNodes();
        int nodeCount = nodelist.getLength();
        HashMap fileMap = new HashMap();
        HashSet output = new HashSet();
        for (int i = 0; i < nodeCount; i++) {
            org.w3c.dom.Node cur = nodelist.item(i);
            String node = (String) cur.getNodeName();

            if (node.equals("filename")) {
                String name = cur.getAttributes().getNamedItem("file").getNodeValue();
                fileMap.put(name, topNode);
            }

            if (node.equals("job")) {
                String id = cur.getAttributes().getNamedItem("id").getNodeValue();
                String name = cur.getAttributes().getNamedItem("name").getNodeValue();
                long wei = Node.DEFAULT_WEIGHT;
                if (cur.getAttributes().getNamedItem("weight") != null) {
                    wei = Long.parseLong(cur.getAttributes().getNamedItem("weight").getNodeValue());
                }
                Node newNode = new Node(id, name, wei);
                totalET += wei;
                nodes.put(id, newNode);

                NodeList usesList = cur.getChildNodes();
                boolean noInput = true;
                boolean noOutput = true;
                for (int j = 0; j < usesList.getLength(); j++) {
                    org.w3c.dom.Node uses = usesList.item(j);
                    if (uses.getNodeName().equals("uses")) {
                        String file = uses.getAttributes().getNamedItem("file").getNodeValue();
                        String link = uses.getAttributes().getNamedItem("link").getNodeValue();
                        if (link.equals("input")) {
                            Node from = (Node) fileMap.get(file);
                            if (from == null) {
                                from = topNode;
                                fileMap.put(file, topNode);
                            }
                            Edge target = new Edge(from, newNode, file, Edge.DEFAULT_SIZE);
                            edges.add(target);
                            from.addOut(target);
                            newNode.addIn(target);
                            output.remove(file);
                            noInput = false;
                        } else if (link.equals("output")) {
                            fileMap.put(file, newNode);
                            output.add(file);
                            noOutput = false;
                        }
                    }
                }
                if (noInput) {
                    Edge nullIn = new Edge(topNode, newNode, id + "_null_input_edge", 0);
                    topNode.addOut(nullIn);
                    newNode.addIn(nullIn);
                }
                if (noOutput) {
                    Edge nullOut = new Edge(newNode, bottomNode, id + "_null_output_edge", 0);
                    newNode.addOut(nullOut);
                    bottomNode.addIn(nullOut);
                }
            }
        }

        // connect input edges to TOP node, and output edges to BOTTOM node
        Iterator iter = output.iterator();
        while (iter.hasNext()) {
            String fileName = (String) iter.next();
            Node from = (Node) fileMap.get(fileName);
            Edge target = new Edge(from, bottomNode, fileName, Edge.DEFAULT_SIZE);
            edges.add(target);
            from.addOut(target);
            bottomNode.addIn(target);
        }
    }

    private void calculateSlotSize() {}

    private void updateETs() {
        Iterator iter = nodes.values().iterator();
        while (iter.hasNext()) {
            Node n = (Node) iter.next();
            long old = n.evalWeight();
            n.setWeight((long) Math.ceil((double) old / this.prec));
        }
        iter = edges.iterator();
        while (iter.hasNext()) {
            Edge e = (Edge) iter.next();
            long old = e.getCost();
            e.setCost((long) Math.ceil((double) old / this.prec));
        }
    }

    private int BTS() {
        topNode.buildDescendants();
        bottomNode.buildAncestors();

        calculateSlotSize();
        updateETs();

        bottomNode.getUpLen();
        topNode.getDownLen();

        RFT = Math.max(RFT, topNode.getDownLen());

        OccupationDiagram od = new OccupationDiagram(RFT);
        Iterator iter = nodes.values().iterator();
        while (iter.hasNext()) {
            Node nextn = (Node) iter.next();
            nextn.init();
            if (nextn.isTop() || nextn.isBottom()) continue;
            nextn.olb = nextn.lb = nextn.getUpLen();
            nextn.orb = nextn.rb = RFT - nextn.getDownLen();
            nextn.tempST = -1;
            nextn.tempFT = -1;
            nextn.stacked = false;
            od.add(nextn);
        }
        topNode.olb = topNode.lb = 0;
        topNode.orb = topNode.rb = 0;
        topNode.tempST = 0;
        topNode.tempFT = 0;
        bottomNode.olb = bottomNode.lb = RFT;
        bottomNode.orb = bottomNode.rb = RFT;
        bottomNode.tempST = RFT;
        bottomNode.tempFT = RFT;

        return od.stack(false);
    }

    private int DSC() {
        topNode.getDownLen();
        bottomNode.getUpLen();

        int totalNodes = nodes.size();
        long[] clusters = new long[totalNodes + 2];
        for (int i = 0; i < clusters.length; i++) clusters[i] = 0;
        int ccnt = 0;

        LinkedList freeTasks = new LinkedList();
        freeTasks.add(topNode);

        while (freeTasks.size() > 0) {
            long max = -1;
            Node ft = null;
            for (int i = 0; i < freeTasks.size(); i++) {
                Node cn = (Node) freeTasks.get(i);
                long prio = cn.tlevel + cn.getDownLen();
                if (prio > max) {
                    max = prio;
                    ft = cn;
                }
            }
            freeTasks.remove(ft);

            LinkedList parents = ft.getIn();
            long otl = 0;
            long temp = 0;
            for (int i = 0; i < parents.size(); i++) {
                Edge ce = (Edge) parents.get(i);
                Node p = (Node) ce.getFrom();
                long newtl = p.tlevel + p.evalWeight() + ce.getCost();
                if (clusters[p.cluster] <= ft.tlevel) {
                    otl = Math.max(otl, temp);
                    ft.tlevel = clusters[p.cluster];
                    ft.cluster = p.cluster;
                    temp = newtl;
                } else {
                    otl = Math.max(otl, newtl);
                }
            }
            if (ft.cluster < 0) {
                ft.cluster = ccnt;
                clusters[ccnt] = ft.tlevel + ft.evalWeight();
                ccnt++;
            } else {
                ft.tlevel = Math.max(otl, clusters[ft.cluster]);
                clusters[ft.cluster] = ft.tlevel + ft.evalWeight();
            }

            ft.examined = true;

            LinkedList childs = ft.getOut();
            for (int i = 0; i < childs.size(); i++) {
                Edge ce = (Edge) childs.get(i);
                Node c = (Node) ce.getTo();
                c.tlevel = Math.max(c.tlevel, ft.tlevel + ft.evalWeight() + ce.getCost());
                if (c.isFree() && !freeTasks.contains(c)) {
                    freeTasks.add(c);
                }
            }
        }

        return ccnt;
    }

    private int IterHEFT() {
        topNode.getDownLen();
        RFT = Math.max(RFT, bottomNode.getUpLen());

        int lb = (int) (totalET / RFT);
        int i = lb;

        int tasks = nodes.size();
        for (i = lb; i <= tasks; i++) { // i = the number of hosts
            long makespan = HEFT(i);
            if (RFT >= makespan) {
                break;
            }
        }

        return i;
    }

    // HEFT algorithm for homogeneous resources
    private long HEFT(int size) {
        // clean up edges and nodes
        Iterator iter = edges.iterator();
        while (iter.hasNext()) {
            Edge next = (Edge) iter.next();
            next.init();
        }

        iter = nodes.values().iterator();
        while (iter.hasNext()) {
            Node nextn = (Node) iter.next();
            nextn.st = 0;
        }

        long[] resTable = new long[size]; // time table for 'size' resources
        for (int i = 0; i < size; i++) resTable[i] = 0;
        LinkedList readySet = new LinkedList(); // queue of tasks whose all parents are finished

        topNode.initOut(true, 0);
        readySet.add(topNode);

        while (readySet.size() > 0) {
            Node cur = (Node) readySet.removeFirst();

            // schedule
            long min = Long.MAX_VALUE;
            long st = cur.st; // startable time
            long et = 0; // end time
            int target = 0;
            boolean sched = false;
            for (int i = 0; i < size; i++) {
                if (resTable[i] <= st) { // if resource(i) is available at the time st,
                    // schedule the task on it
                    resTable[i] = st + cur.evalWeight();
                    sched = true;
                    et = resTable[i];
                    break;
                }
                if (resTable[i] < min) { // find the fastest time when a resource become free
                    min = resTable[i];
                    target = i;
                }
            }
            if (!sched) { // schedule on the freed resource
                resTable[target] += cur.evalWeight();
                et = resTable[target];
            }
            //		System.out.println(cur.getID()+" ends at "+et);
            cur.initOut(true, et); // notify finish time to child tasks

            Iterator ite = cur.getOut().iterator();
            while (ite.hasNext()) {
                Edge nex = (Edge) ite.next();
                Node no = nex.getTo();
                if (readySet.contains(no)) continue;
                if (no.checkIn() && !no.isBottom()) {
                    boolean mid = false;
                    // sorting ready set (based on HEFT algorithm definition)
                    for (int i = 0; i < readySet.size(); i++) {
                        Node tm = (Node) readySet.get(i);
                        if ((tm.getDownLen() + tm.evalWeight())
                                < (no.getDownLen() + no.evalWeight())) {
                            readySet.add(i, no);
                            mid = true;
                            break;
                        }
                    }
                    if (!mid) readySet.add(no);
                    no.initOut(false, 0);
                }
            }
        }

        long max = 0;
        for (int i = 0; i < size; i++) {
            max = Math.max(max, resTable[i]);
        }
        return max;
    }

    /**
     * Estimate the number of processors and return the value.
     *
     * @return Estimated number of processors
     */
    public int estimate() throws RuntimeException {
        int result = -1;

        try {
            readDAX();
        } catch (Exception e) {
            throw new RuntimeException("Invalid DAX file");
        }

        if (this.method.equals("BTS")) {
            result = BTS();
        } else if (this.method.equals("IterHEFT")) {
            result = IterHEFT();
        } else if (this.method.equals("DSC")) {
            result = DSC();
        } else {
            throw new RuntimeException("Invalid estimate method");
        }

        return result;
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println(
                    "Usage:<COMMAND> <DAX file name> <Estimate method> [<Requested finish time>] [<Precision of predicted execution time>]");
            System.out.println(" Estimation method = BTS : IterHEFT : DSC");
            System.out.println(" or = 1 (for BTS) : 2 (for IterHEFT) : 3 (for DSC)");
            return;
        }

        int prec = 1;
        if (args.length > 4) {
            try {
                prec = Integer.parseInt(args[3]);
            } catch (Exception e) {
            }
        }

        long RFT = -1;
        if (args.length > 3) {
            try {
                RFT = Long.parseLong(args[2]);
            } catch (Exception e) {
            }
        }

        Estimator body = new Estimator(args[0], args[1], RFT, prec);

        try {
            int estimate = body.estimate();
            System.out.println(estimate);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
