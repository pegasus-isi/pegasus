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
 * An instance of this class represent an edge of workflow which is data dependencies between two
 * tasks. So this class contains references of two dependent nodes(From node and To node) and
 * information of data between them; file name, data size and data transfer time.
 *
 * @author Eunkyu Byun
 */
public class Edge {

    public static final long DEFAULT_SIZE = 0;
    public static final long DEFAULT_BPS = 1;
    public static final long DEFAULT_LATENCY = 0;

    Node fromNode;
    Node toNode;

    long cost;

    String fileName;
    long fileSize;

    /**
     * Constructor
     *
     * @param from One of nodes this edge connects which generate the data
     * @param to Another node this edge connects which get the data
     * @param fileName the stored name of data
     * @param fileSize the size of the data, DTT is calculated according to BPS and Latency
     */
    public Edge(Node from, Node to, String fileName, long fileSize) {
        this.fromNode = from;
        this.toNode = to;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.cost = (fileSize / DEFAULT_BPS) + DEFAULT_LATENCY;
    }

    /** initiate bit and completion time variable used bt HEFT algorithm */
    public void init() {
        this.complete = false;
        this.compTime = 0;
    }

    /** return the 'from' task */
    public Node getFrom() {
        return fromNode;
    }

    /** return the 'to' task */
    public Node getTo() {
        return toNode;
    }

    /**
     * set the 'from' task
     *
     * @param e the from task to be set
     */
    public void setFrom(Node e) {
        this.fromNode = e;
    }

    /**
     * set the 'to' task
     *
     * @param e the to task to be set
     */
    public void setTo(Node e) {
        this.toNode = e;
    }

    public String getID() {
        return fileName;
    }

    public long getCost(long bandwidth, long latency) {
        return (fileSize / bandwidth) + latency;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long c) {
        this.cost = c;
    }

    public void print() {
        System.out.println(
                "Edge_"
                        + fileName
                        + "("
                        + fileSize
                        + "KB) "
                        + fromNode.getID()
                        + " -> "
                        + toNode.getID());
    }

    public boolean complete = false;
    public long compTime = 0;

    public boolean deleted = false;
}
