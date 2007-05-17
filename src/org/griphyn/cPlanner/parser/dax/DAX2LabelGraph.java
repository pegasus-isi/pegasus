/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.parser.dax;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.partitioner.graph.Bag;
import org.griphyn.cPlanner.partitioner.graph.LabelBag;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import org.griphyn.cPlanner.common.PegasusProperties;

/**
 * The callback, that ends up building a label graph. With each GraphNode a label
 * is associated. The label can be associated via a profile in the VDS namespace
 * with the jobs in the DAX. The key for the profile  can either be set via the
 * constructor, or a default key is used.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class DAX2LabelGraph extends DAX2Graph {

    /**
     * The default key for the VDS namespace profile, that is used in case none
     * is specified by the user.
     */
    public static final String DEFAULT_LABEL_KEY = "label";

    /**
     * The default label value that is to be applied, in case the profile is
     * not associated with the job.
     */
    //public static  String DEFAULT_LABEL_VALUE = "default";

    /**
     * The profile key that is used for the labelling.
     */
    private String mLabelKey;

    /**
     * Sets the default label value that is to be used if the profile is not
     * associated with the job.
     *
     * @param value   the value to be associated.
     */
//    public static void setLabelValue(String value){
//        DEFAULT_LABEL_VALUE = value;
//    }


    /**
     * The overloaded constructor.
     *
     * @param properties  the properties passed to the planner.
     * @param dax         the path to the DAX file.
     */
    public DAX2LabelGraph( PegasusProperties properties, String dax ){
        super( properties, dax );
        mProps = properties;
        this.setLabelKey( DEFAULT_LABEL_KEY );
    }

    /**
     * Set the profile key that is to be used to pick up the labels.
     * Sets the profile key to the value specified. If value passed is
     * null, then is set to the default label key.
     *
     * @param key   the VDS profile key that is to be used.
     *
     * @see #DEFAULT_LABEL_KEY
     */
    public void setLabelKey( String key ){
        mLabelKey = ( key == null )? this.DEFAULT_LABEL_KEY : key;
        LabelBag.setLabelKey(mLabelKey);
    }


    /**
     * This constructs a graph node for the job and ends up storing it in the
     * internal map. In addition assigns a label with the node. The label is
     * is the value of a profile in the VDS namespace. The name of the profile
     * can
     *
     * @param job  the job that was parsed.
     */
    public void cbJob(SubInfo job) {
        GraphNode gn = new GraphNode(job.logicalId,job.logicalName);
        String label = (String)job.vdsNS.get(mLabelKey);
//        label = (label == null)? DEFAULT_LABEL_VALUE : label;
        Bag bag = new LabelBag();
        bag.add(mLabelKey,label);
        gn.setBag(bag);
        put(job.logicalId,gn);
    }

    /**
     * Callback to signal that traversal of the DAX is complete. At this point a
     * dummy root node is added to the graph, that is the parents to all the root
     * nodes in the existing DAX. This method in additions adds the default label
     * to the root.
     */
    public void cbDone() {
        super.cbDone();
        Bag bag = new LabelBag();
//        bag.add(mLabelKey,DEFAULT_LABEL_VALUE);
        mRoot.setBag(bag);
    }

}