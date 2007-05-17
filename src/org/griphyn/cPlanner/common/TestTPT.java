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
package org.griphyn.cPlanner.common;

import org.griphyn.cPlanner.common.TPT;
/**
 * Client for testing  the TPT class.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class TestTPT {
    public static void main(String[] args) {
        TPT tpt = new TPT();
        //build the TPT map
        tpt.buildState();
        //print it out
        tpt.print();

        System.out.println(tpt.stageInThirdParty("X"));
        System.out.println(tpt.interThirdParty("X"));
        System.out.println(tpt.stageOutThirdParty("X"));


        String url = "file:///gpfs-wan/karan.txt";
        System.out.println("Hostname is " + Utility.getHostName(url));
        try{
            System.out.println("Java hostanme is " +
                               new java.net.URL(url).getHost());
        }catch(Exception e){}

        System.out.println("Mount point is " + Utility.getAbsolutePath(url));
    }

}