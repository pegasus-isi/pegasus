/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow.backward;

import edu.isi.pegasus.planner.classes.ADag;
import java.util.Map;

/**
 *
 * @author wchen
 */
public interface BackwardPartitionerImplementation {

    public void run();

    public Map getDAXMap();

    public Map getJob2DAX();

    public ADag getDag();

}
