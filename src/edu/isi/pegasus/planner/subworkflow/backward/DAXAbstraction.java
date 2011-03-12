/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.subworkflow.backward;

import edu.isi.pegasus.planner.dax.ADAG;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author wchen
 */
public class DAXAbstraction
{
    public ADAG adag;
    public double size;
    public int degree;
    public Map depthMap;
    public double limit;
    public DAXAbstraction (ADAG adag)
    {
        this.adag = adag;
        this.size = 0.0;
        this.degree = 0;
        this.limit = 0.0;
        this.depthMap = new HashMap<Integer, ArrayList>();
    }
}