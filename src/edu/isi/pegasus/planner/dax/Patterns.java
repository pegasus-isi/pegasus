/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.dax;

import java.util.regex.*;

/**
 *
 * @author gmehta
 */
public class Patterns {

    private static Pattern mVersionPattern= Pattern.compile("^\\d+(\\.\\d+(\\.\\d+)?)?$");
    private static Pattern mNodeIdPattern = Pattern.compile("^[A-Za-z0-9][-A-Za-z0-9_]*$");

    public static boolean isValid(Pattern p, String s){
         return p.matcher(s).matches();
    }

    public static boolean isVersionValid(String version){
        return isValid(mVersionPattern,version);
    }

    public static boolean isNodeIdValid(String nodeid){
        return isValid(mNodeIdPattern,nodeid);
    }


}
