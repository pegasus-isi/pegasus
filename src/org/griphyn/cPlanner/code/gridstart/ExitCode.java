/**
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

package org.griphyn.cPlanner.code.gridstart;

import java.io.File;

/**
 * The exitcode wrapper, that can parse kickstart output's and put them in the
 * database also.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision: 1.1 $
 */

public class ExitCode extends VDSPOSTScript {

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "exitcode";


    /**
     * The default constructor.
     */
    public ExitCode(){
        super();
    }

    /**
     * Returns a short textual description of the implementing class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return this.SHORT_NAME;
    }


    /**
     * Returns the path to exitcode that is to be used on the kickstart
     * output.
     *
     * @return the path to the exitcode script to be invoked.
     */
    public String getDefaultExitCodePath(){
        StringBuffer sb = new StringBuffer();
        sb.append(mProps.getPegasusHome()).append(File.separator).append("bin");
        sb.append(File.separator).append("exitcode");

        return sb.toString();
    }


}