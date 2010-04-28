/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package edu.isi.pegasus.planner.code.gridstart;

import java.io.File;

/**
 * A wrapper around the Exitcode, that takes care of backing up output and
 * error files.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class ExitPOST extends VDSPOSTScript {

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "exitpost";



    /**
     * The default constructor.
     */
    public ExitPOST(){
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
        sb.append(File.separator).append("exitpost");

        return sb.toString();
    }


}
