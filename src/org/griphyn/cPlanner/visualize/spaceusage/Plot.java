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


package org.griphyn.cPlanner.visualize.spaceusage;

import java.io.IOException;

import java.util.List;

/**
 * A plot interface that allows us to plot the SpaceUsage in different
 * formats.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public interface Plot {

    /**
     * The version of this API
     */
    public static final String VERSION = "1.3";


    /**
     * Initializer method.
     *
     * @param directory  the directory where the plots need to be generated.
     * @param basename   the basename for the files that are generated.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory , String basename, boolean useStatInfo );

    /**
     * Plot out the space usage.
     *
     * @param su          the SpaceUsage.
     * @param u   the size unit.
     * @param timeUnits   the time unit.
     *
     * @return List of file pathnames for the files that are written out.
     *
     * @exception IOException in case of unable to write to the file.
     */
    public List plot( SpaceUsage su, char u , String timeUnits) throws IOException;

}
