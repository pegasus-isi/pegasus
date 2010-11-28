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


package edu.isi.pegasus.planner.selector.transformation;

import edu.isi.pegasus.planner.selector.TransformationSelector;

import java.util.LinkedList;
import java.util.List;

/**
 * This implementation of the Selector select a transformation from a list in a round robin fashion.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class RoundRobin
    extends TransformationSelector {

    private LinkedList tclist;

    public RoundRobin() {

    }

    /**
     *
     * @param tcentries List
     * @return TransformationCatalogEntry
     */
    public List getTCEntry( List tcentries ) {

        return null;
    }
}
