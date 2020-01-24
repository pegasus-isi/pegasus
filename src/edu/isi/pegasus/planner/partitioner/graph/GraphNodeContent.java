/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.partitioner.graph;

/**
 * This inteface defines a common base for all the classes that can reside in a <code>GraphNode
 * </code> object.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public interface GraphNodeContent {
    // an empty interface for grouping purposes

    /** Set a reference back to containing GraphNode */
    public void setGraphNodeReference(GraphNode node);
}
