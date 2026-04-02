/**
 * Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.refiner.cleanup.constraint;

import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Set;

/**
 * @author Sudarshan Srinivasan
 * @author Rafael Ferreira da Silva
 */
public class FloatingFile {

    public final Set<GraphNode> dependencies;
    public final PegasusFile file;

    public FloatingFile(Set<GraphNode> dependencies, PegasusFile file) {
        this.dependencies = dependencies;
        this.file = file;
    }
}
