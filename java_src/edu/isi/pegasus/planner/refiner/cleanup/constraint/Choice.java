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

import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.List;
import java.util.Map;

/**
 * @author Sudarshan Srinivasan
 * @author Rafael Ferreira da Silva
 */
public class Choice {

    public final long intermediateSpaceRequirement;
    public final long balance;
    public final List<GraphNode> listOfJobs;
    public final Map<Long, List<FloatingFile>> floatingFiles;

    public Choice(
            long intermediateSpaceRequirement,
            long balance,
            List<GraphNode> listOfJobs,
            Map<Long, List<FloatingFile>> floatingFiles) {
        this.intermediateSpaceRequirement = intermediateSpaceRequirement;
        this.balance = balance;
        this.listOfJobs = listOfJobs;
        this.floatingFiles = floatingFiles;
    }

    @Override
    public String toString() {
        StringBuilder build = new StringBuilder(128);
        build.append("Choice{intermediateSpaceRequirement=")
                .append(intermediateSpaceRequirement)
                .append(", balance=")
                .append(balance)
                .append(", listOfJobs={");
        for (GraphNode graphNode : listOfJobs) {
            build.append(graphNode.getID()).append(',');
        }
        build.replace(build.length() - 1, build.length(), "}, floatingFiles={");
        if (floatingFiles.isEmpty()) {
            build.append("}}");
        } else {
            for (Map.Entry<Long, List<FloatingFile>> entry : floatingFiles.entrySet()) {
                for (FloatingFile f : entry.getValue()) {
                    build.append(f.file.getLFN()).append(':').append(entry.getKey()).append(',');
                }
            }
            build.replace(build.length() - 1, build.length(), "}}");
        }
        return build.toString();
    }
}
