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
package edu.isi.pegasus.planner.dax.examples;

import edu.isi.pegasus.planner.dax.*;

public class Pipeline {

    public static void main(String[] args) {
        ADAG dax = new ADAG("pipeline");

        File webpage = new File("pegasus.html");

        Job curl = new Job("ID1", "curlwd");
        curl.addArgument("-o").addArgument(webpage);
        curl.addArgument("http://pegasus.isi.edu");
        curl.uses(webpage, File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
        dax.addJob(curl);

        File count = new File("count.txt");
        File stderr = new File("job_stderr.txt");
        File stdin = new File("job_stdin.txt");

        Job wc = new Job("ID2", "wc");
        wc.addArgument("-l").addArgument(webpage);
        wc.setStdout(count);

        /*
        wc.setStderr(stderr);
        wc.setStdin(stdin);
        wc.uses(count, File.LINK.OUTPUT, File.TRANSFER.FALSE, true);
        wc.uses(stderr, File.LINK.OUTPUT, File.TRANSFER.FALSE, true);
        wc.uses(stdin, File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
        */

        wc.uses(webpage, File.LINK.INPUT);
        dax.addJob(wc);

        dax.addDependency(curl, wc);

        dax.writeToSTDOUT();
    }
}
