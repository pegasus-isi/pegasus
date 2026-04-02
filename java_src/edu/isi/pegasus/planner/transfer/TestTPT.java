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
package edu.isi.pegasus.planner.transfer;

import edu.isi.pegasus.common.util.PegasusURL;

/**
 * Client for testing the TPT class.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TestTPT {
    public static void main(String[] args) {
        TPT tpt = new TPT();
        // build the TPT map
        tpt.buildState();
        // print it out
        tpt.print();

        System.out.println(tpt.stageInThirdParty("X"));
        System.out.println(tpt.interThirdParty("X"));
        System.out.println(tpt.stageOutThirdParty("X"));

        String url = "file:///gpfs-wan/karan.txt";
        PegasusURL u = new PegasusURL(url);
        // System.out.println("Hostname is " + Utility.getHostName(url));
        System.out.println("Hostname is " + u.getHost());
        try {
            System.out.println("Java hostanme is " + new java.net.URL(url).getHost());
        } catch (Exception e) {
        }

        System.out.println("Mount point is " + u.getPath());
    }
}
