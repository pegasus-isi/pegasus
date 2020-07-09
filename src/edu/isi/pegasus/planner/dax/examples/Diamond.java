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

/**
 * An example class to highlight how to use the JAVA DAX API to generate a diamond DAX.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 */
public class Diamond {

    public ADAG generate(String site_handle, String pegasus_location) throws Exception {

        java.io.File cwdFile = new java.io.File(".");
        String cwd = cwdFile.getCanonicalPath();

        ADAG dax = new ADAG("diamond");
        dax.addNotification(
                Invoke.WHEN.start, "/pegasus/libexec/notification/email -t notify@example.com");
        dax.addNotification(
                Invoke.WHEN.at_end, "/pegasus/libexec/notification/email -t notify@example.com");
        dax.addMetaData("name", "diamond");
        dax.addMetaData("createdBy", "Karan Vahi");

        File fa = new File("f.a");
        fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
        fa.addMetaData("size", "1024");
        dax.addFile(fa);

        File fb1 = new File("f.b1");
        File fb2 = new File("f.b2");
        File fc1 = new File("f.c1");
        File fc2 = new File("f.c2");
        File fd = new File("f.d");
        fd.setRegister(true);

        Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
        preprocess.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(true);
        preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);
        preprocess.addMetaData("size", "2048");

        Executable findrange = new Executable("pegasus", "findrange", "4.0");
        findrange.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        findrange.setInstalled(true);
        findrange.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);

        Executable analyze = new Executable("pegasus", "analyze", "4.0");
        analyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        analyze.setInstalled(true);
        analyze.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);

        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

        // Add a preprocess job
        Job j1 = new Job("j1", "pegasus", "preprocess", "4.0");
        j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1);
        j1.addArgument(" ").addArgument(fb2);
        j1.addMetaData("time", "60");
        j1.uses(fa, File.LINK.INPUT);
        j1.uses(fb1, File.LINK.OUTPUT);
        j1.uses(fb2, File.LINK.OUTPUT);
        j1.addNotification(
                Invoke.WHEN.start, "/pegasus/libexec/notification/email -t notify@example.com");
        j1.addNotification(
                Invoke.WHEN.at_end, "/pegasus/libexec/notification/email -t notify@example.com");
        dax.addJob(j1);

        // Add left Findrange job
        Job j2 = new Job("j2", "pegasus", "findrange", "4.0");
        j2.addArgument("-a findrange -T 60 -i ").addArgument(fb1);
        j2.addArgument("-o ").addArgument(fc1);
        j2.addMetaData("time", "60");
        j2.uses(fb1, File.LINK.INPUT);
        j2.uses(fc1, File.LINK.OUTPUT);
        j2.addNotification(
                Invoke.WHEN.start, "/pegasus/libexec/notification/email -t notify@example.com");
        j2.addNotification(
                Invoke.WHEN.at_end, "/pegasus/libexec/notification/email -t notify@example.com");
        dax.addJob(j2);

        // Add right Findrange job
        Job j3 = new Job("j3", "pegasus", "findrange", "4.0");
        j3.addArgument("-a findrange -T 60 -i ").addArgument(fb2);
        j3.addArgument("-o ").addArgument(fc2);
        j3.addMetaData("time", "60");
        j3.uses(fb2, File.LINK.INPUT);
        j3.uses(fc2, File.LINK.OUTPUT);
        j3.addNotification(
                Invoke.WHEN.start, "/pegasus/libexec/notification/email -t notify@example.com");
        j3.addNotification(
                Invoke.WHEN.at_end, "/pegasus/libexec/notification/email -t notify@example.com");
        dax.addJob(j3);

        // Add analyze job
        Job j4 = new Job("j4", "pegasus", "analyze", "4.0");
        j4.addArgument("-a analyze -T 60 -i ").addArgument(fc1);
        j4.addArgument(" ").addArgument(fc2);
        j4.addArgument("-o ").addArgument(fd);
        j4.addMetaData("time", "60");
        j4.uses(fc1, File.LINK.INPUT);
        j4.uses(fc2, File.LINK.INPUT);
        j4.uses(fd, File.LINK.OUTPUT);
        j4.addNotification(
                Invoke.WHEN.start, "/pegasus/libexec/notification/email -t notify@example.com");
        j4.addNotification(
                Invoke.WHEN.at_end, "/pegasus/libexec/notification/email -t notify@example.com");
        dax.addJob(j4);

        dax.addDependency("j1", "j2");
        dax.addDependency("j1", "j3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
        return dax;
    }

    /**
     * Create an example DIAMOND DAX
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java GenerateDiamondDAX  <pegasus_location> ");
            System.exit(1);
        }

        try {
            Diamond diamond = new Diamond();
            String pegasusHome = args[0];
            String site = "TestCluster";
            ADAG dag = diamond.generate(site, pegasusHome);
            dag.writeToSTDOUT();
            // generate(args[0], args[1]).writeTo(args[2]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
