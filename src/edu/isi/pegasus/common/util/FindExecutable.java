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
package edu.isi.pegasus.common.util;

import java.io.File;

/**
 * A convenience class that allows us to determine the path to an executable
 *
 * @author Jens Voeckler
 * @author Karan Vahi
 * @version $Revision$
 */
public class FindExecutable {

    /**
     * Finds the path to an executable of a given name , based on the value of PATH environment
     * variable and variable PEGASUS_BIN_DIR .
     *
     * @param name the name of the executable to search for.
     * @return the File object corresponding to the executable if found, else null
     */
    public static File findExec(String name) {
        // pick up value of PEGASUS_BIN_DIR
        // the wrapper script sets value of PEGASUS_BIN_DIR as
        // java property pegasus.home.bindir .
        return FindExecutable.findExec(System.getProperty("pegasus.home.bindir"), name);
    }

    /**
     * Finds the path to an executable of a given name , based on the value of PATH environment
     * variable and any additional directory that is preferred to be searched for
     *
     * @param directory the directory to search the executable in, before searching for in the PATH.
     * @param name the name of the executable to search for.
     * @return the File object corresponding to the executable if found, else null
     */
    public static File findExec(String directory, String name) {
        if (name == null) {
            return null;
        }

        String path = System.getenv("PATH");

        if (directory != null) {
            path = (path == null) ? directory : directory + ":" + path;
        }

        if (path == null) {
            return null;
        }

        String[] list = path.split(":");

        for (int i = 0; i < list.length; ++i) {
            File result = new File(list[i], name);
            if (result.isFile() && result.canExecute()) {
                return result;
            }
        }

        return null;
    }

    /**
     * Test function for the class
     *
     * @param args
     */
    public static void main(String args[]) {
        for (int i = 0; i < args.length; ++i) {
            File f = FindExecutable.findExec(args[i]);
            if (f == null) {
                System.out.println(args[i] + " not found");
            } else {
                System.out.println(args[i] + " -> " + f);
            }
        }
    }
}
