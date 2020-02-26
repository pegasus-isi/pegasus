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

/**
 * This is the test program for the Separator class.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.classes.Definition
 */
public class Separator2Test {
    public static void x(String what, int len) {
        String s = (what == null ? "(null)" : ("\"" + what + "\""));
        System.out.print(s);
        for (int i = s.length(); i < len; ++i) System.out.print(' ');
    }

    public static void show(String what) {
        x(what, 16);
        System.out.print(" => [");
        try {
            String[] x = Separator.splitFQDI(what);
            for (int i = 0; i < x.length; ++i) {
                System.out.print(Integer.toString(i) + ':');
                x(x[i], 8);
                if (i < x.length - 1) System.out.print(", ");
            }
        } catch (IllegalArgumentException iae) {
            System.out.print(iae.getMessage());
        }
        System.out.println(']');
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            for (int i = 0; i < args.length; ++i) show(args[i]);
        } else {
            show("me");
            show("::me");
            show("::me:");
            show("me:");
            show("me:too");
            show("test::me");
            show("test::me:");

            show("::me:");
            show("::me:too");
            show("test::me:too");
            show("::me:too");

            show("::me::");
            show("::me:too:");
            show(":::");
            show("test:::");
            show(":::too");
        }
    }
}
