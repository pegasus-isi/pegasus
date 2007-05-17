package org.griphyn.cPlanner.namespace;

/**
 * Test Class for namespaces.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class TestNamespace {
    public TestNamespace() {
    }

    public static void main(String[] args){
        Condor c = new Condor();
        VDS    v = new VDS();
        System.out.println(v.namespaceName() + " \n" + v.deprecatedTable());
        System.out.println(c.namespaceName() + " \n" + c.deprecatedTable());
    }
}