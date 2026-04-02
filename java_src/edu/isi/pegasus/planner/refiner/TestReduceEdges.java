/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;

/** @author vahi */
public class TestReduceEdges {

    public TestReduceEdges() {}

    public ADag createTest1() {
        ADag a = new ADag();

        Job aa = new Job();
        aa.setName("a");
        aa.setJobType(Job.COMPUTE_JOB);
        Job b = new Job();
        b.setName("b");
        b.setJobType(Job.COMPUTE_JOB);
        Job c = new Job();
        c.setName("c");
        c.setJobType(Job.COMPUTE_JOB);
        Job d = new Job();
        d.setName("d");
        d.setJobType(Job.COMPUTE_JOB);
        Job e = new Job();
        e.setName("e");
        e.setJobType(Job.COMPUTE_JOB);

        a.addNewRelation("a", "b");
        a.addNewRelation("a", "c");

        a.addNewRelation("b", "d");
        a.addNewRelation("c", "d");

        a.addNewRelation("b", "e");
        a.addNewRelation("d", "e");
        a.addNewRelation("a", "e");
        a.addNewRelation("c", "e");

        a.add(aa);
        a.add(b);
        a.add(c);
        a.add(d);
        a.add(e);

        return a;
    }

    public ADag createTest2() {
        ADag ad = new ADag();

        Job a = new Job();
        a.setName("a");
        a.setJobType(Job.COMPUTE_JOB);
        Job b = new Job();
        b.setName("b");
        b.setJobType(Job.COMPUTE_JOB);
        Job c = new Job();
        c.setName("c");
        c.setJobType(Job.COMPUTE_JOB);
        Job d = new Job();
        d.setName("d");
        d.setJobType(Job.COMPUTE_JOB);
        Job e = new Job();
        e.setName("e");
        e.setJobType(Job.COMPUTE_JOB);
        Job f = new Job();
        f.setName("f");
        f.setJobType(Job.COMPUTE_JOB);
        Job g = new Job();
        g.setName("g");
        g.setJobType(Job.COMPUTE_JOB);
        Job h = new Job();
        h.setName("h");
        h.setJobType(Job.COMPUTE_JOB);
        Job i = new Job();
        i.setName("i");
        i.setJobType(Job.COMPUTE_JOB);
        Job j = new Job();
        j.setName("j");
        j.setJobType(Job.COMPUTE_JOB);
        Job k = new Job();
        k.setName("k");
        k.setJobType(Job.COMPUTE_JOB);

        ad.addNewRelation("a", "b");
        ad.addNewRelation("a", "g");

        ad.addNewRelation("b", "c");
        ad.addNewRelation("b", "d");
        ad.addNewRelation("b", "f");

        ad.addNewRelation("c", "f");

        ad.addNewRelation("d", "e");

        ad.addNewRelation("e", "f");

        ad.addNewRelation("g", "d");
        ad.addNewRelation("g", "h");
        ad.addNewRelation("h", "i");
        ad.addNewRelation("i", "j");
        ad.addNewRelation("j", "k");
        ad.addNewRelation("k", "f");

        ad.add(a);
        ad.add(b);
        ad.add(c);
        ad.add(d);
        ad.add(e);
        ad.add(f);
        ad.add(g);
        ad.add(h);
        ad.add(i);
        ad.add(j);
        ad.add(k);

        return ad;
    }

    public static void main(String[] args) {

        TestReduceEdges tr = new TestReduceEdges();
        ReduceEdges re = new ReduceEdges();

        System.out.println("********* Test 1  **********");
        re.reduce(tr.createTest1());
        System.out.println("****************************");

        System.out.println("********* Test 2  **********");
        re.reduce(tr.createTest2());
        System.out.println("****************************");
    }
}
