import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dax.Filename;
import org.griphyn.vdl.dax.Job;
import org.griphyn.vdl.dax.PseudoText;

import java.io.*;
import java.util.*;

public class RosettaDAX{


    public RosettaDAX(){
    }

    public void constructDAX(String daxfile){

        try{
            // construct a dax object 
            ADAG dax = new ADAG(1, 1, "rosetta");
    
            HashSet<Filename> inputs = new HashSet<Filename>();
            recursiveAddToFileNameCollection(inputs, "minirosetta_database");
            inputs.add(new Filename("design.resfile", LFN.INPUT));
            inputs.add(new Filename("repack.resfile", LFN.INPUT));

            File pdbDir = new File("pdbs/");
            String pdbs[] = pdbDir.list();
            for (int i = 0; i < pdbs.length; i++) {
                File pdb = new File("pdbs/" + pdbs[i]);
                if (pdb.isFile()) {
                    Job j = createJobFromPDB(pdb, inputs);
                    dax.addJob(j);
                }
            }

            //write DAX to file
            FileWriter daxFw = new FileWriter(daxfile);
            dax.toXML(daxFw, "", null);
            daxFw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void recursiveAddToFileNameCollection(HashSet<Filename> set, String dir) {
        try {
            File d = new File(dir);
            String items[] = d.list();
            for (int i = 0; i < items.length; i++) {
                if (items[i].substring(0,1).equals(".")) {
                    continue;
                }
                File f = new File(dir + "/" + items[i]);
                if (f.isFile()) {
                    Filename fileName = new Filename(f.getPath(), LFN.INPUT);
                    fileName.setRegister(false);
                    set.add(fileName);
                }
                else {
                    recursiveAddToFileNameCollection(set, f.getPath());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private Job createJobFromPDB(File pdb, HashSet<Filename> inputs) {

        Job job = null;
    
        try {
            String id = pdb.getName();
            id = id.replaceAll(".pdb", "");

            job = new Job("rosetta", "rosetta.exe", "1.0", id);

            // general rosetta inputs (database, design, ...)
            job.setUses(inputs);

            // add the arguments to the job
            job.addArgument(new PseudoText(" -in:file:s "));
            job.addArgument(new Filename(pdb.getPath()));
            job.addArgument(new PseudoText(" -out:prefix " + pdb.getName() + "."));
            job.addArgument(new PseudoText(" -database ./minirosetta_database"));
            job.addArgument(new PseudoText(" -linmem_ig 10"));
            job.addArgument(new PseudoText(" -nstruct 1"));
            job.addArgument(new PseudoText(" -pert_num 2"));
            job.addArgument(new PseudoText(" -inner_num 1"));
            job.addArgument(new PseudoText(" -jd2::ntrials 1"));

            // input pdb
            Filename f = new Filename(pdb.getPath(), LFN.INPUT);
            f.setRegister(false);
            job.addUses(f);

            // outputs
            f = new Filename(pdb.getName() + ".score.sc", LFN.OUTPUT);
            f.setRegister(false);
            job.addUses(f);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return job;
    }


    /**
     * Usage : RosettaDAX daxfile
     *
     * @param args the arguments passed
     */
    public static void main(String[] args) {
        RosettaDAX daxgen = new RosettaDAX();
        if (args.length == 1) {
            daxgen.constructDAX(args[0]);

        } else {
            System.out.println("Usage: CreateDAX <outputdaxfile>");
        }
    }

}

