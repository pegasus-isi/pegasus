package edu.isi.pegasus.planner.dax;

/**
 *
 * @author gmehta
 */
public class DAG extends AbstractJob {

    public DAG(String id, String dagname){
          this(id,dagname,null);
    }

    public DAG(String id, String dagname,String label){
        super();
       checkID(id);
       // to decide whether to exit. Currently just logging error and proceeding.
       mId=id;
       mName=dagname;
       mNodeLabel=label;
    }



}
