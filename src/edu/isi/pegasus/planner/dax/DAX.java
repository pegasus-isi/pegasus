package edu.isi.pegasus.planner.dax;

/**
 *
 * @author gmehta
 */
public class DAX extends AbstractJob {

    public DAX(String id, String daxname){
          this(id,daxname,null);
    }

    public DAX(String id, String daxname,String label){
        super();
       checkID(id);
       // to decide whether to exit. Currently just logging error and proceeding.
       mId=id;
       mName=daxname;
       mNodeLabel=label;
    }


}
