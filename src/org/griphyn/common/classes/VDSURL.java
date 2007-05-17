package org.griphyn.common.classes;


import java.net.URL;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: </p>
 * <p>Company: </p>
 * @author Karan Vahi
 * @version $Revision$
 */

public class VDSURL {
    public VDSURL() {
    }
    public static void main(String[] args) {
        VDSURL VDSURL1 = new VDSURL();

        try{
            URL u = new URL("gsiftp://sukhna.isi.edu");
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }

}