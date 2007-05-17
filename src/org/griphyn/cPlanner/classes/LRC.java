/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.classes;

/**
 * This is a data class that is used to store information about a
 * local replica catalog, that is associated with a site in the pool configuration
 * catalog.
 * <p>
 * The various attributes that can be associated with the the server are
 * displayed in the following table.
 *
 * <p>
 * <table border="1">
 * <tr align="left"><th>Attribute Name</th><th>Attribute Description</th></tr>
 * <tr align="left"><th>url</th>
 *  <td>the url string pointing to local replica catalog.</td>
 * </tr>
 * </table>
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision: 1.2 $
 */
public class LRC {

    /**
     * The contact string to the lrc e.g rls://sukhna.isi.edu .
     */
    private String mLRCURL;

    /**
     * Constructor for the class.
     *
     * @param url the url for the local replica catalog.
     */
    public LRC(String url){
        mLRCURL = url==null ? null : new String(url);
    }

    /**
     * Returns the LRC url associated with a pool.
     *
     * @return the lrc url.
     */
    public String getURL(){
        return mLRCURL;
    }

    /**
     * Sets the url of the LRC associated with the object.
     *
     * @param url the url string.
     */
    public void setURL(String url){
        mLRCURL = url;
    }

    /**
     * Returns the textual description of the  contents of <code>LRC</code>
     * object in the multiline format.
     *
     * @return the textual description in multiline format.
     */
    public String toMultiLine() {
        return this.toString();
    }
    /**
     * Returns the textual description of the  contents of <code>LRC</code> object.
     *
     * @return the textual description.
     */
    public String toString(){
        String output="lrc \""+mLRCURL+"\"";
        return output;
    }

    /**
     * Returns the XML description of the  contents of <code>LRC</code>
     * object.
     *
     * @return the xml description.
     */
    public String toXML(){
        String output="<lrc url=\""+mLRCURL+"\" />";
        return output;
    }

}
