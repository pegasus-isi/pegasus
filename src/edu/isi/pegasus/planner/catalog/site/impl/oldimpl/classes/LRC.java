/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes;

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
 * @version $Revision$
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
