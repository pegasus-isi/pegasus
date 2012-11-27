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


package edu.isi.pegasus.planner.catalog.site.impl.old.classes;

/**
 * This is a data class to store information about gridftp bandwidths between
 * various sites. The class is no longer used, and is there for compilation
 * purposes.
 *
 * @author Saurabh Khurana
 * @author Karan Vahi
 *
 * @version $Revision$
 */

public class GridFTPBandwidth {

 // private String host_subnet_id = null;
  private String dest_subnet_id = null;
  private String avg_bandwidth_range1 = null;
  private String avg_bandwidth_range2 = null;
  private String avg_bandwidth_range3 = null;
  private String avg_bandwidth_range4 = null;
  private String avg_bandwidth = null;
  private String max_bandwidth = null;
  private String min_bandwidth = null;


  public static final String GRIDFTPBANDWIDTHINFO[] =
      {"dest-subnet","avg-bandwidth-range1","avg-bandwidth-range2",
      "avg-bandwidth-range3","avg-bandwidth-range4","avg-bandwidth",
      "max-bandwidth","min-bandwidth"};

  public static final int DEST_ID = 0;
  public static final int AVG_BW_RANGE1 = 1;
  public static final int AVG_BW_RANGE2 = 2;
  public static final int AVG_BW_RANGE3 = 3;
  public static final int AVG_BW_RANGE4 = 4;
  public static final int AVG_BW = 5;
  public static final int MAX_BW = 6;
  public static final int MIN_BW = 7;

  //constructor
  public GridFTPBandwidth() {
  }

/**
*  This method sets the info about gridftpbw based on the gridftpbwkey
* @param gridftpbwkey the key for indicating the information
* @param gridftpbwvalue is the information
*/
  public void setInfo(int gridftpbwkey, String gridftpbwvalue) throws Exception
  {
    switch (gridftpbwkey)
    {
      case 0 : dest_subnet_id = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 1 : avg_bandwidth_range1 = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 2 : avg_bandwidth_range2 = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 3 : avg_bandwidth_range3 = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 4 : avg_bandwidth_range4 = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 5 : avg_bandwidth = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 6 : max_bandwidth = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;
      case 7 : min_bandwidth = gridftpbwvalue==null ? null : new String(gridftpbwvalue);
        break;

      default : throw new Exception("Wrong gridftpbwkey = "+GRIDFTPBANDWIDTHINFO[gridftpbwkey]+" specified. Gridftpbwkey must be one of the predefined types");
    }
  }

/**
 *  This method gets the info about gridftpbw based on the gridftpbwkey
 * @param gridftpbwkey the key for requesting the information
 * @return String
 */
public String getInfo(int gridftpbwkey) throws Exception
{
  switch (gridftpbwkey)
  {
    case 0 : return dest_subnet_id;
    case 1 : return avg_bandwidth_range1;
    case 2 : return avg_bandwidth_range2;
    case 3 : return avg_bandwidth_range3;
    case 4 : return avg_bandwidth_range4;
    case 5 : return avg_bandwidth;
    case 6 : return max_bandwidth;
    case 7 : return min_bandwidth;

    default : throw new Exception("Wrong gridftpbwkey = "+GRIDFTPBANDWIDTHINFO[gridftpbwkey]+" specified. Gridftpbwkey must be one of the predefined types");
  }
}

public String toString()
{
  String output= "GRIDFTPBANDWIDTH(";
  if(dest_subnet_id!=null)
  {
    output+=" "+GRIDFTPBANDWIDTHINFO[DEST_ID]+"="+dest_subnet_id;
  }
  if(avg_bandwidth_range1!=null)
  {
    output+=" "+GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE1]+"="+avg_bandwidth_range1;
  }
  if(avg_bandwidth_range2 !=null)
  {
    output+=" "+GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE2]+"="+avg_bandwidth_range2;
  }
  if(avg_bandwidth_range3 != null)
  {
    output+=" "+GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE3]+"="+avg_bandwidth_range3;
  }
  if(avg_bandwidth_range4 != null)
  {
   output+=" "+GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE4]+"="+avg_bandwidth_range4;
  }
  if(avg_bandwidth !=null)
  {
    output+=" "+GRIDFTPBANDWIDTHINFO[AVG_BW]+"="+avg_bandwidth;
  }
  if(max_bandwidth !=null)
  {
    output += " " + GRIDFTPBANDWIDTHINFO[MAX_BW] + "=" + max_bandwidth;
  }
  if (min_bandwidth != null) {
    output += " " + GRIDFTPBANDWIDTHINFO[MIN_BW] + "=" + min_bandwidth;
  }

  output+=" )";
 // System.out.println(output);
   return output;
}

    /**
     * Returns the XML description of the  contents of Gridftp object.
     *
     * @return the xml description.
     */
    public String toXML(){

        String output= "      <bandwidth";

        if(dest_subnet_id!=null){
            output += " " + GRIDFTPBANDWIDTHINFO[DEST_ID] + "=\"" + dest_subnet_id+"\"";
        }
        if(avg_bandwidth_range1!=null){
            output += " " + GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE1] + "=\"" + avg_bandwidth_range1.trim()+"\"";
        }
        if(avg_bandwidth_range2 !=null){
            output += " " + GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE2] + "=\"" +avg_bandwidth_range2.trim()+"\"";
        }
        if(avg_bandwidth_range3 != null) {
            output += " " + GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE3] + "=\""+avg_bandwidth_range3.trim()+"\"";
        }
        if(avg_bandwidth_range4 != null){
            output += " " + GRIDFTPBANDWIDTHINFO[AVG_BW_RANGE4] + "=\"" +avg_bandwidth_range4.trim()+"\"";
        }
        if(avg_bandwidth !=null){
            output += " " + GRIDFTPBANDWIDTHINFO[AVG_BW] + "=\"" + avg_bandwidth.trim()+"\"";
        }
        if(max_bandwidth !=null) {
            output += " " + GRIDFTPBANDWIDTHINFO[MAX_BW] + "=\"" + max_bandwidth.trim()+"\"";
        }
        if (min_bandwidth != null) {
            output += " " + GRIDFTPBANDWIDTHINFO[MIN_BW] + "=\"" + min_bandwidth.trim()+"\"";
        }

        output+=" />\n";

        /**
         * TODO:sk need to add code which picks up elements from gridftp_bandwidth
         * and prints them out or adds them to the output String .
         */

        return output;
 }




}
