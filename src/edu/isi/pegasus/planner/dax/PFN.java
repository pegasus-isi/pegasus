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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.util.XMLWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * @author gmehta
 * @version $Revision$
 */
public class PFN {

    protected String mURL;
    protected String mSite;
    protected List<Profile> mProfiles;

    public PFN(String url) {
        this(url, null);
    }

    public PFN(String url, String site) {
        mURL = url;
        mSite = site;
        //       mProfiles=new Profiles();
        mProfiles = new LinkedList<Profile>();
    }

    public PFN() {
        this(null, null);
    }

    public String getURL() {
        return mURL;
    }

    public PFN setSite(String site) {
        mSite = site;
        return this;
    }

    public PFN setURL(String url) {
        mURL = url;
        return this;
    }

    public String getSite() {
        return (mSite == null) ? "" : mSite;
    }

    public PFN addProfile(String namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    public PFN addProfile(Profile.NAMESPACE namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    public PFN addProfiles(List<Profile> profiles) {
        mProfiles.addAll(profiles);
        return this;
    }

    public PFN addProfiles(Profile profile) {
        mProfiles.add(profile);
        return this;
    }

    public List<Profile> getProfiles() {
        return mProfiles;
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("pfn", indent);
        writer.writeAttribute("url", mURL);
        if (mSite != null) {
            writer.writeAttribute("site", mSite);
        }
        for (Profile p : mProfiles) {
            p.toXML(writer, indent + 1);
        }
        writer.endElement(indent);
    }
}
