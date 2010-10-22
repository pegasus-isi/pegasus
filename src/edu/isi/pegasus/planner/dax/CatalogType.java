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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import edu.isi.pegasus.common.util.XMLWriter;

//import org.griphyn.cPlanner.classes.Profile;
/**
 *
 * @author gmehta
 */
public class CatalogType {

    protected List<Profile> mProfiles;
    protected List<MetaData> mMetadata;
    protected List<PFN> mPFNs;
    protected LogManager mLogger;

    protected CatalogType() {
        mProfiles = new LinkedList<Profile>();
        mMetadata = new LinkedList<MetaData>();
        mPFNs = new LinkedList<PFN>();
        mLogger = LogManagerFactory.loadSingletonInstance();

    }

    public CatalogType addPhysicalFile(String url) {
        PFN p = new PFN(url);
        mPFNs.add(p);
        return this;
    }

    public CatalogType addPhysicalFile(String url, String site) {
        PFN p = new PFN(url, site);
        mPFNs.add(p);
        return this;
    }

    public CatalogType addPhysicaFile(PFN pfn) {
        mPFNs.add(pfn);
        return this;
    }

    public CatalogType addPhysicalFiles(List<PFN> pfns) {
        mPFNs.addAll(pfns);
        return this;
    }

    public List<PFN> getPhysicalFiles() {
        return Collections.unmodifiableList(mPFNs);
    }

    public CatalogType addMetaData(String type, String key, String value) {
        MetaData m = new MetaData(type, key, value);
        mMetadata.add(m);
        return this;
    }

    public CatalogType addMetaData(MetaData metadata) {
        mMetadata.add(metadata);
        return this;
    }

    public CatalogType addMetaData(List<MetaData> metadata) {
        mMetadata.addAll(metadata);
        return this;
    }

    public List<MetaData> getMetaData() {
        return Collections.unmodifiableList(mMetadata);
    }

    public CatalogType addProfile(String namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    public CatalogType addProfile(Profile.NAMESPACE namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    public CatalogType addProfiles(List<Profile> profiles) {
        mProfiles.addAll(profiles);
        return this;
    }

    public CatalogType addProfiles(Profile profile) {
        mProfiles.add(profile);
        return this;
    }

    public List<Profile> getProfiles() {
        return Collections.unmodifiableList(mProfiles);
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        for (Profile p : mProfiles) {
            p.toXML(writer, indent + 1);
        }
        for (MetaData m : mMetadata) {
            m.toXML(writer, indent + 1);
        }
        for (PFN f : mPFNs) {
            f.toXML(writer, indent + 1);
        }
    }
}
