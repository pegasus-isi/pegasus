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

/**
 *
 * @author gmehta
 * @version $Revision$
 */
public class Job extends AbstractJob {

    public Job(String id, String name) {
        this(id, "", name, "", "");
    }

    public Job(String id, String name, String label) {
        this(id, "", name, "", label);
    }

    public Job(String id, String namespace, String name, String version) {
        this(id, namespace, name, version, "");
    }

    public Job(Job j){
        super(j);
    }
    public Job(String id, String namespace, String name, String version, String label) {
        super();
        checkID(id);

        // to decide whether to exit. Currently just logging error and proceeding.
        mId = id;
        mName = name;
        mNamespace = namespace;

        mVersion = version;
        mNodeLabel = label;
    }

    public String getNamespace() {
        return mNamespace;
    }

    public String getVersion() {
        return mVersion;
    }
    
        
    /**
     * Is this Object a Job
     * @return 
     */
    public boolean isJob(){
        return true;
    }
}
