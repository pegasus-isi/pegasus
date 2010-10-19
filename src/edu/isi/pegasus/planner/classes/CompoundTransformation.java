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

package edu.isi.pegasus.planner.classes;


import java.util.List;
import java.util.LinkedList;

/**
 *
 * A data class to contain compound transformations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CompoundTransformation {

    /**
     * The namespace of the compound transformation.
     */
    protected String mNamespace;

    /**
     * The name of the tranformation.
     */
    protected String mName;

    /**
     * The version
     */
    protected String mVersion;

    /**
     * The list of dependant executables
     */
    protected List<PegasusFile> mUses;

    /**
     * Constructor
     *
     * @param name of transformation
     */
    public CompoundTransformation(String name) {
        this("", name, "");
    }

    /**
     * Overloaded Constructor
     *
     * @param namespace   namespace
     * @param name        name
     * @param version     version
     */
    public CompoundTransformation(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;

        mVersion = (version == null) ? "" : null;
        mUses = new LinkedList<PegasusFile>();
    }

    /**
     * Returns name of compound transformation.
     *
     * @return name
     */
    public String getName() {
        return mName;
    }

    /**
     * Returns the namespace
     *
     * @return namespace
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Returns the version
     *
     * @return  version
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Adds a dependant file.
     * 
     * @param pf
     */
    public void addDependantFile( PegasusFile pf ){
        this.mUses.add( pf );
    }
}
