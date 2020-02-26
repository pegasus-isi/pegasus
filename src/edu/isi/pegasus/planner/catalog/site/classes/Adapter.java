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
package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import java.util.Iterator;

/**
 * An adapter class that converts the SiteCatalogEntry class to older supported formats and
 * vice-versa
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Adapter {

    /**
     * An adapter method that converts the <code>SiteCatalogEntry3</code> object to <code>
     * SiteCatalogEntry</code> object. For the directories, the following mapping is followed. Note
     * that the adapter method while converting does not clone the members. The user need to call on
     * the returned object if they want a cloned copy.
     *
     * <pre>
     *  HeadNodeFS shared scratch  -> shared-scratch
     *  HeadNodeFS shared storage  -> shared-storage
     *  HeadNodeFS local storage   -> local-storage
     *  WorkerNodeFS local scratch -> local-scratch
     * </pre>
     *
     * @param entry <code>SiteCatalogEntry3</code> to be converted.
     * @return the converted <code>SiteCatalogEntry3</code> object.
     */
    public static SiteCatalogEntry convert(SiteCatalogEntry3 entry) {
        SiteCatalogEntry result = new SiteCatalogEntry();

        result.setSiteHandle(entry.getSiteHandle());
        result.setSysInfo((SysInfo) entry.getSysInfo());
        result.setProfiles((Profiles) entry.getProfiles());

        for (Iterator<GridGateway> it = entry.getGridGatewayIterator(); it.hasNext(); ) {
            result.addGridGateway((GridGateway) it.next());
        }

        // set the associated replica catalog objects
        for (Iterator<ReplicaCatalog> it = entry.getReplicaCatalogIterator(); it.hasNext(); ) {
            result.addReplicaCatalog((ReplicaCatalog) it.next());
        }

        // convert HeadNode related directories
        HeadNodeFS headNode = entry.getHeadNodeFS();
        if (headNode != null) {
            // lets covert the scratch and storage if defined
            HeadNodeScratch hnScratch = headNode.getScratch();
            if (hnScratch != null) {
                SharedDirectory sharedDirectory = hnScratch.getSharedDirectory();
                if (!(sharedDirectory == null || sharedDirectory.isEmpty())) {
                    result.addDirectory(
                            new Directory(
                                    (DirectoryLayout) sharedDirectory,
                                    Directory.TYPE.shared_scratch));
                }
            }
            HeadNodeStorage hnStorage = headNode.getStorage();
            if (hnStorage != null) {
                // convert the shared directory to shared storage
                SharedDirectory sharedDirectory = hnStorage.getSharedDirectory();
                if (!(sharedDirectory == null || sharedDirectory.isEmpty())) {
                    result.addDirectory(
                            new Directory(
                                    (DirectoryLayout) sharedDirectory,
                                    Directory.TYPE.shared_storage));
                }

                // convert local directory to local storage
                LocalDirectory localDirectory = hnStorage.getLocalDirectory();
                if (!(localDirectory == null || localDirectory.isEmpty())) {
                    result.addDirectory(
                            new Directory(
                                    (DirectoryLayout) localDirectory,
                                    Directory.TYPE.local_storage));
                }
            }
        }

        // convert the worker node related directory
        WorkerNodeFS workerNodeFS = entry.getWorkerNodeFS();
        if (workerNodeFS != null) {
            // only convert the worker node local scratch
            WorkerNodeScratch wnScratch = workerNodeFS.getScratch();
            if (wnScratch != null) {
                LocalDirectory localDirectory = wnScratch.getLocalDirectory();
                if (!(localDirectory == null || localDirectory.isEmpty())) {
                    result.addDirectory(
                            new Directory(
                                    (DirectoryLayout) localDirectory,
                                    Directory.TYPE.local_scratch));
                }
            }
        }

        return result;
    }

    /**
     * An adapter method that converts the <code>SiteCatalogEntry</code> object to <code>
     * SiteCatalogEntry3</code> object. For the directories, the following mapping is followed.
     *
     * <pre>
     *  shared-scratch  ->  HeadNodeFS shared scratch
     *  shared-storage  ->  HeadNodeFS shared storage
     *  local-scratch   ->  WorkerNodeFS local scratch
     *  local-storage   ->  HeadNodeFS local storage
     * </pre>
     *
     * Note that the adapter method while converting does not clone the members. The user need to
     * call on the returned object if they want a cloned copy.
     *
     * @param entry <code>SiteCatalogEntry</code> to be converted.
     * @return the converted <code>SiteCatalogEntry3</code> object.
     */
    public static SiteCatalogEntry3 convert(SiteCatalogEntry entry) {
        SiteCatalogEntry3 result = new SiteCatalogEntry3();

        result.setSiteHandle(entry.getSiteHandle());
        result.setSysInfo((SysInfo) entry.getSysInfo());
        result.setProfiles((Profiles) entry.getProfiles());

        for (Iterator<GridGateway> it = entry.getGridGatewayIterator(); it.hasNext(); ) {
            result.addGridGateway((GridGateway) it.next());
        }

        // set the associated replica catalog objects
        for (Iterator<ReplicaCatalog> it = entry.getReplicaCatalogIterator(); it.hasNext(); ) {
            result.addReplicaCatalog((ReplicaCatalog) it.next());
        }

        // iterate through the directories and convert to old format as desired
        for (Iterator<Directory> it = entry.getDirectoryIterator(); it.hasNext(); ) {
            Directory d = it.next();
            Adapter.addDirectory(result, d);
        }

        return result;
    }

    /**
     * Adds a directory to the site catalog entry object. Adding a directory automatically will add
     * a head node filesystem and a worker node filesystem to the SiteCatalogEntry if it does not
     * exist already. The mapping followed is
     *
     * <pre>
     *  shared-scratch  ->  HeadNodeFS shared scratch
     *  shared-storage  ->  HeadNodeFS shared storage
     *  local-scratch   ->  WorkerNodeFS local scratch
     *  local-storage   ->  HeadNodeFS local storage
     * </pre>
     *
     * @param entry the Site Catalog Entry object
     * @param directory the directory to be added
     */
    private static void addDirectory(SiteCatalogEntry3 entry, Directory directory) {
        Directory.TYPE type = directory.getType();

        if (type == Directory.TYPE.shared_scratch) {
            HeadNodeFS headnode = entry.getHeadNodeFS();
            if (headnode == null) {
                headnode = new HeadNodeFS();
                HeadNodeScratch scratch = new HeadNodeScratch();
                scratch.setSharedDirectory(directory);
                headnode.setScratch(scratch);
                entry.setHeadNodeFS(headnode);
            } else {
                // retrive from existing
                HeadNodeScratch scratch = headnode.getScratch();
                if (scratch == null) {
                    scratch = new HeadNodeScratch();
                    headnode.setScratch(scratch);
                }
                // get the shared filesystem
                scratch.setSharedDirectory(directory);
            }
        } else if (type == Directory.TYPE.shared_storage) {
            HeadNodeFS headnode = entry.getHeadNodeFS();
            if (headnode == null) {
                headnode = new HeadNodeFS();
                HeadNodeStorage storage = new HeadNodeStorage();
                storage.setSharedDirectory(directory);
                headnode.setStorage(storage);
                entry.setHeadNodeFS(headnode);
            } else {
                // retrieve from existing
                HeadNodeStorage storage = headnode.getStorage();
                if (storage == null) {
                    storage = new HeadNodeStorage();
                    headnode.setStorage(storage);
                }
                // set the shared filesystem
                storage.setSharedDirectory(directory);
            }
        } else if (type == Directory.TYPE.local_scratch) {
            WorkerNodeFS workernode = entry.getWorkerNodeFS();
            if (workernode == null) {
                workernode = new WorkerNodeFS();
                WorkerNodeScratch scratch = new WorkerNodeScratch();
                scratch.setLocalDirectory(directory);
                workernode.setScratch(scratch);
                entry.setWorkerNodeFS(workernode);
            } else {
                // retrieve from existing
                WorkerNodeScratch scratch = workernode.getScratch();
                if (scratch == null) {
                    scratch = new WorkerNodeScratch();
                    workernode.setScratch(scratch);
                }
                // set the shared filesystem
                scratch.setLocalDirectory(directory);
            }
        }
        /// we now map HeadNode local storage
        /*
        else if( type == Directory.TYPE.local_storage ){
            WorkerNodeFS workernode = entry.getWorkerNodeFS();
            if( workernode == null ){
                workernode = new WorkerNodeFS();
                WorkerNodeStorage storage = new WorkerNodeStorage();
                storage.setLocalDirectory( directory );
                workernode.setStorage( storage );
                entry.setWorkerNodeFS( workernode );
            }
            else{
                //retrieve from existing
                WorkerNodeStorage storage = workernode.getStorage();
                if( storage == null ){
                    storage = new WorkerNodeStorage();
                    workernode.setStorage( storage );
                }
                //set the shared filesystem
               storage.setLocalDirectory( directory );
            }
        }
        */
        else if (type == Directory.TYPE.local_storage) {
            HeadNodeFS headnode = entry.getHeadNodeFS();
            if (headnode == null) {
                headnode = new HeadNodeFS();
                HeadNodeStorage storage = new HeadNodeStorage();
                storage.setLocalDirectory(directory);
                headnode.setStorage(storage);
                entry.setHeadNodeFS(headnode);
            } else {
                // retrieve from existing
                HeadNodeStorage storage = headnode.getStorage();
                if (storage == null) {
                    storage = new HeadNodeStorage();
                    headnode.setStorage(storage);
                }
                // set the shared filesystem
                storage.setLocalDirectory(directory);
            }
        }
    }
}
