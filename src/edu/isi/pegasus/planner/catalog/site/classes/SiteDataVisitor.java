/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.catalog.site.classes;

import java.io.IOException;
import java.io.Writer;

/**
 * The Visitor interface for the Site Catalog Data Classes.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface SiteDataVisitor {

    /**
     * Initialize the visitor implementation
     *
     * @param writer the writer
     */
    public void initialize(Writer writer);

    /**
     * Visit the SiteStore object
     *
     * @param entry the site store
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(SiteStore entry) throws IOException;

    /**
     * Depart the Site Store object.
     *
     * @param entry the SiteStore
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(SiteStore entry) throws IOException;

    /**
     * Visit the Site CatalogEntry object
     *
     * @param entry the site catalog entry
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(SiteCatalogEntry entry) throws IOException;

    /**
     * Depart the Site Catalog Entry object.
     *
     * @param entry the site catalog entry
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(SiteCatalogEntry entry) throws IOException;

    /**
     * Visit the GridGateway object
     *
     * @param gateway the grid gateway
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(GridGateway entry) throws IOException;

    /**
     * Depart the GridGateway object
     *
     * @param entry GridGateway object
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(GridGateway entry) throws IOException;

    /**
     * Visit Directory site data object
     *
     * @param headnode the object laying out the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(Directory headnode) throws IOException;

    /**
     * Depart the Directory object
     *
     * @param directory the object laying out the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(Directory directory) throws IOException;

    /**
     * Visit FileServer site data object
     *
     * @param server the object corresponding to the FileServer
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(FileServer server) throws IOException;

    /**
     * Depart the Directory object
     *
     * @param server the object corresponding to the FileServer
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(FileServer server) throws IOException;

    /**
     * Visit the ReplicaCatalog object
     *
     * @param catalog the object describing the catalog
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(ReplicaCatalog catalog) throws IOException;

    /**
     * Depart the ReplicaCatalog object
     *
     * @param catalog the object describing the catalog
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(ReplicaCatalog catalog) throws IOException;

    /**
     * Visit the connection object
     *
     * @param c the connection.
     * @throws IOException in case of error while writing to underlying stream
     */
    public void visit(Connection c) throws IOException;

    /**
     * Depart the connection object
     *
     * @param c the connection.
     * @throws IOException in case of error while writing to underlying stream
     */
    public void depart(Connection c) throws IOException;

    public void visit(SiteData data) throws IOException;

    public void depart(SiteData data) throws IOException;

    /**
     * Visit HeadNodeFS object
     *
     * @param headnode the object laying out the headnode
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( HeadNodeFS headnode  )throws IOException;

    /**
     * Depart the HeadNodeFS object
     *
     * @param headnode the object laying out the headnode
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( HeadNodeFS headnode) throws IOException;

    /**
     * Visit the HeadNodeScratch object
     *
     * @param scratch the object describing the scratch area of the headnode.
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( HeadNodeScratch scratch) throws IOException;

    /**
     * Depart the HeadNodeScratch object
     *
     * @param scratch the object describing the scratch area of the headnode.
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( HeadNodeScratch scratch ) throws IOException;

    /**
     * Visit the HeadNodeStorage object
     *
     * @param storage the object describing the storage area of the headnode
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( HeadNodeStorage storage  ) throws IOException;

    /**
     * Depart the HeadNodeStorage object
     *
     * @param storage the object describing the storage area of the headnode
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( HeadNodeStorage storage  ) throws IOException;

    /**
     * Visit the WorkerNodeFS object
     *
     * @param workernode the object describing the worker node
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( WorkerNodeFS workernode ) throws IOException;

    /**
     * Depart the WorkerNodeFS object
     *
     * @param workernode the object describing the worker node
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( WorkerNodeFS workernode  ) throws IOException;

    /**
     * Visit the WorkerNodeScratch object
     *
     * @param scratch the object describing the scratch area
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( WorkerNodeScratch scratch) throws IOException;

    /**
     * Depart the WorkerNodeScratch object
     *
     * @param scratch the object describing the scratch area
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( WorkerNodeScratch scratch ) throws IOException;

    /**
     * Visit the WorkerNodeStorage object
     *
     * @param storage the object describing the storage area
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( WorkerNodeStorage storage) throws IOException;

    /**
     * Depart the WorkerNodeStorage object
     *
     * @param storage the object describing the storage area
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( WorkerNodeStorage storage ) throws IOException;

    /**
     * Visit the local directory
     *
     * @param directory the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( LocalDirectory directory) throws IOException;

    /**
     * Depart the local directory
     *
     * @param directory the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( LocalDirectory directory  ) throws IOException;

    /**
     * Visit the shared directory
     *
     * @param directory the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( SharedDirectory directory ) throws IOException;

    /**
     * Depart the shared directory
     *
     * @param directory the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( SharedDirectory directory ) throws IOException;

    /**
     * Visit the worker shared directory
     *
     * @param directory the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void visit( WorkerSharedDirectory directory  ) throws IOException;

    /**
     * Depart the worker shared directory
     *
     * @param directory the directory
     * @throws IOException in case of error while writing to underlying stream
     */
    //    public void depart( WorkerSharedDirectory directory ) throws IOException;

}
