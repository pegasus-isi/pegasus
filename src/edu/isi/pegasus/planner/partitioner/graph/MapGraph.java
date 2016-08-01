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


package edu.isi.pegasus.planner.partitioner.graph;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.NameValue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An implementation of the Graph that is backed by a Map.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class MapGraph implements Graph{

    /**
     * The map indexed by the id of the <code>GraphNode</code>, used for storing
     * the nodes of the Graph. The value for each key is the corresponding
     * <code>GraphNode</code> of the class.
     *
     */
    @Expose @SerializedName( "jobs" )
    protected Map mStore;

    /**
     * The handle to the logging manager.
     */
    private LogManager mLogger;
    
    /**
     * Handle to the cycle checker
     */
    private CycleChecker mCycleChecker;

    /**
     * The default constructor.
     */
    public MapGraph(){
        this( false );
    }
    
    /**
     * The overloaded constructor
     * 
     * @param preserveInsertionOrder  a boolean indicating whether you want to
     *                                preserve insertion order. If set to true, the
     *                                nodeIterator() will return nodes in the order
     *                                they were added.
     */
    public MapGraph( boolean preserveInsertionOrder ){
        mStore = ( preserveInsertionOrder ) ? new LinkedHashMap(): new HashMap();
        mLogger =  LogManagerFactory.loadSingletonInstance();
        mCycleChecker = new CycleChecker(this);
    }


    /**
     * Adds a node to the Graph. It overwrites an already existing node with the
     * same ID.
     *
     * @param node  the node to be added to the Graph.
     */
    public void addNode( GraphNode node ){
        mStore.put( node.getID(), node );
    }


    /**
     * Returns the node matching the id passed.
     *
     * @param identifier  the id of the node.
     *
     * @return the node matching the ID else null.
     */
    public GraphNode getNode( String identifier ){
        Object obj = get ( identifier );
        return ( obj == null ) ? null : (GraphNode)obj;
    }

    /**
     * Adds a single root node to the Graph. All the exisitng roots of the
     * Graph become children of the root.
     *
     * @param root  the <code>GraphNode</code> to be added as a root.
     *
     * @throws RuntimeException if a node with the same id already exists.
     */
    public void addRoot( GraphNode root ){
        //sanity check
        if( mStore.containsKey( root.getID() ) ){
            throw new RuntimeException( "Node with ID already exists:"  + root.getID() );
        }

        List existingRoots = getRoots();
        root.setChildren( existingRoots );

        //for existing root nodes, add a parent as the new Root
        for( Iterator it = existingRoots.iterator(); it.hasNext(); ){
            GraphNode existing = ( GraphNode ) it.next();
            existing.addParent( root );
        }

        //add the new root into the graph
        addNode( root );

    }
    
    /**
     * Resets all the dependencies in the Graph, while preserving the nodes. 
     * The resulting Graph is a graph of independent nodes.
     */
    public void resetEdges(){
        for(Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); ){
            GraphNode node = it.next();
            node.resetEdges();
        }
    }

    /**
     * Removes a node from the Graph.
     *
     * @param identifier   the id of the node to be removed.
     *
     * @return boolean indicating whether the node was removed or not.
     */
    public boolean remove( String identifier ){

        Object obj = get( identifier );
        if ( obj == null ){
            //node does not exist only.
            return false;
        }

        GraphNode removalNode = ( GraphNode )obj;

        // the parents of the node now become parents of the children
        // the parents of the node now become parents of the children
        Collection<GraphNode> parents = removalNode.getParents();
        Collection<GraphNode> children = removalNode.getChildren();

        //for each parent make the parent it's parent instead of removed node

        for ( GraphNode child : children ){
            child.removeParent( removalNode );
        } 

        for( GraphNode parent : parents ){
            //for the parent the removal node is no longer a parent
            parent.removeChild( removalNode );

            //for each parent make the parent it's parent instead of removed node
            for ( GraphNode child  : children ){
                child.addParent( parent );
                parent.addChild( child );
            }
        }

        //we have the correct linkages now
        //remove the node from the store.
        mStore.remove( identifier );
        return true;

    }


    /**
     * Returns the root nodes of the Graph.
     *
     * @return  a list containing <code>GraphNode</code> corressponding to the
     *          root nodes.
     */
    public List<GraphNode> getRoots(){
        List rootNodes = new LinkedList();

        for( Iterator it = mStore.entrySet().iterator(); it.hasNext(); ){
            GraphNode gn = (GraphNode)( (Map.Entry)it.next()).getValue();
            if(gn.getParents() == null || gn.getParents().isEmpty()){
                rootNodes.add(gn);
            }
        }

        return rootNodes;

        //Not Generating a dummy node
        //add a dummy node that is a root to all these nodes.
//        String rootId = this.DUMMY_NODE_ID;
//        mRoot = new GraphNode(rootId,rootId);
//        mRoot.setChildren(rootNodes);
//        put(rootId,mRoot);
        //System.out.println(dummyNode);

    }


    /**
     * Returns the leaf nodes of the Graph.
     *
     * @return  a list containing <code>GraphNode</code> corressponding to the
     *          leaf nodes.
     */
    public List<GraphNode> getLeaves(){
        List leaves = new LinkedList();

        for( Iterator it = mStore.entrySet().iterator(); it.hasNext(); ){
            GraphNode gn = (GraphNode)( (Map.Entry)it.next()).getValue();
            if( gn.getChildren() == null || gn.getChildren().isEmpty() ){
                leaves.add(gn);
            }
        }

        return leaves;
    }

    
    
    /**
     * Adds an edge between two already existing nodes in the graph.
     *
     * @param parent   the parent node ID.
     * @param parent    the parent node ID.
     */
    public void addEdge( String parent, String child ){
        //sanity check
        if( parent.equals( child )){
            throw new IllegalArgumentException( "Invalid Edge Specification. An Edge specified from a node to itself  for " + parent );
        }

        GraphNode childNode = (GraphNode)getNode( child );
        GraphNode parentNode = (GraphNode)getNode( parent );

        String notExist = ( childNode == null )? child :
                                                ( parentNode == null ) ? parent : null;

        if ( notExist != null ) {
            /* should be replaced by Graph Exception */
            throw new RuntimeException( "The node with identifier doesnt exist " + notExist );
        }

        this.addEdge(parentNode, childNode);
    }

    /**
     * Adds an edge between two already existing nodes in the graph.
     *
     * @param parent   the parent node .
     * @param child    the child node .
     */
    public void addEdge( GraphNode parent, GraphNode child ){
        child.addParent( parent );
        parent.addChild( child);
    }
    
    
    /**
     * A convenience method that allows for bulk addition of edges between
     * already existing nodes in the graph.
     *
     * @param parent   the parent node ID
     * @param parents list of parent identifiers as <code>String</code>.
     */
    public void addEdges( String child, List parents ){
        //sanity check
        if( parents.contains( child )){
            throw new IllegalArgumentException( "Invalid Edge Specification. Parents " + parents + " include the child " + child );
        }

        GraphNode childNode = (GraphNode)getNode( child );

        if( childNode == null ) {
            /* should be replaced by Graph Exception */
            throw new RuntimeException( "The node with identifier doesnt exist " + child );
        }

        String parentId;
        List parentList = new LinkedList();

        //construct the references to the parent nodes
        for( Iterator it = parents.iterator(); it.hasNext(); ){
            parentId = ( String )it.next();
            GraphNode parentNode = (GraphNode)get( parentId );

            if( parentNode == null ) {
                /* should be replaced by Graph Exception */
                throw new RuntimeException( "The node with identifier doesnt exist " + parentId );
            }

            parentList.add( parentNode );

            //add the parent to the parent's parent list
            parentNode.addChild( childNode );
        }
        childNode.setParents( parentList );

    }

    /**
     * Returns the number of nodes in the graph.
     *
     * @return the number of nodes
     */
    public int size(){
        return mStore.values().size();
    }

    /**
     * Returns an iterator for the nodes in the Graph.
     *
     * @return Iterator
     */
    public Iterator nodeIterator(){
        return mStore.values().iterator();
    }

    /**
     * Returns an iterator that traverses through the graph using a graph
     * traversal algorithm. At any one time, only one iterator can
     * iterate through the graph.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator iterator(){
        return new MapGraphIterator();
    }
    
    /**
     * Returns an iterator that traverses the graph bottom up from the leaves.
     * At any one time, only one iterator can
     * iterate through the graph.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator bottomUpIterator(){
        return new BottomUpIterator();
    }

    /**
     * Returns an iterator for the graph that traverses in topological sort
     * order.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator<GraphNode> topologicalSortIterator(){
        return new TopologicalSortIterator( this );
    }

    /**
     * Returns a boolean indicating whether a graph has cyclic edges or not.
     * 
     * @return boolean
     */
    public boolean hasCycles(){
        return this.mCycleChecker.hasCycles();
    }
    
    /**
     * Returns the detected cyclic edge if , hasCycles returns true
     * 
     * @return 
     */
    public NameValue getCyclicEdge(){
        return this.mCycleChecker.getCyclicEdge();
    }

    /**
     * The textual representation of the graph node.
     *
     * @return textual description.
     */
    public String toString() {
        String newLine = System.getProperty( "line.separator", "\r\n" );
        String indent = "\t";
        StringBuffer sb = new StringBuffer( 32 );

        GraphNode node;
        for( Iterator it = nodeIterator(); it.hasNext(); ){
            node = ( GraphNode )it.next();
            sb.append( newLine ).append( indent ).append( "Job ->" ).append( node.getID() );

            //write out the node children
            sb.append(" Children's {");
            for( Iterator it1 = node.getChildren().iterator(); it1.hasNext(); ){
                sb.append( ((GraphNode)it1.next()).getID() ).append(',');
            }
            sb.append("}");

            //write out the node's parents
            sb.append(" Parents {");
            for( Iterator it1 = node.getParents().iterator(); it1.hasNext(); ){
                sb.append( ((GraphNode)it1.next()).getID() ).append(',');
            }
            sb.append("}");

        }


        return sb.toString();
    }

    /**
     * Returns a boolean if there are no nodes in the graph.
     *
     * @return boolean
     */
    public boolean isEmpty(){
        return this.mStore.isEmpty();
    }

    /**
     * Returns a copy of the object.
     *
     * @return clone of the object.
     */
    public Object clone(){
        return new java.lang.CloneNotSupportedException(
            "Clone() not implemented in GraphNode");
    }

    /**
     * It returns the value associated with the key in the map.
     *
     * @param key  the key to the entry in the map.
     */
    public Object get( Object key ){
        return mStore.get(key);
    }

    /**
     * An inner iterator class that traverses through the Graph.
     * The traversal of the graph is a modified BFS. A node is added to
     * the queue only when all it's parents have been added to the queue.
     */
    protected class MapGraphIterator implements Iterator{

        /**
         * The first in first out queue, that manages the set of gray vertices in a
         * breadth first search.
         */
        private LinkedList mQueue;

        /**
         * The current depth of the nodes that are being traversed in the BFS.
         */
        private int mCurrentDepth;

        /**
         * A temporary list that stores all the nodes on a particular level.
         */
        private List mLevelList;

        /**
         * The default constructor.
         */
        public MapGraphIterator(){
            mQueue = new LinkedList();
            mLevelList = new LinkedList();
            mCurrentDepth = -1;

            //sanity intialization of all nodes depth
            for( Iterator it = nodeIterator(); it.hasNext(); ){
                GraphNode node = ( GraphNode )it.next();
                node.setDepth( mCurrentDepth );
                node.setColor( GraphNode.WHITE_COLOR );
            }

            //intialize all the root nodes depth to 0
            //and put them in the queue
            mCurrentDepth = 0;
            for( Iterator it = getRoots().iterator(); it.hasNext(); ){
                GraphNode node = ( GraphNode )it.next();
                node.setDepth( mCurrentDepth );
                mQueue.add( node );
            }
        }

        /**
         * Always returns false, as an empty iterator.
         *
         * @return true if there are still nodes in the queue.
         */
        public boolean	hasNext(){
            return !mQueue.isEmpty();
        }

        /**
         * Returns the next object in the traversal order.
         *
         * @return null
         */
        public Object next(){
            GraphNode node  = (GraphNode)mQueue.getFirst();

            int depth  = node.getDepth();
            if( mCurrentDepth < depth ){

                if( mCurrentDepth > 0 ){
                    //we are done with one level!
                    //that is when the callback should happen
                }


                //a new level starts
                mCurrentDepth++;
                mLevelList.clear();
            }
            //mLogger.log( "Adding to level " + mCurrentDepth + " " + node.getID(),
            //             LogManager.DEBUG_MESSAGE_LEVEL);
            mLevelList.add( node );

            node.setColor( GraphNode.BLACK_COLOR );

            //add the children to the list only if all the parents
            //of the parent nodes have been traversed.
            for( Iterator it = node.getChildren().iterator(); it.hasNext(); ){
                GraphNode child = (GraphNode)it.next();
                if(!child.isColor( GraphNode.GRAY_COLOR ) &&
                   child.parentsColored( GraphNode.BLACK_COLOR )){
                    //mLogger.log( "Adding to queue " + parent.getID(),
                    //             LogManager.DEBUG_MESSAGE_LEVEL );
                    child.setDepth( depth + 1 );
                    child.setColor( GraphNode.GRAY_COLOR );
                    mQueue.addLast( child );
                }

            }
            node = (GraphNode)mQueue.removeFirst();
            //mLogger.log( "Removed " + node.getID(),
            //             LogManager.DEBUG_MESSAGE_LEVEL);
            return node;

        }

        /**
         * Method is not supported.
         */
        public void remove(){
            throw new java.lang.UnsupportedOperationException( "Method remove() not supported" );
        }

    }//end of internal iterator class

    /**
     * An inner iterator class that traverses through the Graph bottom up.
     * The traversal of the graph is a modified BFS. A node is added to
     * the queue only when all it's parents children been added to the queue.
     */
    protected class BottomUpIterator implements Iterator{

        /**
         * The first in first out queue, that manages the set of gray vertices in a
         * breadth first search.
         */
        private LinkedList mQueue;

        /**
         * The current height of the nodes that are being traversed in the BFS starting from 
         * the leaf.
         */
        private int mCurrentDepth;

        /**
         * A temporary list that stores all the nodes on a particular level.
         */
        private List mLevelList;

        /**
         * The default constructor.
         */
        public BottomUpIterator(){
            mQueue = new LinkedList();
            mLevelList = new LinkedList();
            mCurrentDepth = -1;

            //sanity intialization of all nodes depth
            for( Iterator it = nodeIterator(); it.hasNext(); ){
                GraphNode node = ( GraphNode )it.next();
                node.setDepth( mCurrentDepth );
                node.setColor( GraphNode.WHITE_COLOR );
            }

            //intialize all the root nodes depth to 0
            //and put them in the queue
            mCurrentDepth = 0;
            for( Iterator it = getLeaves().iterator(); it.hasNext(); ){
                GraphNode node = ( GraphNode )it.next();
                node.setDepth( mCurrentDepth );
                mQueue.add( node );
            }
        }

        /**
         * Always returns false, as an empty iterator.
         *
         * @return true if there are still nodes in the queue.
         */
        public boolean	hasNext(){
            return !mQueue.isEmpty();
        }

        /**
         * Returns the next object in the traversal order.
         *
         * @return null
         */
        public Object next(){
            GraphNode node  = (GraphNode)mQueue.getFirst();

            int depth  = node.getDepth();
            if( mCurrentDepth < depth ){

                if( mCurrentDepth > 0 ){
                    //we are done with one level!
                    //that is when the callback should happen
                }


                //a new level starts
                mCurrentDepth++;
                mLevelList.clear();
            }
            //mLogger.log( "Adding to level " + mCurrentDepth + " " + node.getID(),
            //             LogManager.DEBUG_MESSAGE_LEVEL);
            mLevelList.add( node );

            node.setColor( GraphNode.BLACK_COLOR );

            //add the parents to the list only if all the children
            //of the parent nodes have been traversed.
            for( GraphNode parent : node.getParents()){
                if(!parent.isColor( GraphNode.GRAY_COLOR ) &&
                   parent.childrenColored( GraphNode.BLACK_COLOR )){
                    //mLogger.log( "Adding to queue " + parent.getID(),
                    //             LogManager.DEBUG_MESSAGE_LEVEL );
                    parent.setDepth( depth + 1 );
                    parent.setColor( GraphNode.GRAY_COLOR );
                    mQueue.addLast( parent );
                }

            }
            node = (GraphNode)mQueue.removeFirst();
            //mLogger.log( "Removed " + node.getID(),
            //             LogManager.DEBUG_MESSAGE_LEVEL);
            return node;

        }

        /**
         * Method is not supported.
         */
        public void remove(){
            throw new java.lang.UnsupportedOperationException( "Method remove() not supported" );
        }

    }//end of internal iterator class

    
}
