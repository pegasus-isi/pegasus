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

import edu.isi.pegasus.planner.classes.NameValue;
import java.util.Iterator;
import java.util.Stack;

/**
 * Cycle checker.
 *
 * @author Karan Vahi
 */
public class CycleChecker {
    
    
    /**
     *  Key value pairs identifying the cyclic edges. 
     */
    private NameValue mCyclicEdge;
    
    private final Graph mDAG;
    
    public CycleChecker( Graph dag ){
        mCyclicEdge = null;
        mDAG        = dag;
    }
    
    /**
     * Returns a boolean indicating whether a graph has cyclic edges or not.
     * 
     * @return boolean
     */
    public boolean hasCycles(  ){
        mCyclicEdge = null;
        Stack<GraphNode> stack = new Stack();
        
        //sanity intialization of all nodes to white color
        for( Iterator it = mDAG.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            node.setColor( GraphNode.WHITE_COLOR );
        }
        
        if( mDAG.getRoots().isEmpty() ){
            //sanity check if there is a cycle at the whole dag level
            //roots will return empty
            return true;
        }

        //start the DFS
        for( Iterator<GraphNode> it = mDAG.getRoots().iterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            if( dfsVisitForCycleDetection( node ) ){
                return true;
            }
        }
        
        return false;
    }
    
    public boolean dfsVisitForCycleDetection( GraphNode node ){
        
        node.setColor( GraphNode.GRAY_COLOR );
        //System.out.println( "Colored node GREY  " + node.getID());

        for( Iterator it = node.getChildren().iterator(); it.hasNext(); ){
            GraphNode child = (GraphNode)it.next();

            int color = child.getColor();
            switch( color ){
                case GraphNode.GRAY_COLOR:
                    mCyclicEdge = new NameValue( node.getID() , child.getID() );
                    //System.out.println( "Cycic Edge  " + mCyclicEdge.getKey() + "->" + mCyclicEdge.getValue());
                    return true;
                case GraphNode.WHITE_COLOR:
                    //System.out.println( "Recursive call for node " + child.getID());
                    if( dfsVisitForCycleDetection( child ) ){
                        return true;
                    }
                default:
                    break;
            }
        }
        //traversed all the children recursively
        //System.out.println( "Colored node Black " + node.getID());
        node.setColor( GraphNode.BLACK_COLOR );
        return false;
    }
        
    
    /**
     * Returns the detected cyclic edge if , hasCycles returns true
     * 
     * @return 
     */
    public NameValue getCyclicEdge(){
       return this.mCyclicEdge; 
    }
    
    public static void main(String[] args ){
        Graph g = new MapGraph();
        
        g.addNode( new GraphNode("A", "A"));
        g.addNode( new GraphNode("B", "B"));
        g.addNode( new GraphNode("C", "C"));
        g.addNode( new GraphNode("D", "D"));
        
        g.addEdge( "A", "B");
        g.addEdge( "A", "C");
        g.addEdge( "C", "D");
        g.addEdge( "B", "D");
        g.addEdge( "D", "A");
        
        CycleChecker c = new CycleChecker(g);
        if ( c.hasCycles() ){
            System.err.println( "Graph has cycles with edge " + c.getCyclicEdge() );
        }
        else{
            System.out.println( "Graph has no cycles");
        }
        System.out.println( " --------------------------------------- ");
     
        
        g = new MapGraph();
        for( int i=1; i<=6;i++){
            g.addNode( new GraphNode( "j"+i, "j"+i));
        }
        g.addEdge( "j1", "j2");
        g.addEdge( "j1", "j3");
        g.addEdge( "j2", "j4");
        g.addEdge( "j3", "j4");
        g.addEdge( "j4", "j5");
        g.addEdge( "j4", "j6");
        g.addEdge( "j6", "j2");
        
        
        c = new CycleChecker(g);
        if ( c.hasCycles() ){
            System.err.println( " C has cycles with edge " + c.getCyclicEdge() );
        }
        else{
            System.out.println( "Graph has no cycles");
        }
        System.out.println( " --------------------------------------- ");
        
        
    }
    
}
