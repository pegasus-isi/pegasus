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

        //add roots to the stack
        for( Iterator<GraphNode> it = mDAG.getRoots().iterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            node.setColor( GraphNode.WHITE_COLOR);
            stack.push( node );
        }
        
        //keep tracked of the reversed
        Stack<GraphNode>visited = new Stack();
        
        
        if( stack.isEmpty() ){
            //sanity check if there is a cycle at the whole dag level
            //roots will return empty
            throw new RuntimeException( "cycle in the whole workflow");
        }
        
        while( !stack.isEmpty() ){
            GraphNode node = stack.pop();
            node.setColor( GraphNode.GRAY_COLOR );
            boolean allChidrenBlack = true;
            for( Iterator it = node.getChildren().iterator(); it.hasNext(); ){
                GraphNode child = (GraphNode)it.next();
                int color = child.getColor();
                switch( color ){
                    case GraphNode.GRAY_COLOR:
                        mCyclicEdge = new NameValue( node.getID() , child.getID() );
                        return true;
                    case GraphNode.WHITE_COLOR:
                        allChidrenBlack = false;
                        //System.out.println( "Pushed node on stack " + child.getID());
                        stack.push( child );
                        break;
                    default:
                        break;
                }
                        
            }
            if( allChidrenBlack ){
                //System.out.println( "Colored node black A " + node.getID() );
                node.setColor( GraphNode.BLACK_COLOR );
            }
            else{
                visited.push(node);
            }
        }
        
        //all the nodes in visited stack are painted black
        while( !visited.isEmpty() ){
            GraphNode node = visited.pop();
            node.setColor( GraphNode.BLACK_COLOR );
            //System.out.println( "Colored node black B " + node.getID() );
        }
        
        
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
        g.addEdge( "B", "C");
        g.addEdge( "C", "D");
        g.addEdge( "D", "A");
        
        CycleChecker c = new CycleChecker(g);
        c.hasCycles();
    }
    
}
