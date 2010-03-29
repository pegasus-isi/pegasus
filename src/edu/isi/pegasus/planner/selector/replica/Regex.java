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

package edu.isi.pegasus.planner.selector.replica;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.PegRandom;

import org.griphyn.cPlanner.classes.ReplicaLocation;


import org.griphyn.common.catalog.ReplicaCatalogEntry;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * A replica selector that allows the user to specific regex expressions that
 * can be used to rank various PFN's returned from the Replica Catalog for a 
 * particular LFN. This replica selector selects the highest ranked PFN i.e the 
 * replica with the lowest rank value. 
 * 
 * <p>
 * The regular expressions are assigned different rank, that determine 
 * the order in which the expressions are employed. The rank values for
 * the regex can expressed in user properties using the property.
 * pegasus.selector.replica.regex.rank.[value]   
 * 
 * The value is an integer value that denotes the rank of an expression with
 * a rank value of 1 being the highest rank.
 * 
 * <p>
 * A thing to note is that before applying any regular expressions on the PFN's,
 * the file URL's that dont match the preferred site are explicitly filtered out.
 *
 *
 * <p>
 * In order to use the replica selector implemented by this class,
 * <pre>
 *        - the property pegasus.selector.replica.selector must be set to value Regex
 * </pre>
 *
 * @author Karan Vahi
 *
 * @version $Revision$
 */
public class Regex extends Default {

    /**
     * A short description of the replica selector.
     */
    private static final String mDescription = "Regex";

    /**
     * The property prefix for all Regex rank property.
     */
    private static final String PROPERTY_PREFIX = "pegasus.selector.replica.regex.rank.";
    
    /**
     * The highest value of rank. In terms of integers , the lower the int higher
     * the rank with 1 being the highest value.
     */
    private static final int HIGHEST_RANK_VALUE = 1;
    
    /**
     * The lowest rank value
     */
    private static final int LOWEST_RANK_VALUE = Integer.MAX_VALUE;
    
    /**
     * The Set of regular expressions that orders the regex expressions to use 
     * in ascending order.
     */
    private SortedSet mSortedRegexSet;

    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     */
    public Regex(PegasusProperties properties) {
        super(properties);
        mSortedRegexSet = getRegexSet( properties.matchingSubset( Regex.PROPERTY_PREFIX, false ));
        mLogger.log( "[RegexReplicaSelector] User Provided Ranked regexes are " + mSortedRegexSet,
                     LogManager.DEBUG_MESSAGE_LEVEL );
    }

    /**
     * This chooses a location amongst all the locations returned by the replica
     * location service. If a location is found with re attribute same as the
     * preference pool, it is taken. Else a random location is selected and
     * returned. If more than one location for the lfn is found at the preference
     * pool, then also a random location amongst the ones at the preference pool
     * is selected.
     *
     * @param rl         the <code>ReplicaLocation</code> object containing all
     *                   the pfn's associated with that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica
     *                      on the local site / submit host.
     *
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaCatalogEntry selectReplica( ReplicaLocation rl,
                                              String preferredSite,
                                              boolean allowLocalFileURLs ){


        String lfn = rl.getLFN();
        String site;
        ArrayList prefLocs = new ArrayList();
        int locSelected;

        //create a shallow clone as we will be removing
        //using Iterator.remove() methods
        rl = (ReplicaLocation)rl.clone();
        
        //log message
        StringBuffer sb = new StringBuffer();
        sb.append( "[RegexReplicaSelector] Selecting a pfn for lfn ").append( lfn ).
           append( " at site ").append( preferredSite ).append( "\n amongst ").
           append( rl );
        mLogger.log( sb.toString() , LogManager.DEBUG_MESSAGE_LEVEL );

        ReplicaCatalogEntry selectedRCE = null;
        Rank lowestRank = new Rank( Regex.LOWEST_RANK_VALUE, ".*" );
        for ( Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            ReplicaCatalogEntry rce = ( ReplicaCatalogEntry ) it.next();
            site = rce.getResourceHandle();
            String pfn = rce.getPFN();
            
            //if a PFN starts with file url and does
            //not match the preferredSite ignore.
            if( this.removeFileURL( rce, preferredSite, allowLocalFileURLs) ){
                //remove the url and continue
                it.remove();
                continue;                
            }

            //System.out.println( "PFN is " + pfn );
            
            //apply the various Regex till you get the lowest rank value of 1
            
            int lowestRankValue = lowestRank.getRank();
            for( Iterator<Rank> regIt = mSortedRegexSet.iterator(); regIt.hasNext(); ){
                Rank r = regIt.next();
                
                //System.out.println( "Applying regex " + r );
                if( r.matches( pfn ) ){
                    
                    //System.out.println( "Rank for pfn " + pfn + " is " + r.getRank() );
                    if( r.getRank() < lowestRankValue ){
                        selectedRCE = rce;
                        lowestRank  = r;
                        lowestRankValue = r.getRank();
                        
                        //check if the lowest rank is 1 , then we 
                        //have a highest rank replica
                        if( lowestRankValue == Regex.HIGHEST_RANK_VALUE ){
                            break;
                        }
                    }
                }
            }
        }

        int numLocs = rl.getPFNCount();
        if ( selectedRCE == null ){
            if ( numLocs == 0 ) {
               //in all likelihood all the urls were file urls and
                //none were associated with the preference pool.
                //replica not selected
                throw new RuntimeException( "Unable to select any location from " +
                                            "the list passed for lfn "  + lfn );
            }
            else{
               //select a random location from all the matching locations
               selectedRCE = rl.getPFN( PegRandom.getInteger( numLocs - 1 ) );
            }
        }

        //log message
        sb = new StringBuffer();
        sb.append( "[RegexReplicaSelector] Selected for LFN " ).append( rl.getLFN() ).
           append( " " ).append( selectedRCE ).append( " matching " ).append( lowestRank.getRegex() );
        mLogger.log( sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
        return selectedRCE;
    }

    /**
     * Returns a short description of the replica selector.
     *
     * @return string corresponding to the description.
     */
    public String description(){
        return mDescription;
    }


    
    /**
     * Returns a sorted set containing the various Patterns pre-compiled.
     * The order in the set determines the order in which the patterns are 
     * applied on the PFN's
     * 
     * @param properties  with the key being an integer specifying the rank
     *                    and value as regex expression to be applied.
     * 
     * @return Sorted<Rank>
     */
    private SortedSet<Rank> getRegexSet( Properties properties ) {
        SortedSet<Rank> result = new TreeSet<Rank>();
        //traverse through the properties and put them 
        //in a sorted set
        for( Iterator it = properties.keySet().iterator(); it.hasNext() ; ){
            String key = (String)it.next();
            result.add( new Rank( Integer.parseInt(key), properties.getProperty( key )));
        }
        return result;
    }

    
    /**
     * A Data class that allows us to compile a regex expression
     * and associate a rank value with it.
     */
    private class Rank implements Comparable{

        /**
         * The rank value.
         */
        private int mRank;
        
        /**
         * The compiled regex expression
         */
        private Pattern mPattern;
        
        /**
         * The default constructor.
         * 
         * @param rank   The rank value.
         * @param regex   The regex expression.
         */
        public Rank( int rank, String regex ){
            mRank = rank;
            mPattern = Pattern.compile( regex );
        }
        
        /**
         * Matches a string against the compiled regex expression
         * 
         * @param input   the input string to be matched
         * 
         * @return boolean indicating whether input matches or not.
         */
        public boolean matches( String input ){
            return mPattern.matcher(input).matches();
        }
        
        /**
         * Returns the underlying regex pattern associated with the Rank object.
         * 
         * @return the regex pattern
         */
        public Pattern getRegex(){
           return mPattern;
        }
        
        /**
         * Returns the rank associated with it.
         * 
         * @return  the int value of the rank
         */
        public int getRank(){
           return mRank; 
        }
        
        /**
         * Compares the Rank object with another rank object.
         * 
         * @param o   the object to be compared.
         * @return int
         */
        public int compareTo(Object o) {
            if ( o instanceof Rank ) {
                Rank r = ( Rank ) o;
                return ( this.getRank() - r.getRank());
            } else {
                throw new ClassCastException( "Object is not of class Regex.Rank" );
            }
        }
        
        /**
         * Returns the textual representation of this
         */
        public String toString(){
            StringBuffer sb = new StringBuffer();
            sb.append( "( value => " ).append( getRank() ).append( " expr => ").append( getRegex() ).append( ")" );
            return sb.toString();
        }
        
    }

}
