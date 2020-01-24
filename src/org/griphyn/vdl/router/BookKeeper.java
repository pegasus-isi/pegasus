/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.vdl.router;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.*;
import org.griphyn.vdl.dax.*;

import java.util.HashMap;
import java.util.Vector;
import java.util.HashSet;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.io.*;

/**
 * This class stores state when constructing the DAG for output. It
 * is expected that for each DAG generation, one instance of this
 * class performs the state tracking. The class is tightly coupled
 * to the class that performs the routing.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Route
 */
public class BookKeeper
{
  /**
   * This contains the set of visited derivations.
   */
  private HashMap m_visited;

  /**
   * This map tracks the set of parents for each child derivation.
   * Each value item in the map is a set.
   */
  private HashMap m_parent;

  /**
   * The content of the abstract DAG for XML (DAX) is created after
   * the alternatives are found. Maybe in a later version, it will
   * be created in parallel with adding derivations?
   */
  private ADAG m_dax;

  /**
   * Unfortunately, for this release, we need to map the shortID of
   * a derivation to some number that is acceptable to NMTOKEN, ID and
   * IDREF.
   */
  private SequenceMapping m_mapping;

  /**
   * The <code>Profile</code> elements in nested compound TR can
   * be equally nested.
   */
  private ListStack m_profileStack;

  /**
   * The names of compound transformations that we got through. This
   * data will also be added to the job description once a job is added
   * to the book-keeper.
   */
  private ArrayList m_transformations;

  /**
   * The temporary files must be unique on a per-workflow basis.
   */
  private HashSet m_tempfiles;

  /**
   * To create temporary filenames, we need an entropy source.
   */
  private Random m_rng;

  /**
   * Default ctor.
   */
  public BookKeeper()
  {
    this.m_parent = new HashMap();
    this.m_visited = new HashMap();
    this.m_dax = new ADAG();
    this.m_mapping = new SequenceMapping();
    this.m_profileStack = new ListStack();
    this.m_transformations = new ArrayList();
    this.m_tempfiles = new HashSet();
    this.m_rng = new Random();
  }

  /**
   * Accessor: push a vector of profiles down the profile stack.
   * @param profiles is a list of profiles, may be empty.
   * @see #popProfile()
   */
  public void pushProfile( java.util.List profiles )
  {
    this.m_profileStack.push(profiles);
  }

  /**
   * Accessor: pop a vector of profiles from the profile stack.
   * @return the last active vector of profiles, may be empty.
   * @see #pushProfile( java.util.List )
   */
  public java.util.List popProfile()
  {
    return this.m_profileStack.pop();
  }

  /**
   * Accessor: obtains all profiles on the profile stack.
   * @return all stacked profiles in one structure, may be empty.
   */
  public java.util.List getAllProfiles()
  {
    return this.m_profileStack.flatten();
  }

  /**
   * Accessor: push the FQDI of a transformation onto a stack.
   * @param fqdi is the fully-qualified definition identifier.
   * @see #popTransformation()
   */
  public void pushTransformation( String fqdi )
  {
    this.m_transformations.add(fqdi);
  }

  /**
   * Accessor: pop an FQDI from the stack of transformations.
   * @return the last FQDI pushed, may be empty for an empty stack.
   * @see #pushTransformation( String )
   */
  public String popTransformation()
  {
    int pos = this.m_transformations.size()-1;
    if ( pos >= 0 ) return (String) this.m_transformations.remove(pos);
    else return null;
  }

  /**
   * Accessor: obtains all transformations that were pushed as space
   * separated string.
   *
   * @return all transformation names, may be empty.
   * @see #addJob( Job )
   */
  public String getAllTransformations()
  {
    StringBuffer result = new StringBuffer();

    for ( Iterator i=this.m_transformations.iterator(); i.hasNext(); ) {
      result.append( (String) i.next() );
      if ( i.hasNext() ) result.append(' ');
    }

    return result.toString();
  }

  /**
   * Accessor: Returns the constructed internal DAX keeper.
   *
   * @return the abstract DAX description. Note that this may be empty!
   */
  public ADAG getDAX( String name )
  {
    this.m_dax.setName(name);
    return this.m_dax;
  }

  /**
   * Accessor: Appends a job definition to the DAX structure.
   */
  public boolean addJob( Job job )
  {
    return this.m_dax.addJob(job);
  }

  /**
   * This method updates the "cursor" position with a new derivation.
   * The cursor helps tracking derivations while a DAG is being produced.
   *
   * @param dv is the new current derivation that the cursor will be set to.
   * @see #getCurrent()
   */
  public void setCurrent( HasPass dv )
  {
    // assign a new cursor
    Logging.instance().log( "state", 2,
			    "NOOP: setting cursor to " + dv.identify() );
  }

  /**
   * Obtains the current cursor position.
   * @return the derivation that the cursor is located at.
   * @see #setCurrent( HasPass )
   */
  public HasPass getCurrent()
  {
    throw new RuntimeException( "method not implemented" );
  }

  /**
   * Maps a DV identification to a name that can be put into the XML
   * datatypes NMTOKEN, ID and IDREF. The identification used to be
   * the DV's short ID, but recent changes use the full ID.
   *
   * @param id is the derivation identifier
   * @return an XML-compatible job id
   */
  public String mapJob( String id )
  {
    String s = this.m_mapping.forward(id);
    Logging.instance().log( "state", 3, "mapJob(" + id + ") = " + s );
    return s;
  }

  /**
   * Obtains an existing mapping of a DV indentification to a job id.
   * The identification used to be the DV's short ID, but recent changes
   * use the full ID.
   *
   * @param id is the derivation identifier
   * @return a job identifier, or null if no such mapping exists.
   */
  public String jobOf( String id )
  {
    String s = this.m_mapping.get(id);
    Logging.instance().log( "state", 3, "jobOf(" + id + ") = " +
			    (s == null ? "(null)" : s) );
    return s;
  }

  /**
   * Accessor: Obtains the level of a given job from the DAX structure.
   * @param jobid is the job identifier
   * @return the level of the job, or -1 in case of error (no such job).
   */
  private int getJobLevel( String jobid )
  {
    Job j = this.m_dax.getJob(jobid);
    return ( j == null ? -1 : j.getLevel() );
  }

  /**
   * Accessor: Recursively adjusts job level backwards.
   *
   * @see org.griphyn.vdl.dax.ADAG#adjustLevels( String, int )
   */
  private int adjustJobLevels( String startjob, int distance )
  {
    return this.m_dax.adjustLevels( startjob, distance );
  }

  /**
   * Adds a parent set for the given element to the state
   * @param current is the current derivation to add parents for
   * @param parents are the parents, any number, to add for the given element
   * @return true, if the parent was not know before or the currelem is empty,
   * false, if the parent was added previously.
   */
  public boolean addParent( HasPass current, Set parents )
  {
    String id = current.identify();

    if ( parents.size() == 0 ) {
      // nothing to do
      Logging.instance().log( "state", 4, "no parents for " + id );
      return false;
    }

    if ( ! this.m_parent.containsKey(id) ) {
      Logging.instance().log( "state", 4, "adding new parent space for " + id );
      this.m_parent.put( id, new TreeSet() );
    }

    // check for modifications
    boolean modified = ((Set) this.m_parent.get(id)).addAll( parents );
    if ( modified ) {
      Logging.instance().log( "state", 4, "CHILD " + id + " PARENT " +
			      this.m_parent.get(id).toString() );

      // add to parent/child DAX relationship			// OLD
      // NEW: adjust job level count as necessary
      String jobid = mapJob(id);				// OLD
      int level = getJobLevel(jobid);
      for ( Iterator i=parents.iterator(); i.hasNext(); ) {	// OLD
	String pid = mapJob((String) i.next());			// OLD
	this.m_dax.addChild( jobid, pid );			// OLD
	int plevel = getJobLevel(pid);
	if ( level != -1 && plevel != -1 && level >= plevel ) {
	  int adjustment = level - plevel + 1;
 	  Logging.instance().log( "state", 3, "adjusting levels starting at " +
 				  pid + " by " + adjustment );

	  adjustJobLevels( pid, adjustment );
	}
      }
    }

    return modified;
  }

  /**
   * This method marks a derivation as visited in the set of visited nodes.
   *
   * @param current is the caller to add to the visited node set.
   * @param real is the value to add for the given caller.
   * @return <code>true</code>, if the caller was previously unknown
   * to the visited set.
   */
  public boolean addVisited( HasPass current, Set real )
  {
    String id = current.identify();
    boolean exists = this.m_visited.containsKey(id);
    if ( exists ) {
      Set temp = (Set) this.m_visited.get(id);
      Logging.instance().log( "state", 2, "already visited node " + id +
			      " keeping " + temp );
    } else {
      this.m_visited.put( id, real );
      Logging.instance().log( "state", 2, "adding visited node " + id +
			      " with " + real );
    }
    return exists;
  }

  /**
   * Checks if a node was previously visited. The visited nodes have to
   * be tracked in any kind of breadth first and depth first search.
   *
   * @param dv is the derivation to check, if it was visited before.
   * @return true, if the derivation was already visited previously.
   */
  public boolean wasVisited( HasPass dv )
  {
    String id = dv.identify();
    boolean result = this.m_visited.containsKey(id);
    Logging.instance().log( "state", 5, "wasVisited(" + id + ") = " + result );
    return result;
  }

  public Set getVisited( HasPass dv )
  {
    String id = dv.identify();
    Set result = (Set) this.m_visited.get(id);
    if ( result == null ) result = new TreeSet();
    Logging.instance().log( "state", 5, "getVisited(" + id + ") = " + result );
    return result;
  }

  /**
   * This method helps to track the input and output files. From this
   * information, the input, intermediary and output files of the
   * complete DAG can be constructed. This method does allow for inout.
   *
   * @param lfnset is a set of LFN instances, which encapsulate their
   * respective linkage.
   * @see java.util.AbstractCollection#addAll( java.util.Collection )
   */
  public void addFilenames( Collection lfnset )
  {
    if ( lfnset == null )
      return; // nothing to do

    for ( Iterator i=lfnset.iterator(); i.hasNext(); ) {
      LFN lfn = (LFN) i.next();
      String name = lfn.getFilename();
      if ( lfn.getLink() == LFN.INPUT || lfn.getLink() == LFN.INOUT ) {
	this.m_dax.addFilename( name, true , lfn.getTemporary(),
				lfn.getDontRegister(), lfn.getDontTransfer() );
	Logging.instance().log( "state", 5, "adding input filename " + name );
      }
      if ( lfn.getLink() == LFN.OUTPUT || lfn.getLink() == LFN.INOUT ) {
	this.m_dax.addFilename( name, false, lfn.getTemporary(),
				lfn.getDontRegister(), lfn.getDontTransfer() );
	Logging.instance().log( "state", 5, "adding output filename " + name );
      }
    }
  }

  /**
   * String constant describing the six X.
   */
  static final String XXXXXX = "XXXXXX";

  /**
   * Where to draw filename characters from.
   */
  private static final String filenamechars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";

  /**
   * Creates a unique temporary filename. The new name is registered
   * locally to ensure uniqueness. A string of multiple capitol X, at
   * least six, is replaced with some random factor.
   *
   * @param hint is a filename hint.
   * @param suffix is the suffix for the filename.
   * @return a somewhat unique filename - for this workflow only.
   */
  public String createTempName( String hint, String suffix )
  {
    String result = null;

    // sanity checks
    if ( hint == null || hint.length() == 0 ) hint = XXXXXX;
    if ( suffix == null ) suffix = ".tmp";

    // where are the X
    int where = hint.lastIndexOf( XXXXXX );
    if ( where == -1 ) {
      hint += XXXXXX;
      where = hint.lastIndexOf( XXXXXX );
    }

    // create a changable buffer
    StringBuffer sb = new StringBuffer( hint );

    // find end of substitution area
    int last = where;
    while ( last < sb.length() && sb.charAt(last) == 'X' ) ++last;

    // substitute until done (FIXME: may be a forever in rare cases)
    for (;;) {
      // create and substitute
      for ( int i=where; i<last; ++i ) {
	sb.setCharAt(i, filenamechars.charAt( m_rng.nextInt( filenamechars.length() ) ) );
      }

      // check uniqueness
      if ( suffix.length() > 0 )
	result = sb.toString() + suffix;
      if ( ! m_tempfiles.contains(result) ) {
	m_tempfiles.add(result);
	break;
      }
    }

    return result;
  }

  /**
   * Detects valid results in the ADAG as opposed to an empty shell.
   *
   * @return true, if the ADAG is an empty shell.
   */
  public boolean isEmpty()
  {
    Iterator i = this.m_dax.iterateJob();
    return ( ! i.hasNext() );
  }

  /**
   * This method is a possibly more memory efficient version of
   * constructing a DAG.
   * @param stream is a generic stream to put textual results onto.
   */
  public void toString( Writer stream )
    throws IOException
  { this.m_dax.toString(stream); }

  /**
   * dumps the state of this object into human readable format.
   */
  public String toString()
  { return this.m_dax.toString(); }

  /**
   * This method is a possibly more memory efficient version of
   * constructing a DAX.
   * @param stream is a generic stream to put XML results onto.
   * @param indent is the initial indentation level.
   * @param namespace is an optional XML namespace.
   */
  public void toXML( Writer stream, String indent, String namespace )
    throws IOException
  { this.m_dax.toXML(stream,indent,namespace); }

  /**
   * This method is a possibly more memory efficient version of
   * constructing a DAX.
   * @param stream is a generic stream to put XML results onto.
   * @param indent is the initial indentation level.
   */
  public void toXML( Writer stream, String indent )
    throws IOException
  { this.m_dax.toXML(stream,indent,null); }

  /**
   * Dumps the state of this object into machine readable XML.
   * @param indent is the initial indentation level.
   * @param namespace is an optional XML namespace.
   */
  public String toXML( String indent, String namespace )
  { return this.m_dax.toXML(indent,namespace); }
}
