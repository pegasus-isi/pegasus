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

package org.griphyn.vdl.dax;

import edu.isi.pegasus.common.util.Separator;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import org.griphyn.vdl.classes.LFN;

/**
 * This class defines the specifics of a job to run in an abstract manner. All filename references
 * still refer to logical files. All references transformations also refer to logical
 * transformtions, though through <code>Profile</code> physical location hints can be passed.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Profile
 * @see Filename
 */
public class Job extends DAX implements Cloneable {
    /**
     * Each transformation and derivation resides in a namespace. Mind that namespaces are currently
     * flat. If you need to impose any kind of hierarchy, please do so yourself, e.g. use periods
     * between hierarchy intifiers. The namespace is part of the key identifying a logical
     * transformation.
     */
    private String m_namespace = null;

    /**
     * Each transformation and derivation can be identified by a name. For a transformation, this is
     * part of the logical transformation name. Derivations can be anonymous, and not use a name.
     */
    private String m_name = null;

    /**
     * Each transformation and derivation has a version associated with their definition. While a
     * version number is mandatory for transformation, being part of the logical transformation key,
     * a derivation can remain anonymous without version.
     */
    private String m_version = null;

    /** DVs are dynamically mapped to TR. This variable records the namespace of the chosen DV. */
    private String m_dv_namespace = null;

    /** DVs are dynamically mapped to TR. This variable records the name of the chosen DV. */
    private String m_dv_name = null;

    /** DVs are dynamically mapped to TR. This variable records the version of the chosen DV. */
    private String m_dv_version = null;

    /**
     * Each job also gets a unique id to refer to it from references.
     *
     * @see Child
     */
    private String m_id = null;

    /**
     * A compound transformation may be nested. For purposes of display and grouping, it is feasible
     * to record the compound transformation chain. This is an optional argument.
     */
    private String m_chain = null;

    /**
     * stdin is a {@link Filename} which must be of input linkage. Alternatively, it may be unused
     * (null).
     */
    private Filename m_stdin = null;

    /**
     * stdout is a {@link Filename} which must be of output linkage. Alternatively, it may be unused
     * (null).
     */
    private Filename m_stdout = null;

    /**
     * stderr is a {@link Filename} which must be of output linkage. Alternatively, it may be unused
     * (null).
     */
    private Filename m_stderr = null;

    /** The level is set by the topological sort. It is optional. */
    private int m_level = -1;

    /**
     * The argument list describes the command line arguments as sum of strings and filename
     * references. Each element is a <code>Leaf</code>, which is either implemented by a <code>
     * Filename</code> or <code>PseudoText</code> instance.
     *
     * @see Leaf
     * @see Filename
     * @see PseudoText
     */
    private ArrayList m_argumentList;

    /**
     * The profile list encapsulates scheduler specific data in a generic structure.
     *
     * @see Profile
     */
    private ArrayList m_profileList;

    /**
     * The uses list contains all filenames that were originally part of the linkage via the actual
     * arguments and formal argument defaults.
     *
     * @see Filename
     */
    private ArrayList m_usesList;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance, semi-deep copy
     */
    public Object clone() {
        Job result =
                new Job(
                        this.m_namespace,
                        this.m_name,
                        this.m_version,
                        this.m_id,
                        this.m_dv_namespace,
                        this.m_dv_name,
                        this.m_dv_version);

        result.setChain(this.getChain());
        result.setStdin(this.getStdin());
        result.setStdout(this.getStdout());
        result.setStderr(this.getStderr());

        for (int index = 0; index < this.m_argumentList.size(); ++index) {
            result.setArgument(index, (Leaf) this.getArgument(index).clone());
        }
        for (int index = 0; index < this.m_profileList.size(); ++index) {
            result.setProfile(index, (Profile) this.getProfile(index).clone());
        }
        for (int index = 0; index < this.m_usesList.size(); ++index) {
            result.setUses(index, (Filename) this.getUses(index).clone());
        }

        result.setLevel(this.getLevel());
        return result;
    }

    /** Default ctor: Note that a job must be named. */
    public Job() {
        this.m_argumentList = new ArrayList();
        this.m_profileList = new ArrayList();
        this.m_usesList = new ArrayList();
    }

    /**
     * Convenience ctor. Any job must be named. The job id will be assembled from the name.
     *
     * @param namespace is the namespace that the TR resides in.
     * @param name is the name of the job as logical TR identifier.
     * @param version is a version information string of the TR.
     * @param id is a short DAX-unique ID to distinguish this job from other jobs in the same DAG.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public Job(String namespace, String name, String version, String id) {
        this.m_namespace = namespace;
        this.m_name = name;
        this.m_version = version;
        this.m_id = id;
        this.m_argumentList = new ArrayList();
        this.m_profileList = new ArrayList();
        this.m_usesList = new ArrayList();
    }

    /**
     * Convenience ctor. Any job must be named. The job id will be assembled from the name.
     *
     * @param namespace is the namespace that the TR resides in.
     * @param name is the name of the job as logical TR identifier.
     * @param version is a version information string of the TR.
     * @param id is a short DAX-unique ID to distinguish this job from other jobs in the same DAG.
     * @param dv_namespace is the namespace of the DV for this job.
     * @param dv_name is the name of the DV that produced this job.
     * @param dv_version is the version of the DV for this job.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public Job(
            String namespace,
            String name,
            String version,
            String id,
            String dv_namespace,
            String dv_name,
            String dv_version) {
        this.m_namespace = namespace;
        this.m_name = name;
        this.m_version = version;
        this.m_id = id;
        this.m_dv_namespace = dv_namespace;
        this.m_dv_name = dv_name;
        this.m_dv_version = dv_version;
        this.m_argumentList = new ArrayList();
        this.m_profileList = new ArrayList();
        this.m_usesList = new ArrayList();
    }

    /**
     * Accessor: Adds an argument to the list of arguments
     *
     * @param vArgument is an argument fragment to append to the command line arguments.
     * @see Leaf
     */
    public void addArgument(Leaf vArgument) {
        this.m_argumentList.add(vArgument);
    }

    /**
     * Accessor: Inserts an argument at an arbitrary place into the list. Each component in this
     * vector with an index greater or equal to the specified index is shifted upward to have an
     * index one greater than the value it had previously.
     *
     * @param index is the position to insert an argument
     * @param vArgument is the argument fragment to insert at the specified place.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see #setArgument( int, Leaf )
     * @see #getArgument( int )
     * @see Leaf
     */
    public void addArgument(int index, Leaf vArgument) throws IndexOutOfBoundsException {
        this.m_argumentList.add(index, vArgument);
    }

    /**
     * Compares two strings, each of which may be null.
     *
     * @param a is the first string
     * @param b is the other string to compare to
     * @return true, if both strings are null, or both strings match.
     */
    private boolean matchWithNull(String a, String b) {
        return (a == null ? (b == null) : a.equals(b));
    }

    /**
     * Accessor: Appends a profile definition to the list of profiles. This method provides extra
     * magic, ensuring unique namespace and key identifiers in the profile list. If the ns:id of the
     * profile to add already exists in the list, the list entry will be overwritten with the new
     * version.
     *
     * <p>FIXME: The profile should carry a state to signal appending or replacing.
     *
     * @param vProfile is the profile to append to remembered profiles.
     * @return <code>false</code> for a new elements, and <code>true</code>, if an existing element
     *     was overwritten.
     * @see Profile
     */
    public boolean addProfile(Profile vProfile) {
        boolean result = false;

        // the list is not sorted (and should not be sorted), thus linear search
        for (Iterator i = this.m_profileList.iterator(); i.hasNext(); ) {
            Profile p = (Profile) i.next();
            if (matchWithNull(p.getNamespace(), vProfile.getNamespace())
                    && matchWithNull(p.getKey(), vProfile.getKey())) {
                // element of this name exists, remove
                i.remove();
                result = true;
            }
        }
        this.m_profileList.add(vProfile);
        return result;
    }

    /**
     * Accessor: Inserts a profile definition at an arbitrary position into the list of profiles.
     * Each component in this vector with an index greater or equal to the specified index is
     * shifted upward to have an index one greater than the value it had previously.
     *
     * @param index is the position to insert the definitions into.
     * @param vProfile is the profile to append to remembered profiles.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see #setProfile( int, Profile )
     * @see #getProfile( int )
     * @see Profile
     */
    public void addProfile(int index, Profile vProfile) throws IndexOutOfBoundsException {
        this.m_profileList.add(index, vProfile);
    }

    /**
     * Accessor: Appends a filename to the list of used filenames.
     *
     * @param filename is the LFN to append to remembered filenames.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see Filename
     */
    public void addUses(Filename filename) {
        this.m_usesList.add(filename);
    }

    /**
     * Accessor: Inserts a logical filename at an arbitrary position into the list of used
     * filenames. Each component in this vector with an index greater or equal to the specified
     * index is shifted upward to have an index one greater than the value it had previously.
     *
     * @param index is the position to insert the definitions into.
     * @param filename is the LFN to append to remembered filenames.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see #setUses( int, Filename )
     * @see #getUses( int )
     * @see Filename
     */
    public void addUses(int index, Filename filename) throws IndexOutOfBoundsException {
        this.m_profileList.add(index, filename);
    }

    /**
     * Accessor: Provides an iterator for the argument fragment list.
     *
     * @return the iterator for the commandline argument fragments.
     * @see Leaf
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateArgument() {
        return Collections.enumeration(this.m_argumentList);
    }

    /**
     * Accessor: Provides an iterator for the <code>Profile</code> list.
     *
     * @return the iterator for the <code>Profile</code> list.
     * @see Profile
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateProfile() {
        return Collections.enumeration(this.m_profileList);
    }

    /**
     * Accessor: Provides an iterator for the used <code>Filename</code> list.
     *
     * @return the iterator for the <code>Filename</code> list.
     * @see Filename
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateUses() {
        return Collections.enumeration(this.m_usesList);
    }

    /**
     * Accessor: Obtains an argument fragment from an arbitrary position.
     *
     * @param index is the place to look up the element at.
     * @return the argument fragment at the specified place.
     * @throws IndexOutOfBoundsException if the referenced position does not exist.
     * @see #addArgument( int, Leaf )
     * @see #setArgument( int, Leaf )
     * @see Leaf
     */
    public Leaf getArgument(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_argumentList.size()))
            throw new IndexOutOfBoundsException();

        return (Leaf) this.m_argumentList.get(index);
    }

    /**
     * Accessor: Obtains the complete commandline arguments
     *
     * @return an array with all commandline argument fragments inside.
     * @see #setArgument( Leaf[] )
     * @see Leaf
     * @deprecated Use the new Collection based interfaces
     */
    public Leaf[] getArgument() {
        int size = this.m_argumentList.size();
        Leaf[] mArray = new Leaf[size];
        System.arraycopy(this.m_argumentList.toArray(new Leaf[0]), 0, mArray, 0, size);
        return mArray;
    }

    /**
     * Accessor: Obtains the count of fragments in the argument list.
     *
     * @return the number of arguments in the commandline argument list.
     * @see Leaf
     */
    public int getArgumentCount() {
        return this.m_argumentList.size();
    }

    /**
     * Accessor: Gets an array of all values that constitute the current argument line. This list is
     * read-only.
     *
     * @return an array with a mixture of either <code>PseudoText</code> or <code>Filename</code>
     *     values.
     * @see #setArgument( Collection )
     * @see PseudoText
     * @see Filename
     */
    public java.util.List getArgumentList() {
        return Collections.unmodifiableList(this.m_argumentList);
    }

    /**
     * Accessor: Obtains the XML list attribute which contains the chain of compound transformations
     * that lead to this job.
     *
     * @return the chain, which may be empty or even <code>null</code>.
     * @see #setChain( String )
     */
    public String getChain() {
        return this.m_chain;
    }

    /**
     * Accessor: Obtains the unique ID of this job. The ID will be used in references in the
     * dependency list.
     *
     * @return The unique id from this job.
     * @see #setID( String )
     * @see Child
     */
    public String getID() {
        return this.m_id;
    }

    /**
     * Accessor: Obtains the current name of the job.
     *
     * @return the name as logical TR string for this job.
     * @see #setName( String )
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor: Obtains the current namespace that is used for the definition. Note that a
     * namespace is part of the key for any logical transformation.
     *
     * @return the namespace the definition resides in, or null, if no namespace was defined.
     * @see #setNamespace(java.lang.String)
     */
    public String getNamespace() {
        return this.m_namespace;
    }

    /**
     * Accessor: Obtains the current namespace from the DV that created this job. Note that a
     * namespace is part of the key for any DV.
     *
     * @return the namespace the DV resides in, or null, if no namespace was defined.
     * @see #setDVNamespace(java.lang.String)
     * @see #setDV( String, String, String )
     */
    public String getDVNamespace() {
        return this.m_dv_namespace;
    }

    /**
     * Accessor: Obtains the current name from the DV that created this job. Note that a name is
     * part of the key for any DV.
     *
     * @return the name the DV resides in.
     * @see #setDVName(java.lang.String)
     * @see #setDV( String, String, String )
     */
    public String getDVName() {
        return this.m_dv_name;
    }

    /**
     * Accessor: Obtains the current version from the DV that created this job. Note that a version
     * is part of the key triple for any DV.
     *
     * @return the version the DV resides in, or null, if no version was defined.
     * @see #setDVVersion(java.lang.String)
     * @see #setDV( String, String, String )
     */
    public String getDVVersion() {
        return this.m_dv_version;
    }

    /**
     * Accessor: Obtains the current setting of stdin redirection.
     *
     * @return a filename which will be associated with stdin, or <code>null</code>, if unset.
     * @see #setStdin( Filename )
     */
    public Filename getStdin() {
        return this.m_stdin;
    }

    /**
     * Accessor
     *
     * @see #setLevel(int)
     */
    public int getLevel() {
        return this.m_level;
    }

    /**
     * Accessor.
     *
     * @param level
     * @see #getLevel()
     */
    public void setLevel(int level) {
        this.m_level = level;
    }

    /**
     * Accessor: Obtains the current setting of stdout redirection.
     *
     * @return a filename which will be associated with stdout, or <code>null</code>, if unset.
     * @see #setStdout( Filename )
     */
    public Filename getStdout() {
        return this.m_stdout;
    }

    /**
     * Accessor: Obtains the current setting of stderr redirection.
     *
     * @return a filename which will be associated with stderr, or <code>null</code>, if unset.
     * @see #setStderr( Filename )
     */
    public Filename getStderr() {
        return this.m_stderr;
    }

    /**
     * Accessor: Obtains an <code>Profile</code> at an arbitrary position.
     *
     * @param index is the place to look up the element at.
     * @exception IndexOutOfBoundsException if the referenced position does not exist.
     * @see #addProfile( int, Profile )
     * @see #setProfile( int, Profile )
     * @see Profile
     */
    public Profile getProfile(int index) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_profileList.size()))
            throw new IndexOutOfBoundsException();

        return (Profile) this.m_profileList.get(index);
    }

    /**
     * Accessor: Obtains all profiles.
     *
     * @return an array with all profiles inside.
     * @see #setProfile( Profile[] )
     * @see Profile
     * @deprecated Use the new Collection based interfaces
     */
    public Profile[] getProfile() {
        int size = this.m_argumentList.size();
        Profile[] mArray = new Profile[size];
        System.arraycopy(this.m_profileList.toArray(new Profile[0]), 0, mArray, 0, size);
        return mArray;
    }

    /**
     * Accessor: Counts the number of profile specifications known to this job.
     *
     * @return the number of profiles
     * @see Profile
     */
    public int getProfileCount() {
        return this.m_profileList.size();
    }

    /**
     * Accessor: Obtain a read-only copy of the list of all <code>Profile</code> specifications.
     *
     * @return a collection containing the scheduler specific environment options for the job.
     * @see #setProfile( Collection )
     * @see Profile
     */
    public java.util.List getProfileList() {
        return Collections.unmodifiableList(this.m_profileList);
    }

    /**
     * Accessor: Obtains an <code>Filename</code> at an arbitrary position.
     *
     * @param index is the place to look up the element at.
     * @exception IndexOutOfBoundsException if the referenced position does not exist.
     * @see #addUses( int, Filename )
     * @see #setUses( int, Filename )
     * @see Filename
     */
    public Filename getUses(int index) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_usesList.size())) throw new IndexOutOfBoundsException();

        return (Filename) this.m_usesList.get(index);
    }

    /**
     * Accessor: Obtain a copy of the list of all <code>Filename</code> specifications, that were
     * part of the arguments that generated this job.
     *
     * @return an array containing the used filenames for the job.
     * @see #setUses( Filename[] )
     * @see Filename
     * @deprecated Use the new Collection based interfaces
     */
    public Filename[] getUses() {
        int size = this.m_usesList.size();
        Filename[] mArray = new Filename[size];
        System.arraycopy(this.m_usesList.toArray(new Filename[0]), 0, mArray, 0, size);
        return mArray;
    }

    /**
     * Accessor: Counts the number of argument-referenced logical filenames known to this job.
     *
     * @return the number of LFNs.
     * @see Filename
     */
    public int getUsesCount() {
        return this.m_usesList.size();
    }

    /**
     * Accessor: Obtain a copy of the list of all <code>Filename</code> specifications, that were
     * part of the arguments that generated this job. This list is read-only.
     *
     * @return an array containing the used filenames for the job.
     * @see #setUses( Collection )
     * @see Filename
     */
    public java.util.List getUsesList() {
        return Collections.unmodifiableList(this.m_usesList);
    }

    /**
     * Accessor: Obtains the current version of the definition. A version is an integral part of a
     * logical transformation.
     *
     * @return the version number of this definition, or null, if no version number was defined.
     * @see #setVersion(java.lang.String)
     */
    public String getVersion() {
        return this.m_version;
    }

    /**
     * Accessor: Provides an iterator to the internal list of all commandline argument elements.
     *
     * @return an iterator to walk the list with.
     */
    public Iterator iterateArgument() {
        return this.m_argumentList.iterator();
    }

    /**
     * Accessor: Provides an iterator to the internal list of all profiles.
     *
     * @return an iterator to walk the list with.
     */
    public Iterator iterateProfile() {
        return this.m_profileList.iterator();
    }

    /**
     * Accessor: Provides an iterator to the internal list of used filenames.
     *
     * @return an iterator to walk the list with.
     */
    public Iterator iterateUses() {
        return this.m_usesList.iterator();
    }

    /**
     * Accessor: Provides a list iterator to the internal list of all commandline argument elements.
     *
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateArgument() {
        return this.m_argumentList.listIterator();
    }

    /**
     * Accessor: Provides a list iterator to the internal list of all commandline argument elements.
     *
     * @param start is the start index
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateArgument(int start) {
        return this.m_argumentList.listIterator(start);
    }

    /**
     * Accessor: Provides a list iterator to the internal list of all profiles.
     *
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateProfile() {
        return this.m_profileList.listIterator();
    }

    /**
     * Accessor: Provides a list iterator to the internal list of all profiles.
     *
     * @param start is the start index
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateProfile(int start) {
        return this.m_profileList.listIterator(start);
    }

    /**
     * Accessor: Provides a list iterator to the internal list of all used filenames.
     *
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateUses() {
        return this.m_usesList.listIterator();
    }

    /**
     * Accessor: Provides a list iterator to the internal list of all used filenames.
     *
     * @param start is the start index
     * @return a list iterator to walk the list with.
     */
    public ListIterator listIterateUses(int start) {
        return this.m_usesList.listIterator(start);
    }

    /** Accessor: Removes all commandline arguments pieces. */
    public void removeAllArgument() {
        this.m_argumentList.clear();
    }

    /**
     * Accessor: Removes all profile declarations.
     *
     * @see Profile
     */
    public void removeAllProfile() {
        this.m_profileList.clear();
    }

    /**
     * Accessor: Removes all known <code>Filename</code>s.
     *
     * @see Filename
     */
    public void removeAllUses() {
        this.m_usesList.clear();
    }

    /**
     * Accessor: Removes a commandline argument fragment from the commandline. Each component in
     * this vector with an index greater or equal to the specified index is shifted downward to have
     * an index one smaller than the value it had previously. The size of this vector is decreased
     * by 1.
     *
     * @param index is the position to remove the argument fragment from.
     * @return the removed fragment.
     * @exception ArrayIndexOutOfBoundsException if the index was invalid.
     * @see Leaf
     */
    public Leaf removeArgument(int index) {
        return (Leaf) this.m_argumentList.remove(index);
    }

    /**
     * Accessor: Removes a profile. Each component in this vector with an index greater or equal to
     * the specified index is shifted downward to have an index one smaller than the value it had
     * previously. The size of this vector is decreased by 1.
     *
     * @param index is the position to remove the profile from.
     * @return the removed Profile.
     * @exception ArrayIndexOutOfBoundsException if the index was invalid.
     * @see Profile
     */
    public Profile removeProfile(int index) {
        return (Profile) this.m_profileList.remove(index);
    }

    /**
     * Accessor: Removes a used filename from the list of argument-referenced LFNs. Each component
     * in this vector with an index greater or equal to the specified index is shifted downward to
     * have an index one smaller than the value it had previously. The size of this vector is
     * decreased by 1.
     *
     * @param index is the position to remove the filename from.
     * @return the removed Filename
     * @exception ArrayIndexOutOfBoundsException if the index was invalid.
     * @see Filename
     */
    public Filename removeUses(int index) {
        return (Filename) this.m_usesList.remove(index);
    }

    /**
     * Accessor: Overwrites an commandline argument fragment with a new one.
     *
     * @param index is the position to overwrite the element at
     * @param vArgument is the new commandline argument fragment.
     * @exception IndexOutOfBoundsException if the position does not exist.
     * @see Leaf
     * @see #addArgument( int, Leaf )
     * @see #getArgument( int )
     */
    public void setArgument(int index, Leaf vArgument) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_argumentList.size()))
            throw new IndexOutOfBoundsException();

        this.m_argumentList.set(index, vArgument);
    }

    /**
     * Accessor: Replace the commandline arguments with a new commandline argument fragment list.
     *
     * @param argumentArray is the new commandline argument array.
     * @see #getArgument()
     * @see Leaf
     * @deprecated Use the new Collection based interfaces
     */
    public void setArgument(Leaf[] argumentArray) {
        this.m_argumentList.clear();
        this.m_argumentList.addAll(Arrays.asList(argumentArray));
    }

    /**
     * Accessor: Replace the commandline arguments with a new commandline argument fragment list.
     *
     * @param arguments is the new commandline argument collection.
     * @see #getArgumentList()
     * @see Leaf
     */
    public void setArgument(java.util.Collection arguments) {
        this.m_argumentList.clear();
        this.m_argumentList.addAll(arguments);
    }

    /**
     * Accessor: Sets the current name of the job. If the job ID was not set previously, it will be
     * set to the job name. FIXME: We need to use the primary key triple for logical TR!
     *
     * @param name the new name as logical TR string for this job.
     * @see #getName()
     */
    public void setName(String name) {
        if (this.m_id == null) this.m_id = name;
        this.m_name = name;
    }

    /**
     * Accessor: Sets or overwrites the TR namespace identifier.
     *
     * @param namespace
     * @see #getNamespace()
     */
    public void setNamespace(String namespace) {
        this.m_namespace = namespace;
    }

    /**
     * Accessor: Sets or overwrites the DV namespace identifier.
     *
     * @param namespace
     * @see #getDVNamespace()
     * @see #setDV( String, String, String )
     */
    public void setDVNamespace(String namespace) {
        this.m_dv_namespace = namespace;
    }

    /**
     * Accessor: Sets or overwrites the DV name identifier.
     *
     * @param name
     * @see #getDVName()
     * @see #setDV( String, String, String )
     */
    public void setDVName(String name) {
        this.m_dv_name = name;
    }

    /**
     * Accessor: Sets or overwrites the DV version identifier.
     *
     * @param version
     * @see #getDVVersion()
     * @see #setDV( String, String, String )
     */
    public void setDVVersion(String version) {
        this.m_dv_version = version;
    }

    /**
     * Accessor: Sets or overwrites the DV FQDN triple.
     *
     * @param namespace is the DV's namespace, may be null
     * @param name is the DV name, should not be null
     * @param version is the DV version, may be null
     * @see #getDVNamespace()
     * @see #getDVName()
     * @see #getDVVersion()
     * @see #setDVNamespace( String )
     * @see #setDVName( String )
     * @see #setDVVersion( String )
     */
    public void setDV(String namespace, String name, String version) {
        this.m_dv_namespace = namespace;
        this.m_dv_name = name;
        this.m_dv_version = version;
    }

    /**
     * Accessor: Sets the XML list attribute which contains the chain of compound transformations
     * that lead to this job.
     *
     * @param chain is the chain, which may be empty or even <code>null</code>.
     * @see #getChain( )
     */
    public void setChain(String chain) {
        this.m_chain = chain.trim();
    }

    /**
     * Accessor: Sets the unique ID of this job. The ID will be used in references in the dependency
     * list. Please do not use this function.
     *
     * @param id is the new unique id for this job.
     * @see #getID()
     * @see Child
     */
    public void setID(String id) {
        this.m_id = id;
    }

    /**
     * Accessor: Sets or releases the stdin redirection.
     *
     * @param f is a filename which will be associated with stdin, or <code>null</code> to release
     *     redirection.
     * @see #getStdin()
     */
    public void setStdin(Filename f) {
        this.m_stdin = f;
    }

    /**
     * Accessor: Sets or releases the stdout redirection.
     *
     * @param f is a filename which will be associated with stdout, or <code>null</code> to unset.
     * @see #getStdout()
     */
    public void setStdout(Filename f) {
        this.m_stdout = f;
    }

    /**
     * Accessor: Sets or releases the stderr redirection.
     *
     * @param f is a filename which will be associated with stderr, or <code>null</code> to unset.
     * @see #getStderr()
     */
    public void setStderr(Filename f) {
        this.m_stderr = f;
    }

    /**
     * Accessor: Overwrites a profile with a new profile
     *
     * @param index is the position to overwrite the profile at.
     * @param vProfile is the new profile to use in overwriting.
     * @exception IndexOutOfBoundsException if the position does not exist.
     * @see Profile
     * @see #addProfile( int, Profile )
     * @see #getProfile( int )
     */
    public void setProfile(int index, Profile vProfile) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_profileList.size()))
            throw new IndexOutOfBoundsException();

        this.m_profileList.set(index, vProfile);
    }

    /**
     * Accessor: Replace the internal profile list with a new list.
     *
     * @param profileArray is the new list of profiles to use for the job.
     * @see #getProfile()
     * @see Profile
     * @deprecated Use the new Collection based interfaces
     */
    public void setProfile(Profile[] profileArray) {
        this.m_profileList.clear();
        this.m_profileList.addAll(Arrays.asList(profileArray));
    }

    /**
     * Accessor: Replace the internal profile list with a new list.
     *
     * @param profiles is the new list of profiles to use for the job.
     * @see #getProfileList()
     * @see Profile
     */
    public void setProfile(java.util.Collection profiles) {
        this.m_profileList.clear();
        this.m_profileList.addAll(profiles);
    }

    /**
     * Accessor: Overwrites a used filename with a new LFN.
     *
     * @param index is the position to overwrite the LFN at.
     * @param vFilename is the new LFN to use in overwriting.
     * @exception IndexOutOfBoundsException if the position does not exist.
     * @see #addUses( int, Filename )
     * @see #getUses( int )
     * @see Filename
     */
    public void setUses(int index, Filename vFilename) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_usesList.size())) throw new IndexOutOfBoundsException();

        this.m_usesList.set(index, vFilename);
    }

    /**
     * Accessor: Replace the internal list of used LFNs with a new list.
     *
     * @param filenameArray is the new list of argument-referenced filenames to use for the job.
     * @see #getUses()
     * @see Filename
     * @deprecated Use the new Collection based interfaces
     */
    public void setUses(Filename[] filenameArray) {
        this.m_usesList.clear();
        this.m_usesList.addAll(Arrays.asList(filenameArray));
    }

    /**
     * Accessor: Replace the internal list of used LFNs with a new list.
     *
     * @param filenames is a new collection of argument-referenced filenames to use for the job.
     * @see #getUsesList()
     * @see Filename
     */
    public void setUses(java.util.Collection filenames) {
        this.m_usesList.clear();
        this.m_usesList.addAll(filenames);
    }

    /**
     * Accessor: Sets the version of the definition.
     *
     * @param version
     * @see #getVersion()
     */
    public void setVersion(String version) {
        this.m_version = version;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        stream.write("    job ");
        if (this.m_namespace != null) {
            stream.write(this.m_namespace);
            stream.write(Separator.NAMESPACE);
        }
        stream.write(this.m_name);
        if (this.m_version != null) {
            stream.write(Separator.NAME);
            stream.write(this.m_version);
        }

        stream.write(" {");
        stream.write(newline);

        // FIXME: assumes an indentation level
        stream.write("      id=");
        stream.write(this.m_id);
        stream.write(';');
        stream.write(newline);

        // concat all command line fragments into one big string.
        if (this.getArgumentCount() > 0) {
            stream.write("      argument=");
            for (Iterator i = this.m_argumentList.iterator(); i.hasNext(); ) {
                // casting will print a mixed content string or Filename element
                ((Leaf) i.next()).toString(stream);
            }
            stream.write(';');
            stream.write(newline);
        }

        // profiles to be dumped next
        for (Iterator i = this.m_profileList.iterator(); i.hasNext(); ) {
            stream.write("      ");
            ((Profile) i.next()).toString(stream);
            stream.write(';');
            stream.write(newline);
        }

        // finally any bound stdio descriptor
        // FIXME: really need to dump a Filename element!
        if (this.m_stdin != null) {
            stream.write("      stdin=");
            this.m_stdin.toString(stream);
            stream.write(';');
            stream.write(newline);
        }
        if (this.m_stdout != null) {
            stream.write("      stdout=");
            this.m_stdout.toString(stream);
            stream.write(';');
            stream.write(newline);
        }
        if (this.m_stderr != null) {
            stream.write("      stderr=");
            this.m_stderr.toString(stream);
            stream.write(';');
            stream.write(newline);
        }

        // argument-referenced filenames for the job, which may not appear
        // in the argument list nor the profile
        for (Iterator i = this.m_usesList.iterator(); i.hasNext(); ) {
            stream.write("      uses=");
            ((Filename) i.next()).toString(stream);
            stream.write(';');
            stream.write(newline);
        }

        // finish job
        stream.write("    }");
        stream.write(newline);
    }

    /**
     * Helper: Formats the attributes of any {@link Filename} instance, or inherited instances.
     *
     * @param tag is the name of the element to use when formating.
     * @param indent is the indentation of the element, may be null.
     * @param namespace is an optional namespace to use in the tag.
     * @param f is an instance of a <code>Filename</code> object.
     * @param full denotes the full attributes set, or just the stdio attributes set if false.
     * @return the XML-formatted attributes without the element tags.
     */
    private String formatFilename(
            String tag, String indent, String namespace, Filename f, boolean full) {
        StringBuffer result = new StringBuffer(full ? 128 : 80);
        if (namespace != null && namespace.length() > 0) tag = namespace + ":" + tag;
        if (indent != null && indent.length() > 0) result.append(indent);
        result.append('<').append(tag);
        result.append(" file=\"").append(quote(f.getFilename(), true)).append('"');
        result.append(" link=\"").append(LFN.toString(f.getLink())).append('"');

        if (full) {
            result.append(" register=\"").append(Boolean.toString(f.getRegister())).append('"');
            result.append(" transfer=\"").append(LFN.transferString(f.getTransfer())).append('"');
            result.append(" type=\"").append(LFN.typeString(f.getType())).append('"');

            if (f.getOptional()) result.append(" optional=\"").append(f.getOptional()).append("\"");

            if (f.getTemporary() != null)
                result.append(" temporaryHint=\"")
                        .append(quote(f.getTemporary(), true))
                        .append('"');
        } else {
            result.append(" varname=\"").append(f.getVariable()).append('"');
        }

        // add newline and done
        result.append("/>");
        if (indent != null) result.append(System.getProperty("line.separator", "\r\n"));
        return result.toString();
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":job" : "job";
        String tag2 =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":argument"
                        : "argument";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " id=\"", this.m_id);

        // open tag: print TR
        writeAttribute(stream, " namespace=\"", this.m_namespace);
        writeAttribute(stream, " name=\"", this.m_name);
        writeAttribute(stream, " version=\"", this.m_version);

        // misc. attributes like the search tree depth
        if (this.m_level != -1) writeAttribute(stream, " level=\"", Integer.toString(this.m_level));
        if (this.m_chain != null && this.m_chain.length() > 0)
            writeAttribute(stream, " compound=\"", this.m_chain);

        // still opening tag: print DV, if available
        if (this.m_dv_name != null) {
            writeAttribute(stream, " dv-namespace=\"", this.m_dv_namespace);
            writeAttribute(stream, " dv-name=\"", this.m_dv_name);
            writeAttribute(stream, " dv-version=\"", this.m_dv_version);
        }

        // open tag: finish opening tag
        stream.write('>');
        if (indent != null) stream.write(newline);

        // concat all command line fragments into one big string.
        String newindent = indent == null ? null : indent + "  ";
        if (this.getArgumentCount() > 0) {
            if (newindent != null) stream.write(newindent);
            stream.write('<');
            stream.write(tag2);
            stream.write('>');
            for (Iterator i = this.m_argumentList.iterator(); i.hasNext(); ) {
                // casting will print a mixed content string or Filename element
                ((Leaf) i.next()).shortXML(stream, "", namespace, 0x00);
            }
            stream.write("</");
            stream.write(tag2);
            stream.write('>');
            if (indent != null) stream.write(newline);
        }

        // profiles to be dumped next
        for (Iterator i = this.m_profileList.iterator(); i.hasNext(); ) {
            ((Profile) i.next()).toXML(stream, newindent, namespace);
        }

        // finally any bound stdio descriptor
        // FIXME: really need to dump a Filename element!
        if (this.m_stdin != null)
            stream.write(formatFilename("stdin", newindent, namespace, this.m_stdin, false));
        if (this.m_stdout != null)
            stream.write(formatFilename("stdout", newindent, namespace, this.m_stdout, false));
        if (this.m_stderr != null)
            stream.write(formatFilename("stderr", newindent, namespace, this.m_stderr, false));

        // VDL referenced Filenames to be dumped next
        for (Iterator i = this.m_usesList.iterator(); i.hasNext(); ) {
            stream.write(formatFilename("uses", newindent, namespace, (Filename) i.next(), true));
        }

        // finish job
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
