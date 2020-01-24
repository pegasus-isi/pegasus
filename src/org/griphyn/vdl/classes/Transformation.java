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

package org.griphyn.vdl.classes;

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.*;

/**
 * <code>Transformation</code> is an implementation of an abstract VDL <code>Definition</code>. A
 * transformation describes the immutable template of an input, processing, and output (IPO)
 * application. The environment is part of the capture. The template can be parametrized using
 * formal arguments, which are part of the transformation definition. Think of a transformation as
 * something similar to a C function definition. Mutable parts are hidden in the arguments, and
 * instantiated in <code>Derivation</code>.
 *
 * <p>FIXME: The mixing of compounds with simple transformations within the same class instead of
 * hierarchy needs some serious redesigning. Unfortunately, this entails a redesign of just about
 * everything...
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Definition
 * @see Definitions
 * @see Derivation
 */
public class Transformation extends Definition // thus implements VDL
        implements Serializable {
    /**
     * Captures the argument name indexed map of formal arguments. Each element is of type {@link
     * Declare} with an optional default value of type {@link Value}.
     *
     * @see Declare
     */
    private TreeMap m_declareMap;

    /**
     * The profile list encapsulates scheduler specific data in a generic structure.
     *
     * @see Profile
     */
    private ArrayList m_profileList;

    /**
     * The argument list describes the command line arguments as sum of substrings. Each element is
     * an {@link Argument}, and can be optionally addressed by a name.
     *
     * @see Argument
     */
    private ArrayList m_argumentList;

    /**
     * All arguments are, for ease-of-use, separated by a single space. If this is not wanted, the
     * user can overwrite the separator, including setting it to the empty string and null.
     */
    private String m_argumentSeparator = " ";

    /** Each compound transformation may declare local variables. */
    private TreeMap m_localMap;

    /**
     * Each entry in the call list describes an invocation of another transformation, complete with
     * actual arguments etc.
     *
     * @see Call
     */
    private ArrayList m_callList;

    /**
     * Type accessor for quick distinction between definitions.
     *
     * @return the value of {@link Definition#TRANSFORMATION}
     */
    public int getType() {
        return Definition.TRANSFORMATION;
    }

    /**
     * Default ctor: This ctor will frequently be used by the SAX parser to create a hollow
     * instance. Note that a transformation can be named.
     */
    public Transformation() {
        super();
        this.m_argumentList = new ArrayList();
        this.m_declareMap = new TreeMap();
        this.m_profileList = new ArrayList();
        this.m_localMap = new TreeMap();
        this.m_callList = new ArrayList();
    }

    /**
     * Minimum c'tor: Any transformation should be named.
     *
     * @param name is the name of the transformation in the current namespace and version context.
     * @see Definition
     */
    public Transformation(String name) {
        super(name);
        this.m_argumentList = new ArrayList();
        this.m_declareMap = new TreeMap();
        this.m_profileList = new ArrayList();
        this.m_localMap = new TreeMap();
        this.m_callList = new ArrayList();
    }

    /**
     * Standard ctor: This will name a transformation with the complete tripel necessary to access
     * it correctly (w/o guessing).
     *
     * @param name is the name of the transformation in the current namespace and version context.
     * @param namespace is the namespace that the transformation resides in.
     * @param version is a version number.
     * @see Definition
     */
    public Transformation(String namespace, String name, String version) {
        super(namespace, name, version);
        this.m_argumentList = new ArrayList();
        this.m_declareMap = new TreeMap();
        this.m_profileList = new ArrayList();
        this.m_localMap = new TreeMap();
        this.m_callList = new ArrayList();
    }

    /**
     * Accessor: Adds an argument to the list of arguments
     *
     * @param vArgument is the argument to append to the command line arguments.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see Argument
     */
    public void addArgument(Argument vArgument) throws IndexOutOfBoundsException {
        this.m_argumentList.add(vArgument);
    }

    /**
     * Accessor: Inserts an argument at an arbitrary place into the list. Each component in this
     * vector with an index greater or equal to the specified index is shifted upward to have an
     * index one greater than the value it had previously.
     *
     * @param index is the position to insert an argument
     * @param vArgument is the argument to append to the command line arguments.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see #getArgument( int )
     * @see #setArgument( int, Argument )
     * @see Argument
     */
    public void addArgument(int index, Argument vArgument) throws IndexOutOfBoundsException {
        this.m_argumentList.add(index, vArgument);
    }

    /**
     * Accessor: Adds an invocation to the list of calls.
     *
     * @param vCall is the invocation to append to the list of calls.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see Call
     */
    public void addCall(Call vCall) throws IndexOutOfBoundsException {
        this.m_callList.add(vCall);
    }

    /**
     * Accessor: Inserts an invocation at an arbitrary place into the list. Each component in this
     * vector with an index greater or equal to the specified index is shifted upward to have an
     * index one greater than the value it had previously.
     *
     * @param index is the position to insert an invocation
     * @param vCall is the invocation to append to the list of calls.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see #setCall( int, Call )
     * @see #getCall( int )
     * @see Call
     */
    public void addCall(int index, Call vCall) throws IndexOutOfBoundsException {
        this.m_callList.add(index, vCall);
    }

    /**
     * Accessor: Adds a formal argument declaration to the map of declarations.
     *
     * @param vDeclare is the formal argument to add to the declarations.
     * @see #getDeclare( String )
     * @see #setDeclare( Declare )
     * @see Declare
     */
    public void addDeclare(Declare vDeclare) {
        this.m_declareMap.put(vDeclare.getName(), vDeclare);
    }

    /**
     * Accessor: Adds a temporary variable declaration to the map of local variables.
     *
     * @param vLocal is the local variable declaration with value.
     * @see #getLocal( String )
     * @see #setLocal( Local )
     * @see Local
     */
    public void addLocal(Local vLocal) {
        this.m_localMap.put(vLocal.getName(), vLocal);
    }

    /**
     * Accessor: Appends a profile definition to the list of profiles.
     *
     * @param vProfile is the profile to append to remembered profiles.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see Profile
     */
    public void addProfile(Profile vProfile) throws IndexOutOfBoundsException {
        this.m_profileList.add(vProfile);
    }

    /**
     * Accessor: Inserts a profile definition at an arbitrary position into the list of profiles.
     * Each component in this vector with an index greater or equal to the specified index is
     * shifted upward to have an index one greater than the value it had previously.
     *
     * @param index is the position to insert the definitions into.
     * @param vProfile is the profile to append to remembered profiles.
     * @exception IndexOutOfBounds if the argument does not fit into the list.
     * @see #getProfile( int )
     * @see #setProfile( int, Profile )
     * @see Profile
     */
    public void addProfile(int index, Profile vProfile) throws IndexOutOfBoundsException {
        this.m_profileList.add(index, vProfile);
    }

    /**
     * Accessor: Provides an iterator for the <code>Argument</code> list.
     *
     * @return the iterator for the <code>Argument</code> list.
     * @see Argument
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateArgument() {
        return Collections.enumeration(this.m_argumentList);
    }

    /**
     * Accessor: Provides an iterator for the <code>Call</code> list.
     *
     * @return the iterator for the <code>Call</code> list.
     * @see Call
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateCall() {
        return Collections.enumeration(this.m_callList);
    }

    /**
     * Accessor: Provides an iterator for the <code>Declare</code> map.
     *
     * @return the iterator for the <code>Declare</code> list.
     * @see Declare
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateDeclare() {
        return Collections.enumeration(this.m_declareMap.values());
    }

    /**
     * Accessor: Provides an iterator for the local variables.
     *
     * @return the iterator over all locally declared variables.
     * @see Local
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateLocal() {
        return Collections.enumeration(this.m_localMap.values());
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
     * Predicate: Determines, if this is a simple TR (as opposed to compound).
     *
     * @return true, if this a simple transformation.
     */
    public boolean isSimple() {
        return this.m_callList.size() == 0;
    }

    /**
     * Accessor: Obtains an <code>Argument</code> at an arbitrary position.
     *
     * @param index is the place to look up the element at.
     * @return the argument at the specified place.
     * @throws IndexOutOfBoundsException if the referenced position does not exist.
     * @see Argument
     */
    public Argument getArgument(int index) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_argumentList.size())) {
            throw new IndexOutOfBoundsException();
        }

        return (Argument) this.m_argumentList.get(index);
    }

    /**
     * Accessor: Obtains the complete commandline arguments. This array is a copy to avoid
     * write-through modifications.
     *
     * @return an array with all commandline arguments inside.
     * @see #setArgument( Argument[] )
     * @see Argument
     * @deprecated Use the new Collection based interfaces
     */
    public Argument[] getArgument() {
        int size = this.m_argumentList.size();
        Argument[] mArray = new Argument[size];
        System.arraycopy(this.m_argumentList.toArray(new Argument[0]), 0, mArray, 0, size);
        return mArray;
    }

    /**
     * Accessor: Obtains the count of items in the argument list.
     *
     * @return the number of arguments in the commandline argument list.
     * @see Argument
     */
    public int getArgumentCount() {
        return this.m_argumentList.size();
    }

    /**
     * Accessor: Obtains the complete commandline arguments. The resulting list is read-only.
     *
     * @return an array with all commandline arguments inside.
     * @see #setArgument( Collection )
     * @see Argument
     */
    public java.util.List getArgumentList() {
        return Collections.unmodifiableList(this.m_argumentList);
    }

    /**
     * Gets the separating string between multiple {@link Argument} elements.
     *
     * @return The current state of the separator. The text may be null.
     * @see #setArgumentSeparator(String)
     */
    public String getArgumentSeparator() {
        return this.m_argumentSeparator;
    }

    /**
     * Accessor: Obtains an <code>Call</code> at an arbitrary position.
     *
     * @param index is the place to look up the element at.
     * @return the call at the specified place.
     * @throws IndexOutOfBoundsException if the referenced position does not exist.
     * @see #addCall( int, Call )
     * @see #setCall( int, Call )
     * @see Call
     */
    public Call getCall(int index) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_callList.size())) {
            throw new IndexOutOfBoundsException();
        }

        return (Call) this.m_callList.get(index);
    } // -- Call getCall(int)

    /**
     * Accessor: Obtains the invocation list. This array is a copy of the original to avoid
     * write-through modifications.
     *
     * @return an array with all calls inside.
     * @see Call
     * @deprecated Use the new Collection based interfaces
     */
    public Call[] getCall() {
        int size = this.m_callList.size();
        Call[] mArray = new Call[size];
        System.arraycopy(this.m_callList.toArray(new Call[0]), 0, mArray, 0, size);
        return mArray;
    }

    /**
     * Accessor: Obtains the count of items in the call list.
     *
     * @return the number of calls in the call list.
     * @see Call
     */
    public int getCallCount() {
        return this.m_callList.size();
    }

    /**
     * Accessor: Obtains the invocation list.
     *
     * @return a read-only list with all calls inside.
     * @see #setCall( Collection )
     * @see Call
     */
    public java.util.List getCallList() {
        return Collections.unmodifiableList(this.m_callList);
    }

    /**
     * Accessor: Obtains the declaration of a formal argument as referenced by its variable name.
     *
     * @param name is the symbolic index and variable name to obtain the declaration for.
     * @return the <code>Declare</code> object referenced by the name. May return <code>null</code>,
     *     if there is no such object.
     * @see #addDeclare( Declare )
     * @see #setDeclare( Declare )
     * @see Declare
     */
    public Declare getDeclare(String name) {
        return (Declare) this.m_declareMap.get(name);
    }

    /**
     * Accessor: Obtain all known formal arguments. Note that the array will be arbitrarily sorted,
     * depending on Java's hash function.
     *
     * @return a list of all formal arguments.
     * @see Declare
     * @see #setDeclare( Declare[] )
     * @deprecated Use the new Collection based interfaces
     */
    public Declare[] getDeclare() {
        int size = this.m_declareMap.size();
        Declare[] mArray = new Declare[size];
        this.m_declareMap.values().toArray(mArray);
        return mArray;
    }

    /**
     * Accessor: Counts the number of formal arguments known to this transformation.
     *
     * @return the formal argument count
     */
    public int getDeclareCount() {
        return this.m_declareMap.size();
    }

    /**
     * Accessor: Obtain all known formal arguments. Note that the list will be arbitrarily sorted,
     * depending on Java's hash function. It is also a read-only list to avoid modifications outside
     * the API.
     *
     * @return a list of all formal arguments.
     * @see #setDeclare( Collection )
     * @see Declare
     */
    public java.util.List getDeclareList() {
        return Collections.unmodifiableList(new ArrayList(this.m_declareMap.values()));
    }

    /**
     * Accessor: Obtains all known formal arguments. The map is a read-only map to avoid
     * modifications outside the API.
     *
     * @return a map with all formal arguments.
     * @see #setDeclare( Map )
     * @see Declare
     */
    public java.util.Map getDeclareMap() {
        return Collections.unmodifiableMap(this.m_declareMap);
    }

    /**
     * Accessor: Obtains the declaration of a temporary variable as referenced by its name.
     *
     * @param name is the variable name to obtain the declaration for.
     * @return the <code>Local</code> object referenced by the name. May return <code>null</code>,
     *     if there is no such object.
     * @see #addLocal( Local )
     * @see #setLocal( Local )
     * @see Local
     */
    public Local getLocal(String name) {
        return (Local) this.m_localMap.get(name);
    }

    /**
     * Accessor: Obtain all known temporary variables. Note that the array will be arbitrarily
     * sorted, depending on Java's hash function.
     *
     * @return a list of all temporary variables.
     * @see #setLocal( Local[] )
     * @see Local
     * @deprecated Use the new Collection based interfaces
     */
    public Local[] getLocal() {
        int size = this.m_localMap.size();
        Local[] mArray = new Local[size];
        this.m_localMap.values().toArray(mArray);
        return mArray;
    }

    /**
     * Accessor: Counts the number of temporary variables known to this transformation.
     *
     * @return the temporary variable count
     */
    public int getLocalCount() {
        return this.m_localMap.size();
    }

    /**
     * Accessor: Obtains all known temporary variables. Note that the list will be arbitrarily
     * sorted, depending on Java's hash function. It is also a read-only list to avoid modifications
     * outside the API.
     *
     * @return a list of all formal arguments.
     * @see #setLocal( Collection )
     * @see Local
     */
    public java.util.List getLocalList() {
        return Collections.unmodifiableList(new ArrayList(this.m_localMap.values()));
    }

    /**
     * Accessor: Obtains all known temporary variables. The map is a read-only map to avoid
     * modifications outside the API.
     *
     * @return a map with all formal arguments.
     * @see #setLocal( Map )
     * @see Local
     */
    public java.util.Map getLocalMap() {
        return Collections.unmodifiableMap(this.m_localMap);
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
        if ((index < 0) || (index >= this.m_profileList.size())) {
            throw new IndexOutOfBoundsException();
        }

        return (Profile) this.m_profileList.get(index);
    }

    /**
     * Accessor: Obtain a copy of the list of all <code>Profile</code> specifications.
     *
     * @return a collection containing the scheduler specific environment options for the job.
     * @see #setProfile( Profile[] )
     * @see Profile
     * @deprecated Use the new Collection based interfaces
     */
    public Profile[] getProfile() {
        int size = this.m_profileList.size();
        Profile[] mProfile = new Profile[size];
        System.arraycopy(this.m_profileList.toArray(new Profile[0]), 0, mProfile, 0, size);
        return mProfile;
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
     * Accessor: Provides an iterator for the <code>Argument</code> list.
     *
     * @return the iterator for the <code>Argument</code> list.
     * @see Argument
     */
    public Iterator iterateArgument() {
        return this.m_argumentList.iterator();
    }

    /**
     * Accessor: Provides an iterator for the <code>Call</code> list.
     *
     * @return the iterator for the <code>Call</code> list.
     * @see Call
     */
    public Iterator iterateCall() {
        return this.m_callList.iterator();
    }

    /**
     * Accessor: Provides an iterator for the <code>Declare</code> map.
     *
     * @return the iterator for the <code>Declare</code> list.
     * @see Declare
     */
    public Iterator iterateDeclare() {
        return this.m_declareMap.values().iterator();
    }

    /**
     * Accessor: Provides an iterator for local variables.
     *
     * @return the iterator over all local variables.
     * @see Declare
     */
    public Iterator iterateLocal() {
        return this.m_localMap.values().iterator();
    }

    /**
     * Accessor: Provides an iterator for the <code>Profile</code> list.
     *
     * @return the iterator for the <code>Profile</code> list.
     * @see Profile
     */
    public Iterator iterateProfile() {
        return this.m_profileList.iterator();
    }
    /**
     * Accessor: Provides a list iterator for the <code>Argument</code> list.
     *
     * @return the iterator for the <code>Argument</code> list.
     * @see Argument
     */
    public ListIterator listIterateArgument() {
        return this.m_argumentList.listIterator();
    }

    /**
     * Accessor: Provides a list iterator for the <code>Call</code> list.
     *
     * @return the iterator for the <code>Call</code> list.
     * @see Call
     */
    public ListIterator listIterateCall() {
        return this.m_callList.listIterator();
    }

    /**
     * Accessor: Provides a list iterator for the <code>Profile</code> list.
     *
     * @return the iterator for the <code>Profile</code> list.
     * @see Profile
     */
    public ListIterator listIterateProfile() {
        return this.m_profileList.listIterator();
    }

    /**
     * Accessor: Provides a list iterator for the <code>Argument</code> list.
     *
     * @return the iterator for the <code>Argument</code> list.
     * @param start is the start index
     * @see Argument
     */
    public ListIterator listIterateArgument(int start) {
        return this.m_argumentList.listIterator(start);
    }

    /**
     * Accessor: Provides a list iterator for the <code>Call</code> list.
     *
     * @return the iterator for the <code>Call</code> list.
     * @param start is the start index
     * @see Call
     */
    public ListIterator listIterateCall(int start) {
        return this.m_callList.listIterator(start);
    }

    /**
     * Accessor: Provides a list iterator for the <code>Profile</code> list.
     *
     * @return the iterator for the <code>Profile</code> list.
     * @param start is the start index
     * @see Profile
     */
    public ListIterator listIterateProfile(int start) {
        return this.m_profileList.listIterator(start);
    }

    /**
     * Accessor: Removes all commandline arguments.
     *
     * @see Argument
     */
    public void removeAllArgument() {
        this.m_argumentList.clear();
    }

    /**
     * Accessor: Removes all calls.
     *
     * @see Call
     */
    public void removeAllCall() {
        this.m_callList.clear();
    }

    /**
     * Accessor: Removes all formal arguments.
     *
     * @see Declare
     */
    public void removeAllDeclare() {
        this.m_declareMap.clear();
    }

    /**
     * Accessor: Removes all temporary variables.
     *
     * @see Local
     */
    public void removeAllLocal() {
        this.m_localMap.clear();
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
     * Accessor: Removes a commandline argument fragment from the commandline. Each component in
     * this vector with an index greater or equal to the specified index is shifted downward to have
     * an index one smaller than the value it had previously. The size of this vector is decreased
     * by 1.
     *
     * @param index is the position to remove the argument fragment from.
     * @return the removed Argument.
     * @exception ArrayIndexOutOfBoundsException if the index was invalid.
     * @see Argument
     */
    public Argument removeArgument(int index) {
        return (Argument) this.m_argumentList.remove(index);
    }

    /**
     * Accessor: Removes a single call fragment from the list of calls. Each component in this
     * vector with an index greater or equal to the specified index is shifted downward to have an
     * index one smaller than the value it had previously. The size of this vector is decreased by
     * 1.
     *
     * @param index is the position to remove the call fragment from.
     * @return the removed Call.
     * @exception ArrayIndexOutOfBoundsException if the index was invalid.
     * @see Call
     */
    public Call removeCall(int index) {
        return (Call) this.m_callList.remove(index);
    }

    /**
     * Accessor: Removes a formal argument.
     *
     * @param name is the name of the argument to remove
     * @return the removed formal argument.
     * @see Declare
     */
    public Declare removeDeclare(String name) {
        return (Declare) this.m_declareMap.remove(name);
    }

    /**
     * Accessor: Removes a temporary variable.
     *
     * @param name is the name of the temporary variable to remove
     * @return the removed variable.
     * @see Local
     */
    public Local removeLocal(String name) {
        return (Local) this.m_localMap.remove(name);
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
     * Accessor: Overwrites an commandline argument fragment with a new one.
     *
     * @param index is the position to overwrite the element at
     * @param vArgument is the new commandline argument.
     * @exception IndexOutOfBoundsException if the position does not exist.
     * @see Argument
     */
    public void setArgument(int index, Argument vArgument) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_argumentList.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_argumentList.set(index, vArgument);
    }

    /**
     * Accessor: Replace the commandline arguments with a new commandline argument.
     *
     * @param argumentArray is the new commandline argument array.
     * @see Argument
     * @deprecated Use the new Collection based interfaces
     */
    public void setArgument(Argument[] argumentArray) {
        this.m_argumentList.clear();
        this.m_argumentList.addAll(Arrays.asList(argumentArray));
    }

    /**
     * Accessor: Replace the commandline arguments with a new commandline argument.
     *
     * @param arguments is the new commandline argument array.
     * @see Argument
     */
    public void setArgument(java.util.Collection arguments) {
        this.m_argumentList.clear();
        this.m_argumentList.addAll(arguments);
    }

    /**
     * Overwrites the internal separator string between neighbouring {@link Argument} elements with
     * new content.
     *
     * @param separator is the new string separating neighbouring arguments.
     * @see #getArgumentSeparator()
     */
    public void setArgumentSeparator(String separator) {
        this.m_argumentSeparator = separator;
    }

    /**
     * Accessor: Overwrites a call with a new one.
     *
     * @param index is the position to overwrite the element at
     * @param vCall is the new call.
     * @exception IndexOutOfBoundsException if the position does not exist.
     * @see #addCall( int, Call )
     * @see #getCall( int )
     * @see Call
     */
    public void setCall(int index, Call vCall) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_callList.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_callList.set(index, vCall);
    }

    /**
     * Accessor: Replace the calls with a new call list.
     *
     * @param callArray is the new call array.
     * @see Call
     * @deprecated Use the new Collection based interfaces
     */
    public void setCall(Call[] callArray) {
        this.m_callList.clear();
        this.m_callList.addAll(Arrays.asList(callArray));
    }

    /**
     * Accessor: Replace the calls with a new call list.
     *
     * @param calls is the new call array.
     * @see Call
     */
    public void setCall(Collection calls) {
        this.m_callList.clear();
        this.m_callList.addAll(calls);
    }

    /**
     * Accessor: Insert or replace a declaration with a new version.
     *
     * @param vDeclare is the declaration to insert or replace.
     * @see Declare
     * @see java.util.Hashtable#put( Object, Object )
     */
    public void setDeclare(Declare vDeclare) {
        this.m_declareMap.put(vDeclare.getName(), vDeclare);
    }

    /**
     * Accessor: Replace all declarations by a new set of declarations. This method effectively
     * exchanges all formal arguments of a job.
     *
     * @param declareArray is the new set of declarations.
     * @see Declare
     * @deprecated Use the new Collection based interfaces
     */
    public void setDeclare(Declare[] declareArray) {
        // -- copy array
        this.m_declareMap.clear();
        for (int i = 0; i < declareArray.length; i++) {
            this.m_declareMap.put(declareArray[i].getName(), declareArray[i]);
        }
    }

    /**
     * Accessor: Replace all declarations by a new set of declarations. This method effectively
     * exchanges all formal arguments of a job.
     *
     * @param declares is the new set of declarations.
     * @see Declare
     */
    public void setDeclare(Collection declares) {
        this.m_declareMap.clear();
        for (Iterator i = declares.iterator(); i.hasNext(); ) {
            Declare d = (Declare) i.next();
            this.m_declareMap.put(d.getName(), d);
        }
    }
    /**
     * Accessor: Replace all declarations by a new set of declarations. This method effectively
     * exchanges all formal arguments of a job.
     *
     * @param declares is the new set of declarations.
     * @see Declare
     */
    public void setDeclare(Map declares) {
        this.m_declareMap.clear();
        this.m_declareMap.putAll(declares);
    }

    /**
     * Accessor: Inserts or replaces a temporary variable with a new version.
     *
     * @param vLocal is the temporary variable to insert or replace.
     * @see Local
     * @see java.util.Hashtable#put( Object, Object )
     */
    public void setLocal(Local vLocal) {
        this.m_localMap.put(vLocal.getName(), vLocal);
    }

    /**
     * Accessor: Replaces all declarations by a new set of declarations. This method effectively
     * exchanges all temporary variables.
     *
     * @param localArray is the new set of local variable declarations.
     * @see Local
     * @deprecated Use the new Collection based interfaces
     */
    public void setLocal(Local[] localArray) {
        // -- copy array
        this.m_localMap.clear();
        for (int i = 0; i < localArray.length; i++) {
            this.m_localMap.put(localArray[i].getName(), localArray[i]);
        }
    }

    /**
     * Accessor: Replaces all declarations by a new set of declarations. This method effectively
     * exchanges all temporary variables.
     *
     * @param locals is the new set of temporary variable declarations.
     * @see Local
     */
    public void setLocal(Collection locals) {
        this.m_localMap.clear();
        for (Iterator i = locals.iterator(); i.hasNext(); ) {
            Local d = (Local) i.next();
            this.m_localMap.put(d.getName(), d);
        }
    }

    /**
     * Accessor: Replace all declarations by a new set of declarations. This method effectively
     * exchanges all temporary variables.
     *
     * @param locals is the new set of declarations.
     * @see Local
     */
    public void setLocal(Map locals) {
        this.m_localMap.clear();
        this.m_localMap.putAll(locals);
    }

    /**
     * Accessor: Overwrites a profile with a new profile
     *
     * @param index is the position to overwrite the profile at.
     * @param vProfile is the new profile to use in overwriting.
     * @exception IndexOutOfBoundsException if the position does not exist.
     * @see Profile
     */
    public void setProfile(int index, Profile vProfile) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_profileList.size())) {
            throw new IndexOutOfBoundsException();
        }
        this.m_profileList.set(index, vProfile);
    }

    /**
     * Accessor: Replace the internal profiles with a new list.
     *
     * @param profileArray is the new list of profiles to use for the job.
     * @see Profile
     * @deprecated Use the new Collection based interfaces
     */
    public void setProfile(Profile[] profileArray) {
        this.m_profileList.clear();
        this.m_profileList.addAll(Arrays.asList(profileArray));
    }

    /**
     * Accessor: Replace the internal profiles with a new list.
     *
     * @param profiles is the new list of profiles to use for the job.
     * @see Profile
     */
    public void setProfile(java.util.Collection profiles) {
        this.m_profileList.clear();
        this.m_profileList.addAll(profiles);
    }

    /**
     * Constructs dynamically a short descriptive, hopefully unique identifier for this
     * transformation.
     *
     * @return a string describing the transformation
     * @see Object#hashCode()
     */
    public String identify() {
        // short and long ID are identical for transformations.
        return super.shortID();
    }

    /**
     * Dumps the content of the given element into a string. This function traverses all sibling
     * classes as necessary and converts the data into textual output.
     */
    public void toString(Writer stream) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        stream.write("TR ");
        stream.write(this.identify());
        stream.write('(');

        // write formal args
        if (this.m_declareMap.size() > 0) {
            stream.write(newline);
            for (Iterator i = this.m_declareMap.values().iterator(); i.hasNext(); ) {
                stream.write('\t');
                ((Declare) i.next()).toString(stream);
                if (i.hasNext()) stream.write("," + newline);
            }
        }

        stream.write(" )");
        stream.write(newline);
        stream.write('{');
        stream.write(newline);

        if (this.isSimple()) {
            for (Iterator i = this.m_argumentList.iterator(); i.hasNext(); ) {
                stream.write("  ");
                ((Argument) i.next()).toString(stream);
                stream.write(';');
                stream.write(newline);
            }
        } else {
            if (this.m_localMap.size() > 0) {
                for (Iterator i = this.m_localMap.values().iterator(); i.hasNext(); ) {
                    stream.write("  ");
                    ((Local) i.next()).toString(stream);
                    stream.write(';');
                    stream.write(newline);
                }
                stream.write(newline);
            }

            for (Iterator i = this.m_callList.iterator(); i.hasNext(); ) {
                stream.write("  ");
                ((Call) i.next()).toString(stream);
                stream.write(';');
                stream.write(newline);
            }
        }

        for (Iterator i = this.m_profileList.iterator(); i.hasNext(); ) {
            stream.write("  ");
            ((Profile) i.next()).toString(stream);
            stream.write(';');
            stream.write(newline);
        }

        stream.write('}');
        stream.write(newline);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently, if you use a buffered writer.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String tag =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":transformation"
                        : "transformation";

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        super.toXML(stream);

        // add argument separator, if it is not a single space.
        // FIXME: This attribute can only appear in simple TR!
        if (this.m_argumentSeparator == null || !this.m_argumentSeparator.equals(" ")) {
            stream.write(" argumentSeparator=\"");
            if (this.m_argumentSeparator != null)
                stream.write(quote(this.m_argumentSeparator, true));
            stream.write('"');
        }

        if (this.m_declareMap.size()
                        + this.m_argumentList.size()
                        + this.m_callList.size()
                        + this.m_profileList.size()
                == 0) {
            // empty transformation, no fargs, no body
            // FIXME: A compound TR must not be empty!
            stream.write("/>");
        } else {
            // done with opening tag
            stream.write('>');
            if (indent != null) stream.write(newline);

            String newindent = indent == null ? null : indent + "  ";
            for (Iterator i = this.m_declareMap.values().iterator(); i.hasNext(); ) {
                ((Declare) i.next()).toXML(stream, newindent, namespace);
            }

            if (this.isSimple()) {
                for (Iterator i = this.m_argumentList.iterator(); i.hasNext(); ) {
                    ((Argument) i.next()).toXML(stream, newindent, namespace);
                }
            } else {
                for (Iterator i = this.m_localMap.values().iterator(); i.hasNext(); ) {
                    ((Local) i.next()).toXML(stream, newindent, namespace);
                }

                // a compound TR must have at least one call!
                for (Iterator i = this.m_callList.iterator(); i.hasNext(); ) {
                    ((Call) i.next()).toXML(stream, newindent, namespace);
                }
            }

            for (Iterator i = this.m_profileList.iterator(); i.hasNext(); ) {
                ((Profile) i.next()).toXML(stream, newindent, namespace);
            }

            // close tag
            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        }
        if (indent != null) stream.write(newline);
    }
}
