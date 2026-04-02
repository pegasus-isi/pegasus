/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
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

// java.util.List clashes with org.griphyn.vdl.classes.List
import edu.isi.pegasus.common.util.Separator;
import java.io.*;
import java.util.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.util.*;

/**
 * This class traverses the dependency tree. The tree is built by {@link Derivation} objects. The
 * linkage is data-flow oriented. If a derivation requires one or more input files, all derivations
 * that produce these files will be considered etc. Thus, a build-style directed acyclic graph (DAG)
 * is formed.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Route {
    /** Default max depths of recursion into the graph before the circuit breaker hits. */
    public static final int MAXIMUM_DEPTH = 256;

    /**
     * This is a nested class to obscure it from the outside world. It will maintain a stack of
     * database manager backends that are used to resolve compound transformations. For this
     * purpose, it exposes the required backend database searches, and handles stack descend.
     *
     * <p>The stack enforces that any schemas put onto it implement the <code>VDC</code> interface.
     *
     * @see org.griphyn.vdl.dbschema.VDC
     */
    class DatabaseSchemaStack {
        /** Stores a reference to the database schema managers. */
        public ArrayList m_stack;

        /**
         * C'tor: Creates a new stack instance that contains the bottom-most database backend.
         *
         * @param schema is the final database backend.
         */
        public DatabaseSchemaStack(DatabaseSchema schema) {
            this.m_stack = new ArrayList();
            if (!(schema instanceof VDC))
                throw new RuntimeException("illegal database schema: Not a VDC");
            this.m_stack.add(new StackElement(schema));
        }

        /**
         * Pushes a new definition onto the top of the stack.
         *
         * @param nextSchema is a new database schema handler, usually an in-memory one for all
         *     except the root element.
         * @exception NullPointerException, if the argument passed is null.
         */
        public void push(DatabaseSchema nextSchema) throws NullPointerException {
            if (nextSchema == null) throw new NullPointerException();
            if (!(nextSchema instanceof VDC))
                throw new RuntimeException("illegal database schema: Not a VDC");

            Logging.instance().log("stack", 2, "pushing dbmstack[" + this.m_stack.size() + ']');
            this.m_stack.add(new StackElement(nextSchema));
        }

        /**
         * Removes the TOS, thus making the next-lower definition TOS.
         *
         * @return the old top-of-stack.
         * @throws EmptyStackException if the stack did not have any elements.
         */
        public DatabaseSchema pop() {
            int size = this.m_stack.size();
            if (size == 0) throw new EmptyStackException();
            Logging.instance().log("stack", 2, "popping dbmstack[" + (size - 1) + ']');

            StackElement item = (StackElement) this.m_stack.remove(size - 1);
            return item.getDatabaseSchema();
        }

        /**
         * Accessor predicate: Determines if the stack contains any elements.
         *
         * @return true, if the stack is empty; false otherwise.
         */
        public boolean isEmpty() {
            return this.m_stack.isEmpty();
        }

        /**
         * Accessor: Determines the number of database managers on the stack.
         *
         * @return number of elements, or 0 for an empty stack.
         */
        public int size() {
            return this.m_stack.size();
        }

        /**
         * Descends down the stack of database manager, and for each it will determine the list of
         * elegible logical filenames. The search only searches for Derivations and output
         * filenames. The name to search for is the variable.
         *
         * <p>FIXME: There might be a bug/feature with default args in TRs!
         *
         * @param filename is the logical filename to search for as output file.
         * @return a list with all derivations that have this output file.
         */
        public java.util.List derivationsWithOutput(String filename) {
            java.util.List result = new ArrayList();
            Logging.instance().log("trace", 2, "derivationsWithOutput(" + filename + ")");
            try {
                int level = m_stack.size();
                ListIterator i = m_stack.listIterator(level);
                boolean flag = true;
                while (i.hasPrevious() && flag) {
                    StackElement element = (StackElement) i.previous();

                    // check for existence of LFN
                    Cache cache = element.getLFNCache();
                    Object item = cache == null ? null : cache.get(filename);
                    if (item == null) {
                        // unknown or expired, check database
                        Logging.instance()
                                .log("cache", 0, "[" + level + "] LFN cache MISS for " + filename);
                        VDC vdc = (VDC) element.getDatabaseSchema();
                        java.util.List list = vdc.searchFilename(filename, LFN.OUTPUT);
                        if (list != null && !list.isEmpty()) {
                            result.addAll(list);
                            if (cache != null) cache.set(filename, result);
                            flag = false;
                        }
                    } else {
                        // cache hit
                        Logging.instance()
                                .log("cache", 1, "[" + level + "] LFN cache HIT  for " + filename);
                        result.addAll((java.util.List) item);
                        flag = false;
                    }
                    level--;
                }

                if (flag && result.isEmpty()) {
                    // negative caching on the way out
                    if (i.hasNext()) {
                        StackElement element = (StackElement) i.next();
                        Cache cache = element.getLFNCache();
                        if (cache != null) cache.set(filename, result);
                    }
                }

            } catch (Exception e) {
                Logging.instance().log("default", 0, "caught " + e + ", aborting");
                throw new RuntimeException(e.getMessage());
            }

            return result;
        }

        private String genKey(String usesspace, String uses, String min, String max) {
            StringBuffer result = new StringBuffer(32);
            if (usesspace != null) {
                result.append(usesspace);
                result.append(Separator.NAMESPACE);
            }
            result.append(uses);
            if (min != null || max != null) {
                result.append(Separator.NAME);
                if (min != null) result.append(min);
                result.append(Separator.VERSION);
                if (max != null) result.append(max);
            }
            return result.toString();
        }

        /**
         * Obtain all transformations that exactly match the given secondary key triple and version
         * range. Thus, we have to weed out wildcard matches with exact/range matches, before we can
         * add transformations.
         *
         * @param usesspace is the namespace, nullable, not wildcardable
         * @param uses is the name, which must be given
         * @param min is the version minimum, nullable, not wildcardable
         * @param max is the version maximum, nullable, not wildcardable
         * @return a list of all matching transformations
         */
        public java.util.List searchForTR(String usesspace, String uses, String min, String max) {
            java.util.List result = new ArrayList();
            Logging.instance()
                    .log(
                            "trace",
                            2,
                            "searchForTR "
                                    + (usesspace == null ? "null" : usesspace)
                                    + Separator.NAMESPACE
                                    + uses
                                    + Separator.NAME
                                    + (min == null ? "null" : min)
                                    + Separator.VERSION
                                    + (max == null ? "null" : max));
            try {
                String key = genKey(usesspace, uses, min, max);
                boolean flag = true;
                int level = m_stack.size();
                ListIterator i = m_stack.listIterator(level);
                while (i.hasPrevious() && flag) {
                    StackElement element = (StackElement) i.previous();

                    // check for existence of LFN
                    Cache cache = element.getTRCache();
                    Object item = cache == null ? null : cache.get(key);
                    if (item == null) {
                        // unknown or expired, check database
                        Logging.instance()
                                .log("cache", 0, "[" + level + "] TR cache MISS for " + key);

                        VDC vdc = (VDC) element.getDatabaseSchema();
                        java.util.List intermediary =
                                vdc.searchDefinition(
                                        /* usesspace==null ? "" : */ usesspace,
                                        uses,
                                        null,
                                        Definition.TRANSFORMATION);
                        // postcondition: contains TR, joker version, otherwise matches

                        // only add those that exactly match our requirements
                        for (Iterator j = intermediary.iterator(); j.hasNext(); ) {
                            Definition d = (Definition) j.next();
                            Logging.instance().log("route", 2, "looking at TR " + d.identify());
                            if (Route.matchWithNull(usesspace, d.getNamespace())
                                    && Route.matchWithNull(uses, d.getName())
                                    && Derivation.match(min, max, d.getVersion())) result.add(d);
                        }
                        if (!result.isEmpty()) {
                            if (cache != null) cache.set(key, result);
                            flag = false;
                        }
                    } else {
                        // cache hit
                        result.addAll((java.util.List) item);
                        Logging.instance()
                                .log("cache", 1, "[" + level + "] TR cache HIT  for " + key);
                        flag = false;
                    }
                    level--;
                }

                if (flag && result.isEmpty()) {
                    // negative caching on the way out
                    if (i.hasNext()) {
                        StackElement element = (StackElement) i.next();
                        Cache cache = element.getTRCache();
                        if (cache != null) cache.set(key, result);
                    }
                }

            } catch (Exception e) {
                Logging.instance().log("default", 0, "caught " + e + ", aborting");
                throw new RuntimeException(e.getMessage());
            }

            return result;
        }

        /**
         * Obtain all derivations that wildcard match the given secondary key triple.
         *
         * @param namespace is the namespace, nullable, not wildcardable
         * @param name is the name, which must be given
         * @param version is the version, nullable, not wildcardable
         * @return a list of all matching derivations
         */
        public java.util.List searchForDV(String namespace, String name, String version) {
            java.util.List result = new ArrayList();
            Logging.instance()
                    .log(
                            "trace",
                            2,
                            "searchForDV "
                                    + (namespace == null ? "null" : namespace)
                                    + Separator.NAMESPACE
                                    + name
                                    + Separator.NAME
                                    + (version == null ? "null" : version));
            try {
                for (ListIterator i = m_stack.listIterator(m_stack.size()); i.hasPrevious(); ) {
                    StackElement element = (StackElement) i.previous();
                    VDC vdc = (VDC) element.getDatabaseSchema();
                    java.util.List list =
                            vdc.searchDefinition(namespace, name, version, Definition.DERIVATION);
                    if (list != null) result.addAll(list);
                }
            } catch (Exception e) {
                Logging.instance().log("default", 0, "caught " + e + ", aborting");
                throw new RuntimeException(e.getMessage());
            }
            return result;
        }
    };

    /**
     * Stores a reference to the underlying data as top-of-stack. At the bottom of the stack,
     * queries to the database backend are generated. At higher levels in the stack, the
     * stack-existing definitions take precendence.
     */
    private DatabaseSchemaStack m_stack;

    /**
     * Stores a reference to the class that manages arbitration in case multiple derivations produce
     * the same file.
     */
    private Arbiter m_arbiter;

    /**
     * The constructor initializes the stack of definitions with the default database manager
     * backend. This backend will be used to pose the various search requests.
     *
     * @param backend is the backend database manager.
     */
    public Route(DatabaseSchema backend) {
        this.m_stack = new DatabaseSchemaStack(backend);
        this.m_arbiter = new PreferNamespace();
    }

    /**
     * The constructor initializes the stack of definitions with the default database manager
     * backend. Additionally, it will push an in-memory schema constructed from the definitions
     * argument onto the stack.
     *
     * @param backend is the backend database manager.
     * @param defs is the root of an in-memory database fragment.
     * @exception NullPointerException, if the argument is null.
     * @see org.griphyn.vdl.dbschema.InMemorySchema
     * @see org.griphyn.vdl.dbschema.SingleFileSchema
     */
    public Route(DatabaseSchema backend, Definitions defs) throws NullPointerException {
        if (defs == null) throw new NullPointerException();
        this.m_stack = new DatabaseSchemaStack(backend);
        try {
            this.m_stack.push(new InMemorySchema(defs)); // was: SingleFile(defs)
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        this.m_arbiter = new PreferNamespace();
    }

    /**
     * Creates a new top of stack with the given definitions. Effectively a push() operation. The
     * definitions fragment will be converted into an in-memory database.
     *
     * @param defs is the root an in-memory database fragment.
     * @exception NullPointerException, if the argument is null.
     * @see org.griphyn.vdl.dbschema.InMemorySchema
     * @see org.griphyn.vdl.dbschema.SingleFileSchema
     */
    public void addDefinitions(Definitions defs) throws NullPointerException {
        if (defs == null) throw new NullPointerException();
        else {
            try {
                this.m_stack.push(new InMemorySchema(defs)); // was: SingleFile
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * Removes the current top of stack. Effective a pop() operation.
     *
     * @return the top of stack as it was before the pop().
     */
    public DatabaseSchema removeDefinitions() {
        return this.m_stack.pop();
    }

    /** Stores the maximum depths to which we will go. */
    private int m_maxDepth = MAXIMUM_DEPTH;

    /**
     * Allows to limit the maximum depth that the router is willing to go.
     *
     * @param depth is the maximum depth. Use Integer.MAX_VALUE for unlimited.
     * @see #getMaximumDepth()
     */
    public void setMaximumDepth(int depth) {
        this.m_maxDepth = depth;
    }

    /**
     * Queries the current maximum depths that the router is willing to go.
     *
     * @return the current maximum depth, or Integer.MAX_VALUE for unlimited.
     * @see #setMaximumDepth( int )
     */
    public int getMaximumDepth() {
        return this.m_maxDepth;
    }

    /**
     * Queries the current arbitrarion instance.
     *
     * @return a reference to the current arbiter for conflict resolution.
     */
    public Arbiter getArbiter() {
        return this.m_arbiter;
    }

    /**
     * Replaces the current arbiter with a new instance.
     *
     * @param arbiter is a new instance of a conflict resolution class.
     */
    public void setArbiter(Arbiter arbiter) {
        this.m_arbiter = arbiter;
    }

    // ================================================================

    /**
     * DAX bridge: Helps flattening a VDL leaf tree into a DAX leaf list.
     *
     * @param s is the VDL scalar to flatten
     * @param v is a reference to a list which will be appended with DAX leaves
     */
    private void appendScalarDaxLeaf(Scalar s, java.util.List v) {
        for (Iterator i = s.iterateLeaf(); i.hasNext(); ) {
            org.griphyn.vdl.classes.Leaf leaf = (org.griphyn.vdl.classes.Leaf) i.next();
            if (leaf instanceof Text) {
                // do PseudoText
                v.add(new PseudoText(((Text) leaf).getContent()));
            } else if (leaf instanceof LFN) {
                // do Filename
                LFN lfn = (LFN) leaf;
                v.add(new Filename(lfn.getFilename(), lfn.getLink(), lfn.getTemporary()));
            } else {
                // this should not happen
                Logging.instance()
                        .log("default", 0, "WARNING: " + "Illegal argument type in scalar or list");
            }
        }
    }

    /**
     * DAX Bridge: This methods translates a <code>Value</code> tree from a VDL spec into a
     * flattened <code>Leaf</code> tree for a DAX job specification.
     *
     * @param v is a VDL <code>Value</code> tree. This argument must not be null!
     * @param prefix string to be used when rendering a <code>List</code>.
     * @param separator string to be used when rendering a <code>List</code>.
     * @param suffix string to be used when rendering a <code>List</code>.
     * @return a list of <code>Leaf</code> values
     * @see org.griphyn.vdl.classes.Value
     * @see org.griphyn.vdl.dax.Leaf
     */
    private java.util.List flattenScalar(
            org.griphyn.vdl.classes.Value v, String prefix, String separator, String suffix) {
        java.util.List result = new ArrayList();

        // traverse Value tree
        switch (v.getContainerType()) {
                // this is a regular SCALAR
            case org.griphyn.vdl.classes.Value.SCALAR:
                appendScalarDaxLeaf((Scalar) v, result);
                break;

                // this is a regular LIST
            case org.griphyn.vdl.classes.Value.LIST:
                org.griphyn.vdl.classes.List list = (org.griphyn.vdl.classes.List) v;
                if (prefix != null && prefix.length() > 0) result.add(new PseudoText(prefix));
                for (Iterator i = list.iterateScalar(); i.hasNext(); ) {
                    appendScalarDaxLeaf((Scalar) i.next(), result);
                    if (separator != null && separator.length() > 0 && i.hasNext())
                        result.add(new PseudoText(separator));
                }
                if (suffix != null && suffix.length() > 0) result.add(new PseudoText(suffix));
                break;

            default:
                // this should not happen
                Logging.instance()
                        .log(
                                "default",
                                0,
                                "WARNING: An actual argument is neither SCALAR nor LIST");
                break;
        }

        return result;
    }

    /**
     * DAX bridge: Converts a leaf into a mixed form of either the textual string, or the value of
     * the bound variable, as passed from a DV into a TR. The bound variable values will be
     * flattened out.
     *
     * @param old is the VDLx leaf class to convert
     * @param arguments is a map of all declared variables
     * @return a list of DAX leaves, flattened out into one level.
     * @throws UndeclaredVariableException if an unknown bound variable is used.
     * @throws IllegalArgumentException if the Leaf class is invalid.
     */
    private java.util.List convertLeaf(org.griphyn.vdl.classes.Leaf old, Map arguments)
            throws UndeclaredVariableException, IllegalArgumentException {
        java.util.List result = new ArrayList();
        if (old instanceof org.griphyn.vdl.classes.Text) {
            // copy Text into PseudoText (simple)
            org.griphyn.vdl.classes.Text t = (org.griphyn.vdl.classes.Text) old;
            result.add(new org.griphyn.vdl.dax.PseudoText(t.getContent()));
        } else if (old instanceof org.griphyn.vdl.classes.Use) {
            // replace Use element with flattened out argument
            org.griphyn.vdl.classes.Use u = (org.griphyn.vdl.classes.Use) old;
            if (!arguments.containsKey(u.getName())) {
                // FIXME: Another late binding error
                throw new UndeclaredVariableException(
                        "bound variable \"" + u.getName() + "\" is not known");
            }
            //      java.util.List flat =
            //	flattenScalar( (org.griphyn.vdl.classes.Value) arguments.get(u.getName()),
            //		       u.getPrefix(), u.getSeparator(), u.getSuffix() );
            //      for ( Iterator e=flat.iterator(); e.hasNext(); ) {
            //	result.add( (org.griphyn.vdl.dax.Leaf) e.next() );
            //      }
            result.addAll(
                    flattenScalar(
                            (org.griphyn.vdl.classes.Value) arguments.get(u.getName()),
                            u.getPrefix(),
                            u.getSeparator(),
                            u.getSuffix()));
        } else if (old instanceof org.griphyn.vdl.classes.LFN) {
            // I suppose we could allow LFNs here after all
            throw new IllegalArgumentException("LFN is an illegal leaf node");
        } else {
            throw new IllegalArgumentException("unknown class for a leaf node");
        }

        return result;
    }

    /**
     * Finds first TR that matches a DV or Call. If no match is found, an exception will be throws.
     * If more than one TR is found, the first match will be taken - this may not always be the best
     * match.
     *
     * <p>
     *
     * @param usesspace is the namespace to look for, or null for any.
     * @param uses names the TR. It must not be null.
     * @param minInclude is the minimum inclusive version of the range, or null.
     * @param maxInclude is the maximum inclusive version of the range, or null.
     * @return the matching <code>Transformation</code>.
     * @exception TransformationNotFoundException will be thrown, if no match is found
     * @exception NullPointerException will be thrown, if uses is null.
     */
    private Transformation findMatchingTransformation(
            String usesspace, String uses, String minInclude, String maxInclude)
            throws TransformationNotFoundException, NullPointerException {
        // just in case...
        if (uses == null) throw new NullPointerException("Must name a TR");

        // what are we looking for
        String trid =
                (usesspace == null ? "*" : usesspace)
                        + Separator.NAMESPACE
                        + (uses == null ? "*" : uses)
                        + Separator.NAME
                        + (minInclude == null ? "*" : minInclude)
                        + Separator.VERSION
                        + (maxInclude == null ? "*" : maxInclude);
        Logging.instance().log("route", 0, "looking for TR " + trid);

        java.util.List result = this.m_stack.searchForTR(usesspace, uses, minInclude, maxInclude);
        // POSTCONDITION: result contains matching TR

        // select one TR from result set
        int size = result.size();
        Logging.instance()
                .log("route", 0, size + " matching TR" + (size == 1 ? "" : "s") + " found");
        if (size == 0) {
            // nothing found that matches
            throw new TransformationNotFoundException(
                    "Aborting route: No matches found for TR " + trid);
        }

        return (Transformation) result.get(0); // may throw ClassCastException?
    }

    /**
     * DAX bridge: Matches <code>Declare</code> (must be complete) with <code>Pass</code> (can skip
     * default args). Use defaults from <code>Use</code>. Flatten out results into a map which
     * unfolds into a vector of DAX leaves, and which are fit for a <code>Job</code>.
     *
     * @param dv is an implementation of <code>HasPass</code>. Implementing classes include <code>
     *     Derivation</code> and <code>Call</code>.
     * @param tr is the <code>Transformation</code> which supplies the formal arugments.
     * @return a map from each actual argument or un-overwritten formal argument default from the
     *     argument name to its <code>Value</code>.
     * @exception MissingArgumentException is thrown, if a formal argument was declared without a
     *     default, but no actual argument was supplied for it.
     */
    private Map mapActualToFormal(HasPass dv, Transformation tr) {
        Map arguments = new HashMap();
        for (Iterator e = tr.iterateDeclare(); e.hasNext(); ) {
            Declare farg = (Declare) e.next();
            String name = farg.getName();
            Pass aarg = (Pass) dv.getPass(name);

            // FIXME: late type checking performed here. We must have some
            // value to use in the job specification:
            // o the formal argument default value from TR, or
            // o the actual argument supplied value
            if (aarg == null && farg.getValue() == null)
                throw new MissingArgumentException("argument \"" + name + "\" has no value");

            // Save the Value structure for now. We can only flatten it,
            // once we have the rendering.
            if (aarg == null) {
                // use the default value from the formal argument
                arguments.put(name, farg.getValue());
                Logging.instance()
                        .log(
                                "route",
                                2,
                                "adding default value for "
                                        + name
                                        + '='
                                        + farg.getValue().toString());
            } else {
                // use the supplied actual argument.
                arguments.put(name, aarg.getValue());
                Logging.instance()
                        .log(
                                "route",
                                2,
                                "actual argument taken for "
                                        + name
                                        + '='
                                        + aarg.getValue().toString());
            }
        }

        // arguments contains all classes.Value for all arguments, or is empty
        return arguments;
    }

    /**
     * Adds all temporary variables declared <code>Local</code> to the arguments map. Flatten out
     * results into a map which unfolds into a vector of DAX leaves, and which are fit for a <code>
     * Job</code>.
     *
     * @param arguments is the map of variable bindings.
     * @param tr is the <code>Transformation</code> which supplies the formal arugments.
     * @param state is the book-keeper to use for temporary filenames.
     * @exception DuplicateIdentifier is thrown, if a bound name already exists in the map. This
     *     indicates either a re-used name between formal args and temporary variables, or the same
     *     temporary variable was used twice.
     */
    private void updateArgsFromLocal(Map arguments, Transformation tr, BookKeeper state) {
        // nothing to do
        for (Iterator i = tr.iterateLocal(); i.hasNext(); ) {
            Local local = (Local) i.next();
            String name = local.getName();

            // FIXME? late type checking?
            if (arguments.containsKey(name))
                throw new DuplicateIdentifier("identifier \"" + name + "\" already exists");

            if (local.getValue() == null)
                // FIXME: insert Yong's request
                throw new NullPointerException("you must define a value for " + name);

            // Fix LFNs
            Value value = local.getValue();
            java.util.List lfnlist = value.getAllLFN(LFN.INOUT);
            for (Iterator j = lfnlist.iterator(); j.hasNext(); ) {
                LFN lfn = (LFN) j.next();
                Logging.instance().log("route", 4, " oldfn = " + lfn.getFilename());
                lfn.setFilename(state.createTempName(lfn.getTemporary(), null));
                Logging.instance().log("route", 4, " newfn = " + lfn.getFilename());
            }

            // use the supplied parameter
            arguments.put(name, value);
            Logging.instance()
                    .log("route", 2, "adding temporary variable " + name + '=' + value.toString());
        }
    }

    /**
     * DAX bridge: Adds all filenames referenced by args per job to the job "uses" clause note that
     * this is a super-set of filenames, and may exceed the union between profile and CLI filenames.
     *
     * @param job is the job to augment with filenames that are used within.
     * @param arguments are the argument from which LFNs are plucked, and stuck into the "uses"
     *     section of the job.
     * @return a set of all logical filenames from the parametric lists
     */
    private Set augmentJobUses(Job job, Map arguments) {
        Scalar scalar = null;
        Set result = new HashSet();

        for (Iterator i = arguments.values().iterator(); i.hasNext(); ) {
            Value current = (Value) i.next();
            java.util.List list = current.getLFNList(-1);
            for (Iterator f = list.iterator(); f.hasNext(); ) {
                LFN lfn = (LFN) f.next();
                result.add(lfn);
                Filename file = new Filename(lfn);
                job.addUses(file);
                Logging.instance().log("route", 2, "adding uses for " + lfn.getFilename());
            }
        }
        return result;
    }

    /**
     * DAX bridge: Copies VDL profiles into DAX profiles while substituting any <code>Use</code>
     * elements.
     *
     * @param job is the job to augment with profiles.
     * @param arguments are the arguments from which any <code>Use</code> elements is substituted
     *     from. They are just passed through here.
     * @param profiles are all known profiles, from outer to inner TR.
     */
    private void augmentJobProfile(Job job, Map arguments, java.util.List profiles) {
        for (Iterator i = profiles.iterator(); i.hasNext(); ) {
            org.griphyn.vdl.classes.Profile src = (org.griphyn.vdl.classes.Profile) i.next();
            org.griphyn.vdl.dax.Profile dst =
                    new org.griphyn.vdl.dax.Profile(src.getNamespace(), src.getKey());
            for (Iterator e = src.iterateLeaf(); e.hasNext(); ) {
                java.util.List n = convertLeaf((org.griphyn.vdl.classes.Leaf) e.next(), arguments);
                for (Iterator f = n.iterator(); f.hasNext(); ) {
                    dst.addLeaf((org.griphyn.vdl.dax.Leaf) f.next());
                }
            }
            job.addProfile(dst);
        }
    }

    /**
     * DAX bridge: Converts VDL <code>Argument</code> items into DAX arguments while substituting
     * any <code>Use</code> elements againt their value.
     *
     * @param job is the job to augment with profiles.
     * @param arguments are the arguments from which any <code>Use</code> elements is substituted
     *     from. They are just passing through here.
     * @param tr is the Transformation that provides the profiles.
     */
    private void augmentJobArguments(Job job, Map arguments, Transformation tr) {
        // construct DAX argument line from VDL argument list.
        String separator = tr.getArgumentSeparator();
        for (Iterator e = tr.iterateArgument(); e.hasNext(); ) {
            org.griphyn.vdl.classes.Argument src = (org.griphyn.vdl.classes.Argument) e.next();
            String name = src.getName();
            for (Iterator i = src.iterateLeaf(); i.hasNext(); ) {
                org.griphyn.vdl.classes.Leaf leaf = (org.griphyn.vdl.classes.Leaf) i.next();
                java.util.List neu = convertLeaf(leaf, arguments);
                if (name != null
                        && (name.equals("stdin")
                                || name.equals("stdout")
                                || name.equals("stderr"))) {
                    String varname = null;
                    if (leaf instanceof Use) varname = ((Use) leaf).getName();
                    else varname = "(null)";

                    // do stdio parsing. The resulting element must be exactly one
                    // Filename element
                    if (neu.size() != 1 || !(neu.get(0) instanceof Filename))
                        throw new IllegalArgumentException(
                                "invalid spec for stdio: You must use exactly one LFN");
                    else {
                        Filename filename = (Filename) neu.get(0);

                        filename.setVariable(varname);
                        if (name.equals("stdin")) {
                            job.setStdin(filename);
                        } else if (name.equals("stdout")) {
                            job.setStdout(filename);
                        } else if (name.equals("stderr")) {
                            job.setStderr(filename);
                        }
                    }
                } else {
                    // regular arguments, add bits and pieces
                    for (Iterator f = neu.iterator(); f.hasNext(); ) {
                        job.addArgument((org.griphyn.vdl.dax.Leaf) f.next());
                    }
                }
            }

            // add default argument separator here
            if (e.hasNext() && separator != null && separator.length() > 0) {
                job.addArgument(new org.griphyn.vdl.dax.PseudoText(separator));
            }
        }
    }

    /**
     * Helper function to implement the "type casting" of contained LFNs into the necessary usage
     * type. Not all LFNs can be casted.
     *
     * @param ul is the provided linkage in the <code>Use</code> element.
     * @param lfn is the logical filename to adjust. This argument may be adjusted in place, if the
     *     cast condition matches
     * @see #updateScalarFromPreset( String, Scalar, Map )
     * @see #updateFromPreset( String, Scalar, Map )
     */
    private void castLFN(int ul, LFN lfn) {
        if (lfn.getLink() == LFN.INOUT && (ul == LFN.INPUT || ul == LFN.OUTPUT)) lfn.setLink(ul);
    }

    /**
     * Helper function to update the guts of a <code>Scalar</code>, each occurance of a <code>Use
     * </code> by its matching value list from the preset map.
     *
     * @param key is a symbolic representation of the variable that we are currently mapping
     * @param s is the Scalar to remap
     * @param preset is the map with values to replace with.
     * @return the new value from the replacement. This method enforces the return of a Scalar.
     * @see #mapCallToDerivation( String, String, String, Call, Map )
     */
    private Scalar updateScalarFromPreset(String key, Scalar s, Map preset) {
        Logging.instance()
                .log("route", 5, "updateScalarFromPreset( " + key + '=' + s.toString() + " )");
        for (int i = 0; i < s.getLeafCount(); ++i) {
            org.griphyn.vdl.classes.Leaf leaf = s.getLeaf(i);

            // only work on Use elements, ignore other elements
            if (leaf instanceof Use) {
                Use use = (Use) leaf;

                // check existence of key
                if (!preset.containsKey(use.getName()))
                    throw new RuntimeException("unable to resolve " + use.getName());

                Value v2 = (Value) preset.get(use.getName());
                if (v2.getContainerType() != Value.SCALAR)
                    throw new RuntimeException(
                            "cannot map list " + use.getName() + " to scalar " + key);

                // FIXME: worry about linkage

                // remove <code>Use</code> element...
                s.removeLeaf(i);

                Scalar s2 = (Scalar) v2;
                int ul = use.getLink(); // use linkage
                for (int j = 0; j < s2.getLeafCount(); ++j) {
                    org.griphyn.vdl.classes.Leaf l = s2.getLeaf(j);
                    if (ul != -1 && l instanceof LFN) {
                        // adjust linkage for i/o LFN, if use is IN or OUT
                        LFN lfn = (LFN) ((LFN) l).clone();
                        castLFN(ul, lfn);
                        s.addLeaf(i + j, lfn);
                    } else {
                        s.addLeaf(i + j, l);
                    }
                }
                i += s2.getLeafCount() - 1;
            }
        }

        return s;
    }

    /**
     * Helper function to update the guts of a <code>Scalar</code>. This function just checks for
     * the one very special case in argument passing.
     *
     * <ul>
     *   <li>The Scalar value contains just one Leaf.
     *   <li>The Leaf is of type Use.
     *   <li>The Use maps to a List by the presets.
     * </ul>
     *
     * If all these conditions are true, the List is returned. All other cases are handled by {@link
     * #updateScalarFromPreset}.
     *
     * @param key is a symbolic representation of the variable that we are currently mapping
     * @param s is the Scalar to remap
     * @param preset is the map with values to replace with.
     * @return the new value from the replacement. Note that, due to the Use element, the result can
     *     be a list to be passed.
     * @see #mapCallToDerivation( String, String, String, Call, Map )
     */
    private Value updateFromPreset(String key, Scalar s, Map preset) {
        Logging.instance().log("route", 5, "updateFromPreset( " + key + '=' + s.toString() + " )");

        // one very specific circumstance for passing a variable
        // from a use which contains a list
        if (s.getLeafCount() == 1 && s.getLeaf(0) instanceof Use) {
            Use use = (Use) s.getLeaf(0);
            String useName = use.getName();

            // check existence of key
            if (!preset.containsKey(useName))
                throw new RuntimeException("unable to resolve " + useName);

            Value v2 = (Value) preset.get(useName);
            int ul = use.getLink(); // use linkage
            if (v2.getContainerType() == Value.LIST && ul != -1) {
                // do the type casting here and now
                org.griphyn.vdl.classes.List list =
                        (org.griphyn.vdl.classes.List) v2.clone(); // FIXME: memory?

                Logging.instance().log("trace", 4, "mapping list to " + key);
                for (Iterator i = list.iterateScalar(); i.hasNext(); ) {
                    for (Iterator j = ((Scalar) i.next()).iterateLeaf(); j.hasNext(); ) {
                        org.griphyn.vdl.classes.Leaf l = (org.griphyn.vdl.classes.Leaf) j.next();
                        if (l instanceof LFN) castLFN(ul, (LFN) l);
                    }
                }
                return list;
            }
        }

        // old code
        return updateScalarFromPreset(key, s, preset);
    }

    /**
     * Maps an anonymous <code>Call</code> to a named <code>Derivation</code>. The <code>Call</code>
     * may contain references to bound variables as <code>Use</code> leaves. These must be
     * translated into their respective actual argument value with the help of the arguments hash.
     *
     * @param namespace is the namespace in which to produce the new Derivation. The value may be
     *     null.
     * @param name is a name prefix to prepend the call id with.
     * @param version is the version, which may be null.
     * @param me is the <code>Call</code> to translate.
     * @param arguments is a map of actual arguments to substitute for bound references.
     * @return a new <code>Derivation</code> to be used in the stead of the <code>Call</code>.
     */
    private Derivation mapCallToDerivation(
            String namespace, String name, String version, Call me, Map arguments) {
        Derivation result =
                new Derivation(
                        namespace,
                        name + '.' + me.shortID(),
                        version,
                        me.getUsesspace(),
                        me.getUses(),
                        me.getMinIncludeVersion(),
                        me.getMaxIncludeVersion());

        Logging.instance().log("trace", 2, "mapCallToDerivation: " + me.toString());
        Logging.instance().log("route", 2, "creating DV " + result.identify());
        for (Iterator f = me.iteratePass(); f.hasNext(); ) {
            Pass pass = (Pass) ((Pass) f.next()).clone();
            String key = pass.getBind();
            Logging.instance().log("route", 4, "pold " + key + '=' + pass.getValue().toString());

            Value value = null;
            switch (pass.getValue().getContainerType()) {
                case Value.SCALAR:
                    value = updateFromPreset(key, (Scalar) pass.getValue(), arguments);
                    break;
                case Value.LIST:
                    org.griphyn.vdl.classes.List list =
                            (org.griphyn.vdl.classes.List) pass.getValue(); // value;
                    org.griphyn.vdl.classes.List newlist = new org.griphyn.vdl.classes.List();
                    Logging.instance().log("trace", 4, "mapping list to list");
                    for (int i = 0; i < list.getScalarCount(); ++i)
                        newlist.addScalar(
                                updateScalarFromPreset(
                                        key + "[" + i + "]", list.getScalar(i), arguments));
                    value = newlist;
                    break;
                default:
                    throw new RuntimeException("should not happen");
            }

            Logging.instance().log("route", 4, "pnew " + key + '=' + value.toString());
            result.addPass(new Pass(key, value));
        }
        return result;
    }

    /**
     * This private helper methods uses a single derivation or call node and applies the immutable
     * parts stored in the transformation. The result is a job description which merges a call or DV
     * actual argument with transformation formal argument. The result will be stored in the
     * book-keeping DAX.
     *
     * <p>Compound TR will be sequenced into a number of jobs. Each later job will depend on its
     * previous job. The last job in the sequence is the replacement job for the calling derivation.
     * FIXME: what about the parentship of the first job?
     *
     * <p>
     *
     * @param dv is the {@link Derivation} to generate the job DAX info for.
     * @param state is the book-keeper to extend with transformations.
     * @param real are the real derivations to be used as PARENT for the current derivation. In case
     *     of a simple TR, the result is the derivation itself.
     * @param level is the recursion depth to record, set by caller
     * @return <code>true</code> for a simple transformation, <code>false</code> for a compound.
     */
    private boolean applyTransformation(Derivation dv, BookKeeper state, TreeSet real, int level) {
        // startup
        String id = dv.identify();
        Logging.instance().log("trace", 3, "applyTransformation(" + id + ", " + level + ')');

        // find a TR anywhere on the definitions stack, from top to bottom,
        // that matches the DV-specified constraints.
        Transformation tr =
                findMatchingTransformation(
                        dv.getUsesspace(),
                        dv.getUses(),
                        dv.getMinIncludeVersion(),
                        dv.getMaxIncludeVersion());
        Logging.instance().log("route", 2, "taken TR " + tr.identify());

        // map out actual arguments and formal argument defaults with
        // their respective bound variable name.
        Map arguments = mapActualToFormal(dv, tr);
        // POSTCONDITION: arguments contains all classes.Value for all arguments

        // FIXME: Put line below HERE to register IDs for _all_ TRs, simple and compound.
        // String nmtoken = state.mapJob( id );
        if (tr.isSimple()) {
            Logging.instance().log("route", 2, "simple TR");

            // generate DAX job
            String nmtoken = state.mapJob(id);
            Job job =
                    new Job(
                            tr.getNamespace(),
                            tr.getName(),
                            tr.getVersion(),
                            nmtoken,
                            dv.getNamespace(),
                            dv.getName(),
                            dv.getVersion());
            job.setLevel(level);

            // add LFNs from arguments to uses section of job.
            Collection c = augmentJobUses(job, arguments);
            state.addFilenames(c); // and add to DAG files section

            // Convert "use" elements while copying profiles.
            java.util.List profiles = state.getAllProfiles();
            profiles.addAll(tr.getProfileList());
            if (profiles.size() > 0) augmentJobProfile(job, arguments, profiles);

            // Convert "use" elements while constructing the argument line.
            augmentJobArguments(job, arguments, tr);

            // add compound chain
            job.setChain(state.getAllTransformations());

            // add job to DAX
            state.addJob(job);

            // indicate simple TR
            real.add(dv.identify());
        } else {
            Logging.instance().log("route", 2, "compound TR " + tr.identify());

            // new: add local variables from compound statement
            updateArgsFromLocal(arguments, tr, state);

            // create local derivation map, and populate it with compounds
            int position;
            Definitions defs = new Definitions();
            for (Iterator e = tr.iterateCall(); e.hasNext(); ) {
                Call call = (Call) e.next();
                Derivation newdv =
                        mapCallToDerivation(
                                dv.getNamespace(), dv.getName(), dv.getVersion(), call, arguments);
                Logging.instance()
                        .log(
                                "route",
                                3,
                                "adding DV " + newdv.identify() + " from CALL " + call.identify());
                defs.addDefinition(newdv);

                // Definitions tends to sanitize, and do weird things, so check again
                if ((position = defs.positionOfDefinition(newdv)) != -1)
                    Logging.instance()
                            .log("route", 3, "added DV " + defs.getDefinition(position).identify());
            }

            // remember all profiles so far
            state.pushProfile(tr.getProfileList());

            // remember from where we got here
            // state.pushTransformation( dv.identify() );
            state.pushTransformation(tr.shortID());

            // now search through local derivations first by advancing
            // the stack frame to the new derivation set.
            // this.m_stack.push( defs );
            addDefinitions(defs);

            // search in local by going backwards from produced output files
            java.util.List output = dv.getLFNList(LFN.OUTPUT);
            Logging.instance().log("route", 2, "output list " + output.toString());

            for (Iterator i = output.iterator(); i.hasNext(); ) {
                String lfn = (String) i.next();
                Logging.instance().log("route", 2, "looking for producers of LFN " + lfn);
                // compound TR should not count in levels
                Set temp = requestLfn(lfn, state, level - 1, dv.getNamespace());
                Logging.instance()
                        .log("route", 2, "LFN " + lfn + " produced by " + temp.toString());
                real.addAll(temp);
            }

            // restore previous state
            // this.m_stack.pop();
            removeDefinitions();

            // remove level the memorized top transformation from chain.
            state.popTransformation();

            // remove remembered profiles for this TR
            state.popProfile();
        }

        Logging.instance().log("trace", 2, "applyTransformation(" + id + ") := " + real.toString());
        return (tr.isSimple());
    }

    /**
     * This private helper traverses recursively the derivation dependencies, started by querying
     * for a given derivation. FIXME: It is assumed that the derivation is part of the
     * instance-central derivation list.
     *
     * @param dv is the derivation instance that we ask for
     * @param state is the book-keeping to protocol the DAG
     * @param level is the recursion depth, use 1 when calling from outside.
     * @return a set of job identifiers
     */
    private Set requestDerivation(Derivation dv, BookKeeper state, int level) {
        String id = dv.identify();
        Logging.instance().log("trace", 3, "requestDerivation(" + id + ", " + level + ')');

        TreeSet result = new TreeSet();
        if (level <= this.m_maxDepth) {
            // ok, let's work
            if (state.wasVisited(dv)) {
                // already known

                if (level <= 1) {
                    // OK, if requested directly
                    Logging.instance()
                            .log(
                                    "app",
                                    1,
                                    "Skipping direct request for " + "already known DV " + id);
                    result.addAll(state.getVisited(dv));
                } else {
                    // FIXME: Potential problem, if requested indirectly!
                    final String m =
                            "requestDerivation("
                                    + id
                                    + ") reached an unreachable branch. "
                                    + "I am not quite prepared for this, so I will rather exit now than "
                                    + "create havoc. Please contact support, supply your data and actions.";
                    throw new RuntimeException(m);
                }
            } else {
                // DO IT HERE, ...
                // disadvant: it is depth-first, but profiles are mucked up.
                // advantage: "result" is already set correctly for storing in addVisited
                //      // new node, generate job
                //      boolean isSimple = applyTransformation( dv, state, result );

                // store real inner-most simple-TR parentset for this DV
                state.addVisited(dv, result);

                TreeSet parents = new TreeSet();
                java.util.List input = dv.getLFNList(LFN.INPUT);
                for (Iterator i = input.iterator(); i.hasNext(); ) {
                    String lfn = (String) i.next();
                    parents.addAll(requestLfn(lfn, state, level, dv.getNamespace()));
                }

                // ...OR DO IT HERE
                // disadvant: "result" will be set after the fact (but it is a reference!)
                // advantage: breadth-first, correct profiles
                // new node, generate job
                boolean isSimple = applyTransformation(dv, state, result, level);

                // add parents to current node
                if (isSimple) state.addParent(dv, parents);
                else
                    Logging.instance()
                            .log("route", 4, "NOT adding " + id + " PARENTS " + parents.toString());
            }
        } else {
            Logging.instance()
                    .log("route", 0, "maximum recursion " + m_maxDepth + " reached, leaving.");
        }

        Logging.instance().log("trace", 2, "requestDerivation(" + id + ") := " + result.toString());
        return result;
    }

    /**
     * This private helper method is used in recursive calls when requesting a given logical
     * filename.
     *
     * @param lfn is the logical filename that must be a product of some derivation
     * @param state is the book-keeping structure to produce the final DAG.
     * @param level is the recursion depth, use 0 when calling from outside.
     * @param cwns is the current working namespace, may be null.
     * @return a set of job identifiers
     */
    private Set requestLfn(String lfn, BookKeeper state, int level, String cwns) {
        TreeSet result = new TreeSet();
        Logging.instance()
                .log(
                        "trace",
                        3,
                        "requestLfn("
                                + lfn
                                + ", "
                                + level
                                + ", "
                                + (cwns == null ? "null" : cwns)
                                + ')');

        // DON'T. One check in requestDerivation should be enough
        // if ( level < this.m_maxDepth )

        // find all derivations that have this file as output
        java.util.List match = this.m_stack.derivationsWithOutput(lfn);

        // if such a derivation exists...
        if (match.size() > 0) {
            Derivation dv = null;
            if (match.size() > 1) {
                // only create a fuss, if there is anything to chose from
                java.util.Map env = new java.util.TreeMap();
                env.put("cwns", cwns);
                env.put("lfn", lfn);
                env.put("level", new Integer(level));

                dv = m_arbiter.arbit(match, env);

                try {
                    env.clear();
                } catch (UnsupportedOperationException e) {
                    // ignore
                }
            }
            if (dv == null) dv = (Derivation) match.get(0);

            // if we haven't visited here before, recurse into node
            if (state.wasVisited(dv)) {
                // seen before, just return known parental relationship
                result.addAll(state.getVisited(dv));
            } else {
                // not seen before, recurse into graph
                result.addAll(requestDerivation(dv, state, level + 1));
            }
        } else {
            if (level == 0)
                Logging.instance().log("default", 0, "Unknown output LFN \"" + lfn + "\"");
        }

        Logging.instance().log("trace", 2, "requestLfn(" + lfn + ") := " + result.toString());
        return result;
    }

    /**
     * This helper method is the entry point when requesting a certain derivation. As a result, a
     * build-style DAG will be produced and maintained in the book-keeping structure. FIXME: It is
     * assumed that the derivation is part of the DatabaseSchema that was used with the c'tor.
     *
     * @param dv is the derivation asked for.
     * @return a new bookkeeper containing the DAG information.
     * @see #requestDerivation( Collection )
     * @see BookKeeper
     */
    public BookKeeper requestDerivation(Derivation dv) {
        if (m_stack.isEmpty()) return null; // ???
        BookKeeper state = new BookKeeper();
        requestDerivation(dv, state, 1);
        return state;
    }

    /**
     * This helper method is the entry point when requesting a set of derivations. As a result, a
     * build-style DAG will be produced and maintained in the book-keeping structure. Note that the
     * resulting graph may be disconnected. FIXME: It is assumed that the derivation is part of the
     * DatabaseSchema that was used with the c'tor.
     *
     * @param list is the set of derivations to asked for.
     * @return a new bookkeeper containing the DAG information.
     * @see BookKeeper
     */
    public BookKeeper requestDerivation(Collection list) {
        if (m_stack.isEmpty() || list == null) return null;
        BookKeeper state = new BookKeeper();
        for (Iterator i = list.iterator(); i.hasNext(); ) {
            Derivation dv = (Derivation) i.next();
            Logging.instance().log("route", 0, "requesting DV " + dv.identify());
            requestDerivation(dv, state, 1);
        }
        return state;
    }

    /**
     * Compares two strings, each of which may be null. If both are null, they are considered equal
     * by this function. This function relies on the fact that equals() can deal with null
     * arguments.
     *
     * @param a a string which may be null
     * @param b a string which may be null
     * @return true, if the strings equal, or if both are null.
     */
    private static boolean matchWithNull(String a, String b) {
        return (a == null ? b == null : a.equals(b));
    }

    /**
     * This helper retrieves a build-DAG for running the specified logical derivation and all its
     * required predecessors. It assumes a fully qualified derivation specification.
     *
     * @param namespace is the namespace the derivation lives in. A value of {@link
     *     Separator#DEFAULT} will be assumed for a value of null.
     * @param name is the name of the derivation. Although anonymous derivations are allowed, this
     *     method requires a named derivation.
     * @param version is a version string within the minVersion and maxVersion range defined by the
     *     derivation, which might be null.
     * @return a book-keeping structure, or null, if either no matching derivation was found.
     */
    public BookKeeper requestDerivation(String namespace, String name, String version) {
        java.util.List result = m_stack.searchForDV(namespace, name, version);

        int size = result.size();
        if (size >= 1) {
            // request all matches
            BookKeeper state = new BookKeeper();
            for (Iterator i = result.iterator(); i.hasNext(); ) {
                Derivation dv = (Derivation) i.next();
                Logging.instance().log("route", 0, "requesting DV " + dv.identify());
                requestDerivation(dv, state, 1);
            }

            return state;
        }

        // if derivation could not be found.
        return null;
    }

    /**
     * This helper retrieves a DAX for running the specified logical derivation match and all its
     * required predecessors. It assumes a fully qualified or partial-null derivation specification.
     *
     * @param namespace is the namespace the derivation lives in. Null is the joker value.
     * @param name is the name of the derivation. Although anonymous derivations are allowed, this
     *     method requires a named derivation.
     * @param version is a version string within the minVersion and maxVersion range defined by the
     *     derivation, which might be null.
     * @param state is the book-keeping structure
     * @return true, if something was found, and false if not.
     */
    public boolean requestDerivation(
            String namespace, String name, String version, BookKeeper state) {
        // sanity check first
        if (state == null) return false;

        // what have we got here?
        java.util.List result = m_stack.searchForDV(namespace, name, version);

        int size = result.size();
        if (size >= 1) {
            // request all matches
            for (Iterator i = result.iterator(); i.hasNext(); ) {
                Derivation dv = (Derivation) i.next();
                Logging.instance().log("route", 0, "requesting DV " + dv.identify());
                requestDerivation(dv, state, 1);
            }

            return true;
        }

        // if derivation could not be found.
        return false;
    }

    /**
     * This helper retrieves a DAX for a number of symbolically specified derivations. The resulting
     * graph may be disconnected. It assumes a fully qualified or partial-null derivation
     * specification.
     *
     * @param fqdi is a collection of fully-qualified derivation identifiers.
     * @param state is the book-keeping structure
     * @return true, if something was found, and false if not.
     * @see org.griphyn.common.util.Separator#splitFQDI( String )
     */
    public boolean requestDerivation(Collection fqdi, BookKeeper state) {
        // sanity check first
        if (state == null) return false;

        // use a set, so we request each DV only once
        java.util.Set result = new HashSet();
        for (Iterator i = fqdi.iterator(); i.hasNext(); ) {
            String[] n = Separator.splitFQDI((String) i.next());
            result.addAll(m_stack.searchForDV(n[0], n[1], n[2]));
        }

        int size = result.size();
        if (size >= 1) {
            // request all matches
            for (Iterator i = result.iterator(); i.hasNext(); ) {
                Derivation dv = (Derivation) i.next();
                Logging.instance().log("route", 0, "requesting DV " + dv.identify());
                requestDerivation(dv, state, 1);
            }

            return true;
        }

        // if derivation could not be found.
        return false;
    }

    /**
     * This method requests a data product logical filename. As a result, the complete build-style
     * DAG for producing the requested filename will be constructed.
     *
     * @param lfn is the filename that was requested
     * @return the book-keeping information to construct the DAG. Please note that it might be
     *     empty, if no derivation is known to produce the file. It will be null, if the
     *     definitionslist is empty.
     * @see #requestLfn( Collection, BookKeeper )
     * @see #requestLfn( String, BookKeeper, int, String )
     */
    public BookKeeper requestLfn(String lfn) {
        if (m_stack.isEmpty() || lfn == null) return null;
        BookKeeper state = new BookKeeper();
        requestLfn(lfn, state, 0, null);
        return state;
    }

    /**
     * This method requests multiple data product logical filenames. As a result, the complete
     * build-style DAG for producing the requested filename will be constructed. Please note that
     * the result may constitute a disconnected graph.
     *
     * @param list is a collection of logical filename strings.
     * @param state is the book-keeping structure
     * @see #requestLfn( String )
     * @see #requestLfn( String, BookKeeper, int, String )
     */
    public void requestLfn(Collection list, BookKeeper state) {
        if (m_stack.isEmpty() || list == null || state == null) return;
        for (Iterator i = list.iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Logging.instance().log("route", 0, "requesting LFN " + lfn);
            requestLfn(lfn, state, 0, null);
        }
    }
}
