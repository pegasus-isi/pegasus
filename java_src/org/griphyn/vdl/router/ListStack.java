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

import java.util.*;
import org.griphyn.vdl.util.Logging;

/**
 * This class maintains a stack of classes that implement the {@link java.util.List} interface.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 */
public class ListStack {
    /** Stores a reference to the underlying data as top-of-stack. */
    private Vector m_stack;

    /** C'tor: Creates a new stack instance that is empty. */
    public ListStack() {
        this.m_stack = new Vector();
    }

    /**
     * Pushes a new bunch of things onto the stack.
     *
     * @param item is the new List to become new top-of-stack.
     */
    public void push(List item) {
        int size = this.m_stack.size();
        int logsize = (item == null ? 0 : item.size());
        Logging.instance()
                .log("stack", 2, "pushing " + logsize + " items into lstack[" + size + ']');
        this.m_stack.addElement(item);
    }

    /**
     * Removes the tos, thus makeing the next-lower vector the tos.
     *
     * @return the old top-of-stack.
     * @throws EmptyStackException if the stack did not have any elements.
     */
    public List pop() {
        int size = this.m_stack.size();
        if (size == 0) throw new EmptyStackException();

        Logging.instance().log("stack", 2, "popping lstack[" + (size - 1) + ']');
        return (List) this.m_stack.remove(size - 1);
    }

    /**
     * Accessor: Grants access to the top of stack (tos) element.
     *
     * @return the current top of stack vector.
     * @throws EmptyStackException if the stack is empty.
     */
    public List tos() {
        int size = this.m_stack.size();
        if (size == 0) throw new EmptyStackException();
        return (List) this.m_stack.lastElement();
    }

    /**
     * Accessor predicate: Determines, if the stack contains any elements.
     *
     * @return true, if the stack is empty, false otherwise.
     */
    public boolean isEmpty() {
        return (this.m_stack.size() == 0);
    }

    /**
     * Accessor: Determines the number of elements in the stack.
     *
     * @return number of elements, or 0 for an empty stack.
     */
    public int size() {
        return this.m_stack.size();
    }

    /**
     * Accessor to a definition at a certain position. This method is susceptible to exception
     * thrown by the <code>List</code> for inaccessible positions.
     *
     * @return List at a given position in the stack.
     */
    public List at(int index) {
        return (List) this.m_stack.elementAt(index);
    }

    /**
     * Flattens all vectors in the stack into one, starting with the bottom-most vector.
     *
     * @return List containing all vectors, may be empty.
     */
    public List flatten() {
        ArrayList result = new ArrayList();
        for (int i = 0; i < this.m_stack.size(); ++i) {
            result.addAll((List) this.m_stack.elementAt(i));
        }
        return result;
    }
}
