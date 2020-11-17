/*
 * Globus Toolkit Public License (GTPL)
 *
 * Copyright (c) 1999 University of Chicago and The University of
 * Southern California. All Rights Reserved.
 *
 *  1) The "Software", below, refers to the Globus Toolkit (in either
 *     source-code, or binary form and accompanying documentation) and a
 *     "work based on the Software" means a work based on either the
 *     Software, on part of the Software, or on any derivative work of
 *     the Software under copyright law: that is, a work containing all
 *     or a portion of the Software either verbatim or with
 *     modifications.  Each licensee is addressed as "you" or "Licensee."
 *
 *  2) The University of Southern California and the University of
 *     Chicago as Operator of Argonne National Laboratory are copyright
 *     holders in the Software.  The copyright holders and their third
 *     party licensors hereby grant Licensee a royalty-free nonexclusive
 *     license, subject to the limitations stated herein and
 *     U.S. Government license rights.
 *
 *  3) A copy or copies of the Software may be given to others, if you
 *     meet the following conditions:
 *
 *     a) Copies in source code must include the copyright notice and
 *        this license.
 *
 *     b) Copies in binary form must include the copyright notice and
 *        this license in the documentation and/or other materials
 *        provided with the copy.
 *
 *  4) All advertising materials, journal articles and documentation
 *     mentioning features derived from or use of the Software must
 *     display the following acknowledgement:
 *
 *     "This product includes software developed by and/or derived from
 *     the Globus project (http://www.globus.org/)."
 *
 *     In the event that the product being advertised includes an intact
 *     Globus distribution (with copyright and license included) then
 *     this clause is waived.
 *
 *  5) You are encouraged to package modifications to the Software
 *     separately, as patches to the Software.
 *
 *  6) You may make modifications to the Software, however, if you
 *     modify a copy or copies of the Software or any portion of it,
 *     thus forming a work based on the Software, and give a copy or
 *     copies of such work to others, either in source code or binary
 *     form, you must meet the following conditions:
 *
 *     a) The Software must carry prominent notices stating that you
 *        changed specified portions of the Software.
 *
 *     b) The Software must display the following acknowledgement:
 *
 *        "This product includes software developed by and/or derived
 *         from the Globus Project (http://www.globus.org/) to which the
 *         U.S. Government retains certain rights."
 *
 *  7) You may incorporate the Software or a modified version of the
 *     Software into a commercial product, if you meet the following
 *     conditions:
 *
 *     a) The commercial product or accompanying documentation must
 *        display the following acknowledgment:
 *
 *        "This product includes software developed by and/or derived
 *         from the Globus Project (http://www.globus.org/) to which the
 *         U.S. Government retains a paid-up, nonexclusive, irrevocable
 *         worldwide license to reproduce, prepare derivative works, and
 *         perform publicly and display publicly."
 *
 *     b) The user of the commercial product must be given the following
 *        notice:
 *
 *        "[Commercial product] was prepared, in part, as an account of
 *         work sponsored by an agency of the United States Government.
 *         Neither the United States, nor the University of Chicago, nor
 *         University of Southern California, nor any contributors to
 *         the Globus Project or Globus Toolkit nor any of their employees,
 *         makes any warranty express or implied, or assumes any legal
 *         liability or responsibility for the accuracy, completeness, or
 *         usefulness of any information, apparatus, product, or process
 *         disclosed, or represents that its use would not infringe
 *         privately owned rights.
 *
 *         IN NO EVENT WILL THE UNITED STATES, THE UNIVERSITY OF CHICAGO
 *         OR THE UNIVERSITY OF SOUTHERN CALIFORNIA OR ANY CONTRIBUTORS
 *         TO THE GLOBUS PROJECT OR GLOBUS TOOLKIT BE LIABLE FOR ANY
 *         DAMAGES, INCLUDING DIRECT, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL
 *         DAMAGES RESULTING FROM EXERCISE OF THIS LICENSE AGREEMENT OR
 *         THE USE OF THE [COMMERCIAL PRODUCT]."
 *
 *  8) LICENSEE AGREES THAT THE EXPORT OF GOODS AND/OR TECHNICAL DATA
 *     FROM THE UNITED STATES MAY REQUIRE SOME FORM OF EXPORT CONTROL
 *     LICENSE FROM THE U.S. GOVERNMENT AND THAT FAILURE TO OBTAIN SUCH
 *     EXPORT CONTROL LICENSE MAY RESULT IN CRIMINAL LIABILITY UNDER U.S.
 *     LAWS.
 *
 *  9) Portions of the Software resulted from work developed under a
 *     U.S. Government contract and are subject to the following license:
 *     the Government is granted for itself and others acting on its
 *     behalf a paid-up, nonexclusive, irrevocable worldwide license in
 *     this computer software to reproduce, prepare derivative works, and
 *     perform publicly and display publicly.
 *
 * 10) The Software was prepared, in part, as an account of work
 *     sponsored by an agency of the United States Government.  Neither
 *     the United States, nor the University of Chicago, nor The
 *     University of Southern California, nor any contributors to the
 *     Globus Project or Globus Toolkit, nor any of their employees,
 *     makes any warranty express or implied, or assumes any legal
 *     liability or responsibility for the accuracy, completeness, or
 *     usefulness of any information, apparatus, product, or process
 *     disclosed, or represents that its use would not infringe privately
 *     owned rights.
 *
 * 11) IN NO EVENT WILL THE UNITED STATES, THE UNIVERSITY OF CHICAGO OR
 *     THE UNIVERSITY OF SOUTHERN CALIFORNIA OR ANY CONTRIBUTORS TO THE
 *     GLOBUS PROJECT OR GLOBUS TOOLKIT BE LIABLE FOR ANY DAMAGES,
 *     INCLUDING DIRECT, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES
 *     RESULTING FROM EXERCISE OF THIS LICENSE AGREEMENT OR THE USE OF
 *     THE SOFTWARE.
 *
 *                               END OF LICENSE
 */
package org.griphyn.vdl.annotation;

import java.util.*;

/**
 * This class defines the node structure for the tree representation of the query.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class QueryTree {
    /** Stores a predicate, as the data element of the tree node */
    private Predicate m_predicate = null;

    /** Stores the left child */
    private QueryTree m_lchild = null;

    /** Stores the right child */
    private QueryTree m_rchild = null;

    /** Constructor */
    public QueryTree() {}

    /** Constructor */
    public QueryTree(Predicate predicate) {
        m_predicate = predicate;
    }

    /** Get the data element */
    public Predicate getData() {
        return m_predicate;
    }

    /** Set the data element */
    public void setData(Predicate predicate) {
        m_predicate = predicate;
    }

    /** Get the data element */
    public int getPredicate() {
        if (m_predicate != null) return m_predicate.getPredicate();
        else return -1;
    }

    /** Add a child */
    public void addChild(QueryTree node) {
        if (m_lchild == null) m_lchild = node;
        else m_rchild = node;
    }

    /** Get the left child */
    public QueryTree getLchild() {
        return m_lchild;
    }

    /** Set the left child */
    public void setLchild(QueryTree node) {
        m_lchild = node;
    }

    /** Get the right child */
    public QueryTree getRchild() {
        return m_rchild;
    }

    /** Set the right child */
    public void setRchild(QueryTree node) {
        m_rchild = node;
    }

    public void traversal() {
        traversal(this);
    }

    public void traversal(QueryTree root) {
        if (root != null) {
            QueryTree lchild = root.getLchild();
            if (lchild != null) {
                QueryTree llchild = lchild.getLchild();
                if (llchild != null) {
                    System.out.print("(");
                }
                traversal(lchild);
                if (llchild != null) {
                    System.out.print(")");
                }
            }
            if (root.getData() != null) {
                System.out.print(" " + root.getData().toString() + " ");
            }
            QueryTree rchild = root.getRchild();
            if (rchild != null) {
                QueryTree rrchild = rchild.getRchild();
                if (rrchild != null) {
                    System.out.print("(");
                }
                traversal(rchild);
                if (rrchild != null) {
                    System.out.print(")");
                }
            }
        }
    }

    public String toSQL(int annoClass, Object arg) {
        return toSQL(this, annoClass, arg) + ";";
    }

    public String toSQL(QueryTree root, int annoClass, Object arg) {
        StringBuffer sb = new StringBuffer();
        if (root != null) {
            QueryTree lchild = root.getLchild();
            if (lchild != null) {
                QueryTree llchild = lchild.getLchild();
                if (llchild != null) {
                    sb.append("(");
                }
                sb.append(toSQL(lchild, annoClass, arg));
                if (llchild != null) {
                    sb.append(")");
                }
            }
            if (root.getData() != null) {
                sb.append(root.getData().toSQL(annoClass, arg));
            }
            QueryTree rchild = root.getRchild();
            if (rchild != null) {
                QueryTree rrchild = rchild.getRchild();
                if (rrchild != null || root.getPredicate() == Predicate.NOT) {
                    sb.append("(");
                }
                sb.append(toSQL(rchild, annoClass, arg));
                if (rrchild != null || root.getPredicate() == Predicate.NOT) {
                    sb.append(")");
                }
            }
        }
        return sb.toString();
    }

    public String toXQuery(String var) {
        return toXQuery(this, var);
    }

    public String toXQuery(QueryTree root, String var) {
        StringBuffer sb = new StringBuffer();
        if (root != null) {
            QueryTree lchild = root.getLchild();
            if (lchild != null) {
                QueryTree llchild = lchild.getLchild();
                if (llchild != null) {
                    sb.append("(");
                }
                sb.append(toXQuery(lchild, var));
                if (llchild != null) {
                    sb.append(")");
                }
            }
            if (root.getData() != null) {
                sb.append(root.getData().toXQuery(var));
            }
            QueryTree rchild = root.getRchild();
            if (rchild != null) {
                QueryTree rrchild = rchild.getRchild();
                if (rrchild != null || root.getPredicate() == Predicate.NOT) {
                    sb.append("(");
                }
                sb.append(toXQuery(rchild, var));
                if (rrchild != null || root.getPredicate() == Predicate.NOT) {
                    sb.append(")");
                }
            }
        }
        return sb.toString();
    }
}
