/****************************************************************************
* MODULE:       R-Tree library 
*              
* AUTHOR(S):    Antonin Guttman - original code
*               Daniel Green (green@superliminal.com) - major clean-up
*                               and implementation of bounding spheres
*               
* PURPOSE:      Multidimensional index
*
* COPYRIGHT:    (C) 2001 by the GRASS Development Team
*
*               This program is free software under the GNU General Public
*               License (>=v2). Read the file COPYING that comes with GRASS
*               for details.
*****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "assert.h"
#include "index.h"
#include "card.h"
#include "mfmalloc.h"

#define  ADDRESS 0
#define  ID      1

long  nodeCount;
int   maxLevel;
long  rootID;

struct Node *nodes;
struct Node *newNodes;


/* Make a new index, empty.  Consists of a single node. */
struct Node * RTreeNewIndex(void)
{
	struct Node *x;

        nodeCount = 0;
        maxLevel  = 0;
	rootID    = 0;

	/* The tree traversal logic fails if there is    */
	/* "pointer" to node 0.  For real memory this    */
	/* cannot happen but for our memory mapped files */
	/* it can and does.  To avoid this, we will      */
	/* allocate a dummy node which will never be     */
	/* used as part of the tree.  A bit of a kludge  */
	/* but better than mucking around with index     */
	/* offsets all over the place.                   */

	x = RTreeNewNode();


	/* This is the real "first" node */

	x = RTreeNewNode();
	x->level = 0; /* leaf */
	return x;
}

int RTreeGetNodeCount()
{
   return(nodeCount);
}

int RTreeGetMaxLevel()
{
   return(maxLevel);
}

int RTreeGetRootID()
{
   return(rootID);
}


/*
 * Search in an index tree or subtree for all data rectangles that
 * overlap the argument rectangle.
 * Return the number of qualifying data rects.
 */
int RTreeSearch(struct Node *N, struct Rect *R, SearchHitCallback shcb, void* cbarg, int mode)
{
	register struct Node *n = N;
	register struct Rect *r = R; /* NOTE: Suspected bug was R sent in as Node* and cast to Rect* here.*/
				     /*	Fix not yet tested. */
	register int hitCount = 0;
	register int i;

	long  index;

	int sdebug = 0;

        if(mode == ID)
	{
	   index = (long)N;

	   n = &nodes[index];
        }

	if(sdebug)
	{
	   if (n->level > 0)
	      printf("\nChecking internal node %ld\n", index);
	   else
	      printf("\nChecking leaf node %ld\n", index);
	}

	assert(n);
	assert(n->level >= 0);
	assert(r);

	if (n->level > 0) /* this is an internal node in the tree */
	{
		for (i=0; i<NODECARD; i++)
		{
			if (sdebug && n->branch[i].child)
			{
				printf("\nNODECARD %ld\n", (long)n->branch[i].child);
				fflush(stdout);
			}

			if (n->branch[i].child &&
			    RTreeOverlap(r,&n->branch[i].rect))
			{
				hitCount += RTreeSearch(n->branch[i].child, R, shcb, cbarg, mode);
			}
		}
	}
	else /* this is a leaf node */
	{
		for (i=0; i<LEAFCARD; i++)
		{
			if (sdebug && n->branch[i].child)
			{
				printf("\nLEAFCARD %ld\n", (long)n->branch[i].child);
				fflush(stdout);
			}

			if (n->branch[i].child &&
			    RTreeOverlap(r,&n->branch[i].rect))
			{
				hitCount++;

				if(sdebug)
				   printf("---> Found\n");

				if(shcb) /* call the user-provided callback */
					if( ! shcb((long)n->branch[i].child, cbarg))
						return hitCount; /* callback wants to terminate search early */
			}
		}
	}
	return hitCount;
}


int RTreeConvertToID(struct Node *N)
{
        register struct Node *n = N;
        register struct Node *child;
        register int i;
        int      index;

        assert(n);
        assert(n->level >= 0);

        index = n->id;

        for(i=0; i<MAXCARD; ++i)
        {
           if(n->level > 0 && n->branch[i].child)
           {
              child = n->branch[i].child;
              n->branch[i].child = (struct Node *)n->branch[i].child->id;
              RTreeConvertToID(child);
           }
        }

        return 0;
}

/*
 * JCG: Dump the parentage data for a specific item (input rectangle)
 */
int RTreeParentage(struct Node *N, int childID, int mode)
{
        register struct Node *n = N;
        register int i, count = 0;

	int index, status;

	++count;

        if(mode == ID)
	{
	   index = (long)N;

	   n = &nodes[index];
        }

        assert(n);
        assert(n->level >= 0);

        if (n->level > 0) /* this is an internal node in the tree */
        {
                for (i=0; i<NODECARD; i++)
                {
                        if (n->branch[i].child)
                        {
                            status = RTreeParentage(n->branch[i].child, childID, mode);

			    if(status)
			    {
			       if(mode == ID)
				  printf("\nINTERNAL NODE %d (%ld) / CHILD %d\n", 
				     index, (long)n, i);
			       else
				  printf("\nINTERNAL NODE %ld / CHILD %d\n", (long)n, i);

			       printf("TRACE> level = %d\n", n->level);
			       printf("TRACE> count = %d\n", n->count);

			       printf("TRACE> branch[%d].child = %ld\n", i, (long)n->branch[i].child);
			       printf("TRACE> child x:  %13.10f  %13.10f\n",
				  n->branch[i].rect.boundary[0],
				  n->branch[i].rect.boundary[3]);

			       printf("TRACE> child y:  %13.10f  %13.10f\n",
				  n->branch[i].rect.boundary[1],
				  n->branch[i].rect.boundary[4]);

			       printf("TRACE> child z:  %13.10f  %13.10f\n",
				  n->branch[i].rect.boundary[2],
				  n->branch[i].rect.boundary[5]);

			       return 1;
			    }
                        }
                }
        }

        else /* this is a leaf node */
        {
                for (i=0; i<LEAFCARD; i++)
                {
                        if ((long)n->branch[i].child == (long)childID)
                        {
			   if(mode == ID)
			      printf("\nLEAF NODE %d (%ld) / CHILD %d\n", 
				 index, (long)n, i);
			   else
			      printf("\nLEAF NODE %ld / CHILD %d\n", (long)n, i);
			
			   printf("TRACE> level = %d\n", n->level);
			   printf("TRACE> count = %d\n", n->count);

                           printf("TRACE> branch[%d].child = %ld\n", i, (long)(n->branch[i].child));

			   printf("TRACE> x:  %13.10f  %13.10f\n",
			      n->branch[i].rect.boundary[0],
			       n->branch[i].rect.boundary[3]);

			   printf("TRACE> y:  %13.10f  %13.10f\n",
			      n->branch[i].rect.boundary[1],
			      n->branch[i].rect.boundary[4]);

			   printf("TRACE> z:  %13.10f  %13.10f\n",
			      n->branch[i].rect.boundary[2],
			      n->branch[i].rect.boundary[5]);

			   return 1;
                        }
                }
        }

        return 0;
}

/*
 * JCG: Dump all the nodes (tree order)
 */
int dumpCntr;

int RTreeDumpTree(struct Node *N, int dumpcount, int mode, int maxlev, int first)
{
        register struct Node *n = N;
        register int i, j, count = 0;

	int indent;

	char indentStr[1024];

	long index;

	if(first)
	   dumpCntr = 0;

	++dumpCntr;

	++count;

	if(dumpcount > 0 && count > dumpcount)
	   return 0;

        if(mode == ID)
	{
	   index = (long)N;

	   n = &nodes[index];
        }

        assert(n->level >= 0);


	/* Create an "indent string" */

	indent = (maxlev - n->level) * 3;

	for(i=0; i<1024; ++i)
	   indentStr[i] = ' ';

        indentStr[indent] = '\0';



        if (n->level > 0) /* this is an internal node in the tree */
        {
		if(mode == ID)
		   printf("\n%s%d)  INTERNAL NODE (%ld -> %ld):\n", indentStr, dumpCntr, index, (long)n);
		else
		   printf("\n%s%d)  INTERNAL NODE (%ld):\n", indentStr, dumpCntr, (long)n);
	        
		fflush(stdout);

                printf("%sn->count = %d\n", indentStr, n->count);
		fflush(stdout);

                printf("%sn->level = %d\n", indentStr, n->level);
		fflush(stdout);

                for (i=0; i<NODECARD; i++)
                {
			if(mode == ID)
			   printf("\n%sINTERNAL NODE CHILDREN (%ld -> %ld) / CHILD %d:\n", indentStr, index, (long)n, i);
			else
			   printf("\n%sINTERNAL NODE CHILDREN (%ld) / CHILD %d:\n", indentStr, (long)n, i);

		        fflush(stdout);

                        if (n->branch[i].child)
                        {
                           printf("%sn->branch[%d].child = %ld\n", indentStr, i, (long)(n->branch[i].child));
		           fflush(stdout);

                           for (j=0; j<NUMSIDES; j++)
			   {
                              printf("%sn->branch[%d].rect.boundary[%d] = %13.10f\n",
                                 indentStr, i, j, n->branch[i].rect.boundary[j]);
		              fflush(stdout);
			   }

                            RTreeDumpTree(n->branch[i].child, dumpcount, mode, maxlev, 0);

		            if(dumpcount > 0 && count > dumpcount)
			       return 0;
                        }
                }
        }

        else /* this is a leaf node */
        {
		if(mode == ID)
		   printf("\n%s%d)  LEAF NODE (%ld -> %ld):\n", indentStr, dumpCntr, index, (long)n);
		else
		   printf("\n%s%d)  LEAF NODE (%ld):\n", indentStr, dumpCntr, (long)n);

	        fflush(stdout);

                printf("%sn->count = %d\n", indentStr, n->count);
	        fflush(stdout);

                printf("%sn->level = %d\n", indentStr, n->level);
	        fflush(stdout);

                for (i=0; i<LEAFCARD; i++)
                {
                        if (n->branch[i].child)
                        {
                           printf("%sn->branch[%d].child = %ld\n", indentStr, i, (long)(n->branch[i].child));
			   fflush(stdout);

                           for (j=0; j<NUMSIDES; j++)
			   {
                              printf("%sn->branch[%d].rect.boundary[%d] = %13.10f\n",
                                 indentStr, i, j, n->branch[i].rect.boundary[j]);
		              fflush(stdout);
			   }
                        }
                }
        }

        return 0;
}


/*
 * JCG: Traverse the tree depth first and write the nodes
 * (with modified addresses) to a new tree file
 */
long next;

int RTreeReformat(struct Node *N, int maxlev, struct Node *M, int start)
{
        register struct Node *n = N;
        register struct Node *m = M;
        register int i, j;

	int indent;

	int refdebug = 0;

	char indentStr[1024];

	long nindex;
	long mindex;

	if(start)
	{
	   next     = 0;
	   dumpCntr = 0;
        }

	++dumpCntr;

        nindex = (long)N; n = &nodes   [nindex];
	mindex = (long)M; m = &newNodes[mindex];


	/* Create an "indent string" */

	indent = (maxlev - n->level) * 3;

	for(i=0; i<1024; ++i)
	   indentStr[i] = ' ';

        indentStr[indent] = '\0';


        assert(n->level >= 0);

	if(refdebug)
	{
	   printf("\n%s--> COPYING: n = %ld -> m = %ld (%ld -> %ld)\n",
	      indentStr, nindex, mindex, (long)n, (long)m);
	   fflush(stdout);
	}


        if (n->level > 0) /* this is an internal node in the tree */
        {
		if(refdebug)
		{
		   printf("\n%s%d)  INTERNAL NODE %ld:\n", indentStr, dumpCntr, nindex);
		   fflush(stdout);

		   printf("%sn->count = %d\n", indentStr, n->count);
		   fflush(stdout);

		   printf("%sn->level = %d\n", indentStr, n->level);
		   fflush(stdout);
		}

		m->count = n->count;
		m->level = n->level;

                for (i=0; i<NODECARD; i++)
                {
			if(refdebug)
			{
			   printf("\n%sINTERNAL NODE %ld / CHILD %d:\n", indentStr, nindex, i);
			   fflush(stdout);
			}
	        
                        if (n->branch[i].child)
                        {
			   if(refdebug)
			   {
			      printf("%sn->branch[%d].child = %ld\n", indentStr, i, (long)(n->branch[i].child));
			      fflush(stdout);
			   }

			   ++next;

			   m->branch[i].child = (struct Node *)next;

                           for (j=0; j<NUMSIDES; j++)
			   {
			      if(refdebug)
			      {
				 printf("%sn->branch[%d].rect.boundary[%d] = %13.10f\n",
				    indentStr, i, j, n->branch[i].rect.boundary[j]);
				 fflush(stdout);
			      }

			      m->branch[i].rect.boundary[j] = n->branch[i].rect.boundary[j];
			   }

			   if(refdebug)
			   {
			      printf("\n%s--> SWITCHING: n -> %ld / m -> %ld\n", indentStr, (long)(n->branch[i].child), (long)(m->branch[i].child));
			      fflush(stdout);
			   }

                            RTreeReformat(n->branch[i].child, maxlev, m->branch[i].child, 0);
                        }
			else
			   m->branch[i].child = 0;
                }
        }

        else /* this is a leaf node */
        {
			if(refdebug)
			{
			    printf("\n%s%d)  LEAF NODE %ld:\n", indentStr, dumpCntr, nindex);
			    fflush(stdout);

			    printf("%sn->count = %d\n", indentStr, n->count);
			    fflush(stdout);

			    printf("%sn->level = %d\n", indentStr, n->level);
			    fflush(stdout);
			 }

		m->count = n->count;
		m->level = n->level;

                for (i=0; i<LEAFCARD; i++)
                {
                        if (n->branch[i].child)
                        {
			   if(refdebug)
			   {
			      printf("%sn->branch[%d].child = %ld\n", indentStr, i, (long)(n->branch[i].child));
			      fflush(stdout);
			   }

			   m->branch[i].child = n->branch[i].child;

                           for (j=0; j<NUMSIDES; j++)
			   {
			      if(refdebug)
			      {
				 printf("%sn->branch[%d].rect.boundary[%d] = %13.10f\n",
				    indentStr, i, j, n->branch[i].rect.boundary[j]);
				 fflush(stdout);
			      }

			      m->branch[i].rect.boundary[j] = n->branch[i].rect.boundary[j];
			   }
                        }
			else
			   m->branch[i].child = 0;
                }
        }

        return 0;
}


/*
 * JCG: Dump all the nodes (memory order)
 */
int RTreeDumpMem(int count)
{
        register struct Node *n;
        register struct Node *base;
        register int i, j, k;

	base = (struct Node *)mfMemLoc();

	n = base;

	if(count == 0)
	   count = nodeCount;

	for(k=0; k<count; ++k)
	{
	   if (n->level > 0) /* this is an internal node in the tree */
	   {
		   printf("\nINTERNAL NODE %d->%ld (%ld:%ld):\n", k, (long)n,
		      (long)n - (long)base, ((long)n - (long)base) / sizeof(struct Node));

		   printf("DUMP> n->count = %d\n", n->count);
		   printf("DUMP> n->level = %d\n", n->level);

		   for (i=0; i<NODECARD; i++)
		   {
		           printf("\nINTERNAL NODE %ld / CHILD %d\n", (long)n, i);

			   if (n->branch[i].child)
			   {
			      printf("DUMP> n->branch[%d].child = %ld\n", i, (long)(n->branch[i].child));
			      for (j=0; j<NUMSIDES; j++)
				 printf("DUMP> n->branch[%d].rect.boundary[%d] = %13.10f\n",
				    i, j, n->branch[i].rect.boundary[j]);
			   }
		   }
	   }

	   else /* this is a leaf node */
	   {
		   printf("\nLEAF NODE %d->%ld (%ld:%ld):\n", k, (long)n,
		      (long)n - (long)base, ((long)n - (long)base) / sizeof(struct Node));
		   printf("DUMP> n->count = %d\n", n->count);
		   printf("DUMP> n->level = %d\n", n->level);

		   for (i=0; i<LEAFCARD; i++)
		   {
			   if (n->branch[i].child)
			   {
			      printf("DUMP> n->branch[%d].child = %ld\n", i, (long)(n->branch[i].child));
			      for (j=0; j<NUMSIDES; j++)
				 printf("DUMP> n->branch[%d].rect.boundary[%d] = %13.10f\n",
				    i, j, n->branch[i].rect.boundary[j]);
			   }
		   }
	   }

	   ++n;
	}

        return 0;
}

/*
 * Inserts a new data rectangle into the index structure.
 * Recursively descends tree, propagates splits back up.
 * Returns 0 if node was not split.  Old node updated.
 * If node was split, returns 1 and sets the pointer pointed to by
 * new_node to point to the new node.  Old node updated to become one of two.
 * The level argument specifies the number of steps up from the leaf
 * level to insert; e.g. a data rectangle goes in at level = 0.
 */
static int RTreeInsertRect2(struct Rect *r,
		long tid, struct Node *n, struct Node **new_node, int level)
{
/*
	register struct Rect *r = R;
	register long tid = Tid;
	register struct Node *n = N, **new_node = New_node;
	register int level = Level;
*/

	register int i;
	struct Branch b;
	struct Node *n2;

	assert(r && n && new_node);
	assert(level >= 0 && level <= n->level);

	/* Still above level for insertion, go down tree recursively */
	if (n->level > level)
	{
		i = RTreePickBranch(r, n);
		if (!RTreeInsertRect2(r, tid, n->branch[i].child, &n2, level))
		{
			/* child was not split */
			n->branch[i].rect =
				RTreeCombineRect(r,&(n->branch[i].rect));
			return 0;
		}
		else    /* child was split */
		{
			n->branch[i].rect = RTreeNodeCover(n->branch[i].child);
			b.child = n2;
			b.rect = RTreeNodeCover(n2);
			return RTreeAddBranch(&b, n, new_node);
		}
	}

	/* Have reached level for insertion. Add rect, split if necessary */
	else if (n->level == level)
	{
		b.rect = *r;
		b.child = (struct Node *) (tid);
		/* child field of leaves contains tid of data record */
		return RTreeAddBranch(&b, n, new_node);
	}
	else
	{
		/* Not supposed to happen */
		assert (FALSE);
		return 0;
	}
}

/* 
 * Insert a data rectangle into an index structure.
 * RTreeInsertRect provides for splitting the root;
 * returns 1 if root was split, 0 if it was not.
 * The level argument specifies the number of steps up from the leaf
 * level to insert; e.g. a data rectangle goes in at level = 0.
 * RTreeInsertRect2 does the recursion.
 */
int RTreeInsertRect(struct Rect *R, long Tid, struct Node **Root, int Level)
{
	register struct Rect *r = R;
	register long tid = Tid;
	register struct Node **root = Root;
	register int level = Level;
	register int i;
	register struct Node *newroot;
	struct Node *newnode;
	struct Branch b;
	int result;

	assert(r && root);
	assert(level >= 0 && level <= (*root)->level);
	for (i=0; i<NUMDIMS; i++) {
		assert(r->boundary[i] <= r->boundary[NUMDIMS+i]);
	}

	if (RTreeInsertRect2(r, tid, *root, &newnode, level))  /* root split */
	{
		newroot = RTreeNewNode();  /* grow a new root, & tree taller */
		newroot->level = (*root)->level + 1;

		rootID = newroot->id;

		if(newroot->level > maxLevel)
		   maxLevel = newroot->level;

		b.rect = RTreeNodeCover(*root);
		b.child = *root;
		RTreeAddBranch(&b, newroot, NULL);
		b.rect = RTreeNodeCover(newnode);
		b.child = newnode;
		RTreeAddBranch(&b, newroot, NULL);
		*root = newroot;
		result = 1;
	}
	else
		result = 0;

	return result;
}

/*
 * Allocate space for a node in the list used in DeletRect to
 * store Nodes that are too empty.
 */
static struct ListNode * RTreeNewListNode(void)
{
	return (struct ListNode *) mfMalloc(sizeof(struct ListNode));
	/* return new ListNode; */
}

static void RTreeFreeListNode(struct ListNode *p)
{
	mfFree(p);
	/* delete(p); */
}

/* 
 * Add a node to the reinsertion list.  All its branches will later
 * be reinserted into the index structure.
 */
static void RTreeReInsert(struct Node *n, struct ListNode **ee)
{
	register struct ListNode *l;

	l = RTreeNewListNode();
	l->node = n;
	l->next = *ee;
	*ee = l;
}

/*
 * Delete a rectangle from non-root part of an index structure.
 * Called by RTreeDeleteRect.  Descends tree recursively,
 * merges branches on the way back up.
 * Returns 1 if record not found, 0 if success.
 */
static int
RTreeDeleteRect2(struct Rect *R, long Tid, struct Node *N, struct ListNode **Ee)
{
	register struct Rect *r = R;
	register long tid = Tid;
	register struct Node *n = N;
	register struct ListNode **ee = Ee;
	register int i;

	assert(r && n && ee);
	assert(tid >= 0);
	assert(n->level >= 0);

	if (n->level > 0)  /* not a leaf node */
	{
	    for (i = 0; i < NODECARD; i++)
	    {
		if (n->branch[i].child && RTreeOverlap(r, &(n->branch[i].rect)))
		{
			if (!RTreeDeleteRect2(r, tid, n->branch[i].child, ee))
			{
				if (n->branch[i].child->count >= MinNodeFill) {
					n->branch[i].rect = RTreeNodeCover(
						n->branch[i].child);
				}
				else
				{
					/* not enough entries in child, eliminate child node */
					RTreeReInsert(n->branch[i].child, ee);
					RTreeDisconnectBranch(n, i);
				}
				return 0;
			}
		}
	    }
	    return 1;
	}
	else  /* a leaf node */
	{
		for (i = 0; i < LEAFCARD; i++)
		{
			if (n->branch[i].child &&
			    (struct Node *)(n->branch[i].child) == (struct Node *) tid)
			{
				RTreeDisconnectBranch(n, i);
				return 0;
			}
		}
		return 1;
	}
}

/*
 * Delete a data rectangle from an index structure.
 * Pass in a pointer to a Rect, the tid of the record, ptr to ptr to root node.
 * Returns 1 if record not found, 0 if success.
 * RTreeDeleteRect provides for eliminating the root.
 */
int RTreeDeleteRect(struct Rect *R, long Tid, struct Node**Nn)
{
	register struct Rect *r = R;
	register long tid = Tid;
	register struct Node **nn = Nn;
	register int i;
	struct Node *tmp_nptr = NULL;
	struct ListNode *reInsertList = NULL;
	register struct ListNode *e;

	assert(r && nn);
	assert(*nn);
	assert(tid >= 0);

	if (!RTreeDeleteRect2(r, tid, *nn, &reInsertList))
	{
		/* found and deleted a data item */

		/* reinsert any branches from eliminated nodes */
		while (reInsertList)
		{
			tmp_nptr = reInsertList->node;
			for (i = 0; i < MAXKIDS(tmp_nptr); i++)
			{
				if (tmp_nptr->branch[i].child)
				{
					RTreeInsertRect(
						&(tmp_nptr->branch[i].rect),
						(long)(tmp_nptr->branch[i].child),
						nn,
						tmp_nptr->level);
				}
			}
			e = reInsertList;
			reInsertList = reInsertList->next;
			RTreeFreeNode(e->node);
			RTreeFreeListNode(e);
		}
		
		/* check for redundant root (not leaf, 1 child) and eliminate */
		if ((*nn)->count == 1 && (*nn)->level > 0)
		{
			for (i = 0; i < NODECARD; i++)
			{
				tmp_nptr = (*nn)->branch[i].child;
				if(tmp_nptr)
					break;
			}
			assert(tmp_nptr);
			RTreeFreeNode(*nn);
			*nn = tmp_nptr;
		}
		return 0;
	}
	else
	{
		return 1;
	}
}

