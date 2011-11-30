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
#ifndef _PARSE_H
#define _PARSE_H

typedef struct _Node {
  const char*   data;
  struct _Node* next;
} Node;

extern
size_t
countNodes( const Node* head );
/* purpose: count the number of element in list
 * paramtr: head (IN): start of the list.
 * returns: number of elements in list.
 */

extern
void
deleteNodes( Node* head );
/* purpose: clean up the created list and free its memory.
 * paramtr: head (IO): start of the list.
 */

extern
Node*
parseCommandLine( const char* line, int* state );
/* purpose: parse a commandline into a list of arguments while
 *          obeying single quotes, double quotes and replacing
 *          environment variable names.
 * paramtr: line (IN): commandline to parse
 *          state (IO): start state to begin, final state on exit
 *          state==32 is ok, state>32 is an error condition which
 *          lead to a premature exit in parsing.
 * returns: A (partial on error) list of split arguments.
 */

extern
Node*
parseArgVector( int argc, char* const* argv, int* state );
/* purpose: parse an already split commandline into a list of arguments while
 *          ONLY translating environment variable names that are not prohibited
 *          from translation by some form of quoting (not double quotes, though).
 * paramtr: argc (IN): number of arguments in the argument vector
 *          argv (IN): argument vector to parse
 *          state (IO): start state to begin, final state on exit
 *          state==32 is ok, state>32 is an error condition which
 *          lead to a premature exit in parsing.
 * returns: A (partial on error) list of split arguments. The argument number
 *          stays the same, but environment variables were translated.
 */

#endif /* _PARSE_H */
