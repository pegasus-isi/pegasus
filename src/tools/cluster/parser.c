/**
 *  Copyright 2007-2010 University Of Southern California
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "tools.h"

static const char* RCS_ID =
  "$Id$";
extern int debug; 

/* create finite state automaton to remove one level of quoting in the
 * same manner as a shell. This means in particular, in case you are not
 * aware (see man of your shell): 
 *
 * o backslashes are meaningless inside single quotes
 * o single quotes are meaningless inside double quotes
 *
 *     |  sq |  dq |  bs | lws | otr | EOS
 * ----+-----+-----+-----+-----+-----+-----
 *   0 | 3,- | 4,- | 2,- | 0,- | 1,s | F,-
 *   1 | 3,- | 4,- | 2,- | 0,F | 1,s | F,F
 *   2 | 1,s | 1,s | 1,s | 1,s | 1,s | E1
 *   3 | 1,- | 3,s | 3,s | 3,s | 3,s | E2
 *   4 | 4,s | 1,- | 5,- | 4,s | 4,s | E3
 *   5 | 5,s | 5,s | 5,s | 5,s | 5,s | E1
 * ----+-----------------------------------
 *   6 | F   | final state, done with success
 *   7 | E1  | error: premature end of string
 *   8 | E2  | error: missing single quote
 *   9 | E3  | error: missing double quote
 */
static char c_state[6][6] =
  { { 3, 4, 2, 0, 1, 6 },    /* 0: skip linear whitespace */
    { 3, 4, 2, 0, 1, 6 },    /* 1: gobble unquoted nonspaces */
    { 1, 1, 1, 1, 1, 7 },    /* 2: unquoted backslash */
    { 1, 3, 3, 3, 3, 8 },    /* 3: single quote mode */
    { 4, 1, 5, 4, 4, 9 },    /* 4: double quote mode */
    { 4, 4, 4, 4, 4, 7 } };  /* 5: double quote backslash mode */

static char c_action[6][6] = 
  { { 0, 0, 0, 0, 1, 0 },    /* 0: skip linear whitespace */
    { 0, 0, 0, 2, 1, 2 },    /* 1: gobble unquoted nonspaces */
    { 1, 1, 1, 1, 1, 0 },    /* 2: unquoted backslash */
    { 0, 1, 1, 1, 1, 0 },    /* 3: single quote mode */
    { 1, 0, 0, 1, 1, 0 },    /* 4: double quote mode */
    { 1, 1, 1, 1, 1, 0 } };  /* 5: double quote backslash mode */

static
int
charclass( char input )
{
  if ( input == 0 ) return 5;
  else switch ( input ) {
  case '\'': return 0;
  case '"' : return 1;
  case '\\': return 2;
  case ' ' : return 3;
  case '\t': return 3;
  default: return 4;
  }
}

typedef struct s_node {
  char*           data;
  struct s_node*  next;
} t_node;

size_t
interpreteArguments( char* cmd, char*** argv )
/* purpose: removes one layer of quoting and escaping, shell-style
 * paramtr: cmd (IO): commandline to split
 * paramtr: argv (OUT): argv[] vector, newly allocated vector
 * returns: argc 
 */
{
  t_node* head = NULL;
  t_node* tail = NULL;
  char* s = cmd;
  size_t capacity = getpagesize();
  size_t size = 0;
  size_t argc = 0;
  char* store = (char*) malloc( capacity );
  int   class, state = 0;
  char  ch;

  while ( state < 6 ) {
    if ( (class = charclass((ch=*s))) != 5 ) s++;
    if ( debug > 2 ) showerr( "[debug state=\"%d\" class=\"%d\" ch=\"%c\"]\n",
			      state, class, ch );

    /* handle action */
    switch ( c_action[state][class] ) {
    case 0: /* noop */
      break;

    case 1: /* save char */
      if ( size+1 >= capacity ) {
	/* need to increate buffer to accomodate longer string */
	size_t c = capacity << 1;
	char* t = (char*) malloc(c);
	memcpy( t, store, size );
	free((void*) store );
	capacity = c;
	store = t;
      }

      /* put character into buffer, and finish the C-string */
      store[size++] = ch;
      store[size] = 0;
      break;

    case 2: /* finalize this argument */
      if ( head == NULL && tail == NULL ) {
	/* initially */
	head = tail = (t_node*) malloc( sizeof(t_node) );
      } else {
	/* later */
	tail->next = (t_node*) malloc( sizeof(t_node) );
	tail = tail->next;
      }

      /* copy string so far into data section, and reset string */
      tail->data = strdup( store );
      tail->next = NULL;
      size = 0;
      store[size] = 0;

      /* counts number of arguments in vector we later must allocate */
      argc++;
      break;

    default: /* must not happen - FIXME: Complain bitterly */
      break;
    }

    /* advance state */
    state = c_state[state][class];
  }

  /* FIXME: What if state is not 6 ? */
  if ( state != 6 ) {
    showerr( "[syntax-error state=\"%d\" argc=\"%zu\" cmd=\"%s\"]\n",
	     state, argc, cmd );
    free((void*) store);
    return 0;
  }

  /* create result vector from list while freeing list */
  *argv = (char**) calloc( sizeof(char*), argc+1 );
  for ( size=0; head != NULL; ) {
    (*argv)[size++] = head->data;
    tail = head;
    head = head->next;
    free((void*) tail);
  }

  /* finalize argument vector */
  (*argv)[size] = NULL;

  free((void*) store);
  return argc;
}
