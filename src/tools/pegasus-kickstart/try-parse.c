#include <stdio.h>
#include "parse.h"

int
main( int argc, char* argv[] ) 
{
  Node* node, *head = NULL;
  int i, state = 0;
  int l = 1;

  if ( argc >= 10 ) l = 2;
  else if ( argc >= 100 ) l = 3;

  for ( i=1; i<argc; ++i ) 
    printf( "<%*d<<%s<<\n", l, i, argv[i] );
  putchar('\n');

  i = 1;
  for ( node = head = parseArgVector( argc-1, argv+1, &state ); 
	node != NULL; node = node->next ) {
    printf( ">%*d>>%s>>\n", l, i, node->data );
    i++;
  }
  putchar('\n');

  printf( "final state (32==success): %d\n", state );
  return ( state & 0x1F );
}
