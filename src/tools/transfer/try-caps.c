#include <ctype.h>
#include <sys/types.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "mypopen.h"
#include "capabilities.h"

int
main( int argc, char* argv[], char* envp[] )
{
  int i;

  if ( argc < 2 ) {
    fputs( "Only argument(s) are path to g-u-c\n", stderr );
    return 1;
  }

  for ( i=1; i<argc; ++i ) {
    printf( "%s has capabilities %04lx\n", 
	    argv[i], guc_capabilities( argv[i], envp ) );
  }

  return 0;
}
