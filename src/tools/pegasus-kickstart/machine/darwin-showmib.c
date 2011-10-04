#include <sys/sysctl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "../debug.h"

int
main( int argc, char* argv[] ) 
{
  int i, mib[4]; 

  if ( argc == 1 && argc > 5 ) {
    fprintf( stderr, "Usage: %s mib0 [mib1 [mib2 [mib3]]]\n", argv[0] );
    return 0;
  } else {
    size_t len, m = argc - 1;

    for ( i=1; i<argc; ++i )
      mib[i-1] = strcmp(argv[i],"self") ? atoi( argv[i] ) : getpid(); 
    if ( sysctl( mib, m, NULL, &len, NULL, 0 ) == -1 ) {
      fprintf( stderr, "sysctl %d", mib[0] );
      for ( i=1; i<m; ++i ) fprintf( stderr, ":%d", mib[i] ); 
      fprintf( stderr, ": %s\n", strerror(errno) ); 
      return 1;
    } else {
      void* buffer = malloc(len); 
      if ( sysctl( mib, m, buffer, &len, NULL, 0 ) == -1 ) {
	fprintf( stderr, "sysctl %d", mib[0] );
	for ( i=1; i<m; ++i ) fprintf( stderr, ":%d", mib[i] ); 
	fprintf( stderr, ": %s\n", strerror(errno) ); 

	free(buffer); 
	return 1;
      } else {
	hexdump( buffer, len ); 
      }
      free(buffer); 
    }
  }
  return 0; 
}
