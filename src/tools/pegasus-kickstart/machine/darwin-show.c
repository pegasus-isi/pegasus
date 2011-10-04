#include <sys/sysctl.h>
#include <sys/vmmeter.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "../debug.h"

int
main( int argc, char* argv[] ) 
{
  int i; 
  char s[4096]; 
  size_t len = sizeof(s); 

  for ( i=1; i<argc; ++i ) {
    if ( sysctlbyname( argv[i], &s, &len, NULL, 0 ) == -1 ) {
      fprintf( stderr, "sysctl %s: %d: %s\n", argv[i], errno, strerror(errno) );
    } else {
      debugmsg( "len=%d\n", len ); 
      hexdump( (char*) &s, len );
    }
  }
  return 0; 
}
