#include <errno.h>
#include <stdio.h>
#include <unistd.h>

static
int
processors( void )
{
  long config = 
#ifdef _SC_NPROCESSORS_CONF
    sysconf( _SC_NPROCESSORS_CONF )
#else
    1
#endif 
    ; 

  long online = 
#ifdef _SC_NPROCESSORS_ONLN
    sysconf( _SC_NPROCESSORS_ONLN )
#else
    1 
#endif
    ;

  if ( config <= 0 ) config = 1; 
  if ( online <= 0 ) online = 1; 
  return config < online ? config : online;
}

int
main( int argc, char* argv[] )
{
  int cpus = processors(); 
  printf( "found %d processor%s\n", cpus, ( cpus == 1 ? "" : "s" ) ); 
  return 0; 
}
