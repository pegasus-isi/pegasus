#include <sys/types.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

int
main( int argc, char* argv[] )
{
  int which = argc > 1 ? atoi(argv[1]) : SIGTERM; 
  if ( which < 0 || which > 64 ) which = SIGTERM; 
  kill( getpid(), which );
  return 127; 
}
