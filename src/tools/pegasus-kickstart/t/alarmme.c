#include <sys/types.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

volatile sig_atomic_t boo; 

static
void
sig_alarm( int signo )
{
  boo = 1; 
}

int
main( int argc, char* argv[] )
{
  if ( signal( SIGALRM, sig_alarm ) == SIG_ERR ) {
    perror( "signal(SIGALRM)" );
    return 1; 
  }
  alarm(1); 
  while ( ! boo ) {
    usleep(50000); 
  }
  return 0; 
}
