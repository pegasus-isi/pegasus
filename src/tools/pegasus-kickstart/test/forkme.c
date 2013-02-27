#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

int
main( int argc, char* argv[] )
{
  switch ( fork() ) {
  case -1: 
    perror( "fork" );
    return 1; 
  case 0: 
    puts( "child" );
    return 0;
  default: 
    puts( "parent" );
    return ( wait(NULL) == -1 ); 
  }
  return 0;
}
