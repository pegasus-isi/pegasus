#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

void*
doit( void* x )
{
  long id = ((long) x); 
  usleep(50000 + (rand() & 0x1FFFF) ); 
  flockfile(stdout); 
  printf( "I am thread %ld\n", id ); 
  funlockfile(stdout); 
  return NULL; 
}

int
main( int argc, char* argv[] )
{
  long i; 
  int status, max = ( argc > 1 ? atoi(argv[1]) : 3 ); 
  pthread_t* t = calloc( max, sizeof(pthread_t) ); 
  pthread_attr_t attr; 

  srand( time(NULL) ); 

  status = pthread_attr_init( &attr ); 
  if ( status ) {
    fprintf( stderr, "Error: pthread_attr_init: %s\n", 
	     strerror(status) ); 
    return 1;
  }
  status = pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_DETACHED );
  if ( status ) {
    fprintf( stderr, "Error: pthread_attr_setdetachstate: %s\n", 
	     strerror(status) ); 
    return 1;
  }

  for ( i=0; i<max; ++i ) {
    if ( (status = pthread_create( t+i, &attr, doit, (void*) i )) ) {
      fprintf( stderr, "Error: pthread_create(%ld): %s\n", 
	       i, strerror(status) ); 
    }
  }

  sleep(1); 
  free((void*) t); 
  pthread_attr_destroy(&attr); 
  return 0;
}
