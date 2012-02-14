#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>

void*
doit( void* x )
{
  long id = ((long) x); 
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

  for ( i=0; i<max; ++i ) {
    if ( (status = pthread_create( t+i, NULL, doit, (void*) i )) ) {
      fprintf( stderr, "Error: pthread_create(%ld): %s\n", 
	       i, strerror(status) ); 
    }
  }
  for ( i=0; i<max; ++i ) { 
    if ( (status = pthread_join( t[i], NULL )) ) {
      fprintf( stderr, "Error: pthread_join(%ld): %s\n", 
	       i, strerror(status) ); 
    }
  }

  free((void*) t); 
  return 0;
}
