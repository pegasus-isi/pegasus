#include <stdio.h>

int
main( int argc, char* argv[] )
{
  int i;

  if ( argc > 1 ) fputs( "r: »", stdout );
  for ( i=1; i<argc; ++i ) {
    if ( i>1 ) fputc( ' ', stdout );
    fputs( argv[i], stdout );
  }
  if ( argc > 1 ) fputs( "«\n", stdout );

  for ( i=1; i<argc; ++i )
    printf( "%d: »%s«\n", i, argv[i] );

  return 0;
}
