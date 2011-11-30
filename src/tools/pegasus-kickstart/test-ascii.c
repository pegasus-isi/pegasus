#include <stdio.h>

int
main( int argc, char* argv[] )
{
  int i;
  for ( i=0; i<256; ++i ) {
    printf( "i=%-3d (%02x) %c\n", i, i, i );
    if ( (i & 15) == 15 ) putchar('\n');
  }
  return 0;
}
