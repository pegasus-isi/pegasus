#ifdef linux
#include <gnu/libc-version.h>
#else
#error "This will only work with Linux"
#endif

#include <stdio.h>

int
main( void )
{
  int major, minor, level;
  sscanf( gnu_get_libc_version(), "%d.%d.%d", &major, &minor, &level );
  /* enforce at least 2.2.5 */
  if ( ((major == 2 && minor > 2) ||
	(major == 2 && minor == 2 && level >= 5)) ) {
    puts("Your glibc is fresh enough");
    return 0;
  } else {
    puts("Your glibc is too old, required is a minimum of 2.2.5");
    return 1;
  }
}
