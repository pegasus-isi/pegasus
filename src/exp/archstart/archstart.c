#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

/*
 * #!/bin/sh
 * arch=`uname -s | tr 'A-Z' 'a-z'`
 * file=`basename $0`
 * root=`dirname $0`
 * 
 * test -x $root/$arch/$file && exec $root/$arch/$file "$@"
 * echo "Error: unable to locate appropriate executable for architecture $arch" 1>&2
 * exit 1
 */

int
main( int argc, char* argv[] )
{
  size_t size = getpagesize();
  char *s, *base, *dir, *arg0;
  struct utsname u;
  struct stat st;

  /* uname -s */
  if ( uname(&u) == -1 ) {
    fprintf( stderr, "uname: %d: %s\n", errno, strerror(errno) );
    return 1;
  }

  /* tr [[:upper:]] [[:lower:]] */
  for ( s = u.sysname; *s && s - u.sysname < SYS_NMLN; ++s ) {
    if ( ! isalpha(*s) ) {
      *s = '\0';
      break;
    }
    *s = tolower(*s);
  }

  /* basename / dirname */
  if ( (base=strrchr( (dir=argv[0]), '/')) == NULL ) {
    size_t size = getpagesize();
    char*  temp = (char*) malloc(size);

    if ( getcwd(temp,size) == NULL ) {
      fprintf( stderr, "getwd(%u): %d: %s\n", size, errno, strerror(errno) );
      return 1;
    }
    base = argv[0];
    dir = temp;
  } else {
    /* FIXME: may fail on some platforms */
    *base++ = '\0';
  }

  size <<= 1;
  if ( (arg0 = (char*) malloc(size)) == NULL ) {
    fputs( "out of memory\n", stderr );
    return 1;
  }

  strncpy( arg0, dir, size );
  strncat( arg0, "/", size );
  strncat( arg0, u.sysname, size );
  strncat( arg0, "/", size );
  strncat( arg0, base, size );

  if ( stat( arg0, &st ) == -1 || st.st_size == 0 || ! S_ISREG(st.st_mode) ) {
    /* FIXME: do permissions checking, too */
    fprintf( stderr, "unable to locate executable for architecture %s\n"
	     "was using %s\n", u.sysname, arg0 );
    return 2;
  }

  if ( execv( arg0, argv ) == -1 ) perror("exec");
  return 3;
}
