#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>

#include "pegasus-invoke.h"

static const char* RCS_ID =
"$Id$";

int debug = 0;

static
void
helpMe( const char* arg0 )
/* purpose: print invocation quick help with currently set parameters
 * paramtr: arg0 (IN): argument vector element 0 from main
 */
{
  const char* p = strrchr( arg0, '/' );
  if ( p ) ++p;
  else p = arg0;

  puts( RCS_ID );
  printf( "Usage:\t%s [options] (app | @fn) [arg | @fn [..]]\n\n", p ); 
  printf(
"Optional arguments:\n"
" -d\tincrease debug level, show what is going on.\n"
" -h\tprint this help message and exit.\n"
" --\tend options procesinng.\n"
"\n"
"Mandatory arguments (mutually exclusive):\n"
" app\tname of the application to run w/o relying on PATH.\n"
" @fn\tname of a file with one argument per line, app as 1st.\n"
"\n"
"Further options (inclusive, repetitive, any order):\n"
" arg\tcommand-line argument\n"
" @fn\tname of file with one argument per line.\n"
"\n" );
}

static
int
log10( long x )
{
  int result;
  if ( x == 0 ) return 1;
  else for ( result=0; x > 0; ++result ) x/=10;
  return result;
}

static 
long
limit( int name, long defValue )
{
  long result = sysconf( name );
  if ( result <= 0 ) result = defValue;
  return result;
}

int
main( int argc, char* argv[], char* envp[] )
{
  size_t i, j, size, total, capacity = argc;
  char** arg = malloc( capacity * sizeof(char*) );
  int width, keeploop = 1;
  long maxArgSize = limit( _SC_ARG_MAX, ARG_MAX );

  /* show help, if invoked empty */
  if ( argc == 1 ) {
    helpMe(argv[0]);
    return 127;
  }

  /* parse options to invoke without disturbing app options. */
  for ( i=1; i<argc && argv[i][0]=='-' && keeploop; ++i ) {
    switch ( argv[i][1] ) {
    case 'd':
      for ( j=1; argv[i][j]=='d'; ++j ) debug++;
      break;
    case 'h':
      helpMe(argv[0]);
      return 0;
    case '-':
      keeploop = 0;
      break;
    default:
      fprintf( stderr, "Illegal argument %zd: %s", i, argv[i] );
      helpMe(argv[0]);
      return 127;
    }
  }

  /* parse rest of command line */
  for ( size=0; i < argc; ++i ) {
    if ( add_arg( argv[i], &arg, &size, &capacity, 1 ) == -1 ) {
      /* check for errors */
      fprintf( stderr, "Problems with argument %zd: %s\n",
	       i, strerror(errno) );
    }
  }

  /* finalize argument array */
  arg[size] = NULL;

  /* determine size */
  width = log10(size);
  for ( total=size, i=0; i<size; ++i ) {
    if ( debug ) printf( "# %*zd: %s\n", width, i, arg[i] );
    total += strlen(arg[i]); 
  }
  if ( debug ) printf( "# length=%zd, limit=%ld\n", total, maxArgSize );

  /* warn about system limits */
  if ( total >= maxArgSize ) {
    fprintf( stderr, 
	     "Warning: command-line length (%zd) exceeds system limit (%ld)\n", 
	     total, maxArgSize );
  }

  /* run program */
  execve( arg[0], arg, envp );

  /* only reached in case of error */
  perror( arg[0] );
  return 127;
}
