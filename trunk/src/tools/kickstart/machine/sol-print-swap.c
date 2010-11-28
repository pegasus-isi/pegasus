#include <sys/types.h>
#include <sys/stat.h>
#include <sys/swap.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define MAXSTRSIZE 80


char*
sizer( char* buffer, size_t capacity, size_t vsize, const void* value )
/* purpose: format an unsigned integer of less-known size. Note that
 *          64bit ints on 32bit systems need %llu, but 64/64 uses %lu
 * paramtr: buffer (IO): area to output into
 *          capacity (IN): extent of the buffer to store things into
 *          vsize (IN): size of the value
 *          value (IN): value to format
 * warning: only for 32bit and 64bit platforms
 * returns: buffer
 */
{
  switch ( vsize ) {
  case 2:
    snprintf( buffer, capacity, "%hu", 
              *((const short unsigned*) value) );
    break;
  case 4:
    if ( sizeof(long) == 4 ) 
      snprintf( buffer, capacity, "%lu", 
                *((const long unsigned*) value) );
    else 
      snprintf( buffer, capacity, "%u", 
                *((const unsigned*) value) );
    break;
  case 8:
    if ( sizeof(long) == 4 ) {
      snprintf( buffer, capacity, "%llu", 
                *((const long long unsigned*) value) );
    } else {
      snprintf( buffer, capacity, "%lu", 
                *((const long unsigned*) value) );
    }
    break;
  default:
    snprintf( buffer, capacity, "unknown" );
    break;
  }

  return buffer;
}



int
main( int argc, char* argv[] )
{
  swaptbl_t      *s;
  int            i, n, num;
  char           b[32], *strtab;    /* string table for path names */

again:
  if ((num = swapctl(SC_GETNSWP, 0)) == -1) {
    perror("swapctl: GETNSWP");
    exit(1);
  }
  if (num == 0) {
    fprintf(stderr, "No Swap Devices Configured\n");
    exit(2);
  }
  /* allocate swaptable for num+1 entries */
  if ((s = (swaptbl_t *)
       malloc(num * sizeof(swapent_t) +
	      sizeof(struct swaptable))) ==
      (void *) 0) {
    fprintf(stderr, "Malloc Failed\n");
    exit(3);
  }
  /* allocate num+1 string holders */
  if ((strtab = (char *)
       malloc((num + 1) * MAXSTRSIZE)) == (void *) 0) {
    fprintf(stderr, "Malloc Failed\n");
    exit(3);
  }
  /* initialize string pointers */
  for (i = 0; i < (num + 1); i++) {
    s->swt_ent[i].ste_path = strtab + (i * MAXSTRSIZE);
  }
  
  s->swt_n = num + 1;
  if ((n = swapctl(SC_LIST, s)) < 0) {
    perror("swapctl");
    exit(1);
  }
  if (n > num) {        /* more were added */
    free(s);
    free(strtab);
    goto again;
  }
  
  for (i = 0; i < n; i++) {
    double tmp; 
    struct swapent* e = s->swt_ent+i; 
    printf( "DEVICE %s\n", e->ste_path ); 
    printf( "\tstarting block for swapping  : %s\n", 
	    sizer( b, 32, sizeof(e->ste_start), &e->ste_start ) );
    tmp = e->ste_length / ( 1048576.0 / 512 ); 
    printf( "\tlength of swap area in blocks: %s (%.1f MB)\n",
	    sizer( b, 32, sizeof(e->ste_length), &e->ste_length ), tmp );
    tmp = e->ste_pages / ( 1048576.0 / getpagesize() ); 
    printf( "\tnumbers of pages for swapping: %s (%.1f MB)\n", 
	    sizer( b, 32, sizeof(e->ste_pages), &e->ste_pages ), tmp );
    tmp = e->ste_free / ( 1048576.0 / getpagesize() ); 
    printf( "\tnumbers of ste_pages free    : %s (%.1f MB)\n", 
	    sizer( b, 32, sizeof(e->ste_free), &e->ste_free ), tmp );
  }

  return 0;
}
