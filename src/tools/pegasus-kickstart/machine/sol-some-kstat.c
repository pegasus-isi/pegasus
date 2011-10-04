#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <kstat.h>
#include <sys/sysinfo.h>


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

char*
kstat_type( char* buffer, size_t capacity, uchar_t type )
/* purpose: show kstat type
 * paramtr: buffer (IO): area to output into
 *          capacity (IN): extent of the buffer to store things into
 *          type (IN): kstat type
 * returns: buffer
 */
{
  static const char* c_type[] =
    { "RAW", "NAMED", "INTR", "I/O", "TIMER" }; 

  if ( type >= KSTAT_TYPE_RAW && type <= KSTAT_TYPE_TIMER ) 
    strncpy( buffer, c_type[type], capacity );
  else
    snprintf( buffer, capacity, "TYPE:%X", type );

  return buffer; 
}

char*
kstat_name_type( char* buffer, size_t capacity, uchar_t type )
/* purpose: show kstat type
 * paramtr: buffer (IO): area to output into
 *          capacity (IN): extent of the buffer to store things into
 *          type (IN): kstat type
 * returns: buffer
 */
{
  static const char* c_named[] =
    { "CHAR", "INT32", "UINT32", "INT64", "UINT64", 
      "FLOAT", "DOUBLE", "KLONG", "KULONG", "STRING" }; 

  if ( type >= KSTAT_DATA_CHAR && type <= KSTAT_DATA_STRING ) 
    strncpy( buffer, c_named[type], capacity ); 
  else 
    snprintf( buffer, capacity, "TYPE:%X", type );

  return buffer; 
}

char*
hrtime( char* buffer, size_t capacity, hrtime_t t )
/* purpose: 64-bit nano-second counter conversion
 */
{
  char b[32];
  snprintf( buffer, capacity, "%s ns (%.1f s)", 
	    sizer( b, 32, sizeof(t), &t ), t / 1E9 );
  return buffer; 
}

size_t
min( size_t a, size_t b )
{
  return a < b ? a : b; 
}



int 
main( int argc, char *argv[] )
{
  kstat_ctl_t*   kc;
  kstat_t*       ksp; 
  void*          tmp; 
  kstat_named_t* knd; 
  kstat_timer_t  ktd; 
  kstat_intr_t   kid;
  kstat_io_t     kio; 

  unsigned int   count = 0;
  unsigned long  sum = 0; 
  unsigned int   rec = 0; 

  if ( (kc = kstat_open()) == NULL ) {
    perror( "kstat_open" );
    return 1;
  }

  for ( ksp = kc->kc_chain; ksp != NULL; ksp = ksp->ks_next ) {
    char b[32], c[128]; 
    char name[KSTAT_STRLEN+1];
    char class[KSTAT_STRLEN+1];
    char module[KSTAT_STRLEN+1];
    char ident[KSTAT_STRLEN+1]; 

    /* don't trust developers to terminate strings properly */
    strncpy( name, ksp->ks_name, KSTAT_STRLEN ); 
    strncpy( class, ksp->ks_class, KSTAT_STRLEN ); 
    strncpy( module, ksp->ks_module, KSTAT_STRLEN ); 
    class[KSTAT_STRLEN] = '\0'; 
    name[KSTAT_STRLEN] = '\0'; 
    module[KSTAT_STRLEN] = '\0'; 

    snprintf( c, sizeof(c), "%s:%u:%s", module, ksp->ks_instance, name ); 
    printf( "FOUND %-32s %-12s %-8s ",
	    c, class, kstat_type( b, 32, ksp->ks_type) );

    putchar( (ksp->ks_flags & KSTAT_FLAG_VIRTUAL) == 0 ? '-' : 'v' );
    putchar( (ksp->ks_flags & KSTAT_FLAG_VAR_SIZE) == 0 ? '-' : 's' );
    putchar( (ksp->ks_flags & KSTAT_FLAG_WRITABLE) == 0 ? '-' : 'w' );
    putchar( (ksp->ks_flags & KSTAT_FLAG_PERSISTENT) == 0 ? '-' : 'p' );
    putchar( (ksp->ks_flags & KSTAT_FLAG_DORMANT) == 0 ? '-' : 'd' );
    putchar( (ksp->ks_flags & KSTAT_FLAG_INVALID) == 0 ? '-' : 'i' );

    printf( " (records=%u size=%u)\n", 
	    ksp->ks_ndata, ksp->ks_data_size, ksp->ks_type);

    count++;
    sum += ksp->ks_data_size; 
    rec += ksp->ks_ndata; 

    switch ( ksp->ks_type ) {
      /*
       * --------------------------------------------------------
       */
    case KSTAT_TYPE_NAMED: 
      tmp = malloc( ksp->ks_data_size + 16 ); 
      if ( kstat_read( kc, ksp, tmp ) == -1 ) {
	fprintf( stderr, "error reading kstat: %d: %s, skipping\n", 
		 errno, strerror(errno) );
      } else {
	int    i; 
	double dbl;
	float  flt; 
	kstat_named_t* knd = (kstat_named_t*) tmp;

	for ( i=0; i < ksp->ks_ndata; ++i ) {
	  strncpy( ident, knd[i].name, KSTAT_STRLEN );
	  ident[KSTAT_STRLEN] = '\0';
	  printf( "\t%*s %-8s ", -KSTAT_STRLEN, ident,
		  kstat_name_type( c, 80, knd[i].data_type ) ); 
	  switch ( knd[i].data_type ) {
	  case KSTAT_DATA_CHAR: 
	    strncpy( c, knd[i].value.c, 16 );
	    c[16] = '\0';
	    puts(c);
	    break;
	  case KSTAT_DATA_UINT32:
	  case KSTAT_DATA_INT32:
	    puts( sizer( c, sizeof(c), 4, &knd[i].value ) );
	    break;
	  case KSTAT_DATA_INT64:
	  case KSTAT_DATA_UINT64:
	    puts( sizer( c, sizeof(c), 8, &knd[i].value ) );
	    break;
	  case KSTAT_DATA_FLOAT: 
	    memcpy( &flt, &knd[i].value, sizeof(flt) );
	    printf( "%f", flt ); 
	    break;
	  case KSTAT_DATA_DOUBLE: 
	    memcpy( &dbl, &knd[i].value, sizeof(dbl) );
	    printf( "%f", dbl ); 
	    break;
	  case KSTAT_DATA_STRING: 
	    if ( KSTAT_NAMED_STR_PTR(knd+i) == NULL ) {
	      puts("(null)");
	    } else {
	      puts( KSTAT_NAMED_STR_PTR(knd+i) ); 
	    }
	    break;
	  default:
	    puts( "(unsup. data type)" ); 
	    break;
	  }
	}
      }
      free((void*) tmp); 
      break;
      /*
       * --------------------------------------------------------
       */
    case KSTAT_TYPE_TIMER:
      if ( kstat_read( kc, ksp, &ktd ) == -1 ) {
	fprintf( stderr, "error reading kstat: %d: %s, skipping\n", 
		 errno, strerror(errno) );
	continue; 
      } else {
	strncpy( ident, ktd.name, KSTAT_STRLEN );
	ident[KSTAT_STRLEN] = '\0';
	printf( "\tevent name              : %s\n", ident ); 
	printf( "\tnumber of events        : %s\n", 
		sizer( b, 32, sizeof(ktd.num_events), &ktd.num_events ) ); 
	printf( "\tcumulative elapsed time : %s\n", 
		hrtime( c, 80, ktd.elapsed_time ) ); 
	printf( "\tshortest event duration : %s\n", 
		hrtime( c, 80, ktd.min_time ) ); 
	printf( "\tlongest event duration  : %s\n", 
		hrtime( c, 80, ktd.max_time ) ); 
	printf( "\tprevious even start time: %s\n", 
		hrtime( c, 80, ktd.start_time ) ); 
	printf( "\tprevious event stop time: %s\n", 
		hrtime( c, 80, ktd.stop_time ) ); 
      }
      break;
      /*
       * --------------------------------------------------------
       */
    case KSTAT_TYPE_INTR:
      if ( kstat_read( kc, ksp, &kid ) == -1 ) {
	fprintf( stderr, "error reading kstat: %d: %s, skipping\n", 
		 errno, strerror(errno) );
	continue; 
      } else {
	int i; 
	static char* c_short[KSTAT_NUM_INTRS] = 
	  { "HARD", "SOFT", "WATCHDOG", "SPURIOUS", "MULTSVC" }; 

	for ( i=0; i < KSTAT_NUM_INTRS; ++i ) {
	  printf( "\t%-8s %s\n", c_short[i],
		  sizer( b, 32, sizeof(kid.intrs[i]), &kid.intrs[i] ) ); 
	}
      }
      break;
      /*
       * --------------------------------------------------------
       */
    case KSTAT_TYPE_IO:
      if ( kstat_read( kc, ksp, &kio ) == -1 ) {
	fprintf( stderr, "error reading kstat: %d: %s, skipping\n", 
		 errno, strerror(errno) );
	continue; 
      } else {
	printf( "\tnumber of bytes read        : %s\n",
		sizer( b, 32, sizeof(kio.nread), &kio.nread ) ); 
	printf( "\tnumber of bytes written     : %s\n",
		sizer( b, 32, sizeof(kio.nwritten), &kio.nwritten ) ); 
	printf( "\tnumber of read operations   : %s\n",
		sizer( b, 32, sizeof(kio.reads), &kio.reads ) ); 
	printf( "\tnumber of write operations  : %s\n",
		sizer( b, 32, sizeof(kio.writes), &kio.writes ) ); 
	printf( "\tcumulative wait (pre-service) time   : %s\n",
		hrtime( c, 80, kio.wtime ) ); 
	printf( "\tcumulative wait length*time product  : %s\n",
		hrtime( c, 80, kio.wlentime ) ); 
	printf( "\tlast time wait queue changed         : %s\n",
		hrtime( c, 80, kio.wlastupdate ) ); 
	printf( "\tcumulative run (service) time        : %s\n",
		hrtime( c, 80, kio.rtime ) ); 
	printf( "\tcumulative run length*time product   : %s\n",
		hrtime( c, 80, kio.rlentime ) ); 
	printf( "\tlast time run queue changed          : %s\n",
		hrtime( c, 80, kio.wtime ) );
	printf( "\telements in wait state   : %s\n",
		sizer( b, 32, sizeof(kio.wcnt), &kio.wcnt ) ); 
	printf( "\telements in run state    : %s\n",
		sizer( b, 32, sizeof(kio.rcnt), &kio.rcnt ) ); 

      }
      break;
      /*
       * --------------------------------------------------------
       */
    case KSTAT_TYPE_RAW:
      /* I can only deal with very few RAW entries that 
       * I think I know something about 
       */ 
      if ( strcmp( class, "vm" ) == 0 &&
	   strcmp( module, "unix" ) == 0 &&
	   strcmp( name, "vminfo" ) == 0 ) {
	vminfo_t vm; 
	if ( kstat_read( kc, ksp, &vm ) == -1 ) {
	  fprintf( stderr, "error reading kstat: %d: %s, skipping\n", 
		   errno, strerror(errno) );
	} else {
	  printf( "\t/* I think these values are scaled by 2**10 */\n" ); 
	  printf( "\tfree memory   : %s (%.1f MB)\n",
		  sizer( b, 32, sizeof(vm.freemem), &vm.freemem ),
		  vm.freemem / 1073741824.0 );
	  printf( "\tswap reserved : %s (%.1f MB)\n",
		  sizer( b, 32, sizeof(vm.swap_resv), &vm.swap_resv ),
		  vm.swap_resv / 1073741824.0 );
	  printf( "\tswap allocated: %s (%.1f MB)\n",
		  sizer( b, 32, sizeof(vm.swap_alloc), &vm.swap_alloc ),
		  vm.swap_alloc / 1073741824.0 );
	  printf( "\tswap available: %s (%.1f MB)\n",
		  sizer( b, 32, sizeof(vm.swap_avail), &vm.swap_avail ),
		  vm.swap_avail / 1073741824.0 );
	  printf( "\tswap free     : %s (%.1f MB)\n",
		  sizer( b, 32, sizeof(vm.swap_free), &vm.swap_free ),
		  vm.swap_free / 1073741824.0 );
	}



      } else if ( strcmp( module, "cpu_stat" ) == 0 ) {
	cpu_stat_t cpu; 
	if ( kstat_read( kc, ksp, &cpu ) == -1 ) {
	  fprintf( stderr, "error reading kstat: %d: %s, skipping\n", 
		   errno, strerror(errno) );
	} else {
	  const char* c_state[] =
	    { "IDLE", "USER", "SYS", "WAIT" }; 
	  cpu_sysinfo_t* si = &cpu.cpu_sysinfo; 
	  cpu_syswait_t* sw = &cpu.cpu_syswait;
	  cpu_vminfo_t*  vm = &cpu.cpu_vminfo; 
	  unsigned long  i, total; 

	  for ( total=i=0; i < CPU_STATES; ++i ) total += si->cpu[i]; 
	  for ( i=0; i < CPU_STATES; ++i ) {
	    printf( "\tcpu state %-5s: %16u   %5.1f %%\n", 
		    ( i < 4 ? c_state[i] : "???") , si->cpu[i],
		    (100.0 * si->cpu[i]) / ((double) total) );
	  }
	  printf( "\tphysical block reads               %16u\n", si->bread ); 
	  printf( "\tphysical block writes (sync+async) %16u\n", si->bwrite );
	  printf( "\tlogical block reads                %16u\n", si->lread );
	  printf( "\tlogical block writes               %16u\n", si->lwrite );
	  printf( "\traw I/O reads                      %16u\n", si->phread );
	  printf( "\traw I/O writes                     %16u\n", si->phwrite );

	  printf( "\tcontext switches        %16u\n", si->pswitch );
	  printf( "\tinvol. context switches %16u\n", si->inv_swtch );
	  printf( "\ttraps                   %16u\n", si->trap );
	  printf( "\tdevice interrupts       %16u\n", si->intr ); 

	  printf( "\tsystem calls            %16u\n", si->syscall );
	  printf( "\tread()+readv() calls    %16u\n", si->sysread ); 
	  printf( "\twrite()+writev() calls  %16u\n", si->syswrite ); 
	  printf( "\tfork() calls            %16u\n", si->sysfork ); 
	  printf( "\tvfork() calls           %16u\n", si->sysvfork ); 
	  printf( "\texec family calls       %16u\n", si->sysexec ); 
	  printf( "\tthread creation calls   %16u\n", si->nthreads ); 

	  printf( "\tprocesses waiting for block I/O  %16d\n", sw->iowait ); 

	  printf( "\tpage reclaims (includes pageout) %16u\n", vm->pgrec );
	  printf( "\tpage reclaims from free list     %16u\n", vm->pgfrec );
	  printf( "\tpageins                          %16u\n", vm->pgin ); 
	  printf( "\tpages paged in                   %16u\n", vm->pgpgin ); 
	  printf( "\tpageouts                         %16u\n", vm->pgout ); 
	  printf( "\tpages paged out                  %16u\n", vm->pgpgout ); 
	  printf( "\tswapins                          %16u\n", vm->swapin ); 
	  printf( "\tpages swapped in                 %16u\n", vm->pgswapin ); 
	  printf( "\tswapouts                         %16u\n", vm->swapout ); 
	  printf( "\tpages swapped out                %16u\n", vm->pgswapout ); 
	  printf( "\tpages zero-filled on demand      %16u\n", vm->zfod ); 
	}
      }
      break;
    }
    /*
     * --------------------------------------------------------
     */

    putchar('\n');
  }

  printf( "\nTOTAL: %u instances, %u records, %lu total size\n", count, rec, sum ); 
  return 0;
}
