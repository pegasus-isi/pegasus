#include <sys/sysctl.h>
#include <sys/vmmeter.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/time.h>
#include <sys/user.h>
#include <sys/kernel_types.h>
#include <sys/vnode_if.h>
#include <sys/vnode.h>
#include <sys/proc_info.h>
#include <sys/proc.h>
#include <sys/vm.h>

#include <sys/mman.h>
#include <mach/vm_statistics.h>

static unsigned status[SZOMB+1]; 
static const char* c_state[] = 
  { "total", "idle", "runable", "sleeping", "stopped", "zombie", "other" }; 

static char mystate[1 << 20]; 

int 
hexdump( void* area, size_t size )
/* purpose: dump a memory area in old-DOS style hex chars and printable ASCII
 * paramtr: area (IN): pointer to area start
 *          size (IN): extent of area to print
 * returns: number of byte written 
 */ 
{
  static const char digit[16] = "0123456789ABCDEF";
  char a[82];
  unsigned char b[18];
  size_t i, j;
  unsigned char c;
  ssize_t result = 0;
  unsigned char* buffer = (unsigned char*) area; 

  for ( i=0; i<size; i+=16 ) {
    memset( a, 0, sizeof(a) );
    memset( b, 0, sizeof(b) );
    sprintf( a, "%04X: ", i );
    for ( j=0; j<16 && j+i<size; ++j ) {
      c = (unsigned char) buffer[i+j];

      a[6+j*3] = digit[ c >> 4 ];
      a[7+j*3] = digit[ c & 15 ];
      a[8+j*3] = ( j == 7 ? '-' : ' ' );
      b[j] = (char) (c < 32 || c >= 127 ? '.' : c);
    }
    for ( ; j<16; ++j ) {
      a[6+j*3] = a[7+j*3] = a[8+j*3] = b[j] = ' ';
    }
    strncat( a, (char*) b, sizeof(a) );
    strncat( a, "\n", sizeof(a) );
    result += fputs( a, stdout ); 
  }

  return result;
}



void
explore( int* mib, size_t miblen )
{
  size_t len = 0; 
  if ( sysctl( mib, miblen, NULL, &len, NULL, 0 ) == -1 ) {
    fprintf( stderr, "sysctl %d:%d: %s\n", mib[0], mib[1], strerror(errno) );
  } else {
    void* buffer = malloc(len);
    if ( sysctl( mib, miblen, buffer, &len, NULL, 0 ) == -1 ) 
      fprintf( stderr, "sysctl %d:%d: %s\n", mib[0], mib[1], strerror(errno) );
    else
      hexdump( buffer, len ); 
    free(buffer); 
  }
}


void
read_ps( void )
{
  FILE* ps;
  memset( mystate, '?', sizeof(mystate) );
  if ( (ps = popen( "ps ax -o pid,state", "r" )) ) {
    char line[128], state[8]; 
    int  pid; 
    fgets( line, sizeof(line), ps); /* skip */
    while ( fgets( line, sizeof(line), ps ) ) {
      sscanf( line, "%d %8s", &pid, state );
      printf( "# %5d %s\n", pid, state ); 
      mystate[pid] = state[0]; 
    }
    pclose(ps); 
  }
}

void
printkproc( struct kinfo_proc* kp )
{
  struct extern_proc* p = &kp->kp_proc;
  struct eproc*       e = &kp->kp_eproc;
  uint32_t            u[1024]; 
  size_t              len = sizeof(u[0]); 
  int                 mib[4]; 

  status[0]++; 
  if ( p->p_stat <= SZOMB ) {
    status[ p->p_stat ]++; 
  } else {
    status[SZOMB+1]++;
  }

  printf( "PROCESS %d %c\n", p->p_pid, mystate[p->p_pid] );
  if ( p->p_starttime.tv_sec > 1200000000 && 
       p->p_starttime.tv_sec < 2000000000 ) {
    printf( "starttime %10u.%06u", p->p_starttime.tv_sec, p->p_starttime.tv_usec );
  } else {
    printf( "forw %010p back %010p", p->p_forw, p->p_back );
  }
  printf( " vmspace %010p sigacts %010p\n", p->p_vmspace, p->p_sigacts ); 
  printf( "flag %08x stat %08x pid %d oppid %d dupfd %d\n",
	  p->p_flag, p->p_stat, p->p_pid, p->p_oppid, p->p_dupfd ); 
  printf( "user stack %010p exit thread %010p debugger %d sigwait %d\n",
	  (void*) p->user_stack, p->exit_thread, p->p_debugger, p->sigwait ); 
  printf( "estcpu %u cpticks %d pctcpu %.2f sleep address (wchan) %010p\n", 
	  p->p_estcpu, p->p_cpticks, p->p_pctcpu / ((double) LSCALE), p->p_wchan ); 

  printf( "wmesg \"%s\"", p->p_wmesg ? p->p_wmesg : "(null)" );
  printf( " swtime %u slptime %u\n", p->p_swtime, p->p_slptime ); 

  printf( "realtimer interval %.6f value %.6f", 
	  p->p_realtimer.it_interval.tv_sec + p->p_realtimer.it_interval.tv_usec / 1E6,
	  p->p_realtimer.it_value.tv_sec + p->p_realtimer.it_value.tv_usec / 1E6 ); 
  printf( " rtime %.6f\n", p->p_rtime.tv_sec + ( p->p_rtime.tv_usec / 1E6 ) ); 

  printf( "uticks %llu sticks %llu iticks %llu traceflags %x %010p\n", 
	  p->p_uticks, p->p_sticks, p->p_iticks, 
	  p->p_traceflag, p->p_tracep ); 
  printf( "siglist %x textvp %010p noswap %d\n", 
	  p->p_siglist, p->p_textvp, p->p_holdcnt ); 
  printf( "sigmask %08x sigignore %08x sigcatch %08x\n",
	  p->p_sigmask, p->p_sigignore, p->p_sigcatch );
  printf( "priority %u usrpri %u nice %d command \"%s\"\n",
	  p->p_priority, p->p_usrpri, p->p_nice, p->p_comm );
  printf( "progress group %010p u-area %010p x-stat %04x acflag %04x\n", 
	  p->p_pgrp, p->p_addr, p->p_xstat, p->p_acflag ); 

  printf( "proc address %010p session %010p\n", e->e_paddr, e->e_sess ); 
  printf( "real uid %d svuid %d rgid %d svgid %d\n",
	  e->e_pcred.p_ruid, e->e_pcred.p_svuid,
	  e->e_pcred.p_rgid, e->e_pcred.p_svgid ); 
  hexdump( &e->e_vm, sizeof(struct vmspace) ); 
  printf( "ppid %d pgid %d tpgid %d job cc %hd tty %8x\n", 
	  e->e_ppid, e->e_pgid, e->e_tpgid, e->e_jobc, e->e_tdev );  
  printf( "tty session %010p wchan msg \"%s\" login \"%s\"\n", 
	  e->e_tsess, e->e_wmesg, e->e_login ); 
  printf( "size %d rss %hd refs %hd swrss %hd flag %08x\n", 
	  e->e_xsize, e->e_xrssize, e->e_xccount, e->e_xswrss, e->e_flag ); 

  putchar( '\n' );
}

int
main( int argc, char* argv[] ) 
{
  int    i, mib[4];
  size_t len;
  struct kinfo_proc* kp;

  if ( argc > 1 ) {
    /* do specific processes */
    for ( i=1; i<argc; ++i ) {
      mib[0] = CTL_KERN;
      mib[1] = KERN_PROC;
      mib[2] = KERN_PROC_PID;
      mib[3] = strcmp(argv[i],"self") ? atoi(argv[i]) : getpid();
      if ( sysctl( mib, 4, NULL, &len, NULL, 0 ) == -1 ) {
	perror("sysctl"); 
	return 1;
      } else {
	void* buffer = malloc(len);
	if ( sysctl( mib, 4, buffer, &len, NULL, 0 ) == -1 ) {
	  fprintf( stderr, "sysctl %d: %s\n\n", mib[3], strerror(errno) );
	} else {
	  printkproc( (struct kinfo_proc*) buffer ); 
	}
	free( buffer ); 
      }
    } /* for */
  } else {
    /* no args, do all */
    read_ps();

    /* Fill out the first three components of the mib */
    mib[0] = CTL_KERN;
    mib[1] = KERN_PROC;
    mib[2] = KERN_PROC_ALL;
    if ( sysctl( mib, 3, NULL, &len, NULL, 0 ) == -1 ) {
      perror( "sysctl" );
      return 1;
    } else if ( len > 0 ) {
      void* buffer = malloc( len + sizeof(struct kinfo_proc) ); 
      if ( sysctl( mib, 3, buffer, &len, NULL, 0 ) == -1 ) {
	perror( "sysctl" );
	return 1;
      }
      for ( kp = (struct kinfo_proc*) buffer; 
	    kp->kp_proc.p_pid &&
	      kp < ((struct kinfo_proc*) ((char*) buffer)+len); 
	    ++kp ) {
	printkproc(kp); 
      }
      free(buffer);
      for ( i=0; i<SZOMB+1; ++i )
	printf( "%-8s %u\n", c_state[i], status[i] );
    }
  }

  return 0; 
}
