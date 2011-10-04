#include <sys/types.h>
#include <sys/wait.h>
#include <mach/task.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mach/mach.h>
#include <mach/host_info.h>
#include <mach/mach_host.h>
#include <mach/mach_vm.h>
#include <mach/vm_statistics.h>

#include <math.h> /* fmod */
#include <sys/sysctl.h> /* FSCALE */

natural_t
basic_info( mach_port_t port )
{
  host_basic_info_data_t bi;
  mach_msg_type_number_t ic = HOST_BASIC_INFO_COUNT;
  host_info( port, HOST_BASIC_INFO, (host_info_t) &bi, &ic ); 
  puts( "\nHOST_BASIC_INFO" );
  printf( "max number of CPUs possible       %d\n", bi.max_cpus );
  printf( "number of CPUs now available      %d\n", bi.avail_cpus );
  printf( "size of memory in bytes (2GB cap) %lu\n", bi.memory_size ); 
  printf( "actual size of physical memory    %llu\n", bi.max_mem ); 
  printf( "CPU type        %d\n", bi.cpu_type );
  printf( "CPU sub-type    %d\n", bi.cpu_subtype );
  printf( "CPU thread-type %d\n", bi.cpu_threadtype ); 
  printf( "available physical CPUs %d\n", bi.physical_cpu ); 
  printf( "maximum physical CPUs   %d\n", bi.physical_cpu_max ); 
  printf( "available logical CPUs  %d\n", bi.logical_cpu ); 
  printf( "maximum logical CPUs    %d\n", bi.logical_cpu_max ); 

  return bi.max_cpus; 
}

void
host_load_info( mach_port_t port )
{
  static const char* state[3] = { "user", "system", "idle" }; 

  int i;
  host_load_info_data_t li; 
  mach_msg_type_number_t ic = HOST_LOAD_INFO_COUNT;
  host_statistics( port, HOST_LOAD_INFO, (host_info_t) &li, &ic );
  puts( "\nHOST_LOAD_INFO" );
  for ( i=0; i<3; ++i ) {
    printf( "state %-6s average number of runnable processes divided by CPUs     %ld\n",
	    state[i], li.avenrun[i] ); 
    printf( "state %-6s processing resources avail. to new threads (mach factor) %ld\n", 
	    state[i], li.mach_factor[i] ); 
  }
}

static const char* cpu_state[CPU_STATE_MAX] = 
  { "user", "system", "idle", "nice" }; 

void
host_cpu_load_info( mach_port_t port, long ticks, natural_t cpus )
{
  int i;
  double up, sum = 0.0;  
  host_cpu_load_info_data_t li; 
  mach_msg_type_number_t ic = HOST_CPU_LOAD_INFO_COUNT;
  host_statistics( port, HOST_CPU_LOAD_INFO, (host_info_t) &li, &ic );
  puts( "\nHOST_CPU_LOAD_INFO" );
  for ( i=0; i<CPU_STATE_MAX; ++i ) sum += li.cpu_ticks[i]; 
  for ( i=0; i<CPU_STATE_MAX; ++i )
    printf( "tick sum in state %-6s %10ld (%.1f %%)\n", 
	    cpu_state[i], li.cpu_ticks[i], 
	    (100.0 * li.cpu_ticks[i]) / sum ); 

  /* 
   * Note: the difference between boot time and uptime is the
   * amount that the system was put to sleep (laptop lid close)
   */ 
  up = (sum / (ticks * cpus) ) ; 
  printf( "uptime %.0fd%02.0f:%02.0f:%05.2f\n", up / 86400,
	  fmod(up,86400) / 3600, fmod(up,3600) / 60, fmod(up,60) ); 
}

void
host_vm_info( mach_port_t port )
{
  vm_statistics_data_t vm; 
  mach_msg_type_number_t ic = HOST_VM_INFO_COUNT;
  double f = getpagesize() / 1048576.0; 

  host_statistics( port, HOST_VM_INFO, (host_info_t) &vm, &ic );
  puts( "\nHOST_VM_INFO" );
  printf( "free       %8ld %7.2f MB\n", vm.free_count, vm.free_count * f );
  printf( "active     %8ld %7.2f MB\n", vm.active_count, vm.active_count * f );
  printf( "inactive   %8ld %7.2f MB\n", vm.inactive_count,  vm.inactive_count * f );
  printf( "wired down %8ld %7.2f MB\n", vm.wire_count, vm.wire_count * f );
  printf( "# of zero fill pages                  %ld\n", vm.zero_fill_count );
  printf( "# of reactivated pages                %ld\n", vm.reactivations );
  printf( "# of requests from a pager (pageins)  %ld\n", vm.pageins );
  printf( "# of pageouts                         %ld\n", vm.pageouts ); 
  printf( "# of times vm_fault was called        %ld\n", vm.faults );
  printf( "# of copy-on-write faults             %ld\n", vm.cow_faults );
  printf( "object cache lookups %ld (%.0f %% hit-rate)\n", vm.lookups,
	  (100.0 * vm.hits) / vm.lookups ); 
}

#ifdef CPUINFO

void
cpu_info( host_name_port_t port, long ticks ) 
/*
 * warning: This method requires sudo privileges for host_get_host_priv_port()
 */ 
{
  host_priv_t   host_priv;
  kern_return_t kr; 
  processor_port_array_t  processor_list; 
  natural_t               i, j, processor_count, info_count; 

  puts( "\nHOST_PROCESSORS" );

  if ( (kr = host_get_host_priv_port( port, &host_priv )) != KERN_SUCCESS ) {
    mach_error( "host_get_host_priv_port:", kr );
    return ;
  }
  if ( (kr = host_processors( host_priv, &processor_list, &processor_count )) != KERN_SUCCESS ) {
    mach_error( "host_processors:", kr );
    return ;
  }

  for ( i=0; i < processor_count; ++i ) {
    processor_basic_info_data_t    bi;
    processor_cpu_load_info_data_t li;
    double up, sum = 0.0; 

    info_count = PROCESSOR_BASIC_INFO_COUNT;
    if ( (kr=processor_info( processor_list[i],
			     PROCESSOR_BASIC_INFO, 
			     &port,
			     (processor_info_t) &bi,
			     &info_count )) == KERN_SUCCESS ) {
      printf( "CPU: slot %d%s, %srunning, type %d, subtype %d\n", 
	      bi.slot_num, 
	      ( bi.is_master ? " (master)" : "" ),
	      ( bi.running ? "" : "not " ),
	      bi.cpu_type, bi.cpu_subtype ); 
    }

    info_count = PROCESSOR_CPU_LOAD_INFO_COUNT;
    if ( (kr=processor_info( processor_list[i],
			     PROCESSOR_CPU_LOAD_INFO,
			     &port,
			     (processor_info_t)&li,
			     &info_count)) == KERN_SUCCESS ) {

      sum = 0.0; 
      for ( j=0; j<CPU_STATE_MAX; ++j ) sum += li.cpu_ticks[j]; 
      for ( j=0; j<CPU_STATE_MAX; ++j )
	printf( " %s %ld (%.1f %%)",
		cpu_state[j], 
		li.cpu_ticks[j],
		(100.0 * li.cpu_ticks[j]) / sum ); 

      up = (sum / ticks); 
      printf( "\n uptime %.0fd%02.0f:%02.0f:%05.2f\n", up / 86400,
	      fmod(up,86400) / 3600, fmod(up,3600) / 60, fmod(up,60) ); 
    }
  } /* for */

  vm_deallocate( mach_task_self(), (vm_address_t) processor_list,
		 processor_count * sizeof(processor_t*) ); 
}

#endif /* CPUINFO */


int
main( int argc, char* argv[] )
{
  mach_port_t self = mach_host_self();
  long ticks = sysconf(_SC_CLK_TCK); 
  natural_t cpus = basic_info( self ); 
  host_load_info( self ); 
  host_cpu_load_info( self, ticks, cpus ); 
  host_vm_info( self ); 
#ifdef CPUINFO
  cpu_info( self, ticks ); 
#endif 

  return 0;
}

