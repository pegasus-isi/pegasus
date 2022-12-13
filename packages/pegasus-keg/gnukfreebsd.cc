#include "linux.hh"

#include <fstab.h>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "basic.hh"

void
pegasus_statfs( char* buffer, size_t capacity )
{
  if ( setfsent() ) {
    struct fstab* mtab; 
    char line[1024];

    while ( (mtab = getfsent()) ) {
      struct statvfs vfs; 
      /* Linux mount points may use [1] device, [2] label, [3] uuid
       * thus checking fs_spec for slash becomes futile. 
       * Checking mount point for slash instead. */ 
      if ( mtab->fs_file[0] == '/' && statvfs( mtab->fs_file, &vfs ) != -1 ) {
	if ( vfs.f_bsize > 0 && vfs.f_blocks > 0 ) { 
	  char total[16], avail[16]; 
	  unsigned long long size = vfs.f_frsize;
	  smart_units( total, sizeof(total), (size * vfs.f_blocks) );
	  smart_units( avail, sizeof(avail), (size * vfs.f_bavail) ); 

	  snprintf( line, sizeof(line),
		    "Filesystem Info: %-24s %s %s total, %s avail\n",
		    mtab->fs_file, mtab->fs_vfstype, total, avail ); 
	  strncat( buffer, line, capacity );
	}
      }
    }
  }
}


void
pegasus_loadavg( char* buffer, size_t capacity )
{
  char line[128];
  snprintf( line, sizeof(line), "Load Averages  : Unavailable\n");
  strncat( buffer, line, capacity ); 
}

void
pegasus_meminfo( char* buffer, size_t capacity )
{
  char line[128];
  snprintf( line, sizeof(line), "Memory Usage MB:  Unavailable\n");
  strncat( buffer, line, capacity ); 
}

static const char* cpu_info = 0;

void
pegasus_cpuinfo( char* buffer, size_t capacity )
{
  // fill cache once, if empty
  if ( cpu_info == 0 ) {
    unsigned n_cpu = 0;
    char* cpu_speed = 0;
    char* model_name = 0;
    char line[1024];

    // open /proc/cpuinfo to read
    FILE* proc = fopen( "/proc/cpuinfo", "r" );
    if ( proc == 0 ) return;
  
    // FIXME: This assumes SMP for now
    bool within = false;
    while ( fgets( line, sizeof(line), proc ) ) {
      within = true;
      if ( strncasecmp( line, "processor", 9 ) == 0 ) n_cpu++;
      if ( model_name == 0 && strncasecmp( line, "model name", 10 ) == 0 ) {
	line[strlen(line)-1] = '\0';
	char* s = strchr(line,':');
	if ( s ) model_name = strdup(s+2);
      }
      if ( cpu_speed == 0 && strncasecmp( line, "cpu mhz", 7 ) == 0 ) {
	line[strlen(line)-1] = '\0';
	char* s = strchr(line,':');
	if ( s ) cpu_speed = strdup(s+2);
      }
    }
    fclose(proc); 

    if ( within ) {
      size_t cpu_size = 256;
      char* dynamic = static_cast<char*>( malloc(cpu_size) );
      snprintf( dynamic, cpu_size, "Processor Info.: %d x %s @ %s\n",
		n_cpu, model_name ? model_name : "[unknown]", 
		cpu_speed ? cpu_speed : "[unknown]" );
      cpu_info = const_cast<const char*>( dynamic ); 
    } else {
      cpu_info = "";
    }
  }

  // append information to buffer, if we got this far
  if ( cpu_info && *cpu_info ) strncat( buffer, cpu_info, capacity );
}
