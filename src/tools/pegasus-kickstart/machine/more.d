#pragma D option bufsize=16M

int start;
int result_type; 

string mib0[int]; 
string whence[int];

dtrace:::BEGIN
{
   start = walltimestamp;
   result_type = 0; 

   /* there's gotta be a better way than this */
   mib0[1] = "CTL_KERN";
   mib0[2] = "CTL_VM";
   mib0[3] = "CTL_VFS";
   mib0[4] = "CTL_NET";
   mib0[5] = "CTL_DEBUG";
   mib0[6] = "CTL_HW";
   mib0[7] = "CTL_MACHDEP";
   mib0[8] = "CTL_USER"; 
   
   whence[0] = "SEEK_SET";
   whence[1] = "SEEK_CUR";
   whence[2] = "SEEK_END";
}

fbt:::entry
/pid == $target/
{
  printf( "+\t%s\n", probefunc );
}

syscall:::entry
/pid == $target/
{
  self->ts = walltimestamp;
  printf( "%d\t%s", (self->ts - start) / 1000, probefunc ); 
}



syscall::lseek:entry
/pid == $target/
{
  printf( "( %d, %d, %s )", arg0, arg1, whence[arg2] );
}

syscall::__sysctl:entry
/pid == $target && arg1 == 2/
{
   mib = (int*) copyin( arg0, arg1 * 4 );
   len = (int*) copyin( arg3, 4 ); 
   printf( "( 0x%p [%s,%d], %d, 0x%p, 0x%p [%d], 0x%p, %d )", 
   	   arg0, mib0[mib[0]], mib[1], arg1, arg2, arg3, *len, arg4, arg5 );
}

syscall::__sysctl:entry
/pid == $target && arg1 == 3/
{
   mib = (int*) copyin( arg0, arg1 * 4 );
   len = (int*) copyin( arg3, 4 ); 
   printf( "( 0x%p [%s,%d,%d], %d, 0x%p, 0x%p [%d], 0x%p, %d )", 
   	   arg0, mib0[mib[0]], mib[1], mib[2], 
	   arg1, arg2, arg3, *len, arg4, arg5 );
}

syscall::__sysctl:entry
/pid == $target && arg1 == 4/
{
   mib = (int*) copyin( arg0, arg1 * 4 );
   len = (int*) copyin( arg3, 4 ); 
   printf( "( 0x%p [%s,%d,%d,%d], %d, 0x%p, 0x%p [%d], 0x%p, %d )", 
   	   arg0, mib0[mib[0]], mib[1], mib[2], mib[3],
	   arg1, arg2, arg3, *len, arg4, arg5 );
}

syscall::write:entry,
syscall::write_nocancel:entry
/pid == $target/
{
   printf( "( %d, \"%S\", %d )", arg0, stringof(copyinstr(arg1)), arg2 );
}

syscall::read:entry,
syscall::read_nocancel:entry
/pid == $target/
{
   printf( "( %d, 0x%x, %d )", arg0, arg1, arg2 );
   result_type = arg1; 
}

syscall::open:entry,
syscall::open_nocancel:entry,
syscall::shm_open:entry
/pid == $target/
{
   fn = stringof(copyinstr(arg0)); 
   printf( "( \"%s\", 0x%x, 0%o )", fn, arg1, arg2 );
}

syscall::access:entry
/pid == $target/
{
   fn = stringof(copyinstr(arg0)); 
   printf( "( \"%s\", %d )", fn, arg1 );
}

syscall::exit:entry
/pid == $target/
{
   printf( "( 0x%x [%d:%d] )\n", arg0, (arg0 >> 8), (arg0 & 127) );
}

syscall::mmap:entry
/pid == $target && arg4 >= 0 && arg4 < 65536/
{
   printf( "( 0x%p, %d, 0x%x, 0x%x, %d, %d ) ", 
   	   arg0, arg1, arg2, arg3, arg4, arg5 ); 
   self->result_type = 1; 
}

syscall::mmap:entry
/pid == $target && (arg4 < 0 || arg4 >= 65536)/
{
   printf( "( 0x%p, %d, 0x%x, 0x%x, 0x%x, %d )", 
   	   arg0, arg1, arg2, arg3, arg4, arg5 );
   self->result_type = 1; 
}

syscall::munmap:entry
/pid == $target/
{
   printf( "( 0x%p, %d )", arg0, arg1 );
}

syscall::close:entry,
syscall::close_nocancel:entry
/pid == $target/
{
   printf( "( %d )", arg0 );
}

syscall::stat:entry,
syscall::stat64:entry,
syscall::lstat:entry,
syscall::lstat64:entry
/pid == $target/
{
   fn = stringof(copyinstr(arg0)); 
   printf( "( \"%s\", 0x%p )", fn, arg1 );
}

syscall::fstat:entry,
syscall::fstat64:entry
/pid == $target/
{
   printf( "( %d, 0x%p )", arg0, arg1 );
}

syscall::ioctl*:entry,
syscall::fcntl*:entry
/pid == $target/
{
  printf( "( %d, 0x%x, 0x%x )", arg0, arg1, arg2 );
}




syscall:::entry
/pid == $target && self->ts/
{
  printf("\n");
}

syscall:::return
/pid == $target && self->ts && self->result_type == 0/
{
  diff = ( walltimestamp - self->ts ) / 1000;
  printf( " = %d (%d µs)\n", arg1, diff ); 
  self->ts = 0;
}

syscall:::return
/pid == $target && self->ts != 0 && self->result_type == 1/
{
  diff = ( walltimestamp - self->ts ) / 1000;
  printf( " = 0x%p (%d µs)\n", arg1, diff ); 
  self->result_type = 0; 
  self->ts = 0;
}

syscall:::return
/pid == $target && self->ts != 0 && self->result_type > 10/
{
  diff = ( walltimestamp - self->ts ) / 1000;
  printf( " = %d (%d µs) [\"%S\"]\n", 
  	  arg1, diff, stringof(copyinstr(self->result_type)) );
  self->result_type = 0; 
  self->ts = 0;
}
