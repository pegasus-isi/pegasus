/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <unistd.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>

#include "mynss.h"
#include "statinfo.h"
#include "tools.h"

static const char* RCS_ID =
"$Id$";

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */
int make_application_executable = 0;
size_t data_section_size = 262144ul;

int
myaccess( const char* path )
/* purpose: check a given file for being accessible and executable
 *          under the currently effective user and group id. 
 * paramtr: path (IN): current path to check
 * globals: make_application_executable (IN): if true, chmod to exec
 * returns: 0 if the file is accessible, -1 for not
 */
{
  /* sanity check */
  if ( path && *path ) {
    struct stat st;
    if ( stat(path,&st) == 0 && S_ISREG(st.st_mode) ) {
      /* stat on file succeeded, and it is a regular file */
      if ( make_application_executable ) {
	mode_t mode = st.st_mode;
	if ( st.st_uid == geteuid() ) mode |= (S_IXUSR | S_IRUSR);
	if ( st.st_gid == getegid() ) mode |= (S_IXGRP | S_IRGRP);
	mode |= (S_IXOTH | S_IROTH);
	chmod( path, mode );

	/* update stat record */
	if ( stat(path,&st) != 0 || ! S_ISREG(st.st_mode) ) {
	  /* restat'ing the file now failed */
	  return -1;
	}
      }

      if ( ( st.st_uid == geteuid() && (S_IXUSR & st.st_mode) == S_IXUSR ) ||
	   ( st.st_gid == getegid() && (S_IXGRP & st.st_mode) == S_IXGRP ) ||
	   ( (S_IXOTH & st.st_mode) == S_IXOTH ) ) {
	/* all is well, app is executable and accessible */
	return 0;
      } else {
	return -1;
      }
    } else {
      /* stat call failed, or file is not a regular file */
      return -1;
    }
  } else {
    /* illegal filename string (empty or NULL) */
    return -1;
  }
}

char*
findApp( const char* fn )
/* purpose: check the executable filename and correct it if necessary
 * paramtr: fn (IN): current knowledge of filename
 * returns: newly allocated fqpn of path to exectuble, or NULL if not found
 */
{
  char* s, *path, *t = NULL;

  /* sanity check */
  if ( fn == NULL || *fn == '\0' ) return NULL;

  /* don't touch absolute paths */
  if ( *fn == '/' ) {
    if ( myaccess(fn) == 0 ) return strdup(fn);
    else return NULL;
  }

  /* try from CWD */
  if ( myaccess(fn) == 0 ) return strdup(fn);

  /* continue only if there is a PATH to check */
  if ( (s=getenv("PATH")) == NULL ) return NULL;
  else path = strdup(s);

  /* tokenize to compare */
  for ( s=strtok(path,":"); s; s=strtok(NULL,":") ) {
    size_t len = strlen(fn) + strlen(s) + 2;
    t = (char*) malloc(len);
    strncpy( t, s, len );
    strncat( t, "/", len );
    strncat( t, fn, len );
    if ( myaccess(t) == 0 ) break;
    else {
      free((void*) t);
      t = NULL;
    }
  }

  /* some or no matches found */
  free((void*) path);
  return t;
}

int
forcefd( const StatInfo* info, int fd )
/* purpose: force open a file on a certain fd
 * paramtr: info (IN): is the StatInfo of the file to connect to (fn or fd)
 *          the mode for potential open() is determined from this, too.
 *          fd (IN): is the file descriptor to plug onto. If this fd is 
 *          the same as the descriptor in info, nothing will be done.
 * returns: 0 if all is well, or fn was NULL or empty.
 *          1 if opening a filename failed,
 *          2 if dup2 call failed
 */
{
  /* is this a regular file with name, or is this a descriptor to copy from? */
  int isHandle = ( info->source == IS_HANDLE || info->source == IS_TEMP );
  int mode = info->file.descriptor; /* openmode for IS_FILE */

  /* initialize the newHandle variable by opening regular files, or copying the fd */
  int newfd = isHandle ?
    info->file.descriptor :
    ( ( (mode & O_ACCMODE) == O_RDONLY ) ? 
      open( info->file.name, mode ) :
      /* FIXME: as long as stdout/stderr is shared between jobs,
       * we must always use append mode. Truncation happens during
       * initialization of the shared stdio. */
      open( info->file.name, mode | O_APPEND, 0666 ) );

  /* this should only fail in the open() case */
  if ( newfd == -1 ) return 1;

  /* create a duplicate of the new fd onto the given (stdio) fd. This operation
   * is guaranteed to close the given (stdio) fd first, if open. */
  if ( newfd != fd ) {
    /* FIXME: Does dup2 guarantee noop for newfd==fd on all platforms ? */
    if ( dup2( newfd, fd ) == -1 ) return 2;
  }

  /* if we opened a file, we need to close it again. */
  if ( ! isHandle ) close(newfd);

  return 0;
}

int
initStatInfoAsTemp( StatInfo* statinfo, char* pattern )
/* purpose: Initialize a stat info buffer with a temporary file
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          pattern (IO): is the input pattern to mkstemp(), will be modified!
 * returns: a value of -1 indicates an error 
 */
{
  int result = mkstemp(pattern);
  memset( statinfo, 0, sizeof(StatInfo) );

  if ( result == -1 ) {
    /* mkstemp has failed, au weia! */
    statinfo->source = IS_INVALID;
    statinfo->error = errno;

  } else {
    /* try to ensure append mode for the file, because it is shared
     * between jobs. If the SETFL operation fails, well there is nothing
     * we can do about that. */
    int flags = fcntl( result, F_GETFL );
    if ( flags != -1 ) fcntl( result, F_SETFL, flags | O_APPEND );

    /* this file descriptor is NOT to be passed to the jobs? So far, the
     * answer is true. We close this fd on exec of sub jobs, so it will
     * be invisible to them. */
    flags = fcntl( result, F_GETFD );
    if ( flags != -1 ) fcntl( result, F_SETFD, flags | FD_CLOEXEC );

    /* the return is the chosen filename as well as the opened descriptor.
     * we *could* unlink the filename right now, and be truly private, but
     * users may want to look into the log files of long persisting operations. */
    statinfo->source = IS_TEMP;
    statinfo->file.descriptor = result;
    statinfo->file.name = strdup(pattern);

    errno = 0;
    result = fstat( result, &statinfo->info );
    statinfo->error = errno;
  }

  return result;
}

int
initStatInfoAsFifo( StatInfo* statinfo, char* pattern, const char* key )
/* purpose: Initialize a stat info buffer associated with a named pipe
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          pattern (IO): is the input pattern to mkstemp(), will be modified!
 *          key (IN): is the environment key at which to store the filename
 * returns: a value of -1 indicates an error 
 */
{
  int result = -1;
  char* race = strdup(pattern);
  memset( statinfo, 0, sizeof(StatInfo) );

 RETRY:
  if ( (result = mkstemp(pattern)) == -1 ) {
    /* mkstemp has failed, au weia! */
    statinfo->source = IS_INVALID;
    statinfo->error = errno;
    fprintf( stderr, "Warning! Invalid FIFO: mkstemp failed: %d: %s\n",
	     errno, strerror(errno) );

  } else {
    /* create a FIFO instead of a regular tmp file. */
    /* we could have used mktemp() right away, mkstemp() is NOT safer here! */
    close( result );
    unlink( pattern );

    if ( (result = mkfifo( pattern, 0660 )) == -1 ) {
      if ( errno == EEXIST ) {
	/* filename was taken, restore pattern and retry */
	strcpy( pattern, race );
	goto RETRY;
      } else {
	/* other errors are treated as more fatal */
	statinfo->source = IS_INVALID;
	statinfo->error = errno;
	fprintf( stderr, "Warning! Invalid FIFO: mkfifo failed: %d: %s\n",
		 errno, strerror(errno) );
      }
    } else {
      /* open in non-blocking mode for reading.
       * WARNING: DO NOT open in O_RDONLY or suffer the consequences. 
       * You must open in O_RDWR to avoid having to deal with EOF
       * whenever the clients drop to zero. */
      if ( (result = open( pattern, O_RDWR | O_NONBLOCK )) == -1 ) {
	statinfo->source = IS_INVALID;
	statinfo->error = errno;
	fprintf( stderr, "Warning! Invalid FIFO: open failed: %d: %s\n",
		 errno, strerror(errno) );
      } else {
	/* this file descriptor is NOT to be passed to the jobs? So far,
	 * the answer is true. We close this fd on exec of sub jobs, so
	 * it will be invisible to them. */
	int flags = fcntl( result, F_GETFD );
	if ( flags != -1 ) fcntl( result, F_SETFD, flags | FD_CLOEXEC );

	/* the return is the chosen filename as well as the opened
	 * descriptor. We cannot unlink the filename right now. */
	statinfo->source = IS_FIFO;
	statinfo->file.descriptor = result;
	statinfo->file.name = strdup(pattern);

	/* fix environment */
	if ( key != NULL ) {
	  size_t size = strlen(key) + strlen(pattern) + 2;
	  char* temp = (char*) malloc(size);
	  memset( temp, 0, size-- );
	  strncpy( temp, key, size );
	  strncat( temp, "=", size );
	  strncat( temp, pattern, size );
	  if ( putenv( temp ) == -1 )
	    fprintf( stderr, "Warning: Unable to putenv %s: %d: %s\n", 
		     key, errno, strerror(errno) );
	  /* do not free this string here nor now */
	}
	  
	errno = 0;
	result = fstat( result, &statinfo->info );
	statinfo->error = errno;
      }
    }
  }

  free((void*) race);
  return result;
}

static
int
preserveFile( const char* fn )
/* purpose: preserve the given file by renaming it with a backup extension.
 * paramtr: fn (IN): name of the file
 * returns: 0: ok; -1: error, check errno
 */
{
  int i, fd = open( fn, O_RDONLY );
  if ( fd != -1 ) {
    /* file exists, do something */
    size_t size = strlen(fn)+8;
    char* newfn = malloc(size);

    close(fd);
    strncpy( newfn, fn, size );
    for ( i=0; i<1000; ++i ) {
      snprintf( newfn + size-8, 8, ".%03d", i );
      if ( (fd = open( newfn, O_RDONLY )) == -1 ) {
	if ( errno == ENOENT ) break;
	else return -1;
      }
      close(fd);
    }

    if ( i < 1000 ) { 
      return rename( fn, newfn );
    } else {
      /* too many backups */
      errno = EEXIST;
      return -1;
    }
  } else {
    /* file does not exist, nothing to backup */
    errno = 0;
    return 0;
  }
}

int
initStatInfoFromName( StatInfo* statinfo, const char* filename, int openmode,
		      int flag )
/* purpose: Initialize a stat info buffer with a filename to point to
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          filename (IN): the filename to memorize (deep copy)
 *          openmode (IN): are the fcntl O_* flags to later open calls
 *          flag (IN): bit#0 truncate: whether to reset the file size to zero
 *                     bit#1 defer op: whether to defer opening the file for now
 *                     bit#2 preserve: whether to backup existing target file
 * returns: the result of the stat() system call on the provided file */
{
  int result = -1;
  memset( statinfo, 0, sizeof(StatInfo) );
  statinfo->source = IS_FILE;
  statinfo->file.descriptor = openmode;
  statinfo->file.name = strdup(filename);

  if ( (flag & 0x01) == 1 ) {
    /* FIXME: As long as we use shared stdio for stdout and stderr, we need
     * to explicitely truncate (and create) file to zero, if not appending.
     */
    if ( (flag & 0x02) == 0 ) {
      int fd;
      if ( (flag & 0x04) == 4 ) preserveFile( filename );
      fd = open( filename, (openmode & O_ACCMODE) | O_CREAT | O_TRUNC, 0666 );
      if ( fd != -1 ) close(fd);
    } else {
      statinfo->deferred = 1 | (flag & 0x04);
    }
  }
  /* POST-CONDITION: statinfo->deferred == 1, iff (flag & 3) == 3 */

  errno = 0;
  result = stat( filename, &statinfo->info );
  statinfo->error = errno;

  /* special case, read the start of file (for magic) */
  if ( (flag & 0x02) == 0 && result != -1 && 
       S_ISREG(statinfo->info.st_mode) && statinfo->info.st_size > 0 ) {
    int fd = open( filename, O_RDONLY );
    if ( fd != -1 ) {
      read( fd, (char*) statinfo->client.header, sizeof(statinfo->client.header) );
      close(fd);
    }
  }

  return result;
}

int
updateStatInfo( StatInfo* statinfo )
/* purpose: update existing and initialized statinfo with latest info 
 * paramtr: statinfo (IO): stat info pointer to update
 * returns: the result of the stat() or fstat() system call. */
{
  int result = -1;

  if ( statinfo->source == IS_FILE && (statinfo->deferred & 1) == 1 ) {
    /* FIXME: As long as we use shared stdio for stdout and stderr, we need
     * to explicitely truncate (and create) file to zero, if not appending.
     */
    int fd;
    if ( (statinfo->deferred & 4) == 4 ) preserveFile( statinfo->file.name );
    fd = open( statinfo->file.name, 
	       (statinfo->file.descriptor & O_ACCMODE) | O_CREAT | O_TRUNC, 0666 );
    if ( fd != -1 ) close(fd);

    /* once only */
    statinfo->deferred &= ~1;  /* remove deferred bit */
    statinfo->deferred |=  2;  /* mark as having gone here */
  }

  if ( statinfo->source == IS_FILE || 
       statinfo->source == IS_HANDLE || 
       statinfo->source == IS_TEMP ||
       statinfo->source == IS_FIFO ) {

    errno = 0;
    result = statinfo->source == IS_FILE ? 
      stat( statinfo->file.name, &(statinfo->info) ) : 
      fstat( statinfo->file.descriptor, &(statinfo->info) );
    statinfo->error = errno;
      
    if ( result != -1 && statinfo->source == IS_FILE &&
	 S_ISREG(statinfo->info.st_mode) && statinfo->info.st_size > 0 ) {
      int fd = open( statinfo->file.name, O_RDONLY );
      if ( fd != -1 ) {
	read( fd, (char*) statinfo->client.header, sizeof(statinfo->client.header) );
	close(fd);
      }
    }
  }

  return result;
}

int
initStatInfoFromHandle( StatInfo* statinfo, int descriptor )
/* purpose: Initialize a stat info buffer with a filename to point to
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          descriptor (IN): the handle to attach to
 * returns: the result of the fstat() system call on the provided handle */
{
  int result = -1;
  memset( statinfo, 0, sizeof(StatInfo) );
  statinfo->source = IS_HANDLE;
  statinfo->file.descriptor = descriptor;

  errno = 0;
  result = fstat( descriptor, &statinfo->info );
  statinfo->error = errno;

  return result;
}

int
addLFNToStatInfo( StatInfo* info, const char* lfn )
/* purpose: optionally replaces the LFN field with the specified LFN 
 * paramtr: info (IO): stat info pointer to update
 *          lfn (IN): LFN to store, use NULL to free
 * returns: errno in case of error, 0 if OK.
 */
{
  /* sanity check */
  if ( info->source == IS_INVALID ) return EINVAL;

  if ( info->lfn != NULL ) free((void*) info->lfn );
  if ( lfn == NULL ) info->lfn = NULL;
  else if ( (info->lfn = strdup(lfn)) == NULL ) return ENOMEM;
  return 0;
}

size_t
printXMLStatInfo( char* buffer, const size_t size, size_t* len, size_t indent,
		  const char* tag, const char* id, const StatInfo* info )
/* purpose: XML format a stat info record into a given buffer
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level of tag
 *          tag (IN): name of element to generate
 *          id (IN): id attribute, use NULL to not generate
 *          info (IN): stat info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  /* sanity check */
  if ( info->source == IS_INVALID ) return *len;

  /* start main tag */
  myprint( buffer, size, len, "%*s<%s error=\"%d\"", 
	   indent, "", tag, info->error );
  if ( id != NULL ) myprint( buffer, size, len, " id=\"%s\"", id ); 
  if ( info->lfn != NULL ) 
    myprint( buffer, size, len, " lfn=\"%s\"", info->lfn );
  append( buffer, size, len, ">\n" );
  
  /* either a <name> or <descriptor> sub element */
  switch ( info->source ) {
  case IS_TEMP:   /* preparation for <temporary> element */
    /* late update for temp files */
    errno = 0;
    if ( fstat( info->file.descriptor, (struct stat*) &info->info ) != -1 &&
	 ( ((StatInfo*) info)->error = errno) == 0 ) {
      /* obtain header of file */

#if 0
      /* implementation alternative 1: use a new filetable kernel structure */
      int fd = open( info->file.name, O_RDONLY );
      if ( fd != -1 ) {
	read( fd, (char*) info->client.header, sizeof(info->client.header) );
	close(fd);
      }
#else
      /* implementation alternative 2: share the kernel filetable structure */
      int fd = dup( info->file.descriptor );
      if ( fd != -1 ) {
	if ( lseek( fd, 0, SEEK_SET ) != -1 )
	  read( fd, (char*) info->client.header, sizeof(info->client.header) );
	close(fd);
      }
#endif
    }

    myprint( buffer, size, len, 
	     "%*s<temporary name=\"%s\" descriptor=\"%d\"/>\n",
	     indent+2, "", info->file.name, info->file.descriptor );
    break;

  case IS_FIFO: /* <fifo> element */
    myprint( buffer, size, len, 
	     "%*s<fifo name=\"%s\" descriptor=\"%d\" count=\"%u\" rsize=\"%u\" wsize=\"%u\"/>\n",
	     indent+2, "", info->file.name, info->file.descriptor,
	     info->client.fifo.count, info->client.fifo.rsize, 
	     info->client.fifo.wsize );
    break;

  case IS_FILE: /* <file> element */
    /* some debug info - for now */
    myprint( buffer, size, len, "%*s<!-- deferred flag: %d -->\n",
	     indent+2, "", info->deferred );

    myprint( buffer, size, len, 
	     "%*s<file name=\"%s\"", indent+2, "", info->file.name );
    if ( info->error == 0 && S_ISREG(info->info.st_mode) && 
	 info->info.st_size > 0 ) {
      /* optional hex information */
      size_t i, end = sizeof(info->client.header);
      if ( info->info.st_size < end ) end = info->info.st_size;

      append( buffer, size, len, ">" );
      for ( i=0; i<end; ++i )
	myprint( buffer, size, len, "%02X", info->client.header[i] );
      append( buffer, size, len, "</file>\n" );
    } else {
      append( buffer, size, len, "/>\n" );
    }
    break;

  case IS_HANDLE: /* <descriptor> element */
    myprint( buffer, size, len, 
	     "%*s<descriptor number=\"%u\"/>\n", indent+2, "", 
	     info->file.descriptor );
    break;

  default: /* this must not happen! */
    myprint( buffer, size, len, 
	     "%*s<!-- ERROR: No valid file info available -->\n", indent+2, "" );
    break;
  }

  if ( info->error == 0 && info->source != IS_INVALID ) {
    /* <stat> subrecord */
    char my[32];
    struct passwd* user = wrap_getpwuid( info->info.st_uid );
    struct group* group = wrap_getgrgid( info->info.st_gid );

    myprint( buffer, size, len, 
	     "%*s<statinfo mode=\"0%o\"",
	     indent+2, "", info->info.st_mode );

    /* Grmblftz, are we in 32bit, 64bit LFS on 32bit, or 64bit on 64 */
    sizer( my, sizeof(my), 
	   sizeof(info->info.st_size), &info->info.st_size );
    myprint( buffer, size, len, " size=\"%s\"", my );

    sizer( my, sizeof(my), 
	   sizeof(info->info.st_ino), &info->info.st_ino );
    myprint( buffer, size, len, " inode=\"%s\"", my );

    sizer( my, sizeof(my), 
	   sizeof(info->info.st_nlink), &info->info.st_nlink );
    myprint( buffer, size, len, " nlink=\"%s\"", my );

    sizer( my, sizeof(my), 
	   sizeof(info->info.st_blksize), &info->info.st_blksize );
    myprint( buffer, size, len, " blksize=\"%s\"", my );

    /* st_blocks is new in iv-1.8 */
    sizer( my, sizeof(my), 
	   sizeof(info->info.st_blocks), &info->info.st_blocks );
    myprint( buffer, size, len, " blocks=\"%s\"", my );

    append( buffer, size, len, " mtime=\"" );
    mydatetime( buffer, size, len, isLocal, isExtended,
		info->info.st_mtime, -1 );

    append( buffer, size, len, "\" atime=\"" );
    mydatetime( buffer, size, len, isLocal, isExtended,
		info->info.st_atime, -1 );

    append( buffer, size, len, "\" ctime=\"" );
    mydatetime( buffer, size, len, isLocal, isExtended,
		info->info.st_ctime, -1 );

    myprint( buffer, size, len, "\" uid=\"%lu\"", info->info.st_uid );
    if ( user ) myprint( buffer, size, len, " user=\"%s\"", user->pw_name );
    myprint( buffer, size, len, " gid=\"%lu\"", info->info.st_gid );
    if ( group ) myprint( buffer, size, len, " group=\"%s\"", group->gr_name );

    append( buffer, size, len, "/>\n" );
  }

  /* data section from stdout and stderr of application */
  if ( info->source == IS_TEMP && info->error == 0 && info->info.st_size &&
       data_section_size > 0 ) {
    size_t dsize = data_section_size;
    size_t fsize = info->info.st_size;
    myprint( buffer, size, len, "%*s<data%s",
	     indent+2, "", ( fsize > dsize ? " truncated=\"true\"" : "" ) );
    if ( fsize > 0 ) {
      char* data = (char*) malloc(dsize+1);
      int fd = dup(info->file.descriptor);

      append( buffer, size, len, ">" );
      if ( fd != -1 ) {
	if ( lseek( fd, SEEK_SET, 0 ) != -1 ) {
	  ssize_t rsize = read( fd, data, dsize );
	  xmlquote( buffer, size, len, data, rsize );
	}
	close(fd);
      }

      append( buffer, size, len, "</data>\n" );
      free((void*) data);
    } else {
      append( buffer, size, len, "/>\n" );
    }
  }

  myprint( buffer, size, len, "%*s</%s>\n", indent, "", tag );
  return *len;
}

void
deleteStatInfo( StatInfo* statinfo )
/* purpose: clean up and invalidates structure after being done.
 * paramtr: statinfo (IO): clean up record. */
{
#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteStatInfo(%p)\n", statinfo );
#endif

  if ( statinfo->source == IS_FILE || statinfo->source == IS_TEMP || 
       statinfo->source == IS_FIFO ) {
    if ( statinfo->source == IS_TEMP || statinfo->source == IS_FIFO ) { 
      close( statinfo->file.descriptor );
      unlink( statinfo->file.name );
    }

    if ( statinfo->file.name ) {
      free( (void*) statinfo->file.name );
      statinfo->file.name = NULL; /* avoid double free */
    }
  } 

  /* invalidate */
  statinfo->source = IS_INVALID;
}
