/*
 * based on examples in David Butenhof, "Programming with POSIX threads",
 * Addison-Wesley, 1997 
 */
#ifndef _ERROR_H
#define _ERROR_H

#include <errno.h>
#include <stdio.h>

#define err_abort(code,text) do { \
	fprintf( stderr, "%s at \"%s\":%d: %d: %s\n", \
		 text, __FILE__, __LINE__, code, strerror(code) ); \
	abort(); \
	} while (0)

#define errno_abort(text) do { \
	fprintf( stderr, "%s at \"%s\":%d: %d: %s\n", \
		 text, __FILE__, __LINE__, errno, strerror(errno) ); \
	abort(); \
	} while (0)

#endif /* _ERROR_H */
