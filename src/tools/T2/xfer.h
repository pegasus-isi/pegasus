#ifndef _T2_XFER_H
#define _T2_XFER_H

#include "dll.h"

typedef enum {
  XFER_ANY = 0,
  XFER_ALL = 1,
  XFER_OPTIONAL = 2
} section_flags;

typedef struct section_tag {
  unsigned long		m_magic;	/* valid flag */
  const char*		m_lfn;		/* section header */
  section_flags         m_flags;	/* any, all, optional */
  dll_t 		m_src;		/* list of source candidates */
  dll_t 		m_dst;		/* list of destination candidates */
} xfer_t, *xfer_p;

#define T2_SECTION_MAGIC 0xa3a7c135

extern
int
xfer_init( xfer_p xfer, const char* lfn, unsigned flags );

extern
int
xfer_add_src( xfer_p xfer, const char* src );

extern
int
xfer_add_dst( xfer_p xfer, const char* dst );

extern
int
xfer_done( xfer_p xfer );

#endif
