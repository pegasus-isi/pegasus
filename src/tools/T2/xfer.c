#include "xfer.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

int
xfer_init( xfer_p xfer, const char* lfn, unsigned flags )
{
  int status;
  if ( xfer == NULL ) return EINVAL;

  memset( xfer, 0, sizeof(xfer_t) );
  if ( (xfer->m_lfn = strdup(lfn)) == NULL ) return ENOMEM;
  xfer->m_flags = flags;
  if ( (status=dll_init( &(xfer->m_src) )) ) return status;
  if ( (status=dll_init( &(xfer->m_dst) )) ) return status;
  xfer->m_magic = T2_SECTION_MAGIC;
  return 0;
}

int
xfer_done( xfer_p xfer )
{
  if ( xfer == NULL || xfer->m_magic != T2_SECTION_MAGIC ) return EINVAL;

  if ( xfer->m_lfn ) free((void*) xfer->m_lfn);
  dll_done( &(xfer->m_src) );
  dll_done( &(xfer->m_dst) );

  memset( xfer, 0, sizeof(xfer_t) );
  return 0;
}

int
xfer_add_src( xfer_p xfer, const char* src )
{
  if ( xfer == NULL || xfer->m_magic != T2_SECTION_MAGIC ) return EINVAL;

  return dll_add( &(xfer->m_src), src );
}

int
xfer_add_dst( xfer_p xfer, const char* dst )
{
  if ( xfer == NULL || xfer->m_magic != T2_SECTION_MAGIC ) return EINVAL;

  return dll_add( &(xfer->m_dst), dst );
}
