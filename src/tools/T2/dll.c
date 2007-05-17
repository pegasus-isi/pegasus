#include "dll.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

int
dll_item_init( dll_item_p item, const char* data )
{
  if ( item == NULL ) return EINVAL;

  memset( item, 0, sizeof(dll_item_t) );
  item->m_magic = T2_ITEM_MAGIC;
  if ( (item->m_data = strdup(data)) == NULL ) return ENOMEM;
  return 0;
}

int
dll_item_done( dll_item_p item )
{
  if ( item == NULL || item->m_magic != T2_ITEM_MAGIC ) return EINVAL;

  if ( item->m_data ) free((void*) item->m_data);
  memset( item, 0, sizeof(dll_item_t) );
  return 0;
}



int
dll_init( dll_p dll )
{
  if ( dll == NULL ) return EINVAL;
  memset( dll, 0, sizeof(dll_t) );
  dll->m_magic = T2_DLL_MAGIC;
  return 0;
}

int
dll_add( dll_p dll, const char* data )
{
  int status;
  dll_item_p temp;

  if ( dll == NULL || dll->m_magic != T2_DLL_MAGIC ) return EINVAL;

  if ( (temp = malloc( sizeof(dll_item_t) )) == NULL ) return ENOMEM;
  if ( (status = dll_item_init( temp, data )) != 0 ) {
    free((void*) temp);
    return status;
  }

  if ( dll->m_count ) {
    dll->m_tail->m_next = temp;
    dll->m_tail = temp;
  } else {
    dll->m_head = dll->m_tail = temp;
  }

  dll->m_count++;
  return 0;
}

int
dll_done( dll_p dll )
{
  if ( dll == NULL || dll->m_magic != T2_DLL_MAGIC ) return EINVAL;

  if ( dll->m_count ) {
    int status;
    dll_item_p temp;
    while ( (temp = dll->m_head) != NULL ) {
      dll->m_head = dll->m_head->m_next;
      if ( (status=dll_item_done( temp )) != 0 ) return status;
      free((void*) temp);
    }
  }

  memset( dll, 0, sizeof(dll_t) );
  return 0;
}
