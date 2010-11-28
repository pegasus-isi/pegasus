#ifndef _T2_DLL_H
#define _T2_DLL_H

#include <sys/types.h>

typedef struct dll_item_tag {
  unsigned long         m_magic;	/* signal valid element */
  struct dll_item_tag*	m_next;		/* next item in list */
  char*                 m_data;		/* data item */
  int                   m_flag;		/* some flag data */
} dll_item_t, *dll_item_p;

#define T2_ITEM_MAGIC 0xcd82bd08

extern
int
dll_item_init( dll_item_p item, const char* data );

extern
int
dll_item_done( dll_item_p item );

typedef struct dll_tag {
  unsigned long         m_magic;	/* magic for valid members */
  struct dll_item_tag*  m_head;
  struct dll_item_tag*  m_tail;
  size_t                m_count;
} dll_t, *dll_p;

#define T2_DLL_MAGIC 0xbae21bdb

extern
int
dll_init( dll_p dll );

extern
int
dll_add( dll_p dll, const char* data );

extern
int
dll_done( dll_p dll );

#endif
