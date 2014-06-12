#ifndef _ACCESS_H
#define _ACCESS_H

/* TODO Merge this with FileInfo and ProcInfo from procinfo.[ch] */

typedef struct _FileAccess {
  char *filename;
  size_t size;
  struct _FileAccess *next;
} FileAccess;

#endif /* _ACCESS_H */
