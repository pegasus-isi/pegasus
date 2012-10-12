#ifndef _MFMALLOC_
#define _MFMALLOC_

extern int   mfInit(char *fname, long size, int memRead);
extern void *mfMalloc(int size);
extern void  mfFree(void *ptr);
extern long  mfSize();
extern char *mfMemLoc();
extern long mfUsed();

extern char *mfMap;

#endif /* _MFMALLOC_ */
