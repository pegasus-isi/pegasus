#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include "index.h"

#define MFMEM  0
#define MFFILE 1

static char *mfMap;
static char *mfCurrent;

static int   mfMode = MFMEM;

static long  mfCount;

int mfdebug = 0;

int mfInit(char *fname, long size, int isRead)
{
   int fd;

   mfCount = 0;

   if(isRead)
   {
      fd = open(fname, O_RDONLY);

      mfMap = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
   }
   else
   {
      fd = open(fname, O_RDWR | O_CREAT | O_TRUNC, 0664);

      lseek(fd, size-1, SEEK_SET);

      write(fd, "", 1);

      mfMap = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
   }

   if(mfMap == MAP_FAILED)
      return(1);

   if(mfdebug)
   {
      printf("DEBUG> mfMap = %0lxx\n", (long)mfMap);
      fflush(stdout);
   }

   mfMode = MFFILE;

   mfCurrent = mfMap;

   nodes = (struct Node *)mfMap;

   return(0);
}


void *mfMalloc(int size)
{
   char *ptr;
   int   extra;

   ++mfCount;

   if(mfMode == MFMEM)
   {
      ptr = malloc(size);

      if(mfdebug)
      {
	 printf("DEBUG> mfMalloc(): %ld\n", (long)ptr);
	 fflush(stdout);
      }

      return(ptr);
   }

   ptr = (char *)mfCurrent;

   extra = size - (int)(size / sizeof(double)) * sizeof(double);

   mfCurrent = mfCurrent + size + extra;

   if(mfdebug)
   {
      printf("DEBUG> mfMalloc(): %0lx (%ld)\n",
	 (long)mfCurrent, (long)mfCurrent - (long)mfMap);
      fflush(stdout);
   }

   return ptr;
}


void mfFree(void *ptr)
{
   if(mfMode == MFMEM)
      free(ptr);

   return;
}


long mfSize()
{
   long size;

   size = (long)mfCurrent - (long)mfMap;

   return(size);
}


char *mfMemLoc()
{
   return mfMap;
}
