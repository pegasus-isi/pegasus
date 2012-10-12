#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include <math.h>

#define mNaN(x) isnan(x) || !finite(x)
