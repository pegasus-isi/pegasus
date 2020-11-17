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
#ifndef _LIMIT_INFO_H
#define _LIMIT_INFO_H

#include <sys/types.h>
#include <sys/resource.h>

typedef struct {
    int            resource;  /* which resource, e.g. RLIMIT_STACK */
    int            error;     /* errno after call to getrlimit */
    struct rlimit  limit;     /* resource limits acquired */
} SingleLimitInfo;

typedef struct {
    size_t            size;
    SingleLimitInfo*  limits;
} LimitInfo;

extern void initLimitInfo(LimitInfo* limits);
extern void updateLimitInfo(LimitInfo* limits);
extern void deleteLimitInfo(LimitInfo* limits);
extern int printYAMLLimitInfo(FILE *out, int indent, const LimitInfo* limits);

#endif /* _LIMIT_INFO_H */
