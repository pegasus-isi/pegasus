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
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/ioctl.h>

#ifdef DARWIN
#include <sys/sockio.h>
#endif

#include "getif.h"
#include "utils.h"
#include "error.h"

static unsigned long vpn_network[6] = { 0, 0, 0, 0, 0 };
static unsigned long vpn_netmask[6] = { 0, 0, 0, 0, 0 };

static void singleton_init(void) {
    /* singleton init */
    if (vpn_network[0] == 0ul) {
        vpn_network[0] = inet_addr("127.0.0.0");   /* loopbacknet */
        vpn_network[1] = inet_addr("10.0.0.0");    /* class A VPN net */
        vpn_network[2] = inet_addr("172.16.0.0");  /* class B VPN nets */
        vpn_network[3] = inet_addr("192.168.0.0"); /* class C VPN nets */
        vpn_network[4] = inet_addr("169.254.0.0"); /* link-local junk */
        vpn_network[5] = inet_addr("0.0.0.0");     /* no address */
    }

    /* singleton init */
    if (vpn_netmask[0] == 0ul) {
        vpn_netmask[0] = inet_addr("255.0.0.0");   /* loopbackmask */
        vpn_netmask[1] = inet_addr("255.0.0.0");   /* class A mask */
        vpn_netmask[2] = inet_addr("255.240.0.0"); /* class B VPN mask */
        vpn_netmask[3] = inet_addr("255.255.0.0"); /* class C VPN mask */
        vpn_netmask[4] = inet_addr("255.254.0.0"); /* link-local junk */
        vpn_netmask[5] = inet_addr("255.255.255.255"); /* no mask */
    }
}

int interface_list(struct ifconf* ifc) {
    /* purpose: returns the list of interfaces
     * paramtr: ifc (IO): initializes structure with buffer and length
     * returns: sockfd for further queries, or -1 to indicate an error. 
     * warning: caller must free memory in ifc.ifc_buf
     *          caller must close sockfd (result value)
     */
#if defined(SIOCGLIFNUM)
    struct lifnum ifnr;
#endif
    char *buf = 0;
    int lastlen, len, sockfd = 0;

    /* create a socket */
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) { 
        int saverr = errno; 
        printerr("ERROR: socket DGRAM: %d: %s\n",
                errno, strerror(errno));
        errno = saverr; 
        return -1;
    }

    /*
     * phase 1: guestimate size of buffer necessary to contain all interface
     * information records. 
     */
#if defined(SIOCGLIFNUM)
    /* API exists to determine the correct buffer size */
    memset(&ifnr, 0, sizeof(ifnr));
    ifnr.lifn_family = AF_INET;
    if (ioctl(sockfd, SIOCGLIFNUM, &ifnr) < 0) {
        printerr("ERROR: ioctl SIOCGLIFNUM: %d: %s\n",
                errno, strerror(errno));

        if (errno != EINVAL) {
            int saverr = errno;
            close(sockfd);
            errno = saverr; 
            return -1; 
        }
    } else {
        len = lastlen = ifnr.lifn_count * sizeof(struct ifreq);
    }
#else /* does not have SIOCGLIFNUM */
    /* determine by repetitive guessing a buffer size */
    lastlen = len = 3.5 * sizeof(struct ifreq); /* 1st guesstimate */
#endif
    /* POST CONDITION: some buffer size determined */

    /* FIXME: Missing upper bound */
    for (;;) {
        /* guestimate correct buffer length */
        buf = (char*) malloc(len);
        if (buf == NULL) {
            printerr("malloc: %s\n", strerror(errno));
            return -1;
        }
        memset(buf, 0, len);
        ifc->ifc_len = len;
        ifc->ifc_buf = buf;
        if (ioctl(sockfd, SIOCGIFCONF, ifc) < 0) {
            printerr("WARN: ioctl SIOCGIFCONF: %d: %s\n",
                    errno, strerror(errno));
            if (errno != EINVAL || lastlen != 0) {
                int saverr = errno; 
                close(sockfd);
                errno = saverr; 
                return -1; 
            }
        } else {
            if (ifc->ifc_len == lastlen) break; /* success */
            lastlen = ifc->ifc_len;
        }
        len <<= 1;
        free((void*) buf);
    }
    /* POST CONDITION: Now the buffer contains list of all interfaces */

    return sockfd; 
}

struct ifreq* primary_interface(void) {
    /* purpose: obtain the primary interface information
     * returns: a newly-allocated structure containing the interface info,
     *          or NULL to indicate an error. 
     */
    struct sockaddr_in sa;
    struct ifconf ifc;
    struct ifreq result, primary;
    struct ifreq* ifrcopy = NULL;
    char *ptr;
    int sockfd, flag = 0;

    /*
     * phase 0: init
     */
    memset(&result, 0, sizeof(result));
    memset(&primary, 0, sizeof(primary));
    singleton_init();

    /* 
     * phase 1: obtain list of interfaces 
     */
    if ((sockfd = interface_list(&ifc)) == -1) {
        return NULL;
    }

    /*
     * phase 2: walk interface list until a good interface is reached
     */ 
    /* Notice: recycle meaning of "len" in here */
    for (ptr = ifc.ifc_buf; ptr < ifc.ifc_buf + ifc.ifc_len; ) {
        struct ifreq* ifr = (struct ifreq*) ptr;
#ifndef _SIZEOF_ADDR_IFREQ
        size_t len = sizeof(*ifr);
#else
        size_t len = _SIZEOF_ADDR_IFREQ(*ifr);
#endif /* _SIZEOF_ADDR_IFREQ */

        ptr += len;

        /* interested in IPv4 interfaces only */
        if (ifr->ifr_addr.sa_family != AF_INET) {
            continue;
        }

        memcpy(&sa, &(ifr->ifr_addr), sizeof(struct sockaddr_in));

        /* Do not use localhost aka loopback interfaces. While loopback
         * interfaces traditionally start with "lo", this is not mandatory.
         * It is safer to check that the address is in the 127.0.0.0 class A
         * network. */
        if ((sa.sin_addr.s_addr & vpn_netmask[0]) == vpn_network[0]) {
            continue;
        }

        /* prime candidate - check, if interface is UP */
        result = *ifr;
        ioctl(sockfd, SIOCGIFFLAGS, &result);

        /* interface is up - our work is done. Or is it? */
        if ((result.ifr_flags & IFF_UP)) {
            if (! flag) {
                /* remember first found primary interface */
                primary = result;
                flag = 1;
            }

            /* check for VPNs */
            if ((sa.sin_addr.s_addr & vpn_netmask[1]) == vpn_network[1] ||
                (sa.sin_addr.s_addr & vpn_netmask[2]) == vpn_network[2] ||
                (sa.sin_addr.s_addr & vpn_netmask[3]) == vpn_network[3] ||
                (sa.sin_addr.s_addr & vpn_netmask[4]) == vpn_network[4] ||
                (sa.sin_addr.s_addr & vpn_netmask[5]) == vpn_network[5]) {
                /* Nothing */
            } else {
                flag = 2;
                break;
            }
        }
    }

    /* check for loop exceeded - if yes, fall back on first primary */
    if (flag == 1 && ptr >= ifc.ifc_buf + ifc.ifc_len) {
        result = primary;
    }

    /* clean up */
    free((void*) ifc.ifc_buf);
    close(sockfd);

    /* create a freshly allocated copy */
    ifrcopy = (struct ifreq*) malloc(sizeof(struct ifreq));
    if (ifrcopy == NULL) {
        printerr("malloc: %s\n", strerror(errno));
        return NULL;
    }
    memcpy(ifrcopy, &result, sizeof(struct ifreq));
    return ifrcopy;
}

void whoami(char* abuffer, size_t asize, char* ibuffer, size_t isize) {
    /* purpose: copy the primary interface's IPv4 dotted quad into the given buffer
     * paramtr: abuffer (OUT): start of buffer to put IPv4 dotted quad
     *          asize (IN): maximum capacity the abuffer is willing to accept
     *          ibuffer (OUT): start of buffer to put the primary if name
     *          isize (IN): maximum capacity the ibuffer is willing to accept
     * returns: the modified buffers. */
    /* enumerate interfaces, and guess primary one */
    struct ifreq* ifr = primary_interface();
    if (ifr != NULL) {
        struct sockaddr_in sa;
        if (abuffer) {
            memcpy(&sa, &(ifr->ifr_addr), sizeof(struct sockaddr));
            strncpy(abuffer, inet_ntoa(sa.sin_addr), asize);
        }
        if (ibuffer) {
            strncpy(ibuffer, ifr->ifr_name, isize);
        }
        free((void*) ifr);
    } else {
        /* error while trying to determine address of primary interface */
        if (abuffer) strncpy(abuffer, "0.0.0.0", asize);
        if (ibuffer) strncpy(ibuffer, "(none)", isize); 
    }
}

