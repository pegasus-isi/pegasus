/***************************************************************************
/*
/*  ned_err.c  Jay Travisano (STScI)  Apr 26, 1994
/*
/*  Added basic error processing interface functions to simplify
/*  application(s) that just need to pass on error messages.
/*
/*      int   ned_get_errno()
/*      char *ned_get_errmsg()
/*
/***************************************************************************/

#include "ned_client.h"

extern int ned_errno;


int ned_get_errno()
{
        return ned_errno;
}


char *ned_get_errmsg()
{
        char  *msg;

        switch (ned_errno) {

        case NE_NAME:
                msg = "object name not recognized by NED name interpreter";
                break;
        case NE_AMBN:
                msg = "ambiguous input name";
                break;
        case NE_RA:
                msg = "RA is out of range [0.0, 360.0]";
                break;
        case NE_DEC:
                msg = "DEC is out of range [-90.0, 90.0]";
                break;
        case NE_RADIUS:
                msg = "radius is out of range [0.0, 300]";
                break;
        case NE_JB:
                msg = "equinox starts with J or B";
                break;
        case NE_EPOCH:
                msg = "epoch is out of range [1500.0, 2500.0]";
                break;
        case NE_IAU:
                msg = "unacceptible IAU format";
                break;
        case NE_NOBJ:
                msg = "no object found";
                break;
        case NE_NOSPACE:
                msg = "memory allocation failure";
                break;
        case NE_QUERY:
                msg = "can't send query to server, connection problem";
                break;
        case NE_HOST:
                msg = "can't get hostent from the name";
                break;
        case NE_SERVICE:
                msg = "can't get the port number for the service";
                break;
        case NE_PROTO:
                msg = "can't convert tcp to protocol number";
                break;
        case NE_SOCK:
                msg = "can't get a socket allocated";
                break;
        case NE_CONNECT:
                msg = "can't connect to server";
                break;
        case NE_BROKENC:
                msg = "the connection is broken";
                break;
        case NE_TBLSIZE:
                msg = "can't get the descriptor table size";
                break;
        case NE_EREFC:
                msg = "reference code must be 19 digit";
                break;
        case NE_NOREFC:
                msg = "no detailed information for the given refcode";
                break;
        case NE_NOREF:
                msg = "no reference for the given object";
                break;
        default:
                msg = "unknown error message";
                break;
        }

        return msg;
}


