/***************************************************************************
/*  ned_cli.c  Xiuqin Wu  Nov. 9, 1993
/*
/*commands(sent by client, received by server):
/*   . the length of the command is less than 100
/*   . the command is always ended with a '\n', and is always a string
/*   
/*      1. name_resolver        "objname" 
/*      2. obj_byname           "objname"
/*      3. obj_nearname         "objname", radius
/*      4. obj_nearposn         ra(j2000 decimal), dec(j2000 decimal), 
/*                              radius(arcmin)
/*      5. obj_iauformat        iau_format iauname, style, calendar, epoch
/*                                                  (S, L)  (B,J) (1500-2500)
/*
/*results(sent by server, received by client):
/*   . one string for one data, its length is less than 100
/*   . the string is always ended with a '\n'
/*
/*
/* FUNCTIONS in the second level:
/* =============================
/*
/* Following are the functions that will be called by the first level
/* functions (in ned_cif.c), and could be used by the experienced NED 
/* client programmers:
/*
/*
/*   1. ned_query(int cmd_code, va_alist)
/*      generate a standard  command sent to NED server
/*      send the NED command to the server 
/*
/*   2. int ned_qst()
/*      get the status of the query just sent 
/*
/*   3. int ned_qno()
/*      get the number of objects found or the number of ambiguous names for
/*      object name related searches, i.e name_resolver, byname, nearname
/*   
/*   4. int ned_gets(char *s)     in ned_sk.c 
/*   5. int ned_getint(int *n)
/*   6. int ned_getdouble(double *f)
/*      get string, integer, and double 
/*      return -1 when the communication is broken
/*
/*   7. ned_objects(ObjInfo *objp, int no_obj)
/*      allocate the space for all the objects found (total number of no_obj)
/*      and fill the space with real NED data.
/*     
/*      Programmers are responsible to free the space they no longer need.
/*      Two functions (ned_free_objp, ned_free_cp) are provided as the first
/*      level function calls in ned_cif.c.
/*
/***************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <math.h>
#include "nedc.h"
#include "ned_client.h"

#define CMD_LENGTH   100
#define STR_LENGTH   100

extern int  ned_errno;          /* define in ned_cif.c */

int
ned_query(cmd_code, va_alist)

int    cmd_code;
void *va_alist;

{
   va_list   ap;
   char      *objname, *refcode;
   int       begin_year, end_year;
   double    ra_j2000;
   double    dec_j2000;
   double    radius;
   double    epoch;
   char      style, calendar;
   char      cmd[CMD_LENGTH+1];
   int       st;

   va_start(ap);
   switch(cmd_code) {
      case NED_NAME_RESOLVER:
	 objname = va_arg(ap, char *);
	 sprintf(cmd, "name_resolver \"%s\"\n", objname);
	 break;
      case NED_BYNAME:
	 objname = va_arg(ap, char *);
	 sprintf(cmd, "obj_byname \"%s\"\n", objname);
	 break;
      case NED_NEARNAME:
	 objname = va_arg(ap, char *);
	 radius = va_arg(ap, double);
	 sprintf(cmd, "obj_nearname \"%s\", %.5f\n", objname, radius);
	 break;
      case NED_NEARPOSN:
	 ra_j2000 = va_arg(ap, double);
	 dec_j2000 = va_arg(ap, double);
	 radius = va_arg(ap, double);
	 sprintf(cmd, "obj_nearposn %.8f, %.8f, %.5f\n", 
	    ra_j2000, dec_j2000, radius);
	 break;
      case NED_IAUFORMAT:
	 objname = va_arg(ap, char *);
	 style = va_arg(ap, int );
	 calendar = va_arg(ap, int );
	 epoch = va_arg(ap, double);
	 sprintf(cmd, "obj_iauformat %s, %c, %c, %.3f\n", 
	    objname, style, calendar, epoch);
         break;
      case NED_XREFCODE:
	 refcode = va_arg(ap, char *);
	 sprintf(cmd, "expand_refcode %s\n", refcode);
	 break;
      case NED_REF:
	 objname = va_arg(ap, char *);
	 begin_year = va_arg(ap, int);
	 end_year = va_arg(ap, int);
	 sprintf(cmd, "ref_objname \"%s\", %d, %d\n", 
		      objname, begin_year, end_year);
	 break;
      default:
	 break;
      }
 
   va_end(ap);

   st = send_cmd(cmd);
   return(st);
}

int
ned_getint(n)
int *n;
{
   char s[STR_LENGTH+1];

   if (ned_gets(s) < 0)
      return(-1);
   *n = atoi(s);
   return(0);
}

int
ned_getdouble(d)
long double *d;
{
   char s[STR_LENGTH+1]; 

   if (ned_gets(s) < 0) 
      return (-1); 
   /* *d = atof(s); */
   /* See http://www.srware.com/linux_numerics.txt (Mazz, 04Sep2001) */
   sscanf(s, "%lf", d);
   return (0);
}

int
ned_qst(st)
int *st;
{
   return(ned_getint(st));
}

int
ned_qno(no)
int  *no;
{
   return(ned_getint(no));
}

/* allocate space for no_obj objects and fill it with real NED data */
int
ned_objects (objp, no_obj)
ObjInfo **objp;
int no_obj;
{
   int         no_crossids;
   int         i, j;
   int         more;
   char        str[STR_LENGTH+1];
   CrossID     *cp;
   ObjInfo     *obj_tmpp;
   MoreData    *mdatap;

   if (ned_getint(&no_crossids) < 0) return (-1);
   *objp = NED_MALLOC(ObjInfo, no_obj);
   cp = NED_MALLOC(CrossID, no_crossids);
   if (!*objp || !cp) {
      clean_obj_socket(no_obj);
      ned_errno = NE_NOSPACE;
      return(-1);
      }
   /*
   NED_MCHK(*objp);
   */
   /*
   NED_MCHK(cp);
   */

   for (i=0, obj_tmpp=*objp; i<no_obj; i++, obj_tmpp++) {
      if (ned_getint(&obj_tmpp->no_crossid) < 0) return (-1);
      obj_tmpp->cp = cp;
      for (j=0; j<obj_tmpp->no_crossid; j++) {
         if (ned_gets(cp->objname) < 0) return(-1);
         if (ned_gets(cp->objtype) < 0) return(-1);
	 cp++;
	 }
      if (ned_getdouble(&obj_tmpp->dist) < 0) return (-1);
      if (ned_getint(&obj_tmpp->no_ref) < 0) return (-1);
      if (ned_getint(&obj_tmpp->no_note) < 0) return(-1);
      if (ned_getint(&obj_tmpp->no_photom) < 0) return(-1);
      if (ned_gets(obj_tmpp->objtype) < 0) return (-1);
      if (ned_getdouble(&obj_tmpp->ra) < 0) return(-1);
      if (ned_getdouble(&obj_tmpp->dec) < 0) return(-1);
      if (ned_getdouble(&obj_tmpp->unc_maj) < 0) return(-1);
      if (ned_getdouble(&obj_tmpp->unc_min) < 0) return(-1);
      if (ned_getdouble(&obj_tmpp->unc_ang) < 0) return(-1);
      /*
      obj_tmpp->bh_extin = ned_getdouble();
      */
      if (ned_gets(obj_tmpp->refcode) < 0) return(-1);
      if (ned_getint(&more) < 0) return(-1);
      if (!more)
	 obj_tmpp->mdp = NULL;
      else {
	 obj_tmpp->mdp = NED_MALLOC(MoreData, 1);
	 if (!obj_tmpp->mdp) {      /* no space for MoreData */
	    while (more) {          /* clean up the coming data in socket */
	       if (ned_gets(str) < 0) return(-1);
	       if (ned_gets(str) < 0) return(-1);
	       if (ned_getint(&more) < 0) return(-1);
	       }
	    }
	 else {                    /* get the data from socket */
	    mdatap = obj_tmpp->mdp;
	    while (more) {
	       if (ned_gets(mdatap->data_typec) < 0) return(-1);
	       if (ned_gets(mdatap->data) < 0) return(-1);
	       if (ned_getint(&more) < 0) return(-1);
	       if (more) {
		  mdatap->next = NED_MALLOC(MoreData, 1);
		  mdatap = mdatap->next;
		  if (!mdatap) {
		     while (more) {  /* clean up the socket */
			if (ned_gets(str) < 0) return(-1);
			if (ned_gets(str) < 0) return(-1);
			if (ned_getint(&more) < 0) return(-1);
			}
		     }
		  }/* if (more) */
	       else /* no more data */
		  mdatap->next = NULL;
	       }  /* while */
	    }
	 }
		  
      }  /* for */
   return(0);
	 
}

int
clean_obj_socket(no_obj)
int  no_obj;
{
   int         i, j, n;
   int         more;
   int         no_crossid;
   char        str[101];
   double      f;

   for (i=0; i<no_obj; i++) {
      if (ned_getint(&no_crossid) <  0) return(-1);
      for (j=0; j<no_crossid; j++) {
         if (ned_gets(str) <  0) return(-1);      /* objname */
         if (ned_gets(str) <  0) return(-1);      /* obj-type */
	 }
      if (ned_getdouble(&f) <  0) return(-1);                    /* dist */
      if (ned_getint(&n) <  0) return(-1);           /* no_ref */
      if (ned_getint(&n) <  0) return(-1);           /* no_note */
      if (ned_getint(&n) <  0) return(-1);           /* no_photom */
      if (ned_gets(str) <  0) return(-1);          /* prefered obj-type */
      if (ned_getdouble(&f) <  0) return(-1);        /* ra-j2000 */
      if (ned_getdouble(&f) <  0) return(-1);        /* dec j2000 */
      if (ned_getdouble(&f) <  0) return(-1);        /* unc_maj */
      if (ned_getdouble(&f) <  0) return(-1);        /* unc_min */
      if (ned_getdouble(&f) <  0) return(-1);        /* unc_ang */
      /* ned_getdouble();           bh_extin */
      if (ned_gets(str) <  0) return(-1);              /* refcode  */
      if (ned_getint(&more) <  0) return(-1);

      while (more) {          /* clean up the coming data in socket */
	 if (ned_gets(str) <  0) return(-1);
	 if (ned_gets(str) <  0) return(-1);
	 if (ned_getint(&more) <  0) return(-1);
	 }
      }
}

int
radius_out_range(radius)
double radius;
{
   if (radius < 0.0 || radius > 300.0)
      return(1);
   else
      return(0);
}
