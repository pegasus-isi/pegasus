/***************************************************************************
/*  ned_cif.c  Xiuqin Wu  Nov. 9, 1993
/*
/*             Mazz modified 23-Jul-2001 line 501:
/*                  strlen(refcode) counts /n so should be 20, not 19.
/*             XW modified Feb. 15 96 to add functions:
/*                ned_ex_refcode()
/*                ned_ref()
/*                ned_free_refp()
/*
/*FUNCTIONS in the top level, called directly by any client program:
/*=================================================================
/*
/*A. connect/disconnect
/*
/*   ned_connect();
/*       return: 0 connected
/*              -1 connection failed
/*   ned_disconnect();
/*	     
/*   connect and disconnect to and from NED server
/*
/*B. ned_search_functions:
/*   
/*   In the following five functions, we let users
/*   know the data structure we are going to use and they will navigate
/*   through the data structure to get the data they are interested in.
/*      
/*   1) Name resolver:
/*
/*      input: object name
/*
/*      For a given object name, it will return all the Cross Identifiers and
/*      the corresponding object types for the object in NED database.
/*
/*      int ned_name_resolver(char *objname, int *no_names, CrossID **cp)
/*
/*      typedef struct _crossid {
/*	 char  objname[31];
/*	 char  objtype[7];
/*	 } CrossID;
/*
/*
/*      The  results should be interpreted as following:
/*
/*      RETURN VALUE:
/*
/*      -1: check ned_errno for what happened
/*
/*      0:  there are no_names cross identifiers in NED database for the
/*	 object the objname refers to, they are stored in an array of
/*	 type CrossID pointed to by cp.
/*
/*
/*   2) Search by object name
/*
/*      input: object name
/*
/*      int ned_obj_byname(char *objname, int *obj_no, ObjInfo **op, 
/*                        CrossID **cp) 
/*
/*   3) Search near object name
/*
/*      input: object name, search radius(up to 300 arcmins)
/*
/*      int ned_obj_nearname(char *objname, double radius,
/*			  int *obj_no, ObjInfo **op, CrossID **cp) 
/*
/*   4) Search near position
/*
/*      input: ra, dec(J2000 in decimal), search radius (up to 300 arcmin)
/*
/*      int ned_obj_nearposn(double ra, double dec, double radius,
/*			  int *obj_no, ObjInfo **op)
/*
/*   5) Search by IAU format
/*      input: IAU format name, interpretation style, equinox
/*
/*      int ned_obj_iau(char *iauformat, char style, char *equinox,
/*                     char *cra, char *cdec, double *radius,
/*                     int *obj_no, ObjInfo **op)
/*
/*      style:  S for Strict, L for Libral
/*      equinox:  Jnnnn.n or Bnnnn.n
/*
/*      For the given input, the 4 search functions above will return all
/*      the object found, for each object found, there will be a list of
/*      cross IDs and other basic data.
/*
/*
/*      For all the 4 different object searches, each will return the
/*      information in an array of the type ObjInfo structure:
/*
/*      typedef _more_data {
/*	 char       data_typec[61];    data type code to describe the data */
/*	 char       data[101];
/*	 _more_data *next;
/*	 } MoreData;
/*
/*
/*      typedef _obj_info {
/*	 int        no_crossid;  /* number of crossids for the object */
/*	 CrossID    *cp;
/*	 int        no_ref;      /* number of references */
/*	 int        no_note;     /* number of notes */
/*	 int        no_photom;   /* number of photometric data points */
/*	 double     dist;        /* distance to the search center */
/*	 char       objtype[7];
/*	 double     ra;          /* J2000 */
/*	 double     dec;
/*	 double     unc_maj;
/*	 double     unc_min;
/*	 double     unc_ang;
/*	 char       refcode[20];
/*	 MoreData   *mdp;
/*	 } ObjInfo;
/*
/*
/*      With the structure MoreData in the definition of ObjInfo, we will
/*      have the flexibility of providing new data in the future without
/*      changing any code (ideally and hopefully). With any new data we
/*      have, all we need to do is to modify the server part to send the new
/*      data to the client, and provide users with the data type code and
/*      explanation about it. With the "Application protocol between server
/*      and client" design(it is described elsewhere), the client will put the
/*      new data into the structure MoreData for our client users. They can
/*      modify their program to make use of the new data, or they can still
/*      use the old client program with the new server, ignoring the new data.
/*
/*      RETURN VALUE:
/*
/*      -1: check ned_errno for what happened
/*
/*      0:  There are no_obj found for specified objname(position, iauformat),
/*	  and they are stored in an array pointed to by op. 
/* 
/*   6) expand refcode
/*      int ned_ex_refcode(char *refcode, struct reference *full_ref);
/*
/*   7) search references for a given object 
/*      int ned_ref(char *objname, int begin_year, int end_year,
/*                   int *no, CrossID **cp, NedRef **refp);
/*
/* C. free space functions:
/*
/*    ned_free_objp(ObjInfo *objp, int no_obj)
/*        free the space for object pointed to by objp, the space was allocated
/*             for no_obj objects.
/*
/*    ned_free_cp( CrossID    *cp)
/*        free the space for crossids pointed to by cp.
/* 
/*    ned_free_refp(NedRef *refp)
/*        free the space for referencec ponited to by refp
/*
/***************************************************************************/


#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "nedc.h"
#include "ned_client.h"

#define  NED_HOST     "ned.ipac.caltech.edu"
#define  NED_SERVICE  "nedsrv"
#define  NED_PORT     10011

int ned_errno;

/* the SIG_PIPE handling function, may be changed to do your own handling */

void
sig_pipe(dummy)
int dummy;
{
   fprintf(stderr, "sig_pipe communication is broken\n");
   exit(-1);
}

int
ned_connect()
{
   int  st;

   signal(SIGPIPE, sig_pipe);

   st = connect_server(NED_HOST, NED_SERVICE, NED_PORT);
   if (st < 0)
      return(-1);
   else
      return(0);
}
   
void
ned_disconnect()
{
   disconnect_server();
}

int
ned_name_resolver(objname, no_names, cp)
char      *objname;
int       *no_names;
CrossID   **cp;
{
   
   int        st, no;
   int        i;
   char       str[101];
   CrossID   *tmp;

   *cp = (CrossID *)NULL;
   st = ned_query(NED_NAME_RESOLVER, objname);
   if (st < 0) {
      ned_errno = NE_QUERY;
      return(-1); 
      }
   if (ned_qst(&st) < 0) return(-1);
   if (ned_qno(&no) < 0) return(-1);
   *no_names = no;

   if (st < 0 && no <= 0) {      /* objname unrecognized by egret */
      *cp = (CrossID *)NULL;
      ned_errno = NE_NAME;
      return(-1); 
      }
   
   /* if (st==0) there must be at least one objetc name found in NED,
      which means  no>=1, so we can make the memory allocation here 
   */
      
   *cp = NED_MALLOC(CrossID, no);
   /*
   NED_MCHK(*cp);
   */
   if (!*cp) {     /* no space available, clean up the data in socket */
      for (i=0; i<no; i++) 
	 if (ned_gets(str) < 0)
	    return(-1);
      ned_errno = NE_NOSPACE;
      return(-1);
      }

   for (i=0, tmp=*cp; i<no; i++, tmp++)  /* get the names */
      if (ned_gets(tmp->objname) < 0)
	 return(-1);
      
   if (st < 0) {
      if (no > 1)
	 ned_errno = NE_AMBN;  /* input is ambiguous */
      else
	 ned_errno = NE_NOBJ; /* objname not in NED DB */
      return(-1);
      }
   else
      return(0);
}


/* called both by ned_obj_byname(), ned_obj_nearname() */
int
ned_objname_search(obj_no, op, cp)
int     *obj_no;
ObjInfo **op;
CrossID **cp;
{
   int        st, no;
   int        i;
   char       str[101];
   CrossID   *tmp;

   if (ned_qst(&st) < 0) return (-1);
   if (ned_qno(&no) < 0) return (-1);
   *obj_no = no;

   if (st < 0 && no <= 0) {      /* objname unrecognized by egret */
      ned_errno = NE_NAME;
      return(-1); 
      }

   if (st < 0) { /* no>=1 */
      *cp = NED_MALLOC(CrossID, no);
      *op = (ObjInfo *)NULL;
      /*
      NED_MCHK(*cp);
      */
      if (!*cp) {       /* no memory, clean up the data waiting on socket */
	 for (i=0; i<no; i++) 
	    if (ned_gets(str) < 0)
	       return (-1);
	 ned_errno = NE_NOSPACE;
	 return(-1);
	 }

      for (i=0, tmp=*cp; i<no; i++, tmp++) 
	 if (ned_gets(tmp->objname) < 0)
	    return (-1);

      if (no > 1)
	 ned_errno = NE_AMBN; /* ambiguous input object name */
      else
	 ned_errno = NE_NOBJ; /* objname recognized by EGRET, but not in NED */
      return(-1);
      }
   
   /* st == 0, should be some object(s) found if it gets here */

   return(ned_objects(op, no));

}


int
ned_obj_byname (objname, obj_no, op, cp)
char    *objname;
int     *obj_no;
ObjInfo **op;
CrossID **cp;
{
   int  st;

   *op = (ObjInfo *)NULL;
   *cp = (CrossID *)NULL;

   st = ned_query(NED_BYNAME, objname);
   if (st < 0) {
      ned_errno = NE_QUERY;
      return(-1);
      }
   st = ned_objname_search(obj_no, op, cp);
   return(st);
}


int
ned_obj_nearname(objname, radius, obj_no, op, cp) 
char    *objname;
double   radius;
int     *obj_no;
ObjInfo **op;
CrossID **cp;
{
   int  st;

   *op = (ObjInfo *)NULL;
   *cp = (CrossID *)NULL;
   if (radius_out_range(radius)) {
      ned_errno = NE_RADIUS;
      return(-1);
      }

   st = ned_query(NED_NEARNAME, objname, radius);
   if (st < 0) {
      ned_errno = NE_QUERY;
      return(-1);
      }
   st = ned_objname_search(obj_no, op, cp);
   return(st);
}

int
ned_obj_nearposn(ra, dec, radius, obj_no, op)
double    ra;
double    dec;
double    radius;
int       *obj_no;
ObjInfo   **op;
{
   int  st, no;

   *op = (ObjInfo *)NULL;
   if (ra < 0.0 || ra > 360.0) {
      ned_errno = NE_RA;
      return(-1);
      }
   if (dec < -90.0 || dec > 90.0) {
      ned_errno = NE_DEC;
      return(-1);
      }
   if (radius_out_range(radius)) {
      ned_errno = NE_RADIUS;
      return(-1);
      }
   st = ned_query(NED_NEARPOSN, ra, dec, radius);
   if (st < 0) {
      ned_errno = NE_QUERY;
      return(-1);
      }

   if (ned_qst(&st) < 0) return(-1);
   if (ned_qno(&no) < 0) return(-1);
   *obj_no = no;

   if (st < 0 ) {      /* no objects found */
      ned_errno = NE_NOBJ;
      return(-1); 
      }

   return(ned_objects(op, no));
}


int
ned_obj_iau(iauformat, style, equinox,
	       cra, cdec, radius, obj_no, op)
char    *iauformat, style,  *equinox;
char    *cra, *cdec;
double  *radius;
int     *obj_no;
ObjInfo **op;
{
   char   calendar;
   double epoch;
   int    st, no;

   *op = (ObjInfo *)NULL;
   if  (style != 'l' && style != 'L')
      style = 'S';

   calendar = ' ';
   if (*equinox == 'J' || *equinox == 'j') {
      calendar = 'J';
      epoch = atof(equinox+1);
      }
   else if (*equinox == 'B' || *equinox == 'b') {
      calendar = 'B';
      epoch = atof(equinox+1);
      }
   else if (isdigit(*equinox)) {
      epoch = atof(equinox);
      }
   else {
      ned_errno = NE_JB;
      return(-1);
      }
   if (epoch <1500.0 || epoch >2500.0) {
      ned_errno = NE_EPOCH;
      return(-1);
      }
   if (calendar == ' ')
      if (epoch <= 1990)
         calendar = 'B';
      else
         calendar = 'J';

   st = ned_query(NED_IAUFORMAT, iauformat, style, calendar, epoch);

   if (st < 0) {
      ned_errno = NE_QUERY;
      return(-1);
      }

   if (ned_qst(&st) < 0) return(-1);
   if (ned_qno(&no) < 0) return(-1);
   *obj_no = no;

   if (st < 0 ) {
      if (no < 0) {
         ned_errno = NE_IAU;
	 return(-1);
	 }
      else {                      /* no objects found */
         ned_errno = NE_NOBJ;
         if (ned_gets(cra) < 0) return(-1);
         if (ned_gets(cdec) < 0) return(-1);
         if (ned_getdouble(radius) < 0) return (-1);
         return(-1);
         }
      }
   if (ned_gets(cra) < 0) return(-1);
   if (ned_gets(cdec) < 0) return(-1);
   if (ned_getdouble(radius) < 0) return (-1);

   return(ned_objects(op, no));

}

/* Feb 15, 96, XW get one complete reference from socket */
int
ned_get_one_ref(refp)
NedRef *refp;
{
   if (ned_gets(refp->refcode) < 0) return(-1);
   if (ned_gets(refp->pub_name) < 0) return(-1);
   if (ned_getint(&(refp->year)) < 0) return(-1);
   if (ned_gets(refp->vol) < 0) return(-1);
   if (ned_gets(refp->page) < 0) return(-1);
   if (ned_gets(refp->title1) < 0) return(-1);
   if (ned_gets(refp->title2) < 0) return(-1);
   if (ned_gets(refp->author1) < 0) return(-1);
   if (ned_gets(refp->author2) < 0) return(-1);

   return (0);
}


int
ned_ex_refcode(refcode, refp)
char   *refcode;
NedRef *refp;
{
   int        st, no;
   int        i;
   char       str[101];

   if (strlen(refcode) != 20) {
      ned_errno = NE_EREFC; /* has to be a 19-degit code */
      return(-1);
      }

   st = ned_query(NED_XREFCODE, refcode);

   if (st < 0) {
      ned_errno = NE_QUERY;
      return(-1);
      }
   if (ned_qst(&st) < 0) return (-1);

   if (st == 0)
      return(ned_get_one_ref(refp));
   else {
      ned_errno = NE_NOREFC;
      return(-1);
      }

}

int
ned_ref (objname, begin_year, end_year, ref_no, cp, refp)
char    *objname;
int     begin_year;
int     end_year;
int     *ref_no;
CrossID **cp;
NedRef  **refp;
{
   int        st, no;
   int        i;
   char       str[101];
   CrossID   *tmp;

   st = ned_query(NED_REF, objname, begin_year, end_year);

   if (ned_qst(&st) < 0) return (-1);
   if (ned_qno(&no) < 0) return (-1);
   *ref_no = no;

   if (st < 0 && no <= 0) {      /* objname unrecognized by egret */
      ned_errno = NE_NAME;
      return(-1); 
      }

   if (st < 0) { /* no>=1 */
      *cp = NED_MALLOC(CrossID, no);
      *refp = (NedRef *)NULL;
      /*
      NED_MCHK(*cp);
      */
      if (!*cp) {       /* no memory, clean up the data waiting on socket */
	 for (i=0; i<no; i++) 
	    if (ned_gets(str) < 0)
	       return (-1);
	 ned_errno = NE_NOSPACE;
	 return(-1);
	 }

      for (i=0, tmp=*cp; i<no; i++, tmp++) 
	 if (ned_gets(tmp->objname) < 0)
	    return (-1);

      if (no > 1)
	 ned_errno = NE_AMBN; /* ambiguous input object name */
      else
	 ned_errno = NE_NOREF;/*objname recognized by EGRET, but no ref in NED*/
      return(-1);
      }
   
   /* st == 0, should be some reference(s) found if it gets here */

   *refp = NED_MALLOC(NedRef, no);
   for (i=0; i<no; i++) {
      if (ned_get_one_ref(*refp+i) < 0)
	 return(-1);
      }
   return(0);
}


void
ned_free_objp(objp, no_obj)
ObjInfo   *objp;
int        no_obj;
{
   ObjInfo    *tmp_p;
   MoreData   *mdp, *tmp_mdp;
   int        i;

   if (!objp)
      return;

   if (objp->cp)
      free((char *)objp->cp);
   
   for (i=0, tmp_p=objp; i<no_obj; i++, tmp_p++) {
      mdp = tmp_p->mdp;
      while (mdp) {
	 tmp_mdp = mdp->next;
	 free((char *)mdp);
	 mdp = tmp_mdp;
	 }
      }
   free((char *)objp);
    return;
}
      

void
ned_free_cp(cp)
CrossID    *cp;
{
   if (cp)
      free((char *)cp);
   return;
}

   
void
ned_free_refp(refp)
NedRef    *refp;
{
   if (refp)
      free((char *)refp);
   return;
}

