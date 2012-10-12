#ifndef wcslib_h_
#define wcslib_h_

/*=============================================================================
*
*   WCSLIB - an implementation of the FITS WCS proposal.
*   Copyright (C) 1995-2002, Mark Calabretta
*
*   This library is free software; you can redistribute it and/or
*   modify it under the terms of the GNU Lesser General Public
*   License as published by the Free Software Foundation; either
*   version 2 of the License, or (at your option) any later version.
*
*   This library is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
*   Lesser General Public License for more details.
*   
*   You should have received a copy of the GNU Lesser General Public
*   License along with this library; if not, write to the Free Software
*   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*   Correspondence concerning WCSLIB may be directed to:
*      Internet email: mcalabre@atnf.csiro.au
*      Postal address: Dr. Mark Calabretta,
*                      Australia Telescope National Facility,
*                      P.O. Box 76,
*                      Epping, NSW, 2121,
*                      AUSTRALIA
*
*   Author: Mark Calabretta, Australia Telescope National Facility
*   $Id: wcs.h,v 2.9 2002/04/03 01:25:29 mcalabre Exp $
*===========================================================================*/

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(__STDC__) && !defined(__cplusplus)
#ifndef const
#define const
#endif
#endif

extern int npcode;
extern char pcodes[26][4];

struct prjprm {
   char   code[4];
   int flag;
   double phi0, theta0;
   double r0;
   double p[10];
   double w[20];
   int    n;

#if __STDC__  || defined(__cplusplus)
   int (*prjfwd)(const double, const double,
                 struct prjprm *,
                 double *, double *);
   int (*prjrev)(const double, const double,
                 struct prjprm *,
                 double *, double *);
#else
   int (*prjfwd)();
   int (*prjrev)();
#endif
};

#if __STDC__ || defined(__cplusplus)
   int prjset(const char [], struct prjprm *);
   int prjfwd(const double, const double, struct prjprm *, double *, double *);
   int prjrev(const double, const double, struct prjprm *, double *, double *);
   int azpset(struct prjprm *);
   int azpfwd(const double, const double, struct prjprm *, double *, double *);
   int azprev(const double, const double, struct prjprm *, double *, double *);
   int szpset(struct prjprm *);
   int szpfwd(const double, const double, struct prjprm *, double *, double *);
   int szprev(const double, const double, struct prjprm *, double *, double *);
   int tanset(struct prjprm *);
   int tanfwd(const double, const double, struct prjprm *, double *, double *);
   int tanrev(const double, const double, struct prjprm *, double *, double *);
   int stgset(struct prjprm *);
   int stgfwd(const double, const double, struct prjprm *, double *, double *);
   int stgrev(const double, const double, struct prjprm *, double *, double *);
   int sinset(struct prjprm *);
   int sinfwd(const double, const double, struct prjprm *, double *, double *);
   int sinrev(const double, const double, struct prjprm *, double *, double *);
   int arcset(struct prjprm *);
   int arcfwd(const double, const double, struct prjprm *, double *, double *);
   int arcrev(const double, const double, struct prjprm *, double *, double *);
   int zpnset(struct prjprm *);
   int zpnfwd(const double, const double, struct prjprm *, double *, double *);
   int zpnrev(const double, const double, struct prjprm *, double *, double *);
   int zeaset(struct prjprm *);
   int zeafwd(const double, const double, struct prjprm *, double *, double *);
   int zearev(const double, const double, struct prjprm *, double *, double *);
   int airset(struct prjprm *);
   int airfwd(const double, const double, struct prjprm *, double *, double *);
   int airrev(const double, const double, struct prjprm *, double *, double *);
   int cypset(struct prjprm *);
   int cypfwd(const double, const double, struct prjprm *, double *, double *);
   int cyprev(const double, const double, struct prjprm *, double *, double *);
   int ceaset(struct prjprm *);
   int ceafwd(const double, const double, struct prjprm *, double *, double *);
   int cearev(const double, const double, struct prjprm *, double *, double *);
   int carset(struct prjprm *);
   int carfwd(const double, const double, struct prjprm *, double *, double *);
   int carrev(const double, const double, struct prjprm *, double *, double *);
   int merset(struct prjprm *);
   int merfwd(const double, const double, struct prjprm *, double *, double *);
   int merrev(const double, const double, struct prjprm *, double *, double *);
   int sflset(struct prjprm *);
   int sflfwd(const double, const double, struct prjprm *, double *, double *);
   int sflrev(const double, const double, struct prjprm *, double *, double *);
   int parset(struct prjprm *);
   int parfwd(const double, const double, struct prjprm *, double *, double *);
   int parrev(const double, const double, struct prjprm *, double *, double *);
   int molset(struct prjprm *);
   int molfwd(const double, const double, struct prjprm *, double *, double *);
   int molrev(const double, const double, struct prjprm *, double *, double *);
   int aitset(struct prjprm *);
   int aitfwd(const double, const double, struct prjprm *, double *, double *);
   int aitrev(const double, const double, struct prjprm *, double *, double *);
   int copset(struct prjprm *);
   int copfwd(const double, const double, struct prjprm *, double *, double *);
   int coprev(const double, const double, struct prjprm *, double *, double *);
   int coeset(struct prjprm *);
   int coefwd(const double, const double, struct prjprm *, double *, double *);
   int coerev(const double, const double, struct prjprm *, double *, double *);
   int codset(struct prjprm *);
   int codfwd(const double, const double, struct prjprm *, double *, double *);
   int codrev(const double, const double, struct prjprm *, double *, double *);
   int cooset(struct prjprm *);
   int coofwd(const double, const double, struct prjprm *, double *, double *);
   int coorev(const double, const double, struct prjprm *, double *, double *);
   int bonset(struct prjprm *);
   int bonfwd(const double, const double, struct prjprm *, double *, double *);
   int bonrev(const double, const double, struct prjprm *, double *, double *);
   int pcoset(struct prjprm *);
   int pcofwd(const double, const double, struct prjprm *, double *, double *);
   int pcorev(const double, const double, struct prjprm *, double *, double *);
   int tscset(struct prjprm *);
   int tscfwd(const double, const double, struct prjprm *, double *, double *);
   int tscrev(const double, const double, struct prjprm *, double *, double *);
   int cscset(struct prjprm *);
   int cscfwd(const double, const double, struct prjprm *, double *, double *);
   int cscrev(const double, const double, struct prjprm *, double *, double *);
   int qscset(struct prjprm *);
   int qscfwd(const double, const double, struct prjprm *, double *, double *);
   int qscrev(const double, const double, struct prjprm *, double *, double *);
#else
   int prjset(), prjfwd(), prjrev();
   int azpset(), azpfwd(), azprev();
   int szpset(), szpfwd(), szprev();
   int tanset(), tanfwd(), tanrev();
   int stgset(), stgfwd(), stgrev();
   int sinset(), sinfwd(), sinrev();
   int arcset(), arcfwd(), arcrev();
   int zpnset(), zpnfwd(), zpnrev();
   int zeaset(), zeafwd(), zearev();
   int airset(), airfwd(), airrev();
   int cypset(), cypfwd(), cyprev();
   int ceaset(), ceafwd(), cearev();
   int carset(), carfwd(), carrev();
   int merset(), merfwd(), merrev();
   int sflset(), sflfwd(), sflrev();
   int parset(), parfwd(), parrev();
   int molset(), molfwd(), molrev();
   int aitset(), aitfwd(), aitrev();
   int copset(), copfwd(), coprev();
   int coeset(), coefwd(), coerev();
   int codset(), codfwd(), codrev();
   int cooset(), coofwd(), coorev();
   int bonset(), bonfwd(), bonrev();
   int pcoset(), pcofwd(), pcorev();
   int tscset(), tscfwd(), tscrev();
   int cscset(), cscfwd(), cscrev();
   int qscset(), qscfwd(), qscrev();
#endif

extern const char *prjset_errmsg[];
extern const char *prjfwd_errmsg[];
extern const char *prjrev_errmsg[];

#define PRJSET 137

struct celprm {
   int flag;
   double ref[4];
   double euler[5];
};

#if __STDC__  || defined(__cplusplus)
   int celset(const char *, struct celprm *, struct prjprm *);
   int celfwd(const char *,
              const double, const double,
              struct celprm *,
              double *, double *,
              struct prjprm *,
              double *, double *);
   int celrev(const char *,
              const double, const double,
              struct prjprm *,
              double *, double *,
              struct celprm *,
              double *, double *);
#else
   int celset(), celfwd(), celrev();
#endif

extern const char *celset_errmsg[];
extern const char *celfwd_errmsg[];
extern const char *celrev_errmsg[];

#define CELSET 137

struct linprm {
   int flag;
   int naxis;
   double *crpix;
   double *pc;
   double *cdelt;

   /* Intermediates. */
   double *piximg;
   double *imgpix;
};

#if __STDC__  || defined(__cplusplus)
   int linset(struct linprm *);
   int linfwd(const double[], struct linprm *, double[]);
   int linrev(const double[], struct linprm *, double[]);
   int matinv(const int, const double [], double []);
#else
   int linset(), linfwd(), linrev(), matinv();
#endif

extern const char *linset_errmsg[];
extern const char *linfwd_errmsg[];
extern const char *linrev_errmsg[];

#define LINSET 137


struct wcsprm {
   int flag;
   char pcode[4];
   char lngtyp[5], lattyp[5];
   int lng, lat;
   int cubeface;
};

#if __STDC__ || defined(__cplusplus)
   int wcsset(const int,
              const char[][9],
              struct wcsprm *);

   int wcsfwd(const char[][9],
              struct wcsprm *,
              const double[],
              const double[],
              struct celprm *, 
              double *,
              double *, 
              struct prjprm *, 
              double[], 
              struct linprm *,
              double[]);

   int wcsrev(const char[][9],
              struct wcsprm *,
              const double[], 
              struct linprm *,
              double[], 
              struct prjprm *, 
              double *,
              double *, 
              const double[], 
              struct celprm *, 
              double[]);

   int wcsmix(const char[][9],
              struct wcsprm *,
              const int,
              const int,
              const double[],
              const double,
              int,
              double[],
              const double[],
              struct celprm *,
              double *,
              double *,
              struct prjprm *,
              double[], 
              struct linprm *,
              double[]);

#else
   int wcsset(), wcsfwd(), wcsrev(), wcsmix();
#endif

extern const char *wcsset_errmsg[];
extern const char *wcsfwd_errmsg[];
extern const char *wcsrev_errmsg[];
extern const char *wcsmix_errmsg[];

#define WCSSET 137


#if __STDC__  || defined(__cplusplus)
   int sphfwd(const double, const double,
              const double [],
              double *, double *);
   int sphrev(const double, const double,
              const double [],
              double *, double *);
#else
   int sphfwd(), sphrev();
#endif

#ifdef PI
#undef PI
#endif

#ifdef D2R
#undef D2R
#endif

#ifdef R2D
#undef R2D
#endif

#ifdef SQRT2
#undef SQRT2
#endif

#ifdef SQRT2INV
#undef SQRT2INV
#endif

#define PI 3.141592653589793238462643
#define D2R PI/180.0
#define R2D 180.0/PI
#define SQRT2 1.4142135623730950488
#define SQRT2INV 1.0/SQRT2

#if !defined(__STDC__) && !defined(__cplusplus)
#ifndef const
#define const
#endif
#endif

#if __STDC__ || defined(__cplusplus)
   double cosdeg(const double);
   double sindeg(const double);
   double tandeg(const double);
   double acosdeg(const double);
   double asindeg(const double);
   double atandeg(const double);
   double atan2deg(const double, const double);
#else
   double cosdeg();
   double sindeg();
   double tandeg();
   double acosdeg();
   double asindeg();
   double atandeg();
   double atan2deg();
#endif

/* Domain tolerance for asin and acos functions. */
#define WCSTRIG_TOL 1e-10

#ifdef __cplusplus
}
#endif

#endif /* wcslib_h_ */

/* Feb  3 2000	Doug Mink - Make cplusplus ifdefs for braces all-inclusive
 *
 * Feb 15 2001	Doug Mink - Undefine math constants if already defined
 * Sep 19 2001	Doug Mink - Update for WCSLIB 2.7, especially proj.h and cel.h
 *
 * Mar 12 2002	Doug Mink - Update for WCSLIB 2.8.2, especially proj.h
 * Nov 29 2006	Doug Mink - Drop semicolon at end of C++ ifdef
 * Jan  4 2007	Doug Mink - Drop extra declarations of SZP subroutines
 */
