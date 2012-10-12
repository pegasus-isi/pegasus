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
*=============================================================================
*
*   C routines which implement the FITS World Coordinate System (WCS)
*   convention.
*
*   Summary of routines
*   -------------------
*   These utility routines apply the linear transformation defined by the WCS
*   FITS header cards.  There are separate routines for the image-to-pixel,
*   linfwd(), and pixel-to-image, linrev(), transformations.
*
*   An initialization routine, linset(), computes intermediate values from
*   the transformation parameters but need not be called explicitly - see the
*   explanation of lin.flag below.
*
*   An auxiliary matrix inversion routine, matinv(), is included.  It uses
*   LU-triangular factorization with scaled partial pivoting.
*
*
*   Initialization routine; linset()
*   --------------------------------
*   Initializes members of a linprm data structure which hold intermediate
*   values.  Note that this routine need not be called directly; it will be
*   invoked by linfwd() and linrev() if the "flag" structure member is
*   anything other than a predefined magic value.
*
*   Given and/or returned:
*      lin      linprm*  Linear transformation parameters (see below).
*
*   Function return value:
*               int      Error status
*                           0: Success.
*                           1: Memory allocation error.
*                           2: PC matrix is singular.
*
*   Forward transformation; linfwd()
*   --------------------------------
*   Compute pixel coordinates from image coordinates.  Note that where
*   celestial coordinate systems are concerned the image coordinates
*   correspond to (x,y) in the plane of projection, not celestial (lng,lat).
*
*   Given:
*      imgcrd   const double[]
*                        Image (world) coordinate.
*
*   Given and returned:
*      lin      linprm*  Linear transformation parameters (see below).
*
*   Returned:
*      pixcrd   d[]      Pixel coordinate.
*
*   Function return value:
*               int      Error status
*                           0: Success.
*                           1: The transformation is not invertible.
*
*   Reverse transformation; linrev()
*   --------------------------------
*   Compute image coordinates from pixel coordinates.  Note that where
*   celestial coordinate systems are concerned the image coordinates
*   correspond to (x,y) in the plane of projection, not celestial (lng,lat).
*
*   Given:
*      pixcrd   const double[]
*                        Pixel coordinate.
*
*   Given and/or returned:
*      lin      linprm*  Linear transformation parameters (see below).
*
*   Returned:
*      imgcrd   d[]      Image (world) coordinate.
*
*   Function return value:
*               int      Error status
*                           0: Success.
*                           1: Error.
*
*   Linear transformation parameters
*   --------------------------------
*   The linprm struct consists of the following:
*
*      int flag
*         This flag must be set to zero whenever any of the following members
*         are set or modified.  This signals the initialization routine,
*         linset(), to recompute intermediaries.
*      int naxis
*         Number of image axes.
*      double *crpix
*         Pointer to the first element of an array of double containing the
*         coordinate reference pixel, CRPIXn.
*      double *pc
*         Pointer to the first element of the PC (pixel coordinate)
*         transformation matrix.  The expected order is
*
*            lin.pc = {PC1_1, PC1_2, PC2_1, PC2_2};
*
*         This may be conveniently constructed from a two-dimensional array
*         via
*
*            double m[2][2] = {{PC1_1, PC1_2},
*                              {PC2_1, PC2_2}};
*         
*         which is equivalent to,
*
*            double m[2][2];
*            m[0][0] = PC1_1;
*            m[0][1] = PC1_2;
*            m[1][0] = PC2_1;
*            m[1][1] = PC2_2;
*
*         for which the storage order is
*
*            PC1_1, PC1_2, PC2_1, PC2_2
*
*         so it would be legitimate to set lin.pc = *m.
*      double *cdelt
*         Pointer to the first element of an array of double containing the
*         coordinate increments, CDELTn.
*
*   The remaining members of the linprm struct are maintained by the
*   initialization routine and should not be modified.
*
*      double *piximg
*         Pointer to the first element of the matrix containing the product
*         of the CDELTn diagonal matrix and the PC matrix.
*      double *imgpix
*         Pointer to the first element of the inverse of the piximg matrix.
*
*   linset allocates storage for the above arrays using malloc().  Note,
*   however, that these routines do not free this storage so if a linprm
*   variable has itself been malloc'd then these structure members must be
*   explicitly freed before the linprm variable is free'd otherwise a memory
*   leak will result.
*
*   Author: Mark Calabretta, Australia Telescope National Facility
*   $Id: lin.c,v 2.8 2002/01/30 06:04:03 mcalabre Exp $
*===========================================================================*/

#include <stdlib.h>
#include <math.h>
#include "wcslib.h"

/* Map error number to error message for each function. */
const char *linset_errmsg[] = {
   0,
   "Memory allocation error",
   "PC matrix is singular"};

const char *linfwd_errmsg[] = {
   0,
   "Memory allocation error",
   "PC matrix is singular"};

const char *linrev_errmsg[] = {
   0,
   "Memory allocation error",
   "PC matrix is singular"};

int linset(lin)

struct linprm *lin;

{
   int i, ij, j, mem, n;

   n = lin->naxis;

   /* Allocate memory for internal arrays. */
   mem = n * n * sizeof(double);
   lin->piximg = (double*)malloc(mem);
   if (lin->piximg == (double*)0) return 1;

   lin->imgpix = (double*)malloc(mem);
   if (lin->imgpix == (double*)0) {
      free(lin->piximg);
      return 1;
   }

   /* Compute the pixel-to-image transformation matrix. */
   for (i = 0, ij = 0; i < n; i++) {
      for (j = 0; j < n; j++, ij++) {
         lin->piximg[ij] = lin->cdelt[i] * lin->pc[ij];
      }
   }

   /* Compute the image-to-pixel transformation matrix. */
   if (matinv(n, lin->piximg, lin->imgpix)) return 2;

   lin->flag = LINSET;

   return 0;
}

/*--------------------------------------------------------------------------*/

int linfwd(imgcrd, lin, pixcrd)

const double imgcrd[];
struct linprm *lin;
double pixcrd[];

{
   int i, ij, j, n;

   n = lin->naxis;

   if (lin->flag != LINSET) {
      if (linset(lin)) return 1;
   }

   for (i = 0, ij = 0; i < n; i++) {
      pixcrd[i] = 0.0;
      for (j = 0; j < n; j++, ij++) {
         pixcrd[i] += lin->imgpix[ij] * imgcrd[j];
      }
   }

   for (j = 0; j < n; j++) {
      pixcrd[j] += lin->crpix[j];
   }

   return 0;
}

/*--------------------------------------------------------------------------*/

int linrev(pixcrd, lin, imgcrd)

const double pixcrd[];
struct linprm *lin;
double imgcrd[];

{
   int i, ij, j, n;
   double temp;

   n = lin->naxis;

   if (lin->flag != LINSET) {
      if (linset(lin)) return 1;
   }

   for (i = 0; i < n; i++) {
      imgcrd[i] = 0.0;
   }

   for (j = 0; j < n; j++) {
      temp = pixcrd[j] - lin->crpix[j];
      for (i = 0, ij = j; i < n; i++, ij+=n) {
         imgcrd[i] += lin->piximg[ij] * temp;
      }
   }

   return 0;
}

/*--------------------------------------------------------------------------*/

int matinv(n, mat, inv)

const int n;
const double mat[];
double inv[];

{
   register int i, ij, ik, j, k, kj, pj;
   int    itemp, mem, *mxl, *lxm, pivot;
   double colmax, *lu, *rowmax, dtemp;


   /* Allocate memory for internal arrays. */
   mem = n * sizeof(int);
   if ((mxl = (int*)malloc(mem)) == (int*)0) return 1;
   if ((lxm = (int*)malloc(mem)) == (int*)0) {
      free(mxl);
      return 1;
   }

   mem = n * sizeof(double);
   if ((rowmax = (double*)malloc(mem)) == (double*)0) {
      free(mxl);
      free(lxm);
      return 1;
   }

   mem *= n;
   if ((lu = (double*)malloc(mem)) == (double*)0) {
      free(mxl);
      free(lxm);
      free(rowmax);
      return 1;
   }


   /* Initialize arrays. */
   for (i = 0, ij = 0; i < n; i++) {
      /* Vector which records row interchanges. */
      mxl[i] = i;

      rowmax[i] = 0.0;

      for (j = 0; j < n; j++, ij++) {
         dtemp = fabs(mat[ij]);
         if (dtemp > rowmax[i]) rowmax[i] = dtemp;

         lu[ij] = mat[ij];
      }

      /* A row of zeroes indicates a singular matrix. */
      if (rowmax[i] == 0.0) {
         free(mxl);
         free(lxm);
         free(rowmax);
         free(lu);
         return 2;
      }
   }


   /* Form the LU triangular factorization using scaled partial pivoting. */
   for (k = 0; k < n; k++) {
      /* Decide whether to pivot. */
      colmax = fabs(lu[k*n+k]) / rowmax[k];
      pivot = k;

      for (i = k+1; i < n; i++) {
         ik = i*n + k;
         dtemp = fabs(lu[ik]) / rowmax[i];
         if (dtemp > colmax) {
            colmax = dtemp;
            pivot = i;
         }
      }

      if (pivot > k) {
         /* We must pivot, interchange the rows of the design matrix. */
         for (j = 0, pj = pivot*n, kj = k*n; j < n; j++, pj++, kj++) {
            dtemp = lu[pj];
            lu[pj] = lu[kj];
            lu[kj] = dtemp;
         }

         /* Amend the vector of row maxima. */
         dtemp = rowmax[pivot];
         rowmax[pivot] = rowmax[k];
         rowmax[k] = dtemp;

         /* Record the interchange for later use. */
         itemp = mxl[pivot];
         mxl[pivot] = mxl[k];
         mxl[k] = itemp;
      }

      /* Gaussian elimination. */
      for (i = k+1; i < n; i++) {
         ik = i*n + k;

         /* Nothing to do if lu[ik] is zero. */
         if (lu[ik] != 0.0) {
            /* Save the scaling factor. */
            lu[ik] /= lu[k*n+k];

            /* Subtract rows. */
            for (j = k+1; j < n; j++) {
               lu[i*n+j] -= lu[ik]*lu[k*n+j];
            }
         }
      }
   }


   /* mxl[i] records which row of mat corresponds to row i of lu.  */
   /* lxm[i] records which row of lu  corresponds to row i of mat. */
   for (i = 0; i < n; i++) {
      lxm[mxl[i]] = i;
   }


   /* Determine the inverse matrix. */
   for (i = 0, ij = 0; i < n; i++) {
      for (j = 0; j < n; j++, ij++) {
         inv[ij] = 0.0;
      }
   }

   for (k = 0; k < n; k++) {
      inv[lxm[k]*n+k] = 1.0;

      /* Forward substitution. */
      for (i = lxm[k]+1; i < n; i++) {
         for (j = lxm[k]; j < i; j++) {
            inv[i*n+k] -= lu[i*n+j]*inv[j*n+k];
         }
      }

      /* Backward substitution. */
      for (i = n-1; i >= 0; i--) {
         for (j = i+1; j < n; j++) {
            inv[i*n+k] -= lu[i*n+j]*inv[j*n+k];
         }
         inv[i*n+k] /= lu[i*n+i];
      }
   }

   free(mxl);
   free(lxm);
   free(rowmax);
   free(lu);

   return 0;
}
/* Dec 20 1999	Doug Mink - Include wcslib.h, which includes lin.h
 *
 * Feb 15 2001	Doug Mink - Add comments for WCSLIB 2.6; no code changes
 * Sep 19 2001	Doug Mink - Add above change to WCSLIB 2.7 code
 * Nov 20 2001	Doug Mink - Always include stdlib.h
 *
 * Jan 15 2002	Bill Joye - Add ifdef so this compiles on MacOS/X
 *
 * Nov 18 2003	Doug Mink - Include stdlib.h instead of malloc.h
 */
