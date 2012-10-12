/* $Id: two_plane.c,v 1.27 2004/12/10 04:10:49 davidm Exp $ */

/*  "By David Makovoz (davidm\@ipac.caltech.edu)\n".
  "Copyright (C) 2001 California Institute of Technology";
*/
/*********************************************** 
two_plane.c
Purpose
coordinate transformation defined

Definitions of Variables
ifdefined DISTMAX 
See two_plane.c
*********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "fitsio.h"
#include "wcs.h"
#include "two_plane.h"
#include "distort.h"

int plane1_to_plane2_transform(double x_1, double y_1, double *x_2, double *y_2, 
			   struct TwoPlane *two_plane){

  double x_temp, y_temp;
  double cos_delta_theta;
  double sin_delta_theta;
  double cos_theta_plus_delta_theta;
  double sin_theta_plus_delta_theta;
  double cos_phi,sin_phi_squared,tan_phi_squared, phi_squared;
  double sin_half_phi_squared, tan_half_phi_squared, conversion;

  *x_2 = 0;
  *y_2 = 0;

  if(two_plane->initialized != 1)
    return TWO_PLANE_NOT_INITIALIZED;
  /* do undistort if necesssary*/
  if(two_plane->first_distorted > 0){
     undistort(x_1, y_1, two_plane->DistortCoeffFirst, x_2, y_2);
     x_1 = *x_2;
     y_1 = *y_2;
  }
  /*  now rotate x_1 and y_1 to the perp and paral coordinate system */
/*    paral being x and perp y */
  if(two_plane->have_cdmatrix1){
    x_temp = two_plane->cd1_11 * (x_1 - two_plane->x_center_1) +
      two_plane->cd1_12 * (y_1 - two_plane->y_center_1);
    y_temp = two_plane->cd1_21 * (x_1 -two_plane->x_center_1) +
      two_plane->cd1_22 * (y_1 - two_plane->y_center_1);
    
  }
  else{
    x_temp = two_plane->cdelt1_1 * (x_1 - two_plane->x_center_1) * two_plane->cos_phi_1 +
      two_plane->cdelt2_1 * (y_1 - two_plane->y_center_1) * two_plane->sin_phi_1;
    y_temp = -two_plane->cdelt1_1 * (x_1 -two_plane->x_center_1) * two_plane->sin_phi_1 +
      two_plane->cdelt2_1 * (y_1 - two_plane->y_center_1) * two_plane->cos_phi_1;
  }
  x_temp *= PI180;
  if(!strcmp(two_plane->projection_type_1,"TAN")){}
  /**** SIN projection for the first plane ****/
  else if(!strcmp(two_plane->projection_type_1,"SIN")){
    sin_phi_squared = x_temp * x_temp + y_temp * y_temp * PI180 * PI180;
    if(sin_phi_squared > 1)
      return CANNOT_PROJECT;
    else if(sin_phi_squared <1)
      cos_phi = sqrt(1 - sin_phi_squared);
    else
      cos_phi = 0;
    x_temp /= cos_phi;
    y_temp /= cos_phi;
  }
  /****end of SIN projection for the first plane ****/

  /**** ZEA (Lambet's zenithal equal-area) projection for the first plane ****/
  else if(!strcmp(two_plane->projection_type_1,"ZEA")){ 
    sin_half_phi_squared = 0.25*(x_temp * x_temp + y_temp * y_temp * PI180 * PI180); 
    if(sin_half_phi_squared > 0.5) 
      return CANNOT_PROJECT; 
    else
      conversion = 0.5*sqrt(1 - sin_half_phi_squared)/(1-2*sin_half_phi_squared); 

    x_temp *= conversion; 
    y_temp *= conversion; 
  } 
  /****end of  ZEA projection for the first plane ****/

  /**** STG (stereographic) projection for the first plane ****/
  else if(!strcmp(two_plane->projection_type_1,"STG")){ 
    tan_half_phi_squared = 0.25*(x_temp * x_temp + y_temp * y_temp * PI180 * PI180); 
    if(tan_half_phi_squared >= 1) 
      return CANNOT_PROJECT; 
    else
      conversion = 1 - tan_half_phi_squared; 
    
    x_temp *= conversion; 
    y_temp *= conversion; 
  } 
  /****end of  STG projection for the first plane ****/

  /**** ARC (zenithal equidistant) projection for the first plane ****/
  else if(!strcmp(two_plane->projection_type_1,"ARC")){ 
    phi_squared = x_temp * x_temp/(PI180 * PI180) + y_temp * y_temp; 
    if(phi_squared <= 0) 
      return CANNOT_PROJECT; 
    else
      conversion = tan(sqrt(phi_squared))/sqrt(phi_squared); 
    
    x_temp *= conversion; 
    y_temp *= conversion; 
  } 
  /****end of  ARC projection for the first plane ****/

 else if(two_plane->cos_theta - x_temp * two_plane->sin_theta <= 0) 
    return CANNOT_PROJECT; 
  y_1 = y_temp / (two_plane->cos_theta - x_temp * two_plane->sin_theta) ;
  x_1 = (two_plane->sin_theta + x_temp * two_plane->cos_theta)/
    (two_plane->cos_theta - x_temp * two_plane->sin_theta)/ PI180;

  if(!strcmp(two_plane->projection_type_2,"TAN")){}
  /**** SIN projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"SIN")){
    tan_phi_squared = (x_1 * x_1 + y_1 * y_1) * PI180 * PI180;
    cos_phi = 1./ sqrt(1 + tan_phi_squared);
    x_1 *= cos_phi;
    y_1 *= cos_phi;
  }
  /**** end of SIN projection for the second  plane ****/

  /**** ZEA projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"ZEA")){ 
    tan_phi_squared = (x_1 * x_1 + y_1 * y_1) * PI180 * PI180; 
    if(tan_phi_squared > Epsilon){
      conversion = sqrt(2.*(1.-1./sqrt(1+tan_phi_squared)))/sqrt(tan_phi_squared);
      x_1 *= conversion; 
      y_1 *= conversion; 
    }
    /*else conversion = 1*/
  } 
  /**** end of ZEA projection for the second plane ****/

  /**** STG projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"STG")){ 
    tan_phi_squared = (x_1 * x_1 + y_1 * y_1) * PI180 * PI180; 
    conversion = 1./(sqrt(1+tan_phi_squared)+1);
    x_1 *= conversion; 
    y_1 *= conversion; 
  
   } 
  /**** end of STG projection for the second plane ****/

  /**** ARC projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"ARC")){ 
    tan_phi_squared = (x_1 * x_1 + y_1 * y_1) * PI180 * PI180; 
    if(tan_phi_squared <= 0) 
      return CANNOT_PROJECT; 
    else
      conversion = atan(sqrt(tan_phi_squared))/sqrt(tan_phi_squared); 
    x_1 *= conversion; 
    y_1 *= conversion; 
  
   } 
  /**** end of ARC projection for the second plane ****/

  /*  rotate to the x and y coordinates of the output frame */
  if(two_plane->have_cdmatrix2){
    x_temp = two_plane->invcd2_11 * x_1 + two_plane->invcd2_12 * y_1;
    y_temp = two_plane->invcd2_21 * x_1 + two_plane->invcd2_22 * y_1;
    
  }
  else{
    x_temp = (x_1) * two_plane->cos_phi_2 - (y_1) * two_plane->sin_phi_2;
    y_temp = (x_1) * two_plane->sin_phi_2 + (y_1) * two_plane->cos_phi_2;
    x_temp /= two_plane->cdelt1_2;
    y_temp /= two_plane->cdelt2_2;
  }

  x_temp += two_plane->x_center_2;
  y_temp += two_plane->y_center_2;
  
  /* do distort if necesssary*/
  if(two_plane->second_distorted > 0){
     distort(x_temp, y_temp, two_plane->DistortCoeffSecond, x_2, y_2);
  }
  else{
    *x_2 = x_temp;
    *y_2 = y_temp;
  }
  
  if(*x_2 < 0.5 || *x_2 > two_plane->naxis1_2 + 0.5 || 
     *y_2 < 0.5 || *y_2 > two_plane->naxis2_2 + 0.5)
    return PROJECT_OUTSIDE_IMAGE;

  return 0;
}


int plane2_to_plane1_transform(double x_2, double y_2, double *x_1, double *y_1, 
			   struct TwoPlane *two_plane){

 /*  first rotate x_2 and y_2 to the perp and paral coordinate system */
/*   paral being x and perp y */
  double x_temp, y_temp;
  double cos_delta_theta;
  double sin_delta_theta;
  double cos_theta_plus_delta_theta;
  double sin_theta_plus_delta_theta;
  double cos_phi,sin_phi_squared,tan_phi_squared, phi_squared;
  double sin_half_phi_squared, tan_half_phi_squared, conversion;

  *x_1 = 0;
  *y_1 = 0;

  if(two_plane->initialized != 1)
    return TWO_PLANE_NOT_INITIALIZED;
  /* do undistort if necesssary*/
  if(two_plane->second_distorted > 0){
     undistort(x_2, y_2, two_plane->DistortCoeffSecond, x_1, y_1);
     x_2 = *x_1;
     y_2 = *y_1;
  }

  if(two_plane->have_cdmatrix2){
    x_temp = two_plane->cd2_11 * (x_2 - two_plane->x_center_2) +
      two_plane->cd2_12 * (y_2 - two_plane->y_center_2);
    y_temp = two_plane->cd2_21 * (x_2 -two_plane->x_center_2) +
      two_plane->cd2_22 * (y_2 - two_plane->y_center_2);   
  }

  else{
    x_temp = two_plane->cdelt1_2 * (x_2 - two_plane->x_center_2) * two_plane->cos_phi_2 +
      two_plane->cdelt2_2 * (y_2 - two_plane->y_center_2) * two_plane->sin_phi_2;
    y_temp = -two_plane->cdelt1_2 * (x_2 -two_plane-> x_center_2) * two_plane->sin_phi_2 +
      two_plane->cdelt2_2 * (y_2 - two_plane->y_center_2) * two_plane->cos_phi_2;
  }
  x_temp *= PI180;

  if(!strcmp(two_plane->projection_type_2,"TAN")){}
  /**** SIN projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"SIN")){ 
    sin_phi_squared = x_temp * x_temp + y_temp * y_temp * PI180 * PI180; 
    if(sin_phi_squared > 1) 
      return CANNOT_PROJECT; 
    else if(sin_phi_squared <1) 
      cos_phi = sqrt(1 - sin_phi_squared); 
    else 
      cos_phi = 0; 
    x_temp /= cos_phi; 
    y_temp /= cos_phi; 
  } 
  /****end of  SIN projection for the second plane ****/

  /**** ZEA (Lambet's zenithal equal-area) projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"ZEA")){ 
    sin_half_phi_squared = 0.25*(x_temp * x_temp + y_temp * y_temp * PI180 * PI180); 
    if(sin_half_phi_squared > 0.5) 
      return CANNOT_PROJECT; 
    else
      conversion = 0.5*sqrt(1 - sin_half_phi_squared)/(1-2*sin_half_phi_squared); 

    x_temp *= conversion; 
    y_temp *= conversion; 
  } 
  /****end of  ZEA projection for the second plane ****/

  /**** STG (stereographic) projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"STG")){ 
    tan_half_phi_squared = 0.25*(x_temp * x_temp + y_temp * y_temp * PI180 * PI180); 
    if(tan_half_phi_squared >= 1) 
      return CANNOT_PROJECT; 
    else
      conversion = 1 - tan_half_phi_squared; 
    
    x_temp *= conversion; 
    y_temp *= conversion; 
  } 
  /****end of  STG projection for the second plane ****/

  /**** ARC (zenithal equidistant) projection for the second plane ****/
  else if(!strcmp(two_plane->projection_type_2,"ARC")){ 
    phi_squared = x_temp * x_temp/(PI180 * PI180) + y_temp * y_temp; 
    if(phi_squared <= 0) 
      return CANNOT_PROJECT; 
    else
      conversion = tan(sqrt(phi_squared))/sqrt(phi_squared); 
    
    x_temp *= conversion; 
    y_temp *= conversion; 
  } 
  /****end of  ARC projection for the second plane ****/

  /* inverse transformation theta = -theta*/
 if(two_plane->cos_theta + x_temp * two_plane->sin_theta <= 0) 
    return CANNOT_PROJECT; 
  y_2 = y_temp / (two_plane->cos_theta + x_temp * two_plane->sin_theta) ;
  x_2 = (-two_plane->sin_theta + x_temp * two_plane->cos_theta)/
    (two_plane->cos_theta + x_temp * two_plane->sin_theta)/ PI180;
  

  if(!strcmp(two_plane->projection_type_1,"TAN")){} 
  /**** SIN projection for the first plane ****/
   else if(!strcmp(two_plane->projection_type_1,"SIN")){ 
     tan_phi_squared = (x_2 * x_2 + y_2 * y_2) * PI180 * PI180; 
     cos_phi = 1./ sqrt(1 + tan_phi_squared);
     x_2 *= cos_phi; 
     y_2 *= cos_phi; 
   } 
  /**** end of SIN projection for the first plane ****/

  /**** ZEA projection for the first plane ****/
   else if(!strcmp(two_plane->projection_type_1,"ZEA")){ 
     tan_phi_squared = (x_2 * x_2 + y_2 * y_2) * PI180 * PI180; 
     if(tan_phi_squared > Epsilon){
       conversion = sqrt(2.*(1.-1./sqrt(1+tan_phi_squared)))/sqrt(tan_phi_squared);
       x_2 *= conversion; 
       y_2 *= conversion; 
     }
     /*else conversion = 1*/
   } 
  /**** end of ZEA projection for the first plane ****/

  /**** STG projection for the first plane ****/
  else if(!strcmp(two_plane->projection_type_1,"STG")){ 
    tan_phi_squared = (x_2 * x_2 + y_2 * y_2) * PI180 * PI180; 
    conversion = 1./(sqrt(1+tan_phi_squared)+1);
    x_2 *= conversion; 
    y_2 *= conversion; 
  
   } 
  /**** end of STG projection for the first plane ****/

  /**** ARC projection for the first plane ****/
  else if(!strcmp(two_plane->projection_type_1,"ARC")){ 
    tan_phi_squared = (x_2 * x_2 + y_2 * y_2) * PI180 * PI180; 
    if(tan_phi_squared <= 0) 
      return CANNOT_PROJECT; 
    else
      conversion = atan(sqrt(tan_phi_squared))/sqrt(tan_phi_squared); 
    x_2 *= conversion; 
    y_2 *= conversion; 
  
   } 
  /**** end of ARC projection for the first plane ****/

 /*  rotate to the x and y coordinates of the output frame */
  if(two_plane->have_cdmatrix1){
    x_temp = two_plane->invcd1_11 * x_2 + two_plane->invcd1_12 * y_2;
    y_temp = two_plane->invcd1_21 * x_2 + two_plane->invcd1_22 * y_2;
    
  }
  else{
    x_temp = (x_2) * two_plane->cos_phi_1 - (y_2) * two_plane->sin_phi_1;
    y_temp = (x_2) * two_plane->sin_phi_1 + (y_2) * two_plane->cos_phi_1;
    x_temp /= two_plane->cdelt1_1;
    y_temp /= two_plane->cdelt2_1;
  }
  x_temp += two_plane->x_center_1;
  y_temp += two_plane->y_center_1;
 
  /* do distort if necesssary*/
  if(two_plane->first_distorted > 0){
     distort(x_temp, y_temp, two_plane->DistortCoeffFirst, x_1, y_1);
  }
  else{
    *x_1 = x_temp;
    *y_1 = y_temp;
  }
  if(*x_1 < 0.5 || *x_1 > two_plane->naxis1_1 + 0.5 || 
     *y_1 < 0.5 || *y_1 > two_plane->naxis2_1 + 0.5)
    return PROJECT_OUTSIDE_IMAGE;

  return 0;
}
     

int Initialize_TwoPlane_FirstDistort(struct TwoPlane *two_plane, 
				      char *fitsheader, struct WorldCoor *WCS){

  int return_status;
  struct WorldCoor *wcs = NULL;
  wcs = wcsinit(fitsheader);
  if(return_status = Initialize_TwoPlane(two_plane, wcs,WCS)) 
      return return_status;
  
  two_plane->first_distorted = initdata_byheader(fitsheader, &(two_plane->DistortCoeffFirst));
  two_plane->second_distorted = 0;
  if(wcs != NULL)
    free (wcs);

  return 0;
}

int Initialize_TwoPlane_SecondDistort(struct TwoPlane *two_plane, 
				       struct WorldCoor *wcs, char *fitsheader){

  int return_status;
  struct WorldCoor *WCS = NULL;
  WCS = wcsinit(fitsheader);
  if(return_status = Initialize_TwoPlane(two_plane, wcs,WCS)) 
      return return_status;

  two_plane->second_distorted = initdata_byheader(fitsheader, 
						  &(two_plane->DistortCoeffSecond) );
  two_plane->first_distorted = 0;
  if(WCS != NULL)
    free (WCS);

  return 0;
}

int Initialize_TwoPlane_BothDistort(struct TwoPlane *two_plane,
                                     char *fitsheader1, char *fitsheader2){

  int return_status;
  struct WorldCoor *wcs = NULL;
  struct WorldCoor *WCS = NULL;
  wcs = wcsinit(fitsheader1);
  WCS = wcsinit(fitsheader2);
  if(return_status = Initialize_TwoPlane(two_plane, wcs,WCS))
      return return_status;

  two_plane->first_distorted = initdata_byheader(fitsheader1,
                                                 &(two_plane->DistortCoeffFirst));
  two_plane->second_distorted = initdata_byheader(fitsheader2,
                                                  &(two_plane->DistortCoeffSecond));
  if(WCS != NULL)
    free (WCS);
  if(wcs != NULL)
    free (wcs);

  return 0;
}

int SetDistortionPlaneFirstSimple(struct TwoPlane *two_plane, 
			    DistCoeff *coeff){
  two_plane->DistortCoeffFirst = *coeff;
  two_plane->first_distorted = 1;
  
  return 0;

}

int SetDistortionPlaneSecondSimple(struct TwoPlane *two_plane, 
			    DistCoeff *coeff){
  two_plane->DistortCoeffSecond = *coeff;
  two_plane->second_distorted = 1;
  
  return 0;

}


int SetDistortionPlaneFirst(struct TwoPlane *two_plane, 
			    int a_order, double *a,
			    int b_order, double *b,
			    int ap_order, double *ap, 
			    int bp_order, double *bp,
			    double xrefpix, double yrefpix){

  int i, j, m=0, n=0;
  int return_status;
  DistCoeff coeff;

/* initialize to 0's */
  for (i=0; i<MAXORDER; i++)
    for (j=0; j<MAXORDER; j++) {
      coeff.A[i][j] = 0.;
      coeff.AP[i][j] = 0.;
      coeff.B[i][j] = 0.;
      coeff.BP[i][j] = 0.;
    }

  m = coeff.A_ORDER = a_order;
  m++;
  if (m > 1) {
    for (i=0; i<m; i++)
      for (j=0; j<m; j++)
        coeff.A[i][j] = a[i*m + j];
  }

  n = coeff.B_ORDER = b_order;
  n++;
  if ( n > 1) {
    for (i=0; i<n; i++)
      for (j=0; j<n; j++)
        coeff.B[i][j] = b[i*n + j];
  }
  m = coeff.AP_ORDER = ap_order;
  m++;
  if (m > 1) {
    for (i=0; i<m; i++)
      for (j=0; j<m; j++)
        coeff.AP[i][j] = ap[i*m + j];
  }
  n = coeff.BP_ORDER = bp_order;
  n++;
  if ( n > 1) {
    for (i=0; i<n; i++)
      for (j=0; j<n; j++)
        coeff.BP[i][j] = bp[i*n + j];
  }

  two_plane->DistortCoeffFirst = coeff;
  two_plane->DistortCoeffFirst.crpix1 = xrefpix;
  two_plane->DistortCoeffFirst.crpix2 = yrefpix;
  two_plane->first_distorted = 1;

  return 0;
}

int SetDistortionPlaneSecond(struct TwoPlane *two_plane, 
			     int a_order, double *a,
			     int b_order, double *b,
			     int ap_order, double *ap, 
			     int bp_order, double *bp,
			     double xrefpix, double yrefpix){
  
  int i, j, m=0, n=0;
  int return_status;
  DistCoeff coeff;
  
  /* initialize to 0's */
  for (i=0; i<MAXORDER; i++)
    for (j=0; j<MAXORDER; j++) {
      coeff.A[i][j] = 0.;
      coeff.AP[i][j] = 0.;
      coeff.B[i][j] = 0.;
      coeff.BP[i][j] = 0.;
    }
  
  m =  a_order;
  m++;
  if ( m > 1) {
    coeff.A_ORDER = a_order;
    for (i=0; i<m; i++)
      for (j=0; j<m; j++)
	coeff.A[i][j] = a[i*m + j];
  }
  
  n = coeff.B_ORDER = b_order;
  n++;
  if ( n > 1) {
    for (i=0; i<n; i++)
      for (j=0; j<n; j++)
	coeff.B[i][j] = b[i*n + j];
  }
  
  m = coeff.AP_ORDER = ap_order;
  m++;
  if ( m > 1) {
    for (i=0; i<m; i++)
      for (j=0; j<m; j++)
        coeff.AP[i][j] = ap[i*m + j];
  }
  
  n = coeff.BP_ORDER = bp_order;
  n++;
  if ( n > 1) {
    for (i=0; i<n; i++)
      for (j=0; j<n; j++)
        coeff.BP[i][j] = bp[i*n + j];
  }
  
  two_plane->DistortCoeffSecond = coeff;
  two_plane->DistortCoeffSecond.crpix1 = xrefpix;
  two_plane->DistortCoeffSecond.crpix2 = yrefpix;
  two_plane->second_distorted = 1;
  
  return 0;
}

int Initialize_TwoPlane(struct TwoPlane *two_plane, struct WorldCoor *wcs, struct WorldCoor *WCS){

  double X_Cent, Y_Cent, x_cent, y_cent;
  double GAMMA, gamma, invdet, x_temp, y_temp;
  int stat;
  double PHI, phi, theta;
  double crval1, crval2, CRVAL1, CRVAL2, crota2, CROTA2;
  double cdelt1, cdelt2, CDELT1, CDELT2, crpix1, crpix2, CRPIX1, CRPIX2;
  double cd1_11, cd1_12, cd1_21, cd1_22, cd2_11, cd2_12, cd2_21, cd2_22;
  double cos_theta;
 
  if(!wcs->coorflip){
   crval1 = wcs->xref;
   crval2 = wcs->yref;
   cd1_11 = wcs->cd[0];
   cd1_12 = wcs->cd[1];
   cd1_21 = wcs->cd[2];
   cd1_22 = wcs->cd[3];
  }
  else{
   crval1 = wcs->yref;
   crval2 = wcs->xref;
   cd1_21 = wcs->cd[0];
   cd1_22 = wcs->cd[1];
   cd1_11 = wcs->cd[2];
   cd1_12 = wcs->cd[3];
  }
  if(!WCS->coorflip){
    CRVAL1 = WCS->xref;
    CRVAL2 = WCS->yref;
    cd2_11 = WCS->cd[0];
    cd2_12 = WCS->cd[1];
    cd2_21 = WCS->cd[2];
    cd2_22 = WCS->cd[3];
  }
  else{
    CRVAL1 = WCS->yref;
    CRVAL2 = WCS->xref;
    cd2_21 = WCS->cd[0];
    cd2_22 = WCS->cd[1];
    cd2_11 = WCS->cd[2];
    cd2_12 = WCS->cd[3];
  }
  crota2 = wcs->rot;
  CROTA2 = WCS->rot;
  cdelt1 = wcs->xinc;
  cdelt2 = wcs->yinc;
  CDELT1 = WCS->xinc;
  CDELT2 = WCS->yinc;
  crpix1 = wcs->xrefpix;
  crpix2 = wcs->yrefpix;
  CRPIX1 = WCS->xrefpix;
  CRPIX2 = WCS->yrefpix;
  two_plane->naxis1_1 = wcs->nxpix;
  two_plane->naxis2_1 = wcs->nypix;
  two_plane->naxis1_2 = WCS->nxpix;
  two_plane->naxis2_2 = WCS->nypix;

#ifdef DISTMAX
  wcs->distcode = 0;
  WCS->distcode = 0;
#endif

  if(crval1 == CRVAL1 && crval2 == CRVAL2){
    phi = crota2 * PI180;
    PHI = CROTA2 * PI180;
    theta = 0;

    two_plane->cd1_11 = cd1_11;
    two_plane->cd1_12 = cd1_12;
    two_plane->cd1_21 = cd1_21;
    two_plane->cd1_22 = cd1_22;

    two_plane->cd2_11 = cd2_11;
    two_plane->cd2_12 = cd2_12;
    two_plane->cd2_21 = cd2_21;
    two_plane->cd2_22 = cd2_22;

    two_plane->invcd1_11 = wcs->dc[0];
    two_plane->invcd1_12 = wcs->dc[1];
    two_plane->invcd1_21 = wcs->dc[2];
    two_plane->invcd1_22 = wcs->dc[3];

    two_plane->invcd2_11 = WCS->dc[0];
    two_plane->invcd2_12 = WCS->dc[1];
    two_plane->invcd2_21 = WCS->dc[2];
    two_plane->invcd2_22 = WCS->dc[3];
  }
  else{
    wcs2pix( WCS, crval1, crval2,&x_cent, &y_cent, &stat);
    if(x_cent != CRPIX1)
      PHI = atan( CDELT2 * (y_cent - CRPIX2) / CDELT1 / (x_cent - CRPIX1));
    else if(y_cent != CRPIX2)
      PHI = PI/2;
    else
      PHI = 0;
    /* do the transformation of the cd_matrix2*/
    x_temp = cd2_11 * (x_cent - CRPIX1) + cd2_12 * (y_cent - CRPIX2);
    y_temp = cd2_21 * (x_cent - CRPIX1) + cd2_22 * (y_cent - CRPIX2);   
    GAMMA = atan2(y_temp,x_temp);
    /*    if( CDELT1 * (x_cent - CRPIX1) < 0)
	  GAMMA += PI;*/
    two_plane->cd2_11 = cos(GAMMA) * cd2_11 + sin(GAMMA) * cd2_21;
    two_plane->cd2_12 = cos(GAMMA) * cd2_12 + sin(GAMMA) * cd2_22;
    two_plane->cd2_21 = -sin(GAMMA) * cd2_11 + cos(GAMMA) * cd2_21;
    two_plane->cd2_22 = -sin(GAMMA) * cd2_12 + cos(GAMMA) * cd2_22;
    /* find the inverse of cd2*/
    invdet = 1./(two_plane->cd2_11*two_plane->cd2_22 - two_plane->cd2_12*two_plane->cd2_21);
    if(isnan(invdet)){
      fprintf(stderr, "ERROR: DLCS: something is wrong with the cd-matrix for the second frame\n");
      return 1;
    }
    two_plane->invcd2_11 = two_plane->cd2_22 * invdet;
    two_plane->invcd2_12 =-two_plane->cd2_12 * invdet;
    two_plane->invcd2_21 =-two_plane->cd2_21 * invdet;
    two_plane->invcd2_22 = two_plane->cd2_11 * invdet;
    /* done with cd2*/

    wcs2pix( wcs, CRVAL1, CRVAL2,&X_Cent, &Y_Cent, &stat);
    if(X_Cent != crpix1)
      phi = atan( cdelt2 * (Y_Cent - crpix2) / cdelt1 / (X_Cent - crpix1));
    else if(Y_Cent != crpix2)
      phi = PI/2;
    else
      phi = 0;
  
    
    if( CDELT1 * (x_cent - CRPIX1) < 0)
      PHI += PI;
    if( cdelt1 * (X_Cent - crpix1) > 0)
      phi += PI;
    
    /*   x_parallel from X,Y to x,y, thus the choice of signs for phi, PHI and theta. */
       
    /* do the transformation of the cd_matrix1*/
    x_temp = cd1_11 * (X_Cent - crpix1) + cd1_12 * (Y_Cent - crpix2);
    y_temp = cd1_21 * (X_Cent - crpix1) + cd1_22 * (Y_Cent - crpix2);   
    gamma = atan2(y_temp,x_temp);
    /*   gamma = atan(y_temp/x_temp);
	 if( cdelt1 * (X_Cent - crpix1) > 0)*/
       gamma += PI; 
    /*fprintf(stderr, "%f %f\n",gamma,atan2(y_temp,x_temp)+PI);
        gamma = atan2(y_temp,x_temp);
    if( cdelt1 * (X_Cent - crpix1) > 0)
    gamma += PI; */
    two_plane->cd1_11 = cos(gamma) * cd1_11 + sin(gamma) * cd1_21;
    two_plane->cd1_12 = cos(gamma) * cd1_12 + sin(gamma) * cd1_22;
    two_plane->cd1_21 = -sin(gamma) * cd1_11 + cos(gamma) * cd1_21;
    two_plane->cd1_22 = -sin(gamma) * cd1_12 + cos(gamma) * cd1_22;
    /* find the inverse of cd1*/
    invdet = 1./(two_plane->cd1_11*two_plane->cd1_22 - two_plane->cd1_12*two_plane->cd1_21);
    if(isnan(invdet)){
      fprintf(stderr, "ERROR: DLCS: something is wrong with the cd-matrix for the first frame\n");
      return 1;
    }
    two_plane->invcd1_11 = two_plane->cd1_22 * invdet;
    two_plane->invcd1_12 =-two_plane->cd1_12 * invdet;
    two_plane->invcd1_21 =-two_plane->cd1_21 * invdet;
    two_plane->invcd1_22 = two_plane->cd1_11 * invdet;
    /* done with cd1*/

    cos_theta = cos(CRVAL2*PI180) * cos(CRVAL1*PI180) * cos(crval2*PI180) * cos(crval1*PI180) +
                cos(CRVAL2*PI180) * sin(CRVAL1*PI180) * cos(crval2*PI180) * sin(crval1*PI180) +
                sin(CRVAL2*PI180) * sin(crval2*PI180);
    if (cos_theta > 1.)
      cos_theta = 1.;
    else if (cos_theta < -1.)
      cos_theta = -1.;
    theta = acos(cos_theta);
    /*
    theta = acos( cos(CRVAL2*PI180) * cos(CRVAL1*PI180) * cos(crval2*PI180) * cos(crval1*PI180) +
	    cos(CRVAL2*PI180) * sin(CRVAL1*PI180) * cos(crval2*PI180) * sin(crval1*PI180) +
		       sin(CRVAL2*PI180) * sin(crval2*PI180) );
    */
  }  

  two_plane->x_center_1 = crpix1;
  two_plane->y_center_1 = crpix2;
  two_plane->x_center_2 = CRPIX1;
  two_plane->y_center_2 = CRPIX2;
  two_plane->cos_phi_1 = cos(phi);
  two_plane->sin_phi_1 = sin(phi);
  two_plane->cos_phi_2 = cos(PHI);
  two_plane->sin_phi_2 = sin(PHI);
  two_plane->cos_theta = cos(theta);
  two_plane->sin_theta = sin(theta);
  two_plane->cdelt1_1 = cdelt1;
  two_plane->cdelt2_1 = cdelt2;
  two_plane->cdelt1_2 = CDELT1;
  two_plane->cdelt2_2 = CDELT2;

  strcpy(two_plane->projection_type_1,wcs->ptype);
  strcpy(two_plane->projection_type_2,WCS->ptype);
  
  two_plane->initialized = 1;
  two_plane->first_distorted = 0;
  two_plane->second_distorted = 0;
  two_plane->have_cdmatrix1 = 1;
  two_plane->have_cdmatrix2 = 1;

  /*
    if(fabs(two_plane->cd1_11 - two_plane->cdelt1_1 * two_plane->cos_phi_1) > sqrt(Epsilon) ||
     fabs(two_plane->cd1_12 - two_plane->cdelt2_1 * two_plane->sin_phi_1) > sqrt(Epsilon) ||
     fabs(two_plane->cd1_21 + two_plane->cdelt1_1 * two_plane->sin_phi_1) > sqrt(Epsilon) ||
     fabs(two_plane->cd1_22 - two_plane->cdelt2_1 * two_plane->cos_phi_1) > sqrt(Epsilon) )
    fprintf(stderr, "oops1\n");

   if(fabs(two_plane->cd2_11 - two_plane->cdelt1_2 * two_plane->cos_phi_2) > sqrt(Epsilon) ||
     fabs(two_plane->cd2_12 - two_plane->cdelt2_2 * two_plane->sin_phi_2) > sqrt(Epsilon) ||
     fabs(two_plane->cd2_21 + two_plane->cdelt1_2 * two_plane->sin_phi_2) > sqrt(Epsilon) ||
     fabs(two_plane->cd2_22 - two_plane->cdelt2_2 * two_plane->cos_phi_2) > sqrt(Epsilon) )
    fprintf(stderr, "oops2\n");
  */
    
  return 0;
}
