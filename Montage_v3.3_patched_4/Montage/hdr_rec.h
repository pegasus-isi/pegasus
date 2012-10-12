/* Module: hdr_rec.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.4      John Good        21Dec06  Changed file size to "off_t" 
1.3      John Good        12Jan06  Changed file size to "long long" 
1.2      John Good        27Jun03  Added a few comments for clarity 
1.1      John Good        27Mar03  Removed unused 'good' 
				   flag from structure
1.0      John Good        29Jan03  Baseline code

*/

#ifndef HDR_REC_H
#define HDR_REC_H


/* FITS header (mostly      */
/* WCS-related) information */

struct Hdr_rec 
{
   int       cntr;
   char      fname[1024];
   int       hdu;
   off_t     size;
   char      ctype1[10];
   char      ctype2[10];
   int       ns;
   int       nl;
   float     crpix1;
   float     crpix2;
   double    crval1;
   double    crval2;
   double    cdelt1;
   double    cdelt2;
   double    crota2;
   double    ra2000;
   double    dec2000;
   double    ra1, dec1;
   double    ra2, dec2;
   double    ra3, dec3;
   double    ra4, dec4;
   double    radius;

   double    equinox;
};

#endif
