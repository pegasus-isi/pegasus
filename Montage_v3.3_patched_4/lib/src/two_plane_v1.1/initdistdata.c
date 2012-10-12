#include <stdio.h>
#include <strings.h>
#include "fitsio.h"
#include "distort.h"

fitsfile        *ffp_FITS_In;

int openfitsfile(char *fitsfilename) 
{
  int I_fits_return_status=0;
  fits_open_file( &ffp_FITS_In,
                fitsfilename,
                READONLY,
                &I_fits_return_status);

  if (I_fits_return_status != 0)
  {
     fprintf(stderr, "Error openning file %s\n", fitsfilename);
     return -1;
  }
  return 0;
}

closefitsfile()
{ 
  int I_fits_return_status=0;
  fits_close_file(ffp_FITS_In, &I_fits_return_status); 
  if (I_fits_return_status != 0)
  {
     fprintf(stderr, "Error closing file\n");
     return;
  }
  return;    
}

int initdata_byheader(char *fitsheader, DistCoeff *coeff) 
{
  int i, j, m=0, n=0;
  
  int status=0;
  char    CP_Keyname[FLEN_KEYWORD], CP_Comment[FLEN_COMMENT],
          CP_Keyvalue[FLEN_VALUE], extension[FLEN_COMMENT];
 
  int ival=0;

  /* Determine if there is distortion */
  strcpy(CP_Keyname, "CTYPE1");

  if ((status = parse_str(fitsheader, CP_Keyvalue, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
     return -1;
  }  
  
  /* if 8 characters, meaning no extension for CTYPE */
  if (strlen(CP_Keyvalue) == 8 )
    return 0;

  /* if the extension is something else */
  strncpy(extension,CP_Keyvalue+9,4);
  extension[4] = '\0';
  if (strcmp(extension, "-SIP") != 0)
    return -1;

  /* Read in A_ORDER and **A */
  strcpy(CP_Keyname, "A_ORDER");
  
  if ((status = parse_int(fitsheader, &ival, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  }  
  
  m = coeff->A_ORDER = ival;

  for (i=0; i<=m; i++)
    for (j=0; j<=m; j++)
      coeff->A[i][j] = 0.;  

  for (i=0; i<=m; i++) {
    for (j=0; j<=m-i; j++) { 
       sprintf(CP_Keyname,  "A_%d_%d",i,j);
       if ((status = parse_double(fitsheader, &(coeff->A[i][j]),CP_Keyname)) != 0)
         continue;
     }
  }


  /* Read in B_ORDER and **B */

  strcpy(CP_Keyname, "B_ORDER");
  if ((status = parse_int(fitsheader, &ival, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  }  
   
  n = coeff->B_ORDER = ival;

  for (i=0; i<=n; i++)
    for (j=0; j<=n; j++)
      coeff->B[i][j] = 0.; 
 
  for (i=0; i<=n; i++) {
    for (j=0; j<=n-i; j++) {
       sprintf(CP_Keyname, "B_%d_%d", i, j);
       if ((status = parse_double(fitsheader, &(coeff->B[i][j]),CP_Keyname)) != 0)
         continue;      
    }
  }

  /* Read in AP_ORDER and **A */
  strcpy(CP_Keyname, "AP_ORDER");
  if ((status = parse_int(fitsheader, &ival, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  } 
  
  m = coeff->AP_ORDER = ival;

  for (i=0; i<=m; i++)
    for (j=0; j<=m; j++)
      coeff->AP[i][j] = 0.;  

  for (i=0; i<=m; i++) {
    for (j=0; j<=m-i; j++) { 
       *CP_Keyname = '\0';
       sprintf(CP_Keyname,  "AP_%d_%d",i,j);
       if ((status = parse_double(fitsheader, &(coeff->AP[i][j]),CP_Keyname)) != 0)
         continue;           
    }
  }


  /* Read in B_ORDER and **B */

  strcpy(CP_Keyname, "BP_ORDER");
  if ((status = parse_int(fitsheader, &ival, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  } 
  
  n = coeff->BP_ORDER = ival;

  for (i=0; i<=n; i++)
    for (j=0; j<=n; j++)
      coeff->BP[i][j] = 0.; 
 
  for (i=0; i<=n; i++) {
    for (j=0; j<=n-i; j++) {
       sprintf(CP_Keyname, "BP_%d_%d", i, j);
       if ((status = parse_double(fitsheader, &(coeff->BP[i][j]),CP_Keyname)) != 0)
         continue;    
    }
  }

  /* Read in CRPIX1 and CRPIX2 */
  strcpy(CP_Keyname, "CRPIX1");
  if ((status = parse_double(fitsheader, &coeff->crpix1, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading CRPIX1\n");
     return -1;
  }

  strcpy(CP_Keyname, "CRPIX2");
  if ((status = parse_double(fitsheader, &coeff->crpix2, CP_Keyname)) != 0) {
     fprintf(stderr, "Error reading CRPIX1\n");
     return -1;
  }

  return 1;
}

int initdata_byfile(fitsfile *ffp_FITS_In, DistCoeff *coeff) 
{
  int i, j, m=0, n=0;
  
  int I_fits_return_status=0;
  char    CP_Keyname[FLEN_KEYWORD], CP_Comment[FLEN_COMMENT],
          CP_Keyvalue[FLEN_VALUE], extension[FLEN_COMMENT];
 
  long ival=0;

  /* Determine if there is distortion */
  strcpy(CP_Keyname, "CTYPE1");
  fits_read_key_str(ffp_FITS_In, 
                    CP_Keyname,
                    CP_Keyvalue, 
                    NULL, 
                    &I_fits_return_status );

  if (I_fits_return_status != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  }  
  
  /* if 8 characters, meaning no extension for CTYPE */
  if (strlen(CP_Keyvalue) == 8 )
    return 0;

  /* if the extension is something else */
  strncpy(extension,CP_Keyvalue+8,4);
  extension[4] = '\0';
  if (strcmp(extension, "-SIP") != 0)
    return -1;


  /* Read in A_ORDER and **A */
  strcpy(CP_Keyname, "A_ORDER");
  fits_read_key_lng(ffp_FITS_In, 
                    CP_Keyname,
                    &ival, 
                    NULL, 
                    &I_fits_return_status );

  if (I_fits_return_status != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  }  
  
  m = coeff->A_ORDER = (int) ival;

  for (i=0; i<=m; i++)
    for (j=0; j<=m; j++)
      coeff->A[i][j] = 0.;  

  for (i=0; i<=m; i++) {
    for (j=0; j<=m-i; j++) { 
       *CP_Keyname = '\0';
       sprintf(CP_Keyname,  "A_%d_%d",i,j);
       fits_read_key_dbl(ffp_FITS_In, 
                    CP_Keyname,
                    &(coeff->A[i][j]), 
                    NULL, 
                    &I_fits_return_status );
       if (I_fits_return_status != 0) {
	 I_fits_return_status=0;
         continue;
       }
     }
  }


  /* Read in B_ORDER and **B */

  strcpy(CP_Keyname, "B_ORDER");
  fits_read_key_lng(ffp_FITS_In, 
                    CP_Keyname,
                    &ival, 
                    NULL, 
                    &I_fits_return_status );

  n = coeff->B_ORDER = (int) ival;

  for (i=0; i<=n; i++)
    for (j=0; j<=n; j++)
      coeff->B[i][j] = 0.; 
 
  for (i=0; i<=n; i++) {
    for (j=0; j<=n-i; j++) {
       sprintf(CP_Keyname, "B_%d_%d", i, j);
       fits_read_key_dbl(ffp_FITS_In, 
                    CP_Keyname,
                    &(coeff->B[i][j]), 
                    NULL, 
                    &I_fits_return_status );

       if (I_fits_return_status != 0) {
	 I_fits_return_status=0;
         continue;
       }
    }
  }

  /* Read in AP_ORDER and **A */
  strcpy(CP_Keyname, "AP_ORDER");
  fits_read_key_lng(ffp_FITS_In, 
                    CP_Keyname,
                    &ival, 
                    NULL, 
                    &I_fits_return_status );

  if (I_fits_return_status != 0) {
     fprintf(stderr, "Error reading keyword [%s]\n", CP_Keyname);
  }  
  
  m = coeff->AP_ORDER = (int) ival;

  for (i=0; i<=m; i++)
    for (j=0; j<=m; j++)
      coeff->AP[i][j] = 0.;  

  for (i=0; i<=m; i++) {
    for (j=0; j<=m-i; j++) { 
       *CP_Keyname = '\0';
       sprintf(CP_Keyname,  "AP_%d_%d",i,j);
       fits_read_key_dbl(ffp_FITS_In, 
                    CP_Keyname,
                    &(coeff->AP[i][j]), 
                    NULL, 
                    &I_fits_return_status );
       if (I_fits_return_status != 0) {
	 I_fits_return_status=0;
         continue;
       }
    }
  }


  /* Read in B_ORDER and **B */

  strcpy(CP_Keyname, "BP_ORDER");
  fits_read_key_lng(ffp_FITS_In, 
                    CP_Keyname,
                    &ival, 
                    NULL, 
                    &I_fits_return_status );

  n = coeff->BP_ORDER = (int) ival;

  for (i=0; i<=n; i++)
    for (j=0; j<=n; j++)
      coeff->BP[i][j] = 0.; 
 
  for (i=0; i<=n; i++) {
    for (j=0; j<=n-i; j++) {
       sprintf(CP_Keyname, "BP_%d_%d", i, j);
       fits_read_key_dbl(ffp_FITS_In, 
                    CP_Keyname,
                    &(coeff->BP[i][j]), 
                    NULL, 
                    &I_fits_return_status );

       if (I_fits_return_status != 0) {
	 I_fits_return_status=0;
         continue;
       }
    }
  }

  /* Read in CRPIX1 and CRPIX2 */
  strcpy(CP_Keyname, "CRPIX1");
  fits_read_key_dbl(ffp_FITS_In, 
                    CP_Keyname,
                    &coeff->crpix1, 
                    NULL, 
                    &I_fits_return_status );

  strcpy(CP_Keyname, "CRPIX2");
  fits_read_key_dbl(ffp_FITS_In, 
                    CP_Keyname,
                    &coeff->crpix2, 
                    NULL, 
                    &I_fits_return_status );  

  return 1;
}

int initdata_bytable (char *tablefilename, DistCoeff *coeff)
{
  /*
  int i, j, iu=90, nparms, kindex, values;
  char  type[80], unit[80], coeff_name[80];
  float f_value;

  topenr(iu, tablefilename, &nparms);

  if (itstat() != 0)
  {
      fprintf(stderr, "Error openning scale file %s.\n", tablefilename);
      exit(64);
  }

 
  tindk(iu, "A_ORDER", &kindex, &type, &unit, &values);
  tgetki(iu, kindex, &(coeff->A_ORDER));
  tindk(iu, "B_ORDER", &kindex, &type, &unit, &values);
  tgetki(iu, kindex, &(coeff->B_ORDER));
  tindk(iu, "AP_ORDER", &kindex, &type, &unit, &values);
  tgetki(iu, kindex, &(coeff->AP_ORDER));
  tindk(iu, "BP_ORDER", &kindex, &type, &unit, &values);
  tgetki(iu, kindex, &(coeff->BP_ORDER));

  tindk(iu, "CRPIX1", &kindex, &type, &unit, &values);
  tgetkr(iu, kindex, &f_value);
  coeff->crpix1 = f_value;
  tindk(iu, "CRPIX2", &kindex, &type, &unit, &values);
  tgetkr(iu, kindex, &f_value);
  coeff->crpix2 = f_value;

  tindk(iu, "A_DMAX", &kindex, &type, &unit, &values);
  tgetkr(iu, kindex, &f_value);
  coeff->a_dmax = f_value;
  tindk(iu, "B_DMAX", &kindex, &type, &unit, &values);
  tgetkr(iu, kindex, &f_value);
  coeff->b_dmax = f_value;

  for (i=0; i<coeff->A_ORDER; i++)
    for (j=0; j<=coeff->A_ORDER-i; j++) {
      if (i+j != 1) {
	 sprintf(coeff_name,  "A_%d_%d",i,j);
         tindk(iu, coeff_name, &kindex, &type, &unit, &values);
         tgetkr(iu, kindex, &f_value);
         coeff->A[i][j] = f_value;
      }
    }

  for (i=0; i<coeff->B_ORDER; i++)
    for (j=0; j<=coeff->B_ORDER-i; j++) {
      if (i+j != 1) {
	 sprintf(coeff_name,  "B_%d_%d",i,j);
         tindk(iu, coeff_name, &kindex, &type, &unit, &values);
         tgetkr(iu, kindex, &f_value);
         coeff->B[i][j] = f_value;
      }
    }

  for (i=0; i<coeff->AP_ORDER; i++)
    for (j=0; j<=coeff->AP_ORDER-i; j++) {
       sprintf(coeff_name,  "AP_%d_%d",i,j);
       tindk(iu, coeff_name, &kindex, &type, &unit, &values);
       tgetkr(iu, kindex, &f_value);
       coeff->AP[i][j] = f_value;
    }

  for (i=0; i<coeff->BP_ORDER; i++)
    for (j=0; j<=coeff->BP_ORDER-i; j++) {
       sprintf(coeff_name,  "BP_%d_%d",i,j);
       tindk(iu, coeff_name, &kindex, &type, &unit, &values);
       tgetkr(iu, kindex, &f_value);
       coeff->BP[i][j] = f_value;
    }
*/
  return 1;
}

int update_distort_keywords(fitsfile *fp_FITS_In, DistCoeff* coeff)
{
  int i, j, I_fits_return_status=0;
  char CP_Keyname[FLEN_KEYWORD];
  char CP_Comment[FLEN_COMMENT];
  char CP_Keyvalue[FLEN_VALUE];

  sprintf(CP_Keyname, "%s", "A_ORDER");
  sprintf(CP_Comment, "%s", "");

  fits_update_key(fp_FITS_In,
                   TLONG,
                   CP_Keyname,
                   &(coeff->A_ORDER),
                   CP_Comment,
                   &I_fits_return_status);

  if (I_fits_return_status != 0)
  {
         fprintf(stderr, "Error updating keyword for A_ORDER\n");
         return -1;
  }

  for (i=0; i<=coeff->A_ORDER; i++)
     for (j=0; j<=coeff->A_ORDER-i; j++) {
        if ( i+j != 1) {
           sprintf(CP_Keyname,  "A_%d_%d",i,j);
           printf(CP_Comment, "%s", "");
           fits_update_key(fp_FITS_In,
                 TFLOAT,
                 CP_Keyname,
                 &(coeff->A[i][j]),
                 CP_Comment,
                 &I_fits_return_status);

            if (I_fits_return_status != 0)
            {
                  fprintf(stderr, "Error updating keyword for A[%d][%d]\n", i,j);
                  return -1;
            }
         }
      }

   sprintf(CP_Keyname, "%s", "A_DMAX");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                   TFLOAT,
                   CP_Keyname,
                   &(coeff->a_dmax),
                   CP_Comment,
                   &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
         fprintf(stderr, "Error updating keyword for A_DMAX\n");
         return -1;
   }

   sprintf(CP_Keyname, "%s", "B_ORDER");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                   TLONG,
                   CP_Keyname,
                   &(coeff->B_ORDER),
                   CP_Comment,
                   &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
      fprintf(stderr, "Error updating keyword for B_ORDER\n");
      return -1;
   }


   for (i=0; i<=coeff->B_ORDER; i++)
      for (j=0; j<=coeff->B_ORDER-i; j++) {
         if ( i+j != 1) {
            sprintf(CP_Keyname,  "B_%d_%d",i,j);
            sprintf(CP_Comment, "%s", "");

            fits_update_key(fp_FITS_In,
                      TFLOAT,
                      CP_Keyname,
                      &(coeff->B[i][j]),
                      CP_Comment,
                      &I_fits_return_status);

            if (I_fits_return_status != 0)
            {
                   fprintf(stderr, "Error updating keyword for B[%d][%d]\n", i,j);
                   return -1;
            }
         }
      }

   sprintf(CP_Keyname, "%s", "B_DMAX");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                   TFLOAT,
                   CP_Keyname,
                   &(coeff->b_dmax),
                   CP_Comment,
                   &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
         fprintf(stderr, "Error updating keyword for B_DMAX\n");
         return -1;
   }

   sprintf(CP_Keyname, "%s", "AP_ORDER");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                   TLONG,
                   CP_Keyname,
                   &(coeff->AP_ORDER),
                   CP_Comment,
                   &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
         fprintf(stderr, "Error updating keyword for AP_ORDER\n");
         return -1;
   }

   for (i=0; i<coeff->AP_ORDER; i++)
      for (j=0; j<=coeff->AP_ORDER-i; j++) {
         sprintf(CP_Keyname,  "AP_%d_%d",i,j);
         sprintf(CP_Comment, "%s", "");

         fits_update_key(fp_FITS_In,
                         TFLOAT,
                         CP_Keyname,
                         &(coeff->AP[i][j]),
                         CP_Comment,
                         &I_fits_return_status);

         if (I_fits_return_status != 0)
         {
               fprintf(stderr, "Error updating keyword for AP[%d][%d]\n", i,j);
               return -1;
         }
      }

   sprintf(CP_Keyname, "%s", "BP_ORDER");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                   TLONG,
                   CP_Keyname,
                   &(coeff->BP_ORDER),
                   CP_Comment,
                   &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
      fprintf(stderr, "Error updating keyword for BP_ORDER\n");
      return -1;
   }


   for (i=0; i<=coeff->BP_ORDER; i++)
      for (j=0; j<=coeff->BP_ORDER-i; j++) {
         sprintf(CP_Keyname,  "BP_%d_%d",i,j);
         sprintf(CP_Comment, "%s", "");

         fits_update_key(fp_FITS_In,
                      TFLOAT,
                      CP_Keyname,
                      &(coeff->BP[i][j]),
                      CP_Comment,
                      &I_fits_return_status);

         if (I_fits_return_status != 0)
         {
                fprintf(stderr, "Error updating keyword for BP[%d][%d]\n", i,j);
                return -1;
         }
      }

   sprintf(CP_Keyname, "%s", "CRPIX1");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                   TFLOAT,
                   CP_Keyname,
                   &(coeff->crpix1),
                   CP_Comment,
                   &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
      fprintf(stderr, "Error updating keyword for CRPIX1\n");
      return -1;
   }

   sprintf(CP_Keyname, "%s", "CRPIX2");
   sprintf(CP_Comment, "%s", "");

   fits_update_key(fp_FITS_In,
                     TFLOAT,
                     CP_Keyname,
                     &(coeff->crpix2),
                     CP_Comment,
                     &I_fits_return_status);

   if (I_fits_return_status != 0)
   {
         fprintf(stderr, "Error updating keyword for CRPIX2\n");
         return -1;
   }

  return 1;
}
