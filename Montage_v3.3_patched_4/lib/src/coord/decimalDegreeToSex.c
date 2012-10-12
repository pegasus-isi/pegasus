#include <stdio.h>
#include <math.h>
#include <coord.h>




/****************************************************************************/
/*                                                                          */
/* degreeToDMS  Convert decimal degrees to DMS sexigesimal                  */
/* -----------                                                              */
/*                                                                          */
/* Inputs:          deg  - decimal degree value to be converted.  Must be   */
/*                         between -360 and 360 degrees.                    */
/*                  prec - number of decimal places to write on output      */
/*                                                                          */
/* Output:          *neg - flag; if set indicates value is negative;        */
/*                  *d   - output degrees                                   */
/*                  *m   - output minutes                                   */
/*                  *s   - output seconds, rounded to  prec  decimal places */
/*                                                                          */
/*                  Returns 0 if sucessful, -1 if error.                    */
/*                                                                          */
/****************************************************************************/


int degreeToDMS(double deg, int prec, int *neg, int *d, int *m, double *s)
{
   double min;

   if (deg < -360.0  ||  deg > 360.0)
      return(-1);


   /* Set neg flag on negative input */

   if (deg < 0) 
   {
      *neg = 1;
      deg = fabs(deg);
   }
   else
      *neg = 0;


   /* Calculate DMS values */

   *d  = (int)floor(deg);
   min = (deg - (double)(*d))*60.0;        
   *m  = (int)floor(min);
   *s  = (min - (double)(*m))*60.0;


   /* Now do rounding according to precision requested */

   *s = roundValue(*s, prec);


   /* Correct DMS values if necessary due to roundoff */

   if (*s >= 60.) 
   {
      *s -= 60.;
      *m += 1;
   }

   if (*m >= 60) 
   {
      *m -= 60;
      *d += 1;
   }

   return 0;
}




/****************************************************************************/
/*                                                                          */
/* degreeToHMS  Convert decimal degrees to HMS sexigesimal                  */
/* -----------                                                              */
/*                                                                          */
/* Inputs:          deg  - decimal degree value to be converted.  Must be   */
/*                         between -360 and 360 degrees.                    */
/*                  prec - number of decimal places to write on output      */
/*                                                                          */
/* Output:          *neg - flag; if set indicates value is negative;        */
/*                  *h   - output hours                                     */
/*                  *m   - output minutes                                   */
/*                  *s   - output seconds, rounded to  prec  decimal places */
/*                                                                          */
/*                  Returns 0 if sucessful, -1 if error.                    */
/*                                                                          */
/****************************************************************************/


int degreeToHMS(double deg, int prec, int *neg, int *h, int *m, double *s)
{
   double hr, min;

   if (deg < -360.0  ||  deg > 360.0)
      return(-1);


   /* Set neg flag on negative input */

   if (deg < 0) 
   {
      *neg = 1;
      deg = fabs(deg);
   }
   else
      *neg = 0;

   hr = deg/15.;                     /* convert to hours */


   /* Calculate HMS */

   *h  = (int)floor(hr);
   min = (hr - (double)(*h))*60.0;        
   *m  = (int)floor(min);
   *s  = (min - (double)(*m))*60.0;


   /* Now do rounding according to precision requested  */

   *s = roundValue(*s,prec);


   /* Correct HMS values if necessary due to roundoff */

   if (*s >= 60.) 
   {
      *s -= 60.;
      *m += 1;
   }
   if (*m >= 60) 
   {
      *m -= 60;
      *h += 1;
   }

   return 0;
}




/***************************************************************************/
/*                                                                         */
/* degreeToSex  Utility to convert decimal degrees sexigesimal strings     */
/* -----------  (DMS format for longitude)                                 */
/*                                                                         */
/* Inputs:          lon  - decimal degree value for longitude.  Must be    */
/*                         between -360 and 360 degrees.                   */
/*                  lat  - decimal degree value for latitude.  Must be     */
/*                         between -90 and +90 degrees.                    */
/*                                                                         */
/* Output:          *lonstr - Sexigesimal longitude string (dms).          */
/*                  *latstr - Sexigesimal latitude string (dms).           */
/*                                                                         */
/*                  Returns 0 if sucessful, -1 if error.                   */
/*                                                                         */
/***************************************************************************/


int degreeToSex(double lon, double lat, char *lonstr, char *latstr)
{
   int    status, prec;
   int    neg, d, m, h;
   double s;

   prec = 2;



   /* Longitude */

   status = degreeToHMS(lon, prec, &neg, &h, &m, &s);

   if(status < 0)
      return(status);

   sprintf(lonstr, "%s%02dh %02dm %05.2fs", (neg? "-":""), h, m, s);



   /* Latitude */

   status = degreeToDMS(lat, prec, &neg, &d, &m, &s);

   if(status < 0)
      return(status);

   sprintf(latstr, "%s%02dd %02dm %05.2fs", (neg? "-":""), d, m, s);

   return(0);
}




/***************************************************************************/
/*                                                                         */
/* roundValue  Round off value (with precision)                            */
/* ----------                                                              */
/*                                                                         */
/* Inputs:    value     - value to be rounded                              */
/*            precision - number of decimal places to which value          */
/*                        is to be rounded                                 */
/*                                                                         */
/***************************************************************************/


double roundValue(double value, int precision)
{
   double temp;

   if (value < 0)
      temp =  ceil(value * pow(10.,(double)precision) - .5);
   else
      temp = floor(value * pow(10.,(double)precision) + .5);

   value = temp / pow(10.,(double)precision);

   return value;
}
