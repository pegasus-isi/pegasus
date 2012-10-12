/*
Written by: Mih-seh Kong
Date: May 21, 2001 
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <mtbl.h>

char *tval(int);

main(int argc, char *argv[], char *envp[])
{
    char   *col_val, *cptr;
    char   propid[1024], pi[1024], exptime[1024], fov[1024];
    int    f1, ncols, ns;
    int    i, l, icol, istatus;

    static char field_name[14][80] = {
	"tdt",
	"object",
	"aot",
	"ra2000",
	"dec2000",
	"proposal",
	"status",
	"wavelength",
	"fov",
	"propid",
	"pi",
	"obs_date",
	"exptime",
	"reduction"
    };

    ns = topen (argv[1]);
    printf ("ncols= %d\n", ns);
    
    l = 0;
    istatus = 0;
    ncols = 14;

    while (istatus >= 0) {

	istatus = tread();
	printf ("l= %d istatus= %d\n", l, istatus);

	if (istatus < 0) 
	    break;
	
	for (i=0; i<ncols; i++) {
		   
	    printf ("i= %d field_name= %s\n", i, field_name[i]);

	    icol = tcol (field_name[i]);
	    
	    printf ("icol= %d\n", icol);

	    
	    if (icol == -1) continue;
	
	    col_val = tval (icol);
            
	    printf ("i= %d icol= %d field_name= %s col_val= %s\n",
	            i, icol, field_name[i], col_val);

/*
	    if (strcmp (field_name[i], "propid") == 0) {

		strcpy (propid, col_val);

	        icol = tcol ("pi");
                col_val = tval(icol);
		strcpy (pi, col_val);
                
		printf ("abstract= [%s_%s.txt]\n", propid, pi); 

	    }
	    else if (strcmp(field_name[i], "fov") == 0) {

		strcpy (fov, col_val);
		printf ("fov= [%s]\n", fov); 

                cptr = strstr (fov, "f.o.v.");
                strcpy (fov, cptr+6);
		printf ("fov= [%s]\n", fov); 

	    }
	    else if (strcmp(field_name[i], "exptime") == 0) {
			
		strcpy (exptime, col_val);
		printf ("exptime= [%s]\n", exptime); 

                cptr = strstr (exptime, "sec");
                cptr = '\0';
		printf ("exptime= [%s]\n", exptime); 

	    }
*/
        }

	l++;

    }
    tclose();
	    
    printf ("n= %d\n", l);

    exit (0);
}


