	case DTLST:
	case DTGST:
	case DTMST:
	    if (datestring != NULL) {
		if (strchr (datestring,'/'))
		    oldfits = 1;
		else
		    oldfits = 0;
		if (timestring != NULL) {
		    if (oldfits) {
			lfd = strlen (datestring) + strlen (timestring) + 2;
			stdate = (char *) calloc (1, lfd);
			strcpy (stdate, datestring);
			strcat (stdate, "_");
			strcat (stdate, timestring);
			}
		    else {
			lfd = strlen (datestring) + strlen (timestring) + 2;
			stdate = (char *) calloc (1, lfd);
			strcpy (stdate, datestring);
			strcat (stdate, "S");
			strcat (stdate, timestring);
			}
		    }
		else
		    stdate = datestring;

		if (verbose)
		    printf ("%s -> ", stdate);

		/* Convert Sidereal Time to UT */
		fitsdate = lst2fd (stdate);
		if (verbose)
		    printf ("%s -> ", fitsdate);

		/* Convert to desired output format */
		switch (outtype) {
		    case DTEP:
			epoch = fd2ep (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2ep (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTEPB:
			epoch = fd2epb (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2epb (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTEPJ:
			epoch = fd2epj (fitsdate);
			if (oldfits && timestring) {
			    epoch1 = fd2epj (timestring);
			    epoch = epoch + epoch1;
			    }
			printf (outform, epoch);
			break;
		    case DTVIG:
			fd2dt (fitsdate, &vdate, &vtime);
			if (outtime == ET) dt2et (&vdate, &vtime);
			else if (outtime == GST) dt2gst (&vdate, &vtime);
			else if (outtime == MST) dt2mst (&vdate, &vtime);
			else if (outtime == LST) dt2lst (&vdate, &vtime);
			if (oldfits) {
			    if (timestring == NULL)
				vtime = 0.0;
			    else
				vtime = str2dec (timestring);
			    }
			if (dateonly)
			    printf ("%9.4f\n", vdate);
			else if (vdate == 0.0 || timeonly)
			    printf ("%10.7f\n", vtime);
			else
			    printf ("%9.4f %10.7f\n", vdate, vtime);
			break;
		    case DTFITS:
			newfdate = fd2fd (fitsdate);
			if (oldfits && timestring) {
			    strcat (newfdate, "T");
			    strcat (newfdate, timestring);
			    }
			strncpy (fyear, newfdate, 10);
			fyear[10] = (char) 0;
			if (outtime == ET) newfdate = fd2et (newfdate);
			else if (outtime == GST) newfdate = fd2gst (newfdate);
			else if (outtime == MST) newfdate = fd2mst (newfdate);
			else if (outtime == LST) newfdate = fd2lst (newfdate);
			tchar = strchr (newfdate, 'T');
			if (tchar == NULL)
			    tchar = strchr (newfdate, 'S');
			if (dateonly) {
			    strncpy (fyear, newfdate, 10);
			    printf ("%s\n", fyear);
			    }
			else if (timeonly) {
			    if (tchar != NULL)
				printf ("%s\n", tchar+1);
			    else
				printf ("%s\n", newfdate);
			    }
			else if (outtime==GST || outtime==MST || outtime==LST) {
			    printf ("%10sS%s\n", fyear, newfdate);
			    }
			else {
			    if (outtime == ET)
				*tchar = 'E';
			    printf ("%s\n", newfdate);
			    }
			break;
		    case DTJD:
			jd = fd2jd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			if (outtime == ET) jd = jd2jed (jd);
			printf (outform, jd);
			break;
		    case DTMJD:
			jd = fd2mjd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			printf (outform, jd);
			break;
		    case DTHJD:
			jd = fd2jd (fitsdate);
			if (oldfits && timestring) {
			    jd1 = fd2jd (timestring);
			    jd = jd + jd1;
			    }
			jd = jd2hjd (jd, ra, dec, coorsys);
			printf (outform, jd);
			break;
		    case DTOF:
			newfdate = fd2of (fitsdate);
			if (oldfits && timestring) {
			    free (newfdate);
			    newfdate = fd2ofd (fitsdate);
			    strcat (newfdate, " ");
			    strcat (newfdate, timestring);
			    }
			printf ("%s\n", newfdate);
			break;
		    case DTOFD:
			newfdate = fd2ofd (fitsdate);
			printf ("%s\n", newfdate);
			break;
		    case DTOFT:
			newfdate = fd2oft (fitsdate);
			if (oldfits && timestring) {
			    strcpy (newfdate, timestring);
			    }
			printf ("%s\n", newfdate);
			break;
		    case DT1950:
			ts = fd2ts (fitsdate);
			if (oldfits && timestring) {
			    ts1 = fd2ts (timestring);
			    ts = ts + ts1;
			    }
			if (outtime == ET) ts = ts2ets (ts);
			printf (outform, ts);
			break;
		    case DTIRAF:
			its = fd2tsi (fitsdate);
			if (oldfits && timestring) {
			    its1 = (int) fd2ts (timestring);
			    its = its + its1;
			    }
			printf (outform, its);
			break;
		    case DTUNIX:
			lts = fd2tsu (fitsdate);
			if (oldfits && timestring) {
			    its1 = (int) fd2ts (timestring);
			    lts = lts + (time_t) its1;
			    }
			printf (outform, lts);
			break;
		    case DTDOY:
			fd2doy (fitsdate, &year, &doy);
			if (oldfits && timestring) {
			    ts1 = (int) fd2ts (timestring);
			    doy = doy + (ts1 / 86400.0);
			    }
			sprintf (temp, outform, doy);
			printf ("%04d %s\n", year, temp);
			break;
		    default:
			printf ("*** Unknown output type %d\n", outtype);
		    }
		}
	    break;
