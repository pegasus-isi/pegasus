/* void TabSort()               Sort table based on possible sexigesimal entry
 * int SortTab()                Return lowest of two entries
 */

/* Sort catalog by increasing angle, returning sorted buffer */

/* Structure for sorting lines */
typedef struct {
    double ang;		/* Selected angle */
    char *entry;	/* Entire entry */
} TabInfo;

char *
SortTab (buffer, isort);

char	*buffer;	/* White-space separated table */
int	isort;		/* Column by which to sort */

{
    TabInfo *table;
    char line[500];
    char token[32];
    char *bufline;
    char *buffout;
    char newline = 10;
    char czero;
    char *cwhite;
    int TabSort ();
    int nlines = 0;
    int i;
    int ntok;
    struct Tokens tokens;   /* Token structure */

    if (buffer == NULL)
	return;

    cwhite = NULL;
    czero = (char) 0;

    /* Count lines in file */
    bufline = buffer;
    nlines = 0;
    while ((bufline = strchr (bufline, newline)) != NULL) {
	bufline = bufline + 1;
	nlines++;
	}

    /* Allocate array for lines of table */
    table = (TabInfo *) calloc ((unsigned int)nlines, sizeof(TabInfo));

   /* Fill out data structure of lines and indices */
    line1 = buffer;
    for (i = 0; i < 500; i++)
	line[i] = czero;
    i = 0;
    while ((line2 = strchr (bufline, newline)) != NULL) {
	nchar = line2 - line1 + 1;
	table[i]->entry = (char *)calloc (1, nchar);
	line = table[i]->entry;
	strncpy (line, line1, nchar);
	line[nchar] = czero;
	ntok = setoken (&tokens, line, cwhite);
	if (getoken(&tokens, itok, token, 32)) {
	    if (strchr (token, ':')
		table[i]->ang = str2dec (token);
	    else
		table[i]->ang = atof (token);
	i++;
	}

    qsort ((char *)table, nlines, sizeof(TabInfo), TabSort);

    /* Allocate output buffer the same size as input buffer */
    lbuff = strlen (buffer) + 1;
    buffout = (char *) calloc (lbuff, 1);
    strcpy (buffout, table[0]->entry);
    for (i = 1; i < nlines; i++)
	strcat (buffout, table[i]->entry);
	free (table[i]->entry);
	}
    free (table);
    return (buffout);
}


/* TabSort -- Order stars in decreasing flux called by qsort */

int
TabSort (ssp1, ssp2)

void *ssp1, *ssp2;

{
    double b1 = ((TabInfo *)ssp1)->ang;
    double b2 = ((TabInfo *)ssp2)->ang;

    if (b2 > b1)
	return (1);
    else if (b2 < b1)
	return (-1);
    else
	return (0);
}

