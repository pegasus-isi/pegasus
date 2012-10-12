#ifndef MONTAGE_HASH_LIB
#define MONTAGE_HASH_LIB


/* Hash table structures */

typedef struct HT_list_t_ 
{
   char *key;
   char *val;
   struct HT_list_t_ *next;
}
   HT_list_t;

typedef struct _HT_table_t_
{
   int         size;       /* size of the table           */
   HT_list_t **head;       /* first element of each table */
   HT_list_t **tail;       /* last element of each table  */
   HT_list_t  *subset;     /* "current subset" pointer    */
   char       *subkey;     /* "current subset" key val    */
}
   HT_table_t;


/* Function prototypes */

void          HT_set_debug    (int debugval);
HT_table_t   *HT_create_table (int size);

unsigned int  HT_func         (HT_table_t *hashtable, char *key);
int           HT_add_entry    (HT_table_t *hashtable, char *key, char *val);
int           HT_delete_entry (HT_table_t *hashtable, char *key, char *val);
char         *HT_lookup_key   (HT_table_t *hashtable, char *key);
char         *HT_next_entry   (HT_table_t *hashtable);
int           HT_count_entries(HT_table_t *hashtable);
void          HT_free_table   (HT_table_t *hashtable);


#endif /* MONTAGE_HASH_LIB */
