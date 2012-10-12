#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hashtable.h>

char *strdup(const char *s1);

int HT_debug = 0;

/********************************************************/
/*                                                      */
/* Hash table routines.  Stores keyword/value pairs and */
/* looks entries up by keywork.  Multiple instances of  */
/* the same keyword are permitted.                      */
/*                                                      */
/********************************************************/

void HT_set_debug(int debugval)
{
   HT_debug = debugval;
}


/****************/
/* CREATE TABLE */
/****************/

HT_table_t *HT_create_table(int size)
{
   int i;
   HT_table_t *new_table;

   if (size < 1)
      return NULL;


   /* Allocate memory for the table structure */

   new_table = (HT_table_t *)malloc(sizeof(HT_table_t));

   if (new_table == (HT_table_t *)NULL)
      return (HT_table_t *)NULL;


   /* Allocate memory for the table itself */

   new_table->head = (HT_list_t **)malloc(sizeof(HT_list_t *) * size);

   if(new_table->head == (HT_list_t **)NULL)
      return (HT_table_t *)NULL;

   new_table->tail = (HT_list_t **)malloc(sizeof(HT_list_t *) * size);

   if(new_table->head == (HT_list_t **)NULL)
      return (HT_table_t *)NULL;


   /* Initialize the elements of the table */

   for(i=0; i<size; ++i) 
   {
      new_table->head[i] = (HT_list_t *)NULL;
      new_table->tail[i] = (HT_list_t *)NULL;
   }

   new_table->subset = (HT_list_t *)NULL;
   new_table->subkey = (char      *)NULL;


   /* Remember the table size */

   new_table->size = size;

   return new_table;
}


/*****************/
/* HASH FUNCTION */
/*****************/

unsigned int HT_func(HT_table_t *hashtable, char *key)
{
   unsigned int hashval;

   /* for each character, we multiply the old hash by 31 and add the current */
   /* character.  Remember that shifting a number left is equivalent to      */
   /* multiplying it by 2 raised to the number of places shifted.  So we     */
   /* are in effect multiplying hashval by 32 and then subtracting hashval.  */
   /* Why do we do this?  Because shifting and subtraction are much more     */
   /* efficient operations than multiplication.                              */

   hashval = 0;

   while(1)
   {
      if(*key == '\0')
	 break;

      hashval = *key + (hashval << 5) - hashval;

      ++key;
   }


   /* Make the hash value fit into the necessary range */

   hashval =hashval % hashtable->size;

   return hashval;
}


/*************/
/* ADD ENTRY */
/*************/

int HT_add_entry(HT_table_t *hashtable, char *key, char *val)
{
   HT_list_t *new_list;
   HT_list_t *tail;
   unsigned int hashval;

   hashval = HT_func(hashtable, key);

   if(HT_debug)
   {
      printf("DEBUG> add_entry():  \"%s\" = \"%s\" -> hash %d\n",
	 key, val, hashval);
      fflush(stdout);
   }

   /* Attempt to allocate memory for list entry */

   new_list = (HT_list_t *)malloc(sizeof(HT_list_t));

   if (new_list == (HT_list_t *)NULL)
      return 1;


   /* Load new entry and insert it into */
   /* linked list for this hash index   */

   new_list->key  = strdup(key);
   new_list->val  = strdup(val);

   new_list->next = (HT_list_t *)NULL;

   tail = hashtable->tail[hashval];

   if(tail)
      tail->next = new_list;

   hashtable->tail[hashval] = new_list;

   if(hashtable->head[hashval] == (HT_list_t *)NULL)
      hashtable->head[hashval] = new_list;

   return 0;
}


/****************/
/* DELETE ENTRY */
/****************/

int HT_delete_entry(HT_table_t *hashtable, char *key, char *val)
{
   HT_list_t *list, *prev;
   unsigned int hashval;

   hashval = HT_func(hashtable, key);


   /* Find the entry in the table, keeping track of */
   /* the list item that points to it               */

   prev = (HT_list_t *)NULL;
   list = hashtable->head[hashval];

   while(1)
   {
      if(list == (HT_list_t *)NULL
      || (strcmp(key, list->key) == 0 && strcmp(val, list->val) == 0))
	 break;

      prev = list;
      list = list->next;
   }


   /* Entry does not exist in table */

   if (list == (HT_list_t *)NULL)
      return 1;
      

   /* Otherwise, remove it from the table */

   if (prev == (HT_list_t *)NULL)
      hashtable->head[hashval] = list->next;
   else
      prev->next = list->next; 

   if(list->next == (HT_list_t *)NULL)
      hashtable->tail[hashval] = list->next;


   /* free the memory associate with it */

   if(list->key)
   {
      free(list->key);

      list->key = (char *)NULL;
   }

   if(list->val)
   {
      free(list->val);

      list->val = (char *)NULL;
   }

   if(list)
   {
      free(list);

      list = (HT_list_t *)NULL;
   }

   return 0;
}


/******************/
/* LOOK UP By KEY */
/******************/

char *HT_lookup_key(HT_table_t *hashtable, char *key)
{
   unsigned int hashval;

   if(hashtable->subkey)
   {
      free(hashtable->subkey);

      hashtable->subkey = (char *)NULL;
   }
   
   hashtable->subkey = malloc(strlen(key)+1);

   strcpy(hashtable->subkey, key);
   
   hashval = HT_func(hashtable, key);

   if(HT_debug)
   {
      printf("DEBUG> lookup_key(): key \"%s\" -> hashval = %d\n",
	 key, hashval);
      fflush(stdout);
   }

   hashtable->subset = hashtable->head[hashval];

   while(hashtable->subset != (HT_list_t *)NULL
   &&    strcmp(hashtable->subkey, hashtable->subset->key) != 0)
      hashtable->subset = hashtable->subset->next;
   
   if(!hashtable->subset)
      return (char *)NULL;
   else
      return hashtable->subset->val;
}


/******************/
/* GET NEXT ENTRY */
/******************/

char *HT_next_entry(HT_table_t *hashtable)
{
   if(hashtable->subset != (HT_list_t *)NULL)
      hashtable->subset = hashtable->subset->next;

   while(hashtable->subset != (HT_list_t *)NULL
   &&    strcmp(hashtable->subkey, hashtable->subset->key) != 0)
      hashtable->subset = hashtable->subset->next;
   
   if(!hashtable->subset)
      return (char *)NULL;
   else
      return hashtable->subset->val;
}


/*****************/
/* COUNT ENTRIES */
/*****************/

int HT_count_entries(HT_table_t *hashtable)
{
   int i, j, count;
   HT_list_t *list;

   count = 0;


   /* Error check to make sure hashtable exists */

   if (hashtable == (HT_table_t *)NULL)
      return -1;


   /* Go through the indices and count all list elements in each index */

   for(i=0; i<hashtable->size; ++i)
   {
      j = 0;

      for(list=hashtable->head[i]; list != NULL; list = list->next)
      {
	 if(HT_debug)
	 {
	    printf("DEBUG> count_entries(): hash %d / entry %d -> \"%s\" = \"%s\"\n",
	       i, j, list->key, list->val);
	    fflush(stdout);
	 }

	 ++count;
	 ++j;
      }
   }

   return count;
}


/**************/
/* FREE TABLE */
/**************/

void HT_free_table(HT_table_t *hashtable)
{
   int i;
   HT_list_t *list, *temp;

   if (hashtable == (HT_table_t *)NULL) 
      return;


   /* Free the memory for every item in the table, */
   /* including the key/val strings                */

   for(i=0; i<hashtable->size; i++) 
   {
      list = hashtable->head[i];

      while(list!=NULL) 
      {
	 temp = list;
	 list = list->next;

	 if(temp->key)
	 {
	    free(temp->key);

	    temp->key = (char *)NULL;
	 }

	 if(temp->val)
	 {
	    free(temp->val);

	    temp->val = (char *)NULL;
	 }

	 if(temp)
	 {
	    free(temp);

	    temp = (HT_list_t *)NULL;
	 }
      }
   }


   /* Free the table itself */

   if(hashtable->head)
   {
      free(hashtable->head);

      hashtable->head = (HT_list_t **)NULL;
   }

   if(hashtable->tail) 
   {
      free(hashtable->tail);

      hashtable->tail = (HT_list_t **)NULL;
   }

   if(hashtable->subkey) 
   {
      free(hashtable->subkey);

      hashtable->subkey = (char *)NULL;
   }

   if(hashtable)       
   {
      free(hashtable);

      hashtable = (HT_table_t *)NULL;
   }

   return;
}
