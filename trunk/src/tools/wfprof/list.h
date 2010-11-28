#ifndef WFPROF_LIST_H
#define WFPROF_LIST_H

#define LIST_PREPEND(head,add)			\
do {						\
   (add)->next = head;				\
   if (head) {					\
       (add)->prev = (head)->prev;		\
       (head)->prev = (add);			\
   } else {					\
       (add)->prev = (add);			\
   }						\
   (head) = (add);				\
} while (0)

#define LIST_APPEND(head,add)			\
do {						\
    if (head) {					\
        (add)->prev = (head)->prev;		\
        (head)->prev->next = (add);		\
        (head)->prev = (add);			\
        (add)->next = NULL;			\
    } else {					\
        (head)=(add);				\
        (head)->prev = (head);			\
        (head)->next = NULL;			\
    }						\
} while (0)

#define LIST_DELETE(head,del)			\
do {						\
    if ((del)->prev == (del)) {			\
        (head)=NULL;				\
    } else if ((del)==(head)) {			\
        (del)->next->prev = (del)->prev;	\
        (head) = (del)->next;			\
    } else {					\
        (del)->prev->next = (del)->next;	\
        if ((del)->next) {			\
            (del)->next->prev = (del)->prev;	\
        } else {				\
            (head)->prev = (del)->prev;		\
        }					\
    }						\
} while (0)


#define LIST_FOREACH(head,el)			\
    for(el=head;el;el=el->next)

#endif /* WFPROF_LIST_H */
