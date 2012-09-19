#!/bin/bash

cat <<EOF
#ifndef SVN_H
#define SVN_H

EOF

svn info $1 | awk '
    /URL:/ {
        printf("#define SVN_URL \"%s\"\n", $2); 
    }
    /Revision:/ { 
        printf("#define SVN_REVISION \"%s\"\n", $2);
    }
    /Last Changed Date:/ { 
        printf("#define SVN_CHANGED \"");
        for ( i=4; i<NF; i++ )
            printf("%s ", $i);
        printf("%s\"\n", $NF);
    }
'
if [ $? -ne 0 ]; then
    echo "/* Error running 'svn info $1'"
fi

cat <<EOF

#endif /* SVN_H */
EOF
