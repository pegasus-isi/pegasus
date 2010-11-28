#!/bin/sh

maxsize=1000000                                 # MAX FILE SIZE = 1MB
maxfiles=5                                      #MAX LOG FILES = 5
prefix=`date +%s`
#VDS_LOG_FILE="/var/Pegasus/logging/rls.log"
#VDS_LOG_FILE="./rls.log"


if [ -z "${VDS_LOG_FILE}" ]
    then VDS_LOG_FILE="/var/Pegasus/logging/rls.log"
fi

if [ -z "${JAVA_HOME}" ]
    then echo "JAVA_HOME is not defined"
    exit
fi

if [ ! -e $VDS_LOG_FILE ]
    then echo > ${VDS_LOG_FILE}
    chmod 666 $VDS_LOG_FILE
fi

if [ ! -w "${VDS_LOG_FILE}" ]
    then echo "you do not have write permissions for $VDS_LOG_FILE"
    exit
fi



logdir=`dirname $VDS_LOG_FILE`

logname=`basename $VDS_LOG_FILE`
logsize="`ls -l $VDS_LOG_FILE | awk '{print $5}'`"


    
if [[ $logsize -ge $maxsize ]]                                                    #rotate logs
    then
    numfiles=`ls -l $logdir | grep $logname | wc | awk '{print $1}'`
    if [[ $numfiles -ge $maxfiles ]]                                                 #total log files=5
	then 
	min=9999999999     
	for i in `ls $logdir | grep "$logname\."`; do                           #find oldest file
		
	    time=`echo $i | cut -d "." -f 3`
	    if [[ time -le min ]]
		then
		min=$time
	    fi
	done
	rm "$VDS_LOG_FILE.$min"                                             #remove oldest file
    fi
    mv $VDS_LOG_FILE "$VDS_LOG_FILE.$prefix"                                #archive current log
        echo > $VDS_LOG_FILE                                                    #create new log file  
fi

for f in $@; do
    echo $'\n' >> $VDS_LOG_FILE                                                     # log rls stats
    date >> $VDS_LOG_FILE
    $JAVA_HOME/bin/java org/griphyn/cPlanner/common/StatRLS $f >> $VDS_LOG_FILE
done
#echo done
