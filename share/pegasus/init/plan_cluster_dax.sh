#!/bin/bash

DIR=$(cd $(dirname $0) && pwd)

if [ $# -ne 1 ]; then
    echo "Usage: $0 DAXFILE"
    exit 1
fi

DAXFILE=$1

# This command tells Pegasus to plan the workflow contained in 
# dax file passed as an argument. The planned workflow will be stored
# in the "submit" directory. The execution # site is "{{site}}".
# --input-dir tells Pegasus where to find workflow input files.
# --output-dir tells Pegasus where to place workflow output files.
pegasus-plan --conf pegasus.properties \
    --dax $DAXFILE \
    --dir $DIR/submit \
    --input-dir $DIR/input \
    --output-dir $DIR/output \
{% if generate_tutorial == true %}
    --cleanup leaf \
{% if tutorial_setup == "usc-hpcc" or tutorial_setup == "wrangler-glite" or tutorial_setup == "titan-glite" or tutorial_setup == "summit-kub-bosco" %}
    --cluster label \
{% else %}
    --cluster horizontal \
{% endif %}
    --force \
{% endif %}
    --sites {{sitename}} \
{% if staging_site is defined %}
    --staging-site {{staging_site}} \
{% endif %}
    --submit

