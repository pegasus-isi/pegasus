#!/bin/bash
set -e
{% if (daxgen == "python" or daxgen == "tutorial") and tutorial != "r-epa" %}
export PYTHONPATH=$(pegasus-config --python)
exec python daxgen.py "$@"
{% elif daxgen == "perl" %}
export PERL5LIB=$(pegasus-config --perl)
exec perl daxgen.pl "$@"
{% elif daxgen == "java" %}
CLASSPATH=$(pegasus-config --classpath)
javac -cp $CLASSPATH DAXGen.java
exec java -cp .:$CLASSPATH DAXGen "$@"
{% elif daxgen == "r" or tutorial == "r-epa" %}
type Rscript >/dev/null 2>&1 || { echo >&2 "R is not available."; exit 1; }
exec Rscript daxgen.R "$@"
{% endif %}
