#!/bin/bash
set -e
{% if daxgen == "python" or daxgen == "tutorial" %}
export PYTHONPATH=$(pegasus-config --python)
exec python daxgen.py "$@"
{% elif daxgen == "perl" %}
export PERL5LIB=$(pegasus-config --perl)
exec perl daxgen.pl "$@"
{% elif daxgen == "java" %}
CLASSPATH=$(pegasus-config --classpath)
javac -cp $CLASSPATH DAXGen.java
exec java -cp .:$CLASSPATH DAXGen "$@"
{% elif daxgen == "r" %}
exec Rscript daxgen.R "$@"
{% endif %}
