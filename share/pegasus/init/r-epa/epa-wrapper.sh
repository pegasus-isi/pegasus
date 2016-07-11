#!/bin/bash

{% if daxgen == "tutorial" and tutorial_setup == "osg" %}
module load R
{% endif %}
type Rscript >/dev/null 2>&1 || { echo >&2 "R is not available."; exit 1; }
Rscript "$@"