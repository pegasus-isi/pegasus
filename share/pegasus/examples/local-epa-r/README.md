# R-EPA Workflow

This example is to serve as a starting point to enable Pegasus R workflows.

Run the submit script, to see an example of pegasus planning for this
configuration. 


Requirements
------------------

- Pegasus Version 4.7.0 or higher
- Pegasus R DAX API (`dax3`)


Workflow Description
-------------------

Source: https://www3.epa.gov/caddis/da_software_rscript12.html

All the base R scripts and data sets are obtained from the EPA website above.
The R scripts were modified to generate input and output files instead of local 
objects that the scripts initially relied on. Only `setupvar.R`, `weighted.average.R` 
and `cumulative.percentiles.R` scripts were used in this example.

This example demonstrates the usage of a split workflow. The `setupvar.R` script is 
parses the input files into CSV format that the other scripts will import and use.
Once these CSV files are generated, they are used as inputs to `weighted.average.R` and
`cumulative.percentiles.R` scripts. The scripts run their analyses and generate their
respective CSV files and JPG files in the case of cumulative percentiles script. 
There should be a total of 5 output files at the end of the workflow: 3 JPG files and
2 CSV files.

