#!/usr/bin/Rscript

# API Documentation: http://pegasus.isi.edu/documentation

if (is.element("dax3", installed.packages()[,1])) {
  require(dax3)
} else {
  stop("The R DAX Generator API is not installed.\nPlease refer to 'https://pegasus.isi.edu/documentation/dax_generator_api.php' on how to install it.")
}

# Reading arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Usage: daxgen.R DAXFILE\n")
}
daxfile <- args[1]

# Create an abstract DAG
workflow <- ADAG("{{name}}")

# TODO Add some jobs to the workflow
#j <- Job(name="myexe")
#a <- File("a")
#b <- File("b")
#c <- File("c")
#j <- AddArguments(j, list("-i",a,"-o",b,"-o",c))
#j <- Uses(j, a, link=DAX3.Link$INPUT)
#j <- Uses(j, b, link=DAX3.Link$OUTPUT, transfer=FALSE, register=FALSE)
#j <- Uses(j, c, link=DAX3.Link$OUTPUT, transfer=FALSE, register=FALSE)
#workflow <- AddJob(workflow, j)

# TODO Add dependencies
#workflow <- Depends(workflow, parent=j, child=k)

# Write the DAX to file
WriteXML(workflow, daxfile)
