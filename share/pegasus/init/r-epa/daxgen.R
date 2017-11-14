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
workflow <- ADAG("R-EPA")

#Input files
site.species <- File("site.species.txt")
site.species <- AddPFN(site.species, PFN("https://www3.epa.gov/caddis/scripts/site.species.txt", "site"))

site.species.oregon <- File("site.species.or.txt")
site.species.oregon <- AddPFN(site.species.oregon, PFN("https://www3.epa.gov/caddis/scripts/site.species.or.txt", "site"))

env.data <- File("env.data.txt")
env.data <- AddPFN(env.data, PFN("https://www3.epa.gov/caddis/scripts/env.data.txt", "site"))

env.data.oregon <- File("env.data.or.txt")
env.data.oregon <- AddPFN(env.data.oregon, PFN("https://www3.epa.gov/caddis/scripts/env.data.or.txt", "site"))

workflow <- AddFile(workflow, site.species)
workflow <- AddFile(workflow, site.species.oregon)
workflow <- AddFile(workflow, env.data)
workflow <- AddFile(workflow, env.data.oregon)

# Executables
e.wrapper = Executable("e.wrapper")
e.setup = Executable("e.setup")
e.WA = Executable("e.WA")
e.cp = Executable("e.cp")
workflow <- AddExecutable(workflow, e.wrapper)
workflow <- AddExecutable(workflow, e.setup)
workflow <- AddExecutable(workflow, e.WA)
workflow <- AddExecutable(workflow, e.cp)

# Transformation for the wrapper
transformation <- Transformation("e.wrapper")
transformation <- Uses(transformation, e.setup)
transformation <- Uses(transformation, e.WA)
transformation <- Uses(transformation, e.cp)
workflow <- AddTransformation(workflow, transformation)

# Job 1
setup <- Job("e.wrapper")
setup <- AddArguments(setup, "e.setup")

# Output files
setup.out <- File("dfmerge.csv")
setup.out1 <- File("dfmerge.oregon.csv")
setup.out2 <- File("taxa.names.csv")

# Specify Inputs
setup <- Uses(setup, site.species, link = DAX3.Link$INPUT)
setup <- Uses(setup, site.species.oregon, link = DAX3.Link$INPUT)
setup <- Uses(setup, env.data, link = DAX3.Link$INPUT)
setup <- Uses(setup, env.data.oregon, link = DAX3.Link$INPUT)

# Specify Outputs
setup <- Uses(setup, setup.out, link = DAX3.Link$OUTPUT, transfer = FALSE)
setup <- Uses(setup, setup.out1, link = DAX3.Link$OUTPUT, transfer = FALSE)
setup <- Uses(setup, setup.out2, link = DAX3.Link$OUTPUT, transfer = FALSE)

workflow <- AddJob(workflow, setup)

# Job 2
weighted.average <- Job("e.wrapper")
weighted.average <- AddArguments(weighted.average, "e.WA")

# Output files
wa.out <- File("weighted.average.csv")

# Specify Inputs
weighted.average <- Uses(weighted.average, setup.out, link = DAX3.Link$INPUT)
weighted.average <- Uses(weighted.average, setup.out2, link = DAX3.Link$INPUT)

# Specify Outputs
weighted.average <- Uses(weighted.average, wa.out, link = DAX3.Link$OUTPUT, transfer = TRUE)

workflow <- AddJob(workflow, weighted.average)

# Job 3
cumulative.percentiles <- Job("e.wrapper")
cumulative.percentiles <- AddArguments(cumulative.percentiles, "e.cp")
p_local_site <- Profile(DAX3.Namespace$HINTS, "execution.site", "local")
cumulative.percentiles <- AddProfile(cumulative.percentiles, p_local_site)

# Output files
cp.out <- File("ACENTRELLA.jpg")
cp.out1 <- File("DIPHETOR.jpg")
cp.out2 <- File("AMELETUS.jpg")
cp.out3 <- File("taxa.names.evaluated.csv")

# Specify Inputs
cumulative.percentiles <- Uses(cumulative.percentiles, setup.out, link = DAX3.Link$INPUT)
cumulative.percentiles <- Uses(cumulative.percentiles, setup.out2, link = DAX3.Link$INPUT)

# Specify Outputs
cumulative.percentiles <- Uses(cumulative.percentiles, cp.out, link = DAX3.Link$OUTPUT, transfer = TRUE)
cumulative.percentiles <- Uses(cumulative.percentiles, cp.out1, link = DAX3.Link$OUTPUT, transfer = TRUE)
cumulative.percentiles <- Uses(cumulative.percentiles, cp.out2, link = DAX3.Link$OUTPUT, transfer = TRUE)
cumulative.percentiles <- Uses(cumulative.percentiles, cp.out3, link = DAX3.Link$OUTPUT, transfer = TRUE)

workflow <- AddJob(workflow, cumulative.percentiles)

# Dependencies
workflow <- Depends(workflow, parent = setup, child = weighted.average)
workflow <- Depends(workflow, parent = setup, child = cumulative.percentiles)

# Generate Split dax
WriteXML(workflow, daxfile)
