#!/usr/bin/Rscript
#

# Set up R files for running scripts
# Assumes that tab-delimited data files have been stored in
# working directory.

# Load data into R
site.species <- read.delim("site.species.txt")
site.species.or <- read.delim("site.species.or.txt")
env.data <- read.delim("env.data.txt")
env.data.or <- read.delim("env.data.or.txt")


# Merge biological and environmental data
dfmerge <- merge(site.species, env.data, by = "SITE.ID")
write.csv(dfmerge, file = "dfmerge.csv", row.names = FALSE)
dfmerge.or <- merge(site.species.or, env.data.or, by = "SITE.ID")
write.csv(dfmerge.or, file = "dfmerge.oregon.csv", row.names = FALSE)

# Set taxa.names
taxa.names <- c("ACENTRELLA", "DIPHETOR", "AMELETUS")
write.table(taxa.names, "taxa.names.csv", row.names = FALSE, col.names = FALSE, sep = "")

