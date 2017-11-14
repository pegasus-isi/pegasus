#!/usr/bin/Rscript
#

dfmerge <- read.csv("dfmerge.csv")

taxa.names <- read.csv("taxa.names.csv", header = F)$V1
taxa.names <- as.character(taxa.names)


# Script to compute WA tolerance values

WA <- rep(NA, times = length(taxa.names))
                                        # Define a WA to be vector of 
					# length the same as the 
					# number of taxa of interest

for (i in 1:length(taxa.names)) {
  WA[i] <- sum(dfmerge[,taxa.names[i]]*dfmerge$temp)/
    sum(dfmerge[,taxa.names[i]])	
}
names(WA) <- taxa.names

write.table(WA, "weighted.average.csv", col.names = FALSE, sep = "")
