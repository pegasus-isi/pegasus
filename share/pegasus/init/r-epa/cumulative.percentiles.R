#!/usr/bin/Rscript
#

dfmerge <- read.csv("dfmerge.csv")

taxa.names <- read.csv("taxa.names.csv", header = F)$V1
taxa.names <- as.character(taxa.names)

# Script to compute cumulative percentiles
CP <- rep(NA, times = length(taxa.names)) 
# Define a storage vector for the cumulative percentile

dftemp <- dfmerge[order(dfmerge$temp), ]
                                        # Sort sites by the value 
                                        #   of the environmental 
                                        #   variable


cutoff <- 0.75
                                        # Select a cutoff percentile

par(mfrow = c(1,3), pty = "s")
                                        # Specify three plots per page
for (i in 1:length(taxa.names)) {
	csum <- cumsum(dftemp[, taxa.names[i]])/
	sum(dftemp[,taxa.names[i]])
                                        # Compute cumulative sum
                                        # of abundances
	jpeg(paste(taxa.names[i], ".jpg", sep = ""))
	plot(dftemp$temp, csum, type = "l", xlab = "Temperature", 
		ylab = "Proportion of total", main = taxa.names[i]) 
                                        # Make plots like Figure 5
	dev.off()
	ic <- 1
	while (csum[ic] < 0.75) ic <- ic + 1	
                                        # Search for point at which
                    
	                    #   cumulative sum is 0.75
	CP[i] <- dftemp$temp[ic]
                                        # Save the temperature that
                                        #   corresponds to this 
                                        #   percentile.
      }
names(CP) <- taxa.names
write.table(CP, "taxa.names.evaluated.csv", col.names = FALSE, sep = "")

