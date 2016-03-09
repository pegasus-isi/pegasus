PFNMixin <- function() {
  object <- list(pfns=c())
  class(object) <- "PFNMixin"
  return(object)
}

AddPFNMixin <- function(pfn.mixin, pfn) {
  if (HasPFN(pfn.mixin, pfn)) {
    stop(paste("Duplicate PFN:", pfn))
  }
  pfn.mixin$pfns <- AppendToList(pfn.mixin$pfns, pfn)
  return(pfn.mixin)
}

HasPFN <- function(pfn.mixin, pfn) {
  for (i in pfn.mixin$pfns) {
    if (Equals(i, pfn)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

RemovePFN <- function(pfn.mixin, pfn) {
  if (length(pfn.mixin$pfns) > 0) {
    for (i in 1:length(pfn.mixin$pfns)) {
      o <- pfn.mixin$pfns[[i]]
      if (Equals(o, pfn)) {
        pfn.mixin$pfns[i] <- NULL
        return(pfn.mixin)
      }
    }
  }
  stop(paste("PFN not found:",pfn))
}

ClearPFNs <- function(pfn.mixin) {
  pfn.mixin$pfns <- c()
  return(pfn.mixin)
}

AddPFNDeclarative <- function(pfn.mixin, url, site=NULL) {
  return(AddPFNMixin(pfn.mixin, PFN(url, site)))
}
