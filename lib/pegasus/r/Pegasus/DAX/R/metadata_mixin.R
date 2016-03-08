MetadataMixin <- function() {
  object <- list(metadata.l=c())
  class(object) <- "MetadataMixin"
  return(object)
}

AddMetadata <- function(metadata.mixin, metadata) {
  if (HasMetadata(metadata.mixin, metadata)) {
    stop(paste("Duplicate metadata:", metadata))
  }
metadata.mixin$metadata.l <- AppendToList(metadata.mixin$metadata.l, metadata)
  return(metadata.mixin)
}

HasMetadata <- function(metadata.mixin, metadata) {
  for (i in metadata.mixin$metadata.l) {
    if (Equals(i, metadata)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

RemoveMetadata <- function(metadata.mixin, metadata) {
  if (length(metadata.mixin$metadata.l) > 0) {
    for (i in 1:length(metadata.mixin$metadata.l)) {
      o <- metadata.mixin$metadata.l[[i]]
      if (Equals(o, metadata)) {
        metadata.mixin$metadata.l[i] <- NULL
        return(metadata.mixin)
      }
    }
  }
  stop(paste("Metadata not found:",metadata))
}

ClearMetadata <- function(metadata.mixin) {
  metadata.mixin$metadata.l <- c()
  return(metadata.mixin)
}

AddMetadataDeclarative <- function(metadata.mixin, key, value) {
  return(AddMetadata(metadata.mixin, MetadataObj(key, value)))
}
