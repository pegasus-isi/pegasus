MetadataObj <- function(key, value) {
  if (!IsDefined(key)) {
    stop(paste("Invalid key", key))
  }
  if (!IsDefined(value)) {
    stop(paste("Invalid value", value))
  }
  object <- list(key=key, value=value)
  class(object) <- "Metadata"
  return (object)
}

Equals.Metadata <- function(metadata, other) {
  if (class(other) == "Metadata") {
    return(metadata$key == other$key)
  }
  return(FALSE)
}

ToXML.Metadata <- function(metadata) {
  m <- Element('metadata', list(key=metadata$key))
  m <- Text(m, metadata$value)
  m <- Flatten(m)
  return(m)
}
