Use <- function(name, link=NULL, register=NULL, transfer=NULL, optional=NULL, namespace=NULL,
                version=NULL, executable=NULL, size=NULL) {

  if (!IsDefined(name)) {
    stop(paste("Invalid name", name))
  }
  if (!IsDefined(executable)) {
    if (class(name) == "Executable") {
      executable <- TRUE
    } else if (class(name) == "File") {
      executable <- FALSE
    }
  }
  object <- list(metadata.mixin=MetadataMixin(), name=name, link=link, optional=optional,
                 register=register, transfer=transfer, namespace=namespace, version=version,
                 executable=executable, size=size)
  class(object) <- "Use"
  return(object)
}

Equals.Use <- function(use, other) {
  if (class(other) == "Use") {
    if (class(use$name) == "File" || class(use$name) == "Executable") {
      pu.name <- use$name$catalog.type$name
    } else {
      pu.name <- use$name
    }
    if (class(other$name) == "File" || class(other$name) == "Executable") {
      po.name <- other$name$catalog.type$name
    } else {
      po.name <- other$name
    }
    test <- IsEqual(pu.name, po.name)
    test <- test && IsEqual(use$namespace, other$namespace)
    test <- test && IsEqual(use$version, other$version)
    return(test)
  }
  return(FALSE)
}

ToTransformationXML <- function(use) {
  if (class(use$name) == "File" || class(use$name) == "Executable") {
    p.name <- use$name$catalog.type$name
  } else {
    p.name <- use$name
  }
  e <- Element('uses', list(
    namespace=use$namespace,
    name=p.name,
    version=use$version,
    executable=use$executable
  ))
  for(m in use$metadata.mixin$metadata.l) {
    e <- AddChild(e, ToXML(m))
  }
  return(e)
}

ToJobXML <- function(use) {
  e <- Element('uses', list(
    namespace=use$namespace,
    name=use$name,
    version=use$version,
    link=use$link,
    register=use$register,
    transfer=use$transfer,
    optional=use$optional,
    executable=use$executable,
    size=use$size
  ))
  for(m in use$metadata.mixin$metadata.l) {
    e <- AddChild(e, ToXML(m))
  }
  return(e)
}
