UseMixin <- function() {
  object <- list(used=c())
  class(object) <- "UseMixin"
  return(object)
}

AddUse <- function(use.mixin, use) {
  if (HasUse(use.mixin, use)) {
    stop(paste("Duplicate Use:", use$name))
  }
  use.mixin$used <- AppendToList(use.mixin$used, use)
  return(use.mixin)
}

HasUse <- function(use.mixin, use) {
  for (i in use.mixin$used) {
    if(Equals(i, use)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

RemoveUse <- function(use.mixin, use) {
  if (length(use.mixin$used) > 0) {
    for (i in 1:length(use.mixin$used)) {
      o <- use.mixin$used[[i]]
      if (Equals(o, use)) {
        use.mixin$used[i] <- NULL
        return(use.mixin)
      }
    }
  }
  stop(paste("No such Use",use))
}

ClearUses <- function(use.mixin) {
  use.mixin$used <- c()
  return(use.mixin)
}

UsesMixin <- function(use.mixin, arg, link=NULL, register=NULL, transfer=NULL, optional=NULL, namespace=NULL,
                 version=NULL, executable=NULL, size=NULL) {

  if (class(arg) == "CatalogType") {
    p.name <- arg$name
  } else {
    p.name <- arg
  }

  p.namespace <- NULL
  p.version <- NULL
  p.executable <- NULL

  if (class(arg) == "Executable") {
    p.namespace <- arg$namespace
    p.version <- arg$version
    # We only need to set this for jobs
    # the default is True for Transformations
    if(class(use.mixin) == "AbstractJob") {
      p.executable <- TRUE
    }
  }
  if (class(arg) == "File") {
    # We only need to set this for transformations
    # The default is False for Jobs
    if(class(use.mixin) == "Transformation") {
      p.executable <- FALSE
    }
  }

  if (IsDefined(namespace)) {
    p.namespace <- namespace
  }
  if (IsDefined(version)) {
    p.version <- version
  }
  if (IsDefined(executable)) {
    p.executable <- executable
  }

  use.obj <- Use(p.name, link, register, transfer, optional,
                 p.namespace, p.version, p.executable, size)

  # Copy metadata from File or Executable
  # XXX Maybe we only want this if link!=input
  if (class(arg) == "CatalogType") {
    for (m in arg$metadata.mixin$metadata.l) {
      use.obj$metadata_mixin <- AddMetadata(use.obj$metadata_mixin, m)
    }
  }

  use.mixin <- AddUse(use.mixin, use.obj)
  return(use.mixin)
}

