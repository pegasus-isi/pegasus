#' A file entry for the DAX-level replica catalog, or a reference to a logical file used by the workflow
#'
#' @description
#' All arguments specify the workflow-level behavior of this File. Job-level
#' behavior can be defined when adding the File to a Job's uses. If the
#' properties are not overridden at the job-level, then the workflow-level
#' values are used as defaults.
#'
#' If this LFN is to be used as a job's stdin/stdout/stderr then the value
#' of link is ignored when generating the <std*> tags.
#'
#' @param name File name
#' @return A \code{File} object
#' @seealso \code{\link{AddFile}}, \code{\link{RemoveFile}}
#' @export
File <- function(name) {
  object <- list(catalog.type=CatalogType(name))
  class(object) <- "File"
  return (object)
}

Equals.File <- function(file, other) {
  if (class(other) == "File") {
    return(file$catalog.type$name == other$catalog.type$name)
  }
  return(FALSE)
}

ToArgumentXML <- function(file) {
  return(Element('file', list(name=file$catalog.type$name)))
}

ToStdioXML <- function(file, tag) {
  if (tag == "stdin") {
    link <- "input"
  } else if (is.element(tag, c("stdout", "stderr"))) {
    link <- "output"
  } else {
    stop(paste("invalid tag",tag,"should be one of stdin, stdout, stderr"))
  }
  return(Element(tag, list(name=file$catalog.type$name, link=link)))
}

ToXML.File <- function(file) {
  e <- ToArgumentXML(file)
  e <- InnerXML(file$catalog.type, e)
  return(e)
}

# ###############################
# Add-in functions for R
# ###############################

#' @rdname AddPFN
#' @method AddPFN File
#' @seealso \code{\link{File}}
#' @export
AddPFN.File <- function(obj, pfn) {
  obj$catalog.type$pfn.mixin <- AddPFNMixin(obj$catalog.type$pfn.mixin, pfn)
  return(obj)
}

#' @rdname AddProfile
#' @method AddProfile File
#' @seealso \code{\link{File}}
#' @export
AddProfile.File <- function(obj, profile) {
  obj$catalog.type$profile.mixin <- AddProfileMixin(obj$catalog.type$profile.mixin, profile)
  return(obj)
}


#' @rdname Metadata
#' @method Metadata File
#' @seealso \code{\link{File}}
#' @export
Metadata.File <- function(obj, key, value) {
  obj$catalog.type$metadata.mixin <- AddMetadataDeclarative(obj$catalog.type$metadata.mixin, key, value)
  return(obj)
}
