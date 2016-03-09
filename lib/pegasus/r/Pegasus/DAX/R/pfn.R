#' A physical file name. Used to provide URLs for files and executables in the DAX-level replica catalog.
#'
#' @details
#' PFNs can be added to \code{File} and \code{Executable}.
#'
#' @examples
#' PFN('http://site.com/path/to/file.txt', 'site')
#' PFN('http://site.com/path/to/file.txt', site='site')
#' PFN('http://site.com/path/to/file.txt')
#'
#' @param url The url of the file
#' @param site The name of the site
#' @return The PFN object with the URL and site
#' @seealso \code{\link{AddPFN}}, \code{\link{File}}, \code{\link{Executable}}
#' @export
PFN <- function(url, site="local") {
  if (!IsDefined(url)) {
    stop(paste("Invalid url", url))
  }
  if (!IsDefined(site)) {
    stop(paste("Invalid site", site))
  }
  object <- list(profile.mixin=ProfileMixin(), url=url, site=site)
  class(object) <- "PFN"
  return (object)
}

Equals.PFN <- function(pfn, other) {
  if (class(other) == "PFN") {
    test <- IsEqual(pfn$url, other$url)
    test <- test && IsEqual(pfn$site, other$site)
    return(test)
  }
  return(FALSE)
}

ToXML.PFN <- function(pfn) {
  e <- Element('pfn', list(url=pfn$url, site=pfn$site))
  for (p in pfn$profile.mixin$profiles) {
    e <- AddChild(pfn, ToXML(p))
  }
  return(e)
}

# ###############################
# Add-in functions for R
# ###############################

#' @rdname AddProfile
#' @method AddProfile PFN
#' @seealso \code{\link{PFN}}
#' @export
AddProfile.PFN <- function(obj, profile) {
  obj$profile.mixin <- AddProfileMixin(obj$profile.mixin, profile)
  return(obj)
}
