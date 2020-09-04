#' A Profile captures scheduler-, system-, and environment-specific parameters in a uniform fashion
#'
#' @description
#' A Profile captures scheduler-, system-, and environment-specific
#' parameters in a uniform fashion. Each profile declaration assigns
#' a value to a key within a namespace.
#'
#' Profiles can be added to \code{\link{Job}}, \code{\link{DAX}},
#' \code{\link{DAG}}, \code{\link{File}}, \code{\link{Executable}}, and \code{\link{PFN}}.
#'
#' @examples
#' path <- Profile(Pegasus.Namespace$ENV, 'PATH', '/bin')
#' vanilla <- Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla')
#' path <- Profile(namespace='env', key='PATH', value='/bin')
#' path <- Profile('env', 'PATH', '/bin')
#'
#' @param namespace The namespace of the profile
#' @param key The key name. Can be anything that responds to as.character()
#' @param value The value for the profile. Can be anything that responds to as.character()
#' @seealso \code{\link{Pegasus.Namespace}}
#' @return Profile object with the defined key=value pair
#' @export
Profile <- function(namespace, key, value) {
  object <- list(namespace=namespace, key=key, value=value)
  class(object) <- "Profile"
  return(object)
}

Equals.Profile <- function(profile, other) {
  if (class(other) == "Profile") {
    return(profile$namespace == other$namespace && profile$key == other$key)
  }
  return(FALSE)
}

ToXML.Profile <- function(profile) {
  p <- Element('profile', list(namespace=profile$namespace, key=profile$key))
  p <- Text(p, profile$value)
  p <- Flatten(p)
  return(p)
}
