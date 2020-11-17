Unicode <- function(obj) {
  UseMethod("Unicode", obj)
}

#' Get the XML string for the object
#'
#' @param obj Object to parse as XML
#' @return The XML string for the object
ToXML <- function(obj) {
  UseMethod("ToXML", obj)
}

Equals <- function(o1, o2) {
  UseMethod("Equals", o1)
}

InnerXML <- function(o1, o2) {
  UseMethod("InnerXML", o1)
}

#' Declarative metadata addition
#'
#' @param obj Object to append the metadata
#' @param key The metadata key
#' @param value The metadata value
#' @return The object containing the metadata
#' @seealso \code{\link{Metadata}}
#' @export
Metadata <- function(obj, key, value) {
  UseMethod("Metadata", obj)
}

#' Add a PFN to the object
#'
#' @param obj Object to append the PFN
#' @param pfn The PFN
#' @return The object containing the PFN
#' @seealso \code{\link{PFN}}
#' @export
AddPFN <- function(obj, pfn) {
  UseMethod("AddPFN", obj)
}

#' Add a profile to the object
#'
#' @param obj Object to append the profile
#' @param profile The profile
#' @return The object containing the profile
#' @seealso \code{\link{Profile}}
#' @export
AddProfile <- function(obj, profile) {
  UseMethod("AddProfile", obj)
}

#' Add an invoke to the object
#'
#' @param obj Object to append the invoke
#' @param invoke The invocation
#' @return The object containing the invoke
#' @seealso \code{\link{Invoke}}
#' @export
AddInvoke <- function(obj, invoke) {
  UseMethod("AddInvoke", obj)
}
