#' Test whether an object is not \code{NULL} and not \code{NA}
#'
#' @param x object to be tested
#' @return If the object is not \code{NULL} and not \code{NA}
IsDefined <- function(x) {
  return(!is.null(x) && !is.na(x))
}

#' Append a value to a list
#'
#' @param list List of element to where the value will be appended
#' @param value Value to be added to the list
#' @return List with value appended
AppendToList <- function(list, value) {
  i <- length(list) + 1
  list[[i]] <- value
  return(list)
}

#' Test whether to values are equal
#'
#' @param v1 First value
#' @param v2 Second value
#' @return If the values are equal
IsEqual <- function(v1, v2) {
  if (IsDefined(v1) && IsDefined(v2)) {
    return(v1 == v2)
  } else if (!IsDefined(v1) && !IsDefined(v2)) {
    return(TRUE)
  }
  return(FALSE)
}
