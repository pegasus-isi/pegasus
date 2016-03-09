#' Invoke executable \code{what} when job reaches status \code{when}
#'
#' @details
#' The value of \code{what} should be a command that can be executed on the submit host.
#' The list of valid values for 'when' is:
#'
#' \tabular{ll}{
#'  WHEN       \tab  MEANING \cr
#' ==========  \tab  ======================================================= \cr
#' never       \tab  never invoke \cr
#' start       \tab  invoke just before job gets submitted. \cr
#' on_error    \tab  invoke after job finishes with failure (exitcode != 0). \cr
#' on_success  \tab  invoke after job finishes with success (exitcode == 0). \cr
#' at_end      \tab  invoke after job finishes, regardless of exit status. \cr
#' all         \tab  like start and at_end combined. \cr
#' }
#'
#' @examples
#' invoke_1 <- Invoke(DAX3.When$AT_END, '/usr/bin/mail -s "job done" rafsilva@@isi.edu')
#' invoke_2 <- Invoke(DAX3.When$ON_ERROR, '/usr/bin/update_db -failure')
#'
#' @param when Job status
#' @param what Executable to be invoked when job reach status \code{when}
#' @return Invoke object
#' @export
Invoke <- function(when, what) {
  if (!IsDefined(when)) {
    stop(paste("Invalid when:", when))
  }
  if (!IsDefined(what)) {
    stop(paste("Invalid what:", what))
  }
  object <- list(when=when, what=what)
  class(object) <- "Invoke"
  return(object)
}

ToXML.Invoke <- function(invoke) {
  e <- Element("invoke", list("when"=invoke$when))
  e <- Text(e, invoke$what)
  e <- Flatten(e)
  return(e)
}

Equals.Invoke <- function(invoke, other) {
  if (class(other) == "Invoke") {
    return(invoke$when == other$when && invoke$what == other$what)
  }
  return(FALSE)
}
