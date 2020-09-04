#' Manage invocations
#'
#' @return InvokeMixin object with an empty list of invocations
#' @seealso \code{\link{AddInvoke}}, \code{\link{HasInvoke}}, \code{\link{RemoveInvoke}},
#'  \code{\link{ClearInvokes}}, \code{\link{InvokeExecutable}}
InvokeMixin <- function() {
  object <- list(invocations=c())
  class(object) <- "InvokeMixin"
  return(object)
}

#' Add invoke to the InvokeMixin object
#'
#' @param invoke.mixin InvokeMixin object
#' @param invoke invocation to be appended to the list of invocations
#' @return InvokeMixin object with invocation appended to the list of invocations
#' @seealso \code{\link{InvokeMixin}}
AddInvokeMixin <- function(invoke.mixin, invoke) {
  if (HasInvoke(invoke.mixin, invoke)) {
    stop(paste("Duplicate Invoke:", invoke))
  }
  invoke.mixin$invocations <- AppendToList(invoke.mixin$invocations, invoke)
  return(invoke.mixin)
}

#' Test whether an invocation is already appended to the InvokeMixin object.
#'
#' @param invoke.mixin InvokeMixin object
#' @param invoke invocation to be tested
#' @return if the InvokeMixin object has the invocation
#' @seealso \code{\link{InvokeMixin}}
HasInvoke <- function(invoke.mixin, invoke) {
  for (i in invoke.mixin$invocations) {
    if (Equals(i, invoke)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

#' Remove an invocation from the InvokeMixin object
#'
#' @param invoke.mixin InvokeMixin object
#' @param invoke invocation to be removed
#' @return InvokeMixin object without the removed invocation
RemoveInvoke <- function(invoke.mixin, invoke) {
  if (length(invoke.mixin$invocations) > 0) {
    for (i in 1:length(invoke.mixin$invocations)) {
      o <- invoke.mixin$invocations[[i]]
      if (Equals(o, invoke)) {
        invoke.mixin$invocations[i] <- NULL
        return(invoke.mixin)
      }
    }
  }
  stop(paste("Invoke not found:",invoke))
}

#' Remove all Invoke objects
#'
#' @param invoke.mixin InvokeMixin object
#' @return InvokeMixin with no invocations
#' @seealso \code{\link{InvokeMixin}}
ClearInvokes <- function(invoke.mixin) {
  invoke.mixin$invocations <- c()
  return(invoke.mixin)
}

#' Invoke executable \code{what} when job reaches status \code{when}.
#'
#' @details The value of \code{what} should be a command that can be executed on the submit host.
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
#' @param invoke.mixin InvokeMixin object
#' @param when job status
#' @param what executable to be invoked when job reach status \code{when}
#' @return InvokeMixin object with invocation appended to the list of invocations
#' @seealso \code{\link{InvokeMixin}}
InvokeExecutable <- function(invoke.mixin, when, what) {
  return(AddInvokeMixin(invoke.mixin, Invoke(when, what)))
}
