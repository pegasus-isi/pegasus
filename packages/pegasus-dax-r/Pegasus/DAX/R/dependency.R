#' A dependency between two nodes in the ADAG
#'
#' @param parent The parent job/dax/dag or id
#' @param child The child job/dax/dag or id
#' @param edge.label A label for the edge (optional)
#' @return Dependency object between parent and child
#' @export
Dependency <- function(parent, child, edge.label=NULL) {

  if (class(parent) == "Job" || class(parent) == "DAG" || class(parent) == "DAX") {
    if (!IsDefined(parent$abstract.job$id)) {
      stop(paste("Parent job has no id:", parent))
    } else {
      p.parent <- parent$abstract.job$id
    }
  } else if (IsDefined(parent)) {
    p.parent <- parent
  } else {
    stop(paste("Invalid parent:", parent))
  }

  if (class(child) == "Job" || class(child) == "DAG" || class(child) == "DAX") {
    if (!IsDefined(child$abstract.job$id)) {
      stop(paste("Child job has no id:", child))
    } else {
      p.child <- child$abstract.job$id
    }
  } else if (IsDefined(child)) {
    p.child <- child
  } else {
    stop(paste("Invalid child:", child))
  }

  if (p.parent == p.child) {
    stop(paste("No self edges allowed:", p.parent, p.child))
  }

  object <- list(parent=p.parent, child=p.child, edge.label=edge.label)
  class(object) <- "Dependency"
  return(object)
}

Equals.Dependency <- function(dependency, other) {
  if (class(other) == "Dependency") {
    test <- IsEqual(dependency$parent, other$parent)
    test <- test && IsEqual(dependency$child, other$child)
    return(test)
  }
  return(FALSE)
}
