#' A logical transformation
#'
#' @description
#' A logical transformation. This is basically defining one or more
#' entries in the transformation catalog. You can think of it like a macro
#' for adding <uses> to your jobs. You can define a transformation that
#' uses several files and/or executables, and refer to it when creating
#' a job. If you do, then all of the uses defined for that transformation
#' will be copied to the job during planning.
#'
#' This code:\cr
#' \code{in <- File("input.txt")}\cr
#' \code{exe <- Executable("exe")}\cr
#' \code{t <- Transformation(namespace="foo", name="bar", version="baz")}\cr
#' \code{t <- Uses(t, in)}\cr
#' \code{t <- Uses(t, exe)}\cr
#' \code{j <- Job(t)}
#'
#' is equivalent to:\cr
#' \code{in <- File("input.txt")}\cr
#' \code{exe <- Executable("exe")}\cr
#' \code{j <- Job(namespace="foo", name="bar", version="baz")}\cr
#' \code{j <- Uses(j, in)}\cr
#' \code{j <- Uses(j, exe)}\cr
#'
#' @details
#' The name argument can be either a string or an \code{Executable} object.
#' If it is an \code{Executable} object, then the \code{Transformation} inherits
#' its name, namespace and version from the \code{Executable}, and the
#' \code{Transformation} is set to use the Executable with \code{link=input},
#' \code{transfer=TRUE}, and \code{register=FALSE}.
#'
#' @examples
#' Transformation(name='mDiff')
#' Transformation(namespace='montage',name='mDiff')
#' Transformation(namespace='montage',name='mDiff',version='3.0')
#'
#' # Using one executable:
#'   mProjectPP <- Executable(namespace="montage", name="mProjectPP", version="3.0")
#'   x_mProjectPP <- Transformation(mProjectPP)
#'
#' # Using several executables:
#'   mDiff <- Executable(namespace="montage", name="mProjectPP", version="3.0")
#'   mFitplane <- Executable(namespace="montage", name="mFitplane", version="3.0")
#'   mDiffFit <- Executable(namespace="montage", name="mDiffFit", version="3.0")
#'   x_mDiffFit <- Transformation(mDiffFit)
#'   x_mDiffFit <- Uses(x_mDiffFit, mDiff)
#'   x_mDiffFit <- Uses(x_mDiffFit, mFitplane)
#'
#' # Config files too:
#'   conf <- File("jbsim.conf")
#'   jbsim <- Executable(namespace="scec",name="jbsim")
#'   x_jbsim <- Transformation(jbsim)
#'   x_jbsim <- Uses(x_jbsim, conf)
#'
#' @param name The name of the transformation
#' @param namespace The namespace of the xform (optional)
#' @param version The version of the xform (optional)
#' @return Transformation object
#' @export
Transformation <- function(name, namespace=NULL, version=NULL) {

  p.name <- NULL
  p.namespace <- NULL
  p.version <- NULL

  if (class(name) == "Executable") {
    p.name <- name$catalog.type$name
    p.namespace <- name$namespace
    p.version <- name$version
  } else {
    p.name <- name
  }

  if (IsDefined(namespace)) {
    p.namespace <- namespace
  }
  if (IsDefined(version)) {
    p.version <- version
  }

  object <- list(use.mixin=UseMixin(), invoke.mixin=InvokeMixin(), metadata.mixin=MetadataMixin(),
                 name=p.name, namespace=p.namespace, version=p.version)
  class(object) <- "Transformation"
  return(object)
}

Equals.Transformation <- function(transformation, other) {
  if (class(other) == "Transformation") {
    if (class(transformation$name) == "File") {
      pt.name <- transformation$name$catalog.type$name
    } else {
      pt.name <- transformation$name
    }
    if (class(other$name) == "File") {
      po.name <- other$name$catalog.type$name
    } else {
      po.name <- other$name
    }
    test <- IsEqual(transformation$namespace, other$namespace)
    test <- test && IsEqual(pt.name, po.name)
    test <- test && IsEqual(transformation$version, other$version)
    return(test)
  }
  return(FALSE)
}

#'
#' @rdname ToXML
#' @method ToXML Transformation
#' @export
ToXML.Transformation <- function(obj) {
  e <- Element('transformation', list(
    namespace=obj$namespace,
    name=obj$name,
    version=obj$version
  ))

  # Metadata
  for (m in obj$metadata.mixin$metadata.l) {
    e <- AddChild(e, ToXML(m))
  }

  # Uses
  um <- obj$use.mixin
  sorted.used <- um$used[order(unlist(lapply(um$used, function(x) suppressWarnings(if (IsDefined(x$link)) {as.integer(x$link)} else {0}))))]
  for (u in sorted.used) {
    e <- AddChild(e, ToTransformationXML(u))
  }

  # Invocations
  for (inv in obj$invoke.mixin$invocations) {
    e <- AddChild(e, ToXML(inv))
  }

  return(e)
}

# ###############################
# Add-in functions for R
# ###############################

#' @rdname AddInvoke
#' @method AddInvoke Transformation
#' @seealso \code{\link{Transformation}}
#' @export
AddInvoke.Transformation <- function(obj, invoke) {
  obj$invoke.mixin <- AddInvokeMixin(obj$invoke.mixin, invoke)
  return(obj)
}

#' @rdname Metadata
#' @method Metadata Transformation
#' @seealso \code{\link{Transformation}}
#' @export
Metadata.Transformation <- function(obj, key, value) {
  obj$metadata.mixin <- AddMetadataDeclarative(obj$metadata.mixin, key, value)
  return(obj)
}
