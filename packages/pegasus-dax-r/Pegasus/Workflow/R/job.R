#' This class defines the specifics of a job to run in an abstract manner
#'
#' @description
#' All filename references still refer to logical files. All references
#' transformations also refer to logical transformations, though
#' physical location hints can be passed through profiles.
#'
#' @details
#' The ID for each job should be unique in the DAX. If it is None, then
#' it will be automatically generated when the job is added to the DAX.
#'
#' The name, namespace, and version should match what you have in your
#' transformation catalog. For example, if namespace="foo" name="bar"
#' and version="1.0", then the transformation catalog should have an
#' entry for "foo::bar:1.0".
#'
#' The name argument can be either a string, or a Transformation object. If
#' it is a Transformation object, then the job will inherit the name, namespace,
#' and version from the Transformation.
#'
#' @examples
#' sleep <- Job(id="ID0001", name="sleep")
#' jbsim <- Job(id="ID0002", name="jbsim", namespace="cybershake", version="2.1")
#' merge <- Job("jbsim")
#'
#' # You can create a Job based on a Transformation:
#'   mDiff_xform <- Transformation("mDiff", namespace="montage", version="3.0")
#'   mDiff_job <- Job(mDiff_xform)
#'
#' # Or an Executable:
#'   mDiff_exe <- Executable("mDiff", namespace="montage", version="3.0")
#'   mDiff_job <- Job(mDiff_exe)
#'
#' # Several arguments can be added at the same time:
#'   input <- File("i1.txt")
#'   output <- File("o1.txt")
#'   mDiff_job <- AddArguments(mDiff_job, list("-i", input, "-o", output))
#'
#' # Profiles are added similarly:
#'   mDiff_job <- AddProfile(mDiff_job, Profile(Pegasus.Namespace$ENV, key='PATH', value='/bin'))
#'
#' # Adding file uses is simple, and you can override global File attributes:
#'   mDiff_job <- Uses(mDiff_job, input, Pegasus.Link$INPUT)
#'   mDiff_job <- Uses(mDiff_job, output, Pegasus.Link$OUTPUT, transfer=TRUE, register=TRUE)
#'
#' @param name The transformation name or Transformation object (required)
#' @param id A unique identifier for the job (optional)
#' @param namespace The namespace of the transformation (optional)
#' @param version The transformation version (optional)
#' @param node.label The label for this job to use in graphing (optional)
#' @return The job object
#' @seealso \code{\link{AddJob}}, \code{\link{Transformation}}, \code{\link{Executable}},
#'   \code{\link{File}}, \code{\link{Profile}}
#' @export
Job <- function(name, id=NULL, namespace=NULL, version=NULL, node.label=NULL) {

  p.namespace <- NULL
  p.version <- NULL

  if (class(name) == "Transformation") {
    p.name <- name$name
    p.namespace <- name$namespace
    p.version <- name$version

  } else if(class(name) == "Executable") {
    p.name <- name$catalog.type$name
    p.namespace <- name$namespace
    p.version <- name$version

  } else if(is.character(name)) {
    p.name <- name

  } else {
    stop("Name must be a string, Transformation or Executable")
  }

  abstract.job <- AbstractJob(id=id, node.label=node.label)
  if (IsDefined(namespace)) {
    p.namespace <- namespace
  }
  if (IsDefined(version)) {
    p.version <- version
  }

  object <- list(abstract.job=abstract.job, name=p.name, namespace=p.namespace, version=p.version)
  class(object) <- "Job"
  return (object)
}

Unicode.Job <- function(job) {
  return(paste("<Job ", job$abstract.job$id, " ", job$namespace, "::",
               job$name, ":", job$version, ">", sep=""))
}

ToXML.Job <- function(job) {
  e <- Element('job', list(
    id=job$abstract.job$id,
    namespace=job$namespace,
    name=job$name,
    version=job$version,
    `node-label`=job$abstract.job$node.label
  ))
  e <- InnerXML(job$abstract.job, e)

  return(e)
}

# ###############################
# Add-in functions for R
# ###############################

Equals.Job <- function(job, other) {
  if (class(other) == "Job") {
    test <- IsEqual(job$name, other$name)
    test <- test && IsEqual(job$namespace, other$namespace)
    test <- test && IsEqual(job$version, other$version)
    if (!test) {
      return(FALSE)
    }
    return(Equals(job$abstract.job, other$abstract.job))
  }
  return(FALSE)
}

#' @rdname AddProfile
#' @method AddProfile Job
#' @seealso \code{\link{Job}}
#' @export
AddProfile.Job <- function(obj, profile) {
  obj$abstract.job$profile.mixin <- AddProfileMixin(obj$abstract.job$profile.mixin, profile)
  return(obj)
}

#' @rdname AddInvoke
#' @method AddInvoke Job
#' @seealso \code{\link{Job}}
#' @export
AddInvoke.Job <- function(obj, invoke) {
  obj$abstract.job$invoke.mixin <- AddInvokeMixin(obj$abstract.job$invoke.mixin, invoke)
  return(obj)
}

#' @rdname Metadata
#' @method Metadata Job
#' @seealso \code{\link{Job}}
#' @export
Metadata.Job <- function(obj, key, value) {
  obj$abstract.job$metadata.mixin <- AddMetadataDeclarative(obj$abstract.job$metadata.mixin, key, value)
  return(obj)
}

#' Add one or more arguments to the job
#'
#' @description
#' Add one or more arguments to the job (this will add whitespace).
#'
#' @param job Job object
#' @param arguments List of arguments defined as \code{list()}
#' @return Job with appended list of arguments
#' @seealso \code{\link{Job}}
#' @export
AddArguments <- function(job, arguments) {
   job$abstract.job <- AddArgs(job$abstract.job, arguments)
   return(job)
}

#' Removes all arguments from the job
#'
#' @param job Job object
#' @return Job with no arguments
#' @seealso \code{\link{Job}}
#' @export
ClearArguments <- function(job) {
  job$abstract.job <- ClearArgs(job$abstract.job)
  return(job)
}

#' Use of a logical file name
#'
#' @description
#' Use of a logical file name. Used for referencing files in the DAX.
#'
#' @details
#' For Use objects that are added to Transformations, the attributes 'link', 'register',
#' 'transfer', 'optional' and 'size' are ignored.
#'
#' If a File object is passed in as 'file', then the default value for executable
#' is 'false'. Similarly, if an Executable object is passed in, then the default
#' value for executable is 'true'.
#'
#' @param obj Object (Transformation or Job)
#' @param arg A string, an \code{Executable}, or a \code{File} representing the logical file
#' @param link Is this file a job input, output or both (See LFN) (optional)
#' @param register Should this file be registered in RLS? (True/False) (optional)
#' @param transfer Should this file be transferred? (True/False or See LFN) (optional)
#' @param optional Is this file optional, or should its absence be an error? (optional)
#' @param namespace Namespace of executable (optional)
#' @param version version of executable (optional)
#' @param executable Is file an executable? (\code{TRUE}/\code{FALSE}) (optional)
#' @param size The size of the file (optional)
#' @return Job with references to the files
#' @seealso \code{\link{Job}}, \code{\link{Executable}}, \code{\link{File}}
#' @export
Uses <- function(obj, arg, link=NULL, register=NULL, transfer=NULL, optional=NULL, namespace=NULL,
                 version=NULL, executable=NULL, size=NULL) {
  if (class(obj) == 'Transformation') {
    obj$use.mixin <- UsesMixin(obj$use.mixin, arg, link, register, transfer,
                                             optional, namespace, version, executable, size)
  } else {
    obj$abstract.job$use.mixin <- UsesMixin(obj$abstract.job$use.mixin, arg, link, register, transfer,
                                             optional, namespace, version, executable, size)
  }

  return(obj)
}
