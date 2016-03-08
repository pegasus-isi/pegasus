#' An entry for an executable in the DAX-level replica catalog
#'
#' @examples
#' grep <- Executable("grep")
#' grep <- Executable(namespace="os",name="grep",version="2.3")
#' grep <- Executable(namespace="os",name="grep",version="2.3",arch=DAX3.Arch$X86)
#' grep <- Executable(namespace="os",name="grep",version="2.3",arch=DAX3.Arch$X86,os=DAX3.OS$LINUX)
#'
#' @param name Logical name of executable
#' @param namespace Executable namespace
#' @param version Executable version
#' @param arch Architecture that this exe was compiled for
#' @param os Name of os that this exe was compiled for
#' @param osrelease Release of os that this exe was compiled for
#' @param osversion Version of os that this exe was compiled for
#' @param glibc Version of glibc this exe was compiled against
#' @param installed Is the executable installed (true), or stageable (false)
#' @return The executable object for the program
#' @seealso \code{\link{AddExecutable}}
#' @export
Executable <- function(name, namespace=NULL, version=NULL, arch=NULL, os=NULL,
                       osrelease=NULL, osversion=NULL, glibc=NULL, installed=NULL) {

  object <- list(catalog.type=CatalogType(name), invoke.mixin=InvokeMixin(), namespace=namespace,
                 version=version, arch=arch, os=os, osrelease=osrelease, osversion=osversion,
                 glibc=glibc, installed=installed)
  class(object) <- "Executable"
  return (object)
}

Equals.Executable <- function(executable, other) {
  if(class(other) == "Executable") {
    test <- IsEqual(executable$catalog.type$name, other$catalog.type$name)
    test <- test && IsEqual(executable$namespace, other$namespace)
    test <- test && IsEqual(executable$version, other$version)
    test <- test && IsEqual(executable$arch, other$arch)
    test <- test && IsEqual(executable$os, other$os)
    test <- test && IsEqual(executable$osrelease, other$osrelease)
    test <- test && IsEqual(executable$osversion, other$osversion)
    test <- test && IsEqual(executable$glibc, other$glibc)
    test <- test && IsEqual(executable$installed, other$installed)
    return(test)
  }
  return(FALSE)
}

ToXML.Executable <- function(executable) {
  e <- Element('executable', list(
    name=executable$catalog.type$name,
    namespace=executable$namespace,
    version=executable$version,
    arch=executable$arch,
    os=executable$os,
    osrelease=executable$osrelease,
    osversion=executable$osversion,
    glibc=executable$glibc,
    installed=executable$installed
  ))
  e <- InnerXML(executable$catalog.type, e)

  for(inv in executable$invoke.mixin$invocations) {
    e <- AddChild(e, ToXML(inv))
  }
  return(e)
}

# ###############################
# Add-in functions for R
# ###############################

#' @rdname AddPFN
#' @method AddPFN Executable
#' @seealso \code{\link{Executable}}
#' @export
AddPFN.Executable <- function(obj, pfn) {
  obj$catalog.type$pfn.mixin <- AddPFNMixin(obj$catalog.type$pfn.mixin, pfn)
  return(obj)
}

#' @rdname AddProfile
#' @method AddProfile Executable
#' @seealso \code{\link{Executable}}
#' @export
AddProfile.Executable <- function(obj, profile) {
  obj$catalog.type$profile.mixin <- AddProfileMixin(obj$catalog.type$profile.mixin, profile)
  return(obj)
}

#' @rdname AddInvoke
#' @method AddInvoke Executable
#' @seealso \code{\link{Executable}}
#' @export
AddInvoke.Executable <- function(obj, invoke) {
  obj$invoke.mixin <- AddInvokeMixin(obj$invoke.mixin, invoke)
  return(obj)
}


#' @rdname Metadata
#' @method Metadata Executable
#' @seealso \code{\link{Executable}}
#' @export
Metadata.Executable <- function(obj, key, value) {
  obj$catalog.type$metadata.mixin <- AddMetadataDeclarative(obj$catalog.type$metadata.mixin, key, value)
  return(obj)
}
