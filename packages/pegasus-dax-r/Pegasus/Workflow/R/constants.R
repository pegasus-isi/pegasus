kSchema.Namespace <- "http://pegasus.isi.edu/schema/DAX"
kSchema.Location <- "http://pegasus.isi.edu/schema/dax-3.6.xsd"
kSchema.Version <- "3.6"

#' Namespace values recognized by Pegasus
#'
#' @seealso \code{\link{Executable}}, \code{\link{Transformation}}, \code{\link{Job}}
#' @export
Pegasus.Namespace <- list(
  PEGASUS='pegasus',
  CONDOR='condor',
  DAGMAN='dagman',
  ENV='env',
  HINTS='hints',
  GLOBUS='globus',
  SELECTOR='selector',
  STAT='stat'
)

#' Architecture types
#'
#' @seealso \code{\link{Executable}}
#' @export
Pegasus.Arch <- list(
  X86='x86',
  X86_64='x86_64',
  PPC='ppc',
  PPC_64='ppc_64',
  IA64='ia64',
  SPARCV7='sparcv7',
  SPARCV9='sparcv9',
  AMD64='amd64'
)

#' Linkage attributes
#'
#' @seealso \code{\link{File}}, \code{\link{Executable}}, \code{\link{Uses}}
#' @export
Pegasus.Link <- list(
  NONE='none',
  INPUT='input',
  OUTPUT='output',
  INOUT='inout',
  CHECKPOINT='checkpoint'
)

#' Transfer types for uses
#'
#' @seealso \code{\link{Executable}}, \code{\link{File}}
#' @export
Pegasus.Transfer <- list(
  `FALSE`='false',
  OPTIONAL='optional',
  `TRUE`='true'
)

#' OS types
#'
#' @seealso \code{\link{Executable}}
#' @export
Pegasus.OS <- list(
  LINUX='linux',
  SUNOS='sunos',
  AIX='aix',
  MACOS='macos',
  WINDOWS='windows'
)

#' Job states for notifications
#'
#' @seealso \code{\link{Job}}, \code{\link{DAX}}, \code{\link{DAG}}, \code{\link{Invoke}}
#' @export
Pegasus.When <- list(
  NEVER='never',
  START='start',
  ON_ERROR='on_error',
  ON_SUCCESS='on_success',
  AT_END='at_end',
  ALL='all'
)
