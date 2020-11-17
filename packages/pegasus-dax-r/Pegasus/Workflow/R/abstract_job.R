AbstractJob <- function(id=NULL, node.label=NULL) {

  object <- list(profile.mixin=ProfileMixin(), use.mixin=UseMixin(), invoke.mixin=InvokeMixin(),
                 metadata.mixin=MetadataMixin(), id=id, node.label=node.label, arguments=c(),
                 stdout=NULL, stderr=NULL, stdin=NULL)
  class(object) <- "AbstractJob"
  return(object)
}

AddArgs <- function(abstract.job, arguments) {
  for (arg in arguments) {
    if (!is.character(arg) && class(arg) != "File") {
      stop(paste("Invalid argument:", arg))
    }
    if (length(abstract.job$arguments) > 0) {
      abstract.job$arguments <- AppendToList(abstract.job$arguments, ' ')
    }
    abstract.job$arguments <- AppendToList(abstract.job$arguments, arg)
  }
  return(abstract.job)
}

AddRawArguments <- function(abstract.job, arguments) {
  for (arg in arguments) {
    if (!is.character() && class(arg) != "File") {
      abstract.job$arguments <- c()
      stop(paste("Invalid argument:", arg))
    }
    abstract.job$arguments <- AppendToList(abstract.job$arguments, arguments)
  }
  return(abstract.job)
}

ClearArgs <- function(abstract.job) {
  abstract.job$arguments <- c()
  return(abstract.job)
}

GetArguments <- function(abstract.job) {
  args <- c()
  for(a in abstract.job$arguments) {
    if(class(a) == "File") {
      args <- AppendToList(args, Unicode(ToArgumentXML(a)))
    } else {
      args <- AppendToList(args, a)
    }
  }
  return(paste(args, collapse=''))
}

SetStdout <- function(abstract.job, filename) {
  if(class(filename) == "File") {
    abstract.job$stdout = filename
  } else {
    abstract.job$stdout = File(filename)
  }
  return(abstract.job)
}

ClearStdout <- function(abstract.job) {
  abstract.job$stdout <- NULL
  return(abstract.job)
}

SetStderr <- function(abstract.job, filename) {
  if(class(filename) == "File") {
    abstract.job$stderr = filename
  } else {
    abstract.job$stderr = File(filename)
  }
  return(abstract.job)
}

ClearStderr <- function(abstract.job) {
  abstract.job$stderr <- NULL
  return(abstract.job)
}

SetStdin <- function(abstract.job, filename) {
  if(class(filename) == "File") {
    abstract.job$stdin = filename
  } else {
    abstract.job$stdin = File(filename)
  }
  return(abstract.job)
}

ClearStdin <- function(abstract.job) {
  abstract.job$stdin <- NULL
  return(abstract.job)
}

InnerXML.AbstractJob <- function(abstract.job, element) {
  # Arguments
  if (length(abstract.job$arguments) > 0) {
    args <- Element('argument')
    args <- Flatten(args)
    for (x in abstract.job$arguments) {
      if (class(x) == "File") {
        args <- AddChild(args, ToArgumentXML(x))
      } else {
        args <- Text(args, x)
      }
    }
    element <- AddChild(element, args)
  }

  # Metadata
  for (m in abstract.job$metadata.mixin$metadata.l) {
    element <- AddChild(element, ToXML(m))
  }

  # Profiles
  for (pro in abstract.job$profile.mixin$profiles) {
    element <- AddChild(element, ToXML(pro))
  }

  # Stdin/xml/err
  if (IsDefined(abstract.job$stdin)) {
    element <- AddChild(element, ToStdioXML(abstract.job$stdin, 'stdin'))
  }
  if (IsDefined(abstract.job$stdout)) {
    element <- AddChild(element, ToStdioXML(abstract.job$stdout, 'stdout'))
  }
  if (IsDefined(abstract.job$stderr)) {
    element <- AddChild(element, ToStdioXML(abstract.job$stderr, 'stderr'))
  }

  # Uses
  um <- abstract.job$use.mixin
  if (IsDefined(um$used)) {
    sorted.used <- um$used[order(unlist(lapply(um$used, function(x) suppressWarnings(as.integer(x$link)))))]
    for (u in sorted.used) {
      element <- AddChild(element, ToJobXML(u))
    }
  }

  # Invocations
  for (inv in abstract.job$invoke.mixin$invocations) {
    element <- AddChild(element, ToXML(inv))
  }

  return(element)
}

# Add-in functions for R
Equals.AbstractJob <- function(abstract.job, other) {
  if (class(other) == "AbstractJob") {
    test <- IsEqual(abstract.job$node.label, other$node.label)
    if (!is.null(abstract.job$id) && !is.null(other$id)) {
      test <- test && IsEqual(abstract.job$id, other$id)
    }
    if (!test) {
      return(FALSE)
    }
    if (length(abstract.job$arguments) != length(other$arguments)) {
      return(FALSE)
    }
    if (length(abstract.job$arguments) > 0) {
      for (i in 1:length(abstract.job$arguments)) {
        if (class(abstract.job$arguments[[i]]) != class(other$arguments[[i]])) {
          return(FALSE)
        }
        if (class(abstract.job$arguments[[i]]) == "File") {
          if (!Equals(abstract.job$arguments[[i]], other$arguments[[i]])) {
            return(FALSE)
          }
        } else {
          if (abstract.job$arguments[[i]] != other$arguments[[i]]) {
            return(FALSE)
          }
        }
      }
    }
  }
  return(TRUE)
}
