#' Representation of an XML element for formatting output
#'
#' @param name element name
#' @param attrs list of element attributes
#' @return an element object
Element <- function(name, attrs=list()) {
  attributes <- vector("list", length(attrs))
  for (attr.name in names(attrs)) {
    value <- attrs[[attr.name]]
    if (IsDefined(value)) {
      if (is.logical(value)) {
        value <- tolower(value)
      } else if (is.character(value)) {
        #value <- paste("'",value,"'",sep="")
        value <- value
      }
    }
    attributes[[attr.name]] <- value
  }
  object <- list(name=name, attrs=attributes, children=list(), flat=FALSE)
  class(object) <- "Element"
  return(object)
}

#' Escape special characters in XML
#'
#' @param text Text to be escaped
#' @return Escaped special character
Escape <- function(text) {
  o = vector("list", length(text))
  i <- 1
  text.split <- strsplit(as.character(text), "")[[1]]
  for(c in text.split) {
    if(c == '"') {
      o[[i]] <- "&quot;"
    } else if(c == "'") {
      o[[i]] <- "&apos;"
    } else if(c == "<") {
      o[[i]] <- "&lt;"
    } else if(c == ">") {
      o[[i]] <- "&gt;"
    } else if(c == "&") {
      o[[i]] <- "&amp;"
    } else {
      o[[i]] <- c
    }
    i <- i + 1
  }
  return(paste(o,collapse=''))
}

#' Append a child element to a parent element
#'
#' @param element parent element
#' @param child element to be appended to the parent element
#' @return parent element with the appended child
AddChild <- function(element, child) {
  element$children <- AppendToList(element$children, child)
  return(element)
}

Text <- function(element, value) {
  if(!is.character(value)) {
    value = as.character(value)
  }
  element$children <- AppendToList(element$children, Escape(value))
  return(element)
}

Comment <- function(element, message) {
  element$children <- AppendToList(element$children, paste("<!--", Escape(message), "-->"))
  return(element)
}

Flatten <- function(element) {
  element$flat = TRUE
  return(element)
}

WriteElement <- function(element, stream=stdout(), level=0, flatten=FALSE) {
  flat <- element$flat || flatten

  sink(stream)
  cat(paste("<", element$name, sep=""))

  attrs.names <- names(element$attrs)
  for (name in attrs.names) {
    value <- element$attrs[[name]]
    if (IsDefined(value)) {
      if (class(value) == "File") {
        value <- value$catalog.type$name
      }
      value <- Escape(value)
      cat(paste(' ', name, '="', value, '"', sep=""))
    }
  }

  if(length(element$children) == 0) {
    cat('/>')
  } else {
    cat('>')
    if (!flat) {
      cat('\n')
    }
    for (child in element$children) {
      if (!flat && level > 0) {
        for (y in 1:(level+1)) {
          cat('\t')
        }
      }
      if (is.character(child)) {
        cat(child)
      } else {
        WriteElement(child, stream, (level+1), flat)
      }
      if (!flat) {
        cat('\n')
      }
    }
    if (!flat && level > 0) {
      for(y in 1:level) {
        cat('\t')
      }
    }
    cat(paste('</', element$name, '>', sep=""))
  }
  if(is.character(stream)) {
    sink()
  }
}

Unicode.Element <- function(element) {
  return(capture.output(WriteElement(element, stream=stdout())))
}
