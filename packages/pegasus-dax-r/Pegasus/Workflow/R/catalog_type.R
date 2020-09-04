CatalogType <- function(name) {
  if (!IsDefined(name)) {
    stop("Name required")
  }
  object <- list(name=name, profile.mixin=ProfileMixin(),
                 metadata.mixin=MetadataMixin(), pfn.mixin=PFNMixin())
  class(object) <- "CatalogType"
  return (object)
}

InnerXML.CatalogType <- function(catalog.type, parent) {
  for (p in catalog.type$profile.mixin$profiles) {
    p <- ToXML(p)
    parent <- AddChild(parent, p)
  }
  for (m in catalog.type$metadata.mixin$metadata.l) {
    m <- ToXML(m)
    parent <- AddChild(parent, m)
  }
  for (p in catalog.type$pfn.mixin$pfns) {
    p <- ToXML(p)
    parent <- AddChild(parent, p)
  }
  return(parent)
}
