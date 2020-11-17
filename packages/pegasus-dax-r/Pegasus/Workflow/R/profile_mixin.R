ProfileMixin <- function() {
  object <- list(profiles=c())
  class(object) <- "ProfileMixin"
  return(object)
}

#' Add a profile to the object
#'
#' @param profile.mixin Profile_mixin object
#' @param profile Profile object to be added
#' @return The profile_mixin object with the profile appended
#' @seealso \code{\link{Profile}}
AddProfileMixin <- function(profile.mixin, profile) {
  if (HasProfile(profile.mixin, profile)) {
    stop(paste("Duplicate profile:", profile))
  }
  profile.mixin$profiles <- AppendToList(profile.mixin$profiles, profile)
  return(profile.mixin)
}

HasProfile <- function(profile.mixin, profile) {
  for (i in profile.mixin$profiles) {
    if (Equals(i, profile)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

RemoveProfile <- function(profile.mixin, profile) {
  if (length(profile.mixin$profiles) > 0) {
    for (i in 1:length(profile.mixin$profiles)) {
      o <- profile.mixin$profiles[[i]]
      if (Equals(o, profile)) {
        profile.mixin$profiles[i] <- NULL
        return(profile.mixin)
      }
    }
  }
  stop(paste("Profile not found:",profile))
}

ClearProfiles <- function(profile.mixin) {
  profile.mixin$profiles <- c()
  return(profile.mixin)
}

AddProfileDeclarative <- function(profile.mixin, namespace, key, value) {
  return(AddProfileMixin(profile.mixin, profile(namespace, key, value)))
}
