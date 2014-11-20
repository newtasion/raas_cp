class MatchEngineError(Exception):
    """A generic exception for all others to extend."""
    pass


class SearchBackendError(MatchEngineError):
    """Raised when a backend can not be found."""
    pass


class FieldError(MatchEngineError):
    """Raised when a field encounters an error."""
    pass


class MissingDependency(MatchEngineError):
    """Raised when a library a backend depends on can not be found."""
    pass


class NotHandled(MatchEngineError):
    """Raised when a model is not handled by the router setup."""
    pass


class MoreLikeThisError(MatchEngineError):
    """Raised when a model instance has not been provided for More Like This."""
    pass


class SpatialError(MatchEngineError):
    """Raised when incorrect arguments have been provided for spatial."""
    pass


class StatsError(MatchEngineError):
    "Raised when incorrect arguments have been provided for stats"
    pass
