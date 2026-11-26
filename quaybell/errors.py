class RelayError(Exception):
    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status, self.code = status, code
        # Structured details are merged into the JSON error body (top level and
        # under ``details``) so callers can correlate a conflict with a drain
        # round without parsing the human readable message.
        self.details = details or {}


def invalid(message, details=None):
    return RelayError(400, 'invalid_input', message, details)


def missing(message, details=None):
    return RelayError(404, 'not_found', message, details)


def conflict(message, details=None):
    return RelayError(409, 'conflict', message, details)
