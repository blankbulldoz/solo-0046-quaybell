class RelayError(Exception):
    def __init__(self, status, code, message, info=None):
        super().__init__(message)
        self.status, self.code = status, code
        # Optional structured fields merged into the JSON error body, so a
        # 409 can carry the review document an operator needs next.
        self.info = info or {}


def invalid(message, info=None):
    return RelayError(400, 'invalid_input', message, info)


def missing(message, info=None):
    return RelayError(404, 'not_found', message, info)


def conflict(message, info=None):
    return RelayError(409, 'conflict', message, info)
