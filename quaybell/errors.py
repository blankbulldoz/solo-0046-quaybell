class RelayError(Exception):
    def __init__(self, status, code, message, detail=None):
        super().__init__(message)
        self.status, self.code, self.detail = status, code, detail


def invalid(message):
    return RelayError(400, 'invalid_input', message)


def missing(message):
    return RelayError(404, 'not_found', message)


def conflict(message):
    return RelayError(409, 'conflict', message)


def stale(message, detail=None):
    return RelayError(409, 'stale_preview', message, detail)
