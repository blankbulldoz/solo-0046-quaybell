class RelayError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code = status, code


def invalid(message):
    return RelayError(400, 'invalid_input', message)


def missing(message):
    return RelayError(404, 'not_found', message)


def conflict(message):
    return RelayError(409, 'conflict', message)
