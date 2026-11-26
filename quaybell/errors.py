class RelayError(Exception):
    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status, self.code, self.details = status, code, details

    def body(self):
        data = {'error': self.code, 'message': str(self)}
        if self.details:
            data.update(self.details)
        return data


def invalid(message):
    return RelayError(400, 'invalid_input', message)


def missing(message):
    return RelayError(404, 'not_found', message)


def conflict(message, details=None):
    return RelayError(409, 'conflict', message, details)
