class CustomResponse:
    def __init__(self, code, message, data):
        self.code = code
        self.message = message
        self.data = data
    
    def JSON(self):
        response = {
            "status": True,
            "code": self.code,
            "message": self.message,
            "data": self.data
        }

        return response