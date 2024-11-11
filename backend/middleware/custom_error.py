class CustomError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(self.message)
    
    def JSON(self):
        response = {
            "status": False,
            "code": self.code,
            "message": self.message
        }

        return response