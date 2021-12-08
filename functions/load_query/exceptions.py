from werkzeug.exceptions import HTTPException


class QueryNotFound(HTTPException):
    code = 404
    description = 'Query file not found'


class QueryInvalid(HTTPException):
    code = 400
    description = 'Query file invalid'


class CreationFailed(HTTPException):
    code = 500
    description = 'Error during table creation'
